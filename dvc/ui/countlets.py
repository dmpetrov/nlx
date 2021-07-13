import time
from argparse import Namespace
from datetime import timedelta
from time import monotonic
from typing import Any, Dict, Optional, Tuple, Union

from rich.progress import (
    DownloadColumn,
    ProgressColumn,
    ProgressSample,
    Task,
    TaskID,
    TextColumn,
    TransferSpeedColumn,
)
from rich.text import Text
from tqdm import tqdm

from dvc.ui import ui
from dvc.utils import colorize


class Countlet:
    def __init__(self, col):
        self.col = col
        self.task = None

    def set_task(self, task):
        self.task = task

    @property
    def _value(self):
        return self.col.render(self.task)


class ETAColumn(ProgressColumn):
    max_refresh = 0.5

    def render(self, task: "Task") -> Text:
        """Show time remaining."""
        remaining = task.time_remaining
        elapsed = task.finished_time if task.finished else task.elapsed
        if elapsed is None:
            elapsed_text = Text("-:--:--", style="progress.elapsed")
        else:
            delta = timedelta(seconds=int(elapsed))
            elapsed_text = Text(str(delta), style="progress.elapsed")

        if remaining is None:
            remaining_text = Text("-:--:--", style="progress.remaining")
        else:
            remaining_delta = timedelta(seconds=int(remaining))
            remaining_text = Text(
                str(remaining_delta), style="progress.remaining"
            )
        elapsed_text.append("/")
        return elapsed_text.append_text(remaining_text)


def sample_task(task: Task, get_mtime, completed_start):
    speed_estimate_period = 30
    update_completed = task.completed - completed_start

    current_time = get_mtime()
    old_sample_time = current_time - speed_estimate_period
    _progress = task._progress  # pylint: disable=protected-access

    popleft = _progress.popleft
    while _progress and _progress[0].timestamp < old_sample_time:
        popleft()
    while len(_progress) > 1000:
        popleft()
    _progress.append(ProgressSample(current_time, update_completed))
    if task.completed >= task.total and task.finished_time is None:
        task.finished_time = task.elapsed
        task.finished_speed = task.speed


class Counter:
    def __init__(self, desc: str, persistent: bool = True):
        self.desc: str = desc
        self._opts: Dict[str, Any] = {"leave": persistent}
        self._task_index: int = 0
        self.get_time = monotonic
        self.tasks_kw: Dict[str, Dict] = {}
        self.cols_kw: Dict[str, Union[str, Dict]] = {}
        self._end_replacer: Optional[Tuple[str, str]] = None
        self.pbar: Optional[tqdm] = None
        self._tasks: Dict[str, Task] = {}
        self._cols: Dict[str, Countlet] = {}
        self._main_task: Optional[str] = None
        self.default_sep: str = " "
        self.data: Dict[str, str] = {}

    def style_description(self, **kwargs: Any):
        self.desc = colorize(self.desc, **kwargs)
        return self

    def separate_components_with(self, sep: str = " "):
        self.default_sep = sep
        return self

    def delay(self, seconds: float = 0):
        self._opts["delay"] = seconds
        return self

    def persistent(self, value: bool = False):
        self._opts["leave"] = value
        return self

    def show_bar(self):
        return self.push_props("bar", " |{bar}|")

    def push_props(self, col_id: str, prop: Union[str, Dict]):
        self.cols_kw[col_id] = prop
        return self

    def add_task(
        self,
        task_id: str,
        n: int = 0,
        total: int = 0,
        main: bool = False,
        **fields,
    ):
        self.tasks_kw[task_id] = {
            "total": total,
            "completed": n,
            "fields": fields,
        }
        if main:
            self._main_task = task_id
        return self

    def add_column(
        self,
        col_id: str,
        column: Union["ProgressColumn", "str"],
        task: str = None,
        **extra,
    ):
        column = TextColumn(column) if isinstance(column, str) else column
        return self.push_props(
            col_id, {"column": column, "task": task, **extra}
        )

    def update(
        self, task_id: str = None, n: int = 0, total: int = None, **fields
    ):
        if n < 0:
            return

        task_id = task_id or self._main_task
        assert task_id
        task = self._tasks[task_id]
        old_value = task.completed
        task.completed += n
        if total is not None:
            task.total += total

        task.fields.update(fields)
        self.sample_task(task, old_value)
        if task_id == self._main_task:
            self._sync_pbar(task)
        self._lazy_pbar_refresh()

    def update_to(
        self, task_id: str = None, n: int = 0, total: int = None, **fields
    ):
        task_id = task_id or self._main_task
        assert task_id
        task = self._tasks[task_id]
        old_value = task.completed
        task.completed = n
        if total is not None:
            task.total = total
        task.fields.update(fields)
        self.sample_task(task, old_value=old_value)
        if task_id == self._main_task:
            self._sync_pbar(task)
        self._lazy_pbar_refresh()

    def sample_task(self, task: Task, old_value):
        sample_task(task, self.get_time, old_value)

    def _sync_pbar(self, task):
        """Sync progress bar to the main task."""
        self.pbar.n = task.completed
        self.pbar.total = task.total

    def _lazy_pbar_refresh(self):
        """Asks tqdm to update pbar lazily.

        We don't expect it that it always does.
        """
        self.pbar.update(n=0)

    def on_finished(self, replace: str, by: str):
        self._end_replacer = replace, by
        return self

    def _compile_format(self):
        fmt = "{desc} "
        prev = None
        for idx, (col_id, col_data) in enumerate(self.cols_kw.items()):
            if not idx or prev == "bar" or col_id == "bar":
                sep = " "
            else:
                sep = self.default_sep
            if isinstance(col_data, str):
                s = col_data
            else:
                assert isinstance(col_data, dict)
                task_id = col_data["task"]
                col = col_data["column"]
                countlet = Countlet(col)
                countlet.set_task(self._tasks[task_id or self._main_task])
                self.data[col_id] = countlet
                if sep:
                    sep = col_data.get("separate_with", sep)
                s = "{postfix[" + col_id + "]." + "_value}"
                s = f"{sep}{s}"
            prev = col_id
            fmt += f"{s}"
        return fmt

    def _start_tasks(self):
        task_id = 0
        lock = tqdm.get_lock()
        for key, task_kw in self.tasks_kw.items():
            if not self._main_task:
                self._main_task = key
            self._tasks[key] = task = Task(
                TaskID(task_id),
                "",
                _get_time=self.get_time,
                _lock=lock,
                **task_kw,
            )
            task.start_time = self.get_time()
            task_id += 1

    def __enter__(self):
        self._start_tasks()
        assert self._main_task
        fmt = self._compile_format()
        return self.render(fmt)

    def __exit__(self, *exc_args):
        self.close()

    def _stop_tasks(self):
        for _, task in self._tasks.items():
            current_time = self.get_time()
            if task.start_time is None:
                task.start_time = current_time
            task.stop_time = current_time

    def _do_end_task(self):
        if self._end_replacer and self.pbar is not None:
            col_id, replacement = self._end_replacer
            self.data[col_id] = Namespace(_value=replacement)
            self.pbar.refresh()

    def sample_tasks(self):
        for _, task in self._tasks.items():
            sample_task(task, self.get_time, task.completed)

    def render(self, fmt):
        self.pbar = pbar = tqdm(desc=self.desc, **self._opts, bar_format="")
        pbar.postfix = self.data
        pbar.bar_format = fmt
        return self

    def track(self, iterable):
        for item in iterable:
            yield item
            self.update(n=1)

    def close(self):
        if self.pbar is not None:
            self._stop_tasks()
            self._do_end_task()
            self.pbar.close()


if __name__ == "__main__":
    ui.enable()

    def fake_iter(total_time: float = 10, maxiters: int = 10000):
        sleep_time = total_time / maxiters
        for i in range(maxiters):
            yield i
            time.sleep(sleep_time)

    c = (
        Counter("Computing hashes")
        .persistent(True)
        .style_description(color="green", style="bold")
        .add_task("files", total=1000)
        .add_task("message", message="")
        .add_column("files_counter", "{task.completed} files")
        .add_column(
            "message_field",
            "{task.fields[message]}",
            separate_with="",
            task="message",
        )
        .separate_components_with(",\t".expandtabs(4))
        .delay(0.5)
        .on_finished("message_field", by=", done.")
    )
    with c:
        for _ in c.track(fake_iter(5, 1000)):
            pass

    size = 10_000_000_000
    c = (
        Counter("Saving files")
        .persistent(True)
        .style_description(color="green", style="bold")
        .add_task("files", total=1000)
        .add_task("sizes", total=size)
        .add_column("files_counter", "{task.completed} files", task="files")
        .add_column("size_counter", DownloadColumn(), task="sizes")
        # .show_bar()
        .add_column("size_speed", TransferSpeedColumn(), task="sizes")
        .add_column("size_eta", ETAColumn(), task="sizes")
        .separate_components_with(",\t".expandtabs(4))
        .delay(0.5)
        .on_finished("size_eta", by="done.")
    )
    with c:
        for _ in c.track(fake_iter(10, 1000)):
            for _ in range(10):
                c.update("sizes", n=size / 10_000)
