import functools
import logging
import os
import subprocess
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, List

from dvc.repo.live import create_summary
from dvc.stage.decorators import relock_repo
from dvc.stage.exceptions import StageCmdFailedError

if TYPE_CHECKING:
    from dvc.output import Output
    from dvc.stage import Stage


logger = logging.getLogger(__name__)


class CheckpointKilledError(StageCmdFailedError):
    pass


class LiveKilledError(StageCmdFailedError):
    pass


@dataclass
class MonitorTask:
    stage: "Stage"
    execute: Callable
    proc: subprocess.Popen
    done: threading.Event = threading.Event()
    killed: threading.Event = threading.Event()

    @property
    def name(self) -> str:
        raise NotImplementedError

    @property
    def SIGNAL_FILE(self) -> str:
        raise NotImplementedError

    @property
    def error_cls(self) -> type:
        raise NotImplementedError

    @property
    def signal_path(self) -> str:
        return os.path.join(self.stage.repo.tmp_dir, self.SIGNAL_FILE)

    def after_run(self):
        pass


class CheckpointTask(MonitorTask):
    name = "checkpoint"
    SIGNAL_FILE = "DVC_CHECKPOINT"
    error_cls = CheckpointKilledError

    def __init__(
        self, stage: "Stage", callback_func: Callable, proc: subprocess.Popen
    ):
        super().__init__(
            stage,
            functools.partial(
                CheckpointTask._run_callback, stage, callback_func
            ),
            proc,
        )

    @staticmethod
    @relock_repo
    def _run_callback(stage, callback_func):
        stage.save(allow_missing=True)
        stage.commit(allow_missing=True)
        stage.unprotect_outs()
        logger.debug("Running checkpoint callback for stage '%s'", stage)
        callback_func()


class LiveTask(MonitorTask):
    name = "live"
    SIGNAL_FILE = "DVC_LIVE"
    error_cls = LiveKilledError

    def __init__(self, stage: "Stage", out: "Output", proc: subprocess.Popen):
        super().__init__(stage, functools.partial(create_summary, out), proc)

    def after_run(self):
        # make sure summary is prepared for all the data
        self.execute()


class Monitor:
    AWAIT: float = 1.0

    def __init__(self, tasks: List[MonitorTask]):
        self.done = threading.Event()
        self.tasks = tasks
        self.monitor_thread = threading.Thread(
            target=Monitor._loop, args=(self.tasks, self.done)
        )

    def __enter__(self):
        self.monitor_thread.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.done.set()
        self.monitor_thread.join()
        for t in self.tasks:
            t.after_run()

    @staticmethod
    def kill(proc):
        if os.name == "nt":
            return Monitor._kill_nt(proc)
        proc.terminate()
        proc.wait()

    @staticmethod
    def _kill_nt(proc):
        # windows stages are spawned with shell=True, proc is the shell process
        # and not the actual stage process - we have to kill the entire tree
        subprocess.call(["taskkill", "/F", "/T", "/PID", str(proc.pid)])

    @staticmethod
    def _loop(tasks: List[MonitorTask], done: threading.Event):
        while True:
            for task in tasks:
                if os.path.exists(task.signal_path):
                    try:
                        task.execute()
                    except Exception:  # pylint: disable=broad-except
                        logger.exception(
                            "Error running '%s' task, '%s' will be aborted",
                            task.name,
                            task.stage,
                        )
                        Monitor.kill(task.proc)
                        task.killed.set()
                    finally:
                        logger.debug(
                            "Removing signal file for '%s' task", task.name
                        )
                        os.remove(task.signal_path)
            if done.wait(Monitor.AWAIT):
                return
