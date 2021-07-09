import logging
import os
from contextlib import contextmanager
from itertools import tee
from typing import TYPE_CHECKING, Any, Iterator, List

import colorama

from dvc.ui import ui

from ..exceptions import (
    CacheLinkError,
    InvalidArgumentError,
    OutputDuplicationError,
    OverlappingOutputPathsError,
    RecursiveAddingWhileUsingFilename,
)
from ..repo.scm_context import scm_context
from ..utils import LARGE_DIR_SIZE, glob_targets, resolve_output, resolve_paths
from ..utils.collections import ensure_list, validate
from . import locked

if TYPE_CHECKING:
    from dvc.repo import Repo
    from dvc.stage import Stage
    from dvc.types import TargetType

Stages = List["Stage"]
logger = logging.getLogger(__name__)


OVERLAPPING_OUTPUT_FMT = (
    "Cannot add '{out}', because it is overlapping with other "
    "DVC tracked output: '{parent}'.\n"
    "To include '{out}' in '{parent}', run "
    "'dvc commit {parent_stage}'"
)


def check_recursive_and_fname(args):
    if args.recursive and args.fname:
        raise RecursiveAddingWhileUsingFilename()


def transform_targets(args):
    args.targets = ensure_list(args.targets)


def check_arg_combinations(args):
    kwargs = args.kwargs
    invalid_opt = None
    to_remote = args.to_remote
    to_cache = kwargs.get("out") and not to_remote

    if to_remote or to_cache:
        message = "{option} can't be used with "
        message += "--to-remote" if to_remote else "-o"
        if len(args.targets) != 1:
            invalid_opt = "multiple targets"
        elif args.no_commit:
            invalid_opt = "--no-commit option"
        elif args.recursive:
            invalid_opt = "--recursive option"
        elif kwargs.get("external"):
            invalid_opt = "--external option"
    else:
        message = "{option} can't be used without --to-remote"
        if kwargs.get("remote"):
            invalid_opt = "--remote"
        elif kwargs.get("jobs"):
            invalid_opt = "--jobs"

    if invalid_opt is not None:
        raise InvalidArgumentError(message.format(option=invalid_opt))


@contextmanager
def translate_graph_error(stages: Stages) -> Iterator[None]:
    try:
        yield
    except OverlappingOutputPathsError as exc:
        msg = OVERLAPPING_OUTPUT_FMT.format(
            out=exc.overlapping_out.path_info,
            parent=exc.parent.path_info,
            parent_stage=exc.parent.stage.addressing,
        )
        raise OverlappingOutputPathsError(exc.parent, exc.overlapping_out, msg)
    except OutputDuplicationError as exc:
        raise OutputDuplicationError(
            exc.output, list(set(exc.stages) - set(stages))
        )


def progress_iter(stages: Stages) -> Iterator["Stage"]:
    total = len(stages)
    desc = "Adding..."
    with ui.progress(
        stages, total=total, desc=desc, unit="file", leave=True
    ) as pbar:
        if total == 1:
            pbar.bar_format = desc
            pbar.refresh()

        for stage in pbar:
            if total > 1:
                pbar.set_msg(f"{stage.outs[0]}")
            yield stage
            if total == 1:  # restore bar format for stats
                # pylint: disable=no-member
                pbar.bar_format = pbar.BAR_FMT_DEFAULT


LINK_FAILURE_MESSAGE = (
    "\nSome targets could not be linked from cache to workspace.\n{}\n"
    "To re-link these targets, reconfigure cache types and then run:\n"
    "\n\tdvc checkout {}"
)


@contextmanager
def warn_link_failures() -> Iterator[List[str]]:
    link_failures: List[str] = []
    try:
        yield link_failures
    finally:
        if link_failures:
            msg = LINK_FAILURE_MESSAGE.format(
                CacheLinkError.SUPPORT_LINK,
                " ".join(link_failures),
            )
            ui.error_write(msg)


VALIDATORS = (
    check_recursive_and_fname,
    transform_targets,
    check_arg_combinations,
)


@validate(*VALIDATORS)
@locked
@scm_context
def add(  # noqa: C901
    repo: "Repo",
    targets: "TargetType",
    recursive: bool = False,
    no_commit: bool = False,
    fname: str = None,
    to_remote: bool = False,
    **kwargs: Any,
):
    to_cache = bool(kwargs.get("out")) and not to_remote
    transfer = to_remote or to_cache

    glob = kwargs.get("glob", False)
    add_targets = collect_targets(repo, targets, recursive, glob)
    # pass one for creating stages, other one is used for iterating here
    add_targets, sources = tee(add_targets)

    # collect targets and build stages as we go
    desc = "Collecting targets"
    stages_it = create_stages(repo, add_targets, fname, transfer, **kwargs)
    stages = list(ui.progress(stages_it, desc=desc, unit="file"))
    msg = "Collecting stages from the workspace"
    with translate_graph_error(stages), ui.status(msg) as status:
        # remove existing stages that are to-be replaced with these
        # new stages for the graph checks.
        new_index = repo.index.update(stages)
        status.update("Checking graph")
        new_index.check_graph()

    odb = None
    if to_remote:
        remote = repo.cloud.get_remote(kwargs.get("remote"), "add")
        odb = remote.odb

    with warn_link_failures() as link_failures:
        for stage, source in zip(progress_iter(stages), sources):
            if to_remote or to_cache:
                stage.transfer(source, to_remote=to_remote, odb=odb, **kwargs)
            else:
                try:
                    stage.save()
                    if not no_commit:
                        stage.commit()
                except CacheLinkError:
                    link_failures.append(str(stage.relpath))
            stage.dump()
    return stages


LARGE_DIR_RECURSIVE_ADD_WARNING = (
    "You are adding a large directory '{target}' recursively.\n"
    "Consider tracking it as a whole instead with "
    "`{cyan}dvc add {target}{nc}`."
)


def collect_targets(
    repo: "Repo",
    targets: "TargetType",
    recursive: bool = False,
    glob: bool = False,
) -> Iterator[str]:
    for target in glob_targets(ensure_list(targets), glob=glob):
        expanded_targets = _find_all_targets(repo, target, recursive=recursive)
        for index, path in enumerate(expanded_targets):
            if index == LARGE_DIR_SIZE:
                msg = LARGE_DIR_RECURSIVE_ADD_WARNING.format(
                    cyan=colorama.Fore.CYAN,
                    nc=colorama.Style.RESET_ALL,
                    target=target,
                )
                ui.error_write(msg)
            yield path


def _find_all_targets(
    repo: "Repo", target: str, recursive: bool = False
) -> Iterator[str]:
    from dvc.dvcfile import is_dvc_file

    if os.path.isdir(target) and recursive:
        files = map(os.fspath, repo.dvcignore.walk_files(repo.fs, target))
        yield from (
            path
            for path in files
            if not repo.is_dvc_internal(path)
            if not is_dvc_file(path)
            if not repo.scm.belongs_to_scm(path)
            if not repo.scm.is_tracked(path)
        )
    else:
        yield target


def create_stages(
    repo: "Repo",
    targets: Iterator[str],
    fname: str = None,
    transfer: bool = False,
    external: bool = False,
    **kwargs: Any,
) -> Iterator["Stage"]:
    for target in targets:
        if kwargs.get("out"):
            target = resolve_output(target, kwargs["out"])
        path, wdir, out = resolve_paths(
            repo, target, always_local=transfer and not kwargs.get("out")
        )

        stage = repo.stage.create(
            single_stage=True,
            validate=False,
            fname=fname or path,
            wdir=wdir,
            outs=[out],
            external=external,
        )
        if kwargs.get("desc"):
            stage.outs[0].desc = kwargs["desc"]
        yield stage
