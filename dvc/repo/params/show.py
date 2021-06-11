import logging
from collections import defaultdict
from copy import copy
from typing import TYPE_CHECKING, List, Tuple

from dvc.dependency.param import ParamsDependency
from dvc.exceptions import DvcException
from dvc.path_info import PathInfo
from dvc.repo import locked
from dvc.repo.collect import collect
from dvc.scm.base import SCMError
from dvc.stage import PipelineStage
from dvc.utils.serialize import LOADERS, ParseError

if TYPE_CHECKING:
    from dvc.output import Output
    from dvc.repo import Repo
    from dvc.types import DvcPath

logger = logging.getLogger(__name__)


class NoParamsError(DvcException):
    pass


def _is_params(dep: "Output"):
    return isinstance(dep, ParamsDependency)


def _collect_configs(
    repo: "Repo", rev, targets=None
) -> Tuple[List["Output"], List["DvcPath"]]:
    params, path_infos = collect(
        repo,
        targets=targets or [],
        deps=True,
        output_filter=_is_params,
        rev=rev,
    )
    all_path_infos = path_infos + [p.path_info for p in params]
    if not targets:
        default_params = (
            PathInfo(repo.root_dir) / ParamsDependency.DEFAULT_PARAMS_FILE
        )
        if default_params not in all_path_infos:
            path_infos.append(default_params)
    return params, path_infos


def _read_path_info(fs, path_info, rev):
    if not fs.exists(path_info):
        return None

    suffix = path_info.suffix.lower()
    loader = LOADERS[suffix]
    try:
        return loader(path_info, fs=fs)
    except ParseError:
        logger.debug(
            "failed to read '%s' on '%s'", path_info, rev, exc_info=True
        )
        return None


def _read_params(repo, params, params_path_infos, rev, deps=False):
    res = defaultdict(dict)
    path_infos = copy(params_path_infos)

    if deps:
        for param in params:
            res[str(param.path_info)].update(param.read_params_d())
    else:
        path_infos += [param.path_info for param in params]

    for path_info in path_infos:
        from_path = _read_path_info(repo.fs, path_info, rev)
        if from_path:
            res[str(path_info)] = from_path

    return res


def _collect_vars(repo, params):
    vars_params = defaultdict(dict)
    for stage in repo.stages:
        if isinstance(stage, PipelineStage) and stage.tracked_vars:
            for file, vars_ in stage.tracked_vars.items():
                # `params` file are shown regardless of `tracked` or not
                # to reduce noise and duplication, they are skipped
                if file in params:
                    continue

                vars_params[file].update(vars_)
    return vars_params


@locked
def show(repo, revs=None, targets=None, deps=False):
    res = {}

    for branch in repo.brancher(revs=revs):
        params, params_path_infos = _collect_configs(repo, branch, targets)
        params = _read_params(repo, params, params_path_infos, branch, deps)
        vars_params = _collect_vars(repo, params)

        # NOTE: only those that are not added as a ParamDependency are included
        # so we don't need to recursively merge them yet.
        params.update(vars_params)

        if params:
            res[branch] = params

    if not res:
        raise NoParamsError("no parameter configs files in this repository")

    # Hide workspace params if they are the same as in the active branch
    try:
        active_branch = repo.scm.active_branch()
    except (TypeError, SCMError):
        # TypeError - detached head
        # SCMError - no repo case
        pass
    else:
        if res.get("workspace") == res.get(active_branch):
            res.pop("workspace", None)

    return res
