import os
import pathlib
from itertools import product

from funcy import lsplit, rpartial

from dvc import dependency, output
from dvc.utils.fs import path_isin

from ..dependency import ParamsDependency
from ..hash_info import HashInfo
from ..tree.local import LocalTree
from ..tree.s3 import S3Tree
from ..utils import dict_md5, format_link, relpath
from .exceptions import (
    MissingDataSource,
    StageExternalOutputsError,
    StagePathNotDirectoryError,
    StagePathNotFoundError,
    StagePathOutsideError,
)


def check_stage_path(repo, path, is_wdir=False):
    assert repo is not None

    error_msg = "{wdir_or_path} '{path}' {{}}".format(
        wdir_or_path="stage working dir" if is_wdir else "file path",
        path=path,
    )

    real_path = os.path.realpath(path)
    if not os.path.exists(real_path):
        raise StagePathNotFoundError(error_msg.format("does not exist"))

    if not os.path.isdir(real_path):
        raise StagePathNotDirectoryError(error_msg.format("is not directory"))

    proj_dir = os.path.realpath(repo.root_dir)
    if real_path != proj_dir and not path_isin(real_path, proj_dir):
        raise StagePathOutsideError(error_msg.format("is outside of DVC repo"))


def fill_stage_outputs(stage, **kwargs):
    assert not stage.outs

    keys = [
        "outs_persist",
        "outs_persist_no_cache",
        "metrics_no_cache",
        "metrics",
        "plots_no_cache",
        "plots",
        "outs_no_cache",
        "outs",
        "checkpoints",
        "outs_no_store",
    ]

    stage.outs = []
    for key in keys:
        stage.outs += output.loads_from(
            stage,
            kwargs.get(key, []),
            use_cache="no_cache" not in key,
            store="no_store" not in key and "no_cache" not in key,
            persist="persist" in key,
            metric="metrics" in key,
            plot="plots" in key,
            checkpoint="checkpoints" in key,
        )


def fill_stage_dependencies(stage, deps=None, erepo=None, params=None):
    assert not stage.deps
    stage.deps = []
    stage.deps += dependency.loads_from(stage, deps or [], erepo=erepo)
    stage.deps += dependency.loads_params(stage, params or [])


def check_no_externals(stage):
    from urllib.parse import urlparse

    # NOTE: preventing users from accidentally using external outputs. See
    # https://github.com/iterative/dvc/issues/1545 for more details.

    def _is_external(out):
        # NOTE: in case of `remote://` notation, the user clearly knows that
        # this is an advanced feature and so we shouldn't error-out.
        if out.is_in_repo or urlparse(out.def_path).scheme == "remote":
            return False
        return True

    outs = [str(out) for out in stage.outs if _is_external(out)]
    if not outs:
        return

    str_outs = ", ".join(outs)
    link = format_link("https://dvc.org/doc/user-guide/managing-external-data")
    raise StageExternalOutputsError(
        f"Output(s) outside of DVC project: {str_outs}. "
        f"See {link} for more info."
    )


def check_circular_dependency(stage):
    from dvc.exceptions import CircularDependencyError

    circular_dependencies = {d.path_info for d in stage.deps} & {
        o.path_info for o in stage.outs
    }

    if circular_dependencies:
        raise CircularDependencyError(str(circular_dependencies.pop()))


def check_duplicated_arguments(stage):
    from collections import Counter

    from dvc.exceptions import ArgumentDuplicationError

    path_counts = Counter(edge.path_info for edge in stage.deps + stage.outs)

    for path, occurrence in path_counts.items():
        if occurrence > 1:
            raise ArgumentDuplicationError(str(path))


def check_missing_outputs(stage):
    paths = [str(out) for out in stage.outs if not out.exists]
    if paths:
        raise MissingDataSource(paths)


def stage_dump_eq(stage_cls, old_d, new_d):
    # NOTE: need to remove checksums from old dict in order to compare
    # it to the new one, since the new one doesn't have checksums yet.
    old_d.pop(stage_cls.PARAM_MD5, None)
    new_d.pop(stage_cls.PARAM_MD5, None)
    outs = old_d.get(stage_cls.PARAM_OUTS, [])
    for out in outs:
        out.pop(LocalTree.PARAM_CHECKSUM, None)
        out.pop(S3Tree.PARAM_CHECKSUM, None)
        out.pop(HashInfo.PARAM_SIZE, None)
        out.pop(HashInfo.PARAM_NFILES, None)

    # outs and deps are lists of dicts. To check equality, we need to make
    # them independent of the order, so, we convert them to dicts.
    combination = product(
        [old_d, new_d], [stage_cls.PARAM_DEPS, stage_cls.PARAM_OUTS]
    )
    for coll, key in combination:
        if coll.get(key):
            coll[key] = {item["path"]: item for item in coll[key]}
    return old_d == new_d


def compute_md5(stage):
    from dvc.output.base import BaseOutput

    d = stage.dumpd()

    # Remove md5 and meta, these should not affect stage md5
    d.pop(stage.PARAM_MD5, None)
    d.pop(stage.PARAM_META, None)
    d.pop(stage.PARAM_DESC, None)

    # Ignore the wdir default value. In this case DVC-file w/o
    # wdir has the same md5 as a file with the default value specified.
    # It's important for backward compatibility with pipelines that
    # didn't have WDIR in their DVC-files.
    if d.get(stage.PARAM_WDIR) == ".":
        del d[stage.PARAM_WDIR]

    return dict_md5(
        d,
        exclude=[
            stage.PARAM_LOCKED,  # backward compatibility
            stage.PARAM_FROZEN,
            BaseOutput.PARAM_DESC,
            BaseOutput.PARAM_METRIC,
            BaseOutput.PARAM_PERSIST,
            BaseOutput.PARAM_CHECKPOINT,
            HashInfo.PARAM_SIZE,
            HashInfo.PARAM_NFILES,
        ],
    )


def resolve_wdir(wdir, path):
    rel_wdir = relpath(wdir, os.path.dirname(path))
    return pathlib.PurePath(rel_wdir).as_posix() if rel_wdir != "." else None


def resolve_paths(path, wdir=None):
    path = os.path.abspath(path)
    wdir = wdir or os.curdir
    wdir = os.path.abspath(os.path.join(os.path.dirname(path), wdir))
    return path, wdir


def get_dump(stage):
    return {
        key: value
        for key, value in {
            stage.PARAM_DESC: stage.desc,
            stage.PARAM_MD5: stage.md5,
            stage.PARAM_CMD: stage.cmd,
            stage.PARAM_WDIR: resolve_wdir(stage.wdir, stage.path),
            stage.PARAM_FROZEN: stage.frozen,
            stage.PARAM_DEPS: [d.dumpd() for d in stage.deps],
            stage.PARAM_OUTS: [o.dumpd() for o in stage.outs],
            stage.PARAM_ALWAYS_CHANGED: stage.always_changed,
            stage.PARAM_META: stage.meta,
        }.items()
        if value
    }


def split_params_deps(stage):
    return lsplit(rpartial(isinstance, ParamsDependency), stage.deps)
