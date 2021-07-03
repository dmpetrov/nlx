import logging
import os
import string
from collections import defaultdict
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, Optional, Set

from funcy import cached_property, project

from dvc import prompt
from dvc.exceptions import (
    CacheLinkError,
    CheckoutError,
    DvcException,
    MergeError,
)
from dvc.utils import relpath

from . import params
from .decorators import rwlocked
from .exceptions import StageUpdateError
from .imports import sync_import, update_import
from .run import run_stage
from .utils import (
    check_circular_dependency,
    check_duplicated_arguments,
    check_missing_outputs,
    check_no_externals,
    check_stage_path,
    compute_md5,
    fill_stage_dependencies,
    fill_stage_outputs,
    get_dump,
)

if TYPE_CHECKING:
    from dvc.dvcfile import DVCFile
    from dvc.objects.db.base import ObjectDB
    from dvc.objects.file import HashFile

logger = logging.getLogger(__name__)
# Disallow all punctuation characters except hyphen and underscore
INVALID_STAGENAME_CHARS = set(string.punctuation) - {"_", "-"}
Env = Dict[str, str]


def loads_from(cls, repo, path, wdir, data):
    kw = {
        "repo": repo,
        "path": path,
        "wdir": wdir,
        **project(
            data,
            [
                Stage.PARAM_CMD,
                Stage.PARAM_LOCKED,
                Stage.PARAM_FROZEN,
                Stage.PARAM_ALWAYS_CHANGED,
                Stage.PARAM_MD5,
                Stage.PARAM_DESC,
                Stage.PARAM_META,
                "name",
            ],
        ),
    }
    return cls(**kw)


@dataclass
class RawData:
    parametrized: bool = False
    generated_from: Optional[str] = None


def create_stage(cls, repo, path, external=False, **kwargs):
    from dvc.dvcfile import check_dvcfile_path

    wdir = os.path.abspath(kwargs.get("wdir", None) or os.curdir)
    path = os.path.abspath(path)
    check_dvcfile_path(repo, path)
    check_stage_path(repo, wdir, is_wdir=kwargs.get("wdir"))
    check_stage_path(repo, os.path.dirname(path))

    stage = loads_from(cls, repo, path, wdir, kwargs)
    fill_stage_outputs(stage, **kwargs)
    if not external:
        check_no_externals(stage)
    fill_stage_dependencies(
        stage, **project(kwargs, ["deps", "erepo", "params"])
    )
    check_circular_dependency(stage)
    check_duplicated_arguments(stage)

    return stage


def restore_meta(stage):
    from .exceptions import StageNotFound

    if not stage.dvcfile.exists():
        return

    try:
        old = stage.reload()
    except StageNotFound:
        return

    # will be used to restore comments later
    # noqa, pylint: disable=protected-access
    stage._stage_text = old._stage_text

    stage.meta = old.meta
    stage.desc = old.desc

    old_desc = {out.def_path: out.desc for out in old.outs}

    for out in stage.outs:
        out.desc = old_desc.get(out.def_path, None)


class Stage(params.StageParams):
    # pylint:disable=no-value-for-parameter
    # rwlocked() confuses pylint

    def __init__(
        self,
        repo,
        path=None,
        cmd=None,
        wdir=os.curdir,
        deps=None,
        outs=None,
        md5=None,
        locked=False,  # backward compatibility
        frozen=False,
        always_changed=False,
        stage_text=None,
        dvcfile=None,
        desc: Optional[str] = None,
        meta=None,
    ):
        if deps is None:
            deps = []
        if outs is None:
            outs = []

        self.repo = repo
        self._path = path
        self.cmd = cmd
        self.wdir = wdir
        self.outs = outs
        self.deps = deps
        self.md5 = md5
        self.frozen = locked or frozen
        self.always_changed = always_changed
        self._stage_text = stage_text
        self._dvcfile = dvcfile
        self.desc: Optional[str] = desc
        self.meta = meta
        self.raw_data = RawData()

    @property
    def path(self) -> str:
        return self._path

    @path.setter
    def path(self, path: str):
        self._path = path
        self.__dict__.pop("path_in_repo", None)
        self.__dict__.pop("relpath", None)

    @property
    def dvcfile(self) -> "DVCFile":
        if self.path and self._dvcfile and self.path == self._dvcfile.path:
            return self._dvcfile

        if not self.path:
            raise DvcException(
                "Stage does not have any path set "
                "and is detached from dvcfile."
            )

        from dvc.dvcfile import make_dvcfile

        self._dvcfile = make_dvcfile(self.repo, self.path)
        return self._dvcfile

    @dvcfile.setter
    def dvcfile(self, dvcfile: "DVCFile") -> None:
        self._dvcfile = dvcfile

    def __repr__(self):
        return f"Stage: '{self.addressing}'"

    def __str__(self):
        return f"stage: '{self.addressing}'"

    @property
    def addressing(self) -> str:
        """
        Useful for alternative presentations where we don't need
        `Stage:` prefix.
        """
        return self.relpath if self.path else "No path"

    def __hash__(self):
        return hash(self.path_in_repo)

    def __eq__(self, other):
        return (
            self.__class__ == other.__class__
            and self.repo is other.repo
            and self.path_in_repo == other.path_in_repo
        )

    @cached_property
    def path_in_repo(self):
        return relpath(self.path, self.repo.root_dir)

    @cached_property
    def relpath(self):
        return relpath(self.path)

    @property
    def is_data_source(self):
        """Whether the DVC file was created with `dvc add` or `dvc import`"""
        return self.cmd is None

    @property
    def is_callback(self):
        """
        A callback stage is always considered as changed,
        so it runs on every `dvc repro` call.
        """
        return self.cmd and not any((self.deps, self.outs))

    @property
    def is_import(self):
        """Whether the DVC file was created with `dvc import`."""
        return not self.cmd and len(self.deps) == 1 and len(self.outs) == 1

    @property
    def is_repo_import(self):
        if not self.is_import:
            return False

        from dvc.dependency import RepoDependency

        return isinstance(self.deps[0], RepoDependency)

    @property
    def is_checkpoint(self):
        """
        A stage containing checkpoint outs is always considered as changed
        since the checkpoint out is a circular dependency.
        """
        return any(out.checkpoint for out in self.outs)

    def short_description(self) -> Optional["str"]:
        desc: Optional["str"] = None
        if self.desc:
            with suppress(ValueError):
                # try to use first non-empty line as a description
                line = next(filter(None, self.desc.splitlines()))
                desc = line.strip()
        return desc

    def _read_env(self, out, checkpoint_func=None) -> Env:
        env: Env = {}
        if out.live:
            from dvc.env import DVCLIVE_HTML, DVCLIVE_PATH, DVCLIVE_SUMMARY
            from dvc.output import Output
            from dvc.schema import LIVE_PROPS

            env[DVCLIVE_PATH] = str(out.path_info)
            if isinstance(out.live, dict):
                config = project(out.live, LIVE_PROPS)

                env[DVCLIVE_SUMMARY] = str(
                    int(config.get(Output.PARAM_LIVE_SUMMARY, True))
                )
                env[DVCLIVE_HTML] = str(
                    int(config.get(Output.PARAM_LIVE_HTML, True))
                )
        elif out.checkpoint and checkpoint_func:
            from dvc.env import DVC_CHECKPOINT

            env.update({DVC_CHECKPOINT: "1"})
        return env

    def env(self, checkpoint_func=None) -> Env:
        from dvc.env import DVC_ROOT

        env: Env = {}
        if self.repo:
            env.update({DVC_ROOT: self.repo.root_dir})

        for out in self.outs:
            current = self._read_env(out, checkpoint_func=checkpoint_func)
            if any(
                env.get(key) != current.get(key)
                for key in set(env.keys()).intersection(current.keys())
            ):
                raise DvcException("Conflicting values for env variable")
            env.update(current)
        return env

    def changed_deps(self):
        if self.frozen:
            return False

        if self.is_callback or self.always_changed or self.is_checkpoint:
            return True

        return self._changed_deps()

    @rwlocked(read=["deps"])
    def _changed_deps(self):
        for dep in self.deps:
            status = dep.status()
            if status:
                logger.debug(
                    "Dependency '{dep}' of {stage} changed because it is "
                    "'{status}'.".format(
                        dep=dep, stage=self, status=status[str(dep)]
                    )
                )
                return True
        return False

    @rwlocked(read=["outs"])
    def changed_outs(self):
        for out in self.outs:
            status = out.status()
            if status:
                logger.debug(
                    "Output '{out}' of {stage} changed because it is "
                    "'{status}'".format(
                        out=out, stage=self, status=status[str(out)]
                    )
                )
                return True

        return False

    def changed_stage(self):
        changed = self.md5 != self.compute_md5()
        if changed:
            logger.debug(self._changed_stage_entry())
        return changed

    @rwlocked(read=["deps", "outs"])
    def changed(self):
        is_changed = (
            # Short-circuit order: stage md5 is fast,
            # deps are expected to change
            self.changed_stage()
            or self.changed_deps()
            or self.changed_outs()
        )
        if is_changed:
            logger.debug("%s changed.", self)
        return is_changed

    @rwlocked(write=["outs"])
    def remove_outs(self, ignore_remove=False, force=False):
        """Used mainly for `dvc remove --outs` and :func:`Stage.reproduce`."""
        for out in self.outs:
            if (out.persist or out.checkpoint or out.live) and not force:
                out.unprotect()
                continue

            logger.debug(f"Removing output '{out}' of {self}.")
            out.remove(ignore_remove=ignore_remove)

    def unprotect_outs(self):
        for out in self.outs:
            out.unprotect()

    def ignore_remove_outs(self):
        for out in self.outs:
            out.ignore_remove()

    @rwlocked(write=["outs"])
    def remove(self, force=False, remove_outs=True, purge=True):
        if remove_outs:
            self.remove_outs(ignore_remove=True, force=force)
        else:
            self.unprotect_outs()
            self.ignore_remove_outs()
        if purge:
            self.dvcfile.remove_stage(self)

    def reproduce(self, interactive=False, **kwargs):
        if not (kwargs.get("force", False) or self.changed()):
            if not isinstance(self, PipelineStage) and self.is_data_source:
                logger.info("'%s' didn't change, skipping", self.addressing)
            else:
                logger.info(
                    "Stage '%s' didn't change, skipping", self.addressing
                )
            return None

        msg = (
            "Going to reproduce {stage}. "
            "Are you sure you want to continue?".format(stage=self)
        )

        if interactive and not prompt.confirm(msg):
            raise DvcException("reproduction aborted by the user")

        self.run(**kwargs)

        logger.debug(f"{self} was reproduced")

        return self

    def update(self, rev=None, to_remote=False, remote=None, jobs=None):
        if not (self.is_repo_import or self.is_import):
            raise StageUpdateError(self.relpath)
        update_import(
            self, rev=rev, to_remote=to_remote, remote=remote, jobs=jobs
        )

    def reload(self):
        return self.dvcfile.stage

    def dumpd(self):
        return get_dump(self)

    def compute_md5(self):
        # `dvc add`ed files don't need stage md5
        if self.is_data_source and not (self.is_import or self.is_repo_import):
            m = None
        else:
            m = compute_md5(self)
        logger.debug(f"Computed {self} md5: '{m}'")
        return m

    def save(self, allow_missing=False):
        self.save_deps(allow_missing=allow_missing)
        self.save_outs(allow_missing=allow_missing)
        self.md5 = self.compute_md5()

        self.repo.stage_cache.save(self)

    def save_deps(self, allow_missing=False):
        from dvc.dependency.base import DependencyDoesNotExistError

        for dep in self.deps:
            try:
                dep.save()
            except DependencyDoesNotExistError:
                if not allow_missing:
                    raise

    def save_outs(self, allow_missing=False):
        from dvc.output import OutputDoesNotExistError

        for out in self.outs:
            try:
                out.save()
            except OutputDoesNotExistError:
                if not (allow_missing or out.checkpoint):
                    raise

    def ignore_outs(self):
        for out in self.outs:
            out.ignore()

    @staticmethod
    def _changed_entries(entries):
        return [str(entry) for entry in entries if entry.workspace_status()]

    def _changed_stage_entry(self):
        return f"'md5' of {self} changed."

    def changed_entries(self):
        changed_deps = self._changed_entries(self.deps)
        changed_outs = self._changed_entries(self.outs)
        return (
            changed_deps,
            changed_outs,
            self._changed_stage_entry() if self.changed_stage() else None,
        )

    @rwlocked(write=["outs"])
    def commit(self, allow_missing=False, filter_info=None):
        from dvc.output import OutputDoesNotExistError

        link_failures = []
        for out in self.filter_outs(filter_info):
            try:
                out.commit(filter_info=filter_info)
            except OutputDoesNotExistError:
                if not (allow_missing or out.checkpoint):
                    raise
            except CacheLinkError:
                link_failures.append(out.path_info)
        if link_failures:
            raise CacheLinkError(link_failures)

    @rwlocked(read=["deps", "outs"])
    def run(
        self,
        dry=False,
        no_commit=False,
        force=False,
        allow_missing=False,
        **kwargs,
    ):
        if (self.cmd or self.is_import) and not self.frozen and not dry:
            self.remove_outs(ignore_remove=False, force=False)

        if not self.frozen and self.is_import:
            jobs = kwargs.get("jobs", None)
            self._sync_import(dry, force, jobs)
        elif not self.frozen and self.cmd:
            self._run_stage(dry, force, **kwargs)
        else:
            args = (
                ("outputs", "frozen ") if self.frozen else ("data sources", "")
            )
            logger.info("Verifying %s in %s%s", *args, self)
            if not dry:
                self._check_missing_outputs()

        if not dry:
            if kwargs.get("checkpoint_func", None):
                allow_missing = True
            self.save(allow_missing=allow_missing)
            if not no_commit:
                self.commit(allow_missing=allow_missing)

    @rwlocked(read=["deps"], write=["outs"])
    def _run_stage(self, dry, force, **kwargs):
        return run_stage(self, dry, force, **kwargs)

    @rwlocked(read=["deps"], write=["outs"])
    def _sync_import(self, dry, force, jobs):
        sync_import(self, dry, force, jobs)

    @rwlocked(read=["outs"])
    def _check_missing_outputs(self):
        check_missing_outputs(self)

    def filter_outs(self, path_info):
        def _func(o):
            return path_info.isin_or_eq(o.path_info)

        return filter(_func, self.outs) if path_info else self.outs

    @rwlocked(write=["outs"])
    def checkout(self, allow_missing=False, **kwargs):
        stats = defaultdict(list)
        for out in self.filter_outs(kwargs.get("filter_info")):
            key, outs = self._checkout(
                out,
                allow_missing=allow_missing or self.is_checkpoint,
                **kwargs,
            )
            if key:
                stats[key].extend(outs)
        return stats

    @staticmethod
    def _checkout(out, **kwargs):
        try:
            result = out.checkout(**kwargs)
            added, modified = result or (None, None)
            if not (added or modified):
                return None, []
            return "modified" if modified else "added", [out.path_info]
        except CheckoutError as exc:
            return "failed", exc.target_infos

    @rwlocked(read=["deps", "outs"])
    def status(self, check_updates=False, filter_info=None):
        ret = []
        show_import = self.is_repo_import and check_updates

        if not self.frozen or show_import:
            self._status_deps(ret)
        self._status_outs(ret, filter_info=filter_info)
        self._status_always_changed(ret)
        self._status_stage(ret)
        return {self.addressing: ret} if ret else {}

    @staticmethod
    def _status(entries):
        ret = {}

        for entry in entries:
            ret.update(entry.status())

        return ret

    def _status_deps(self, ret):
        deps_status = self._status(self.deps)
        if deps_status:
            ret.append({"changed deps": deps_status})

    def _status_outs(self, ret, filter_info):
        filter_outs = self.filter_outs(filter_info)
        outs_status = self._status(filter_outs)
        if outs_status:
            ret.append({"changed outs": outs_status})

    def _status_always_changed(self, ret):
        if self.is_callback or self.always_changed or self.is_checkpoint:
            ret.append("always changed")

    def _status_stage(self, ret):
        if self.changed_stage():
            ret.append("changed checksum")

    def already_cached(self):
        return (
            not self.changed_stage()
            and self.deps_cached()
            and self.outs_cached()
        )

    def deps_cached(self):
        return all(not dep.changed() for dep in self.deps)

    def outs_cached(self):
        return all(
            not out.changed_cache() if out.use_cache else not out.changed()
            for out in self.outs
        )

    def get_all_files_number(self, filter_info=None):
        return sum(
            out.get_files_number(filter_info)
            for out in self.filter_outs(filter_info)
        )

    def get_used_objs(
        self, *args, **kwargs
    ) -> Dict[Optional["ObjectDB"], Set["HashFile"]]:
        """Return set of objects used by this stage."""
        used_objs = defaultdict(set)
        for out in self.filter_outs(kwargs.get("filter_info")):
            for odb, objs in out.get_used_objs(*args, **kwargs).items():
                used_objs[odb].update(objs)
        return used_objs

    @staticmethod
    def _check_can_merge(stage, ancestor_out=None):
        if isinstance(stage, PipelineStage):
            raise MergeError("unable to auto-merge pipeline stages")

        if not stage.is_data_source or stage.deps or len(stage.outs) > 1:
            raise MergeError(
                "unable to auto-merge DVC files that weren't "
                "created by `dvc add`"
            )

        if ancestor_out and not stage.outs:
            raise MergeError(
                "unable to auto-merge DVC files with deleted outputs"
            )

    def merge(self, ancestor, other):
        assert other

        if not other.outs:
            return

        if not self.outs:
            self.outs = other.outs
            return

        if ancestor:
            self._check_can_merge(ancestor)
            outs = ancestor.outs
            ancestor_out = outs[0] if outs else None
        else:
            ancestor_out = None

        self._check_can_merge(self, ancestor_out)
        self._check_can_merge(other, ancestor_out)

        self.outs[0].merge(ancestor_out, other.outs[0])

    def dump(self, **kwargs):
        self.dvcfile.dump(self, **kwargs)


class PipelineStage(Stage):
    def __init__(self, *args, name=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        self.cmd_changed = False
        self.tracked_vars = {}

    def __eq__(self, other):
        return super().__eq__(other) and self.name == other.name

    def __hash__(self):
        return hash((self.path_in_repo, self.name))

    @property
    def addressing(self):
        from dvc.dvcfile import PIPELINE_FILE

        if self.path and self.relpath == PIPELINE_FILE:
            return self.name
        return f"{super().addressing}:{self.name}"

    def reload(self):
        return self.dvcfile.stages[self.name]

    def _status_stage(self, ret):
        if self.cmd_changed:
            ret.append("changed command")

    def changed_stage(self):
        if self.cmd_changed:
            logger.debug(self._changed_stage_entry())
        return self.cmd_changed

    def _changed_stage_entry(self):
        return f"'cmd' of {self} has changed."

    def merge(self, ancestor, other):
        raise NotImplementedError
