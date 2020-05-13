import logging
import os
import pathlib
import string

from funcy import project

import dvc.dependency as dependency
import dvc.prompt as prompt
from dvc.exceptions import CheckoutError, DvcException
from dvc.utils import relpath

from . import params
from .decorators import rwlocked
from .exceptions import StageCommitError, StageUpdateError
from .utils import (
    check_circular_dependency,
    check_duplicated_arguments,
    check_missing_outputs,
    check_stage_path,
    compute_md5,
    fill_stage_dependencies,
    fill_stage_outputs,
    stage_dump_eq,
)

logger = logging.getLogger(__name__)
# Disallow all punctuation characters except hyphen and underscore
INVALID_STAGENAME_CHARS = set(string.punctuation) - {"_", "-"}


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
                Stage.PARAM_ALWAYS_CHANGED,
                Stage.PARAM_MD5,
                "name",
            ],
        ),
    }
    return cls(**kw)


def create_stage(cls, repo, path, **kwargs):
    from dvc.dvcfile import check_dvc_filename

    wdir = os.path.abspath(kwargs.get("wdir", None) or os.curdir)
    path = os.path.abspath(path)
    check_dvc_filename(path)
    check_stage_path(repo, wdir, is_wdir=kwargs.get("wdir"))
    check_stage_path(repo, os.path.dirname(path))

    stage = loads_from(cls, repo, path, wdir, kwargs)
    fill_stage_outputs(stage, **kwargs)
    fill_stage_dependencies(
        stage, **project(kwargs, ["deps", "erepo", "params"])
    )
    check_circular_dependency(stage)
    check_duplicated_arguments(stage)

    if stage and stage.dvcfile.exists():
        has_persist_outs = any(out.persist for out in stage.outs)
        ignore_run_cache = (
            not kwargs.get("run_cache", True) or has_persist_outs
        )
        if has_persist_outs:
            logger.warning("Build cache is ignored when persisting outputs.")

        if not ignore_run_cache and stage.can_be_skipped:
            logger.info("Stage is cached, skipping.")
            return None

    return stage


class Stage(params.StageParams):
    from .run import run_stage, cmd_run

    def __init__(
        self,
        repo,
        path=None,
        cmd=None,
        wdir=os.curdir,
        deps=None,
        outs=None,
        md5=None,
        locked=False,
        always_changed=False,
        stage_text=None,
        dvcfile=None,
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
        self.locked = locked
        self.always_changed = always_changed
        self._stage_text = stage_text
        self._dvcfile = dvcfile

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, path):
        self._path = path

    @property
    def dvcfile(self):
        if self.path and self._dvcfile and self.path == self._dvcfile.path:
            return self._dvcfile

        if not self.path:
            raise DvcException(
                "Stage does not have any path set "
                "and is detached from dvcfile."
            )

        from dvc.dvcfile import Dvcfile

        self._dvcfile = Dvcfile(self.repo, self.path)
        return self._dvcfile

    @dvcfile.setter
    def dvcfile(self, dvcfile):
        self._dvcfile = dvcfile

    def __repr__(self):
        return "Stage: '{path}'".format(
            path=self.path_in_repo if self.path else "No path"
        )

    def __str__(self):
        return "stage: '{path}'".format(
            path=self.relpath if self.path else "No path"
        )

    @property
    def addressing(self):
        """
        Useful for alternative presentations where we don't need
        `Stage:` prefix.
        """
        return self.relpath

    def __hash__(self):
        return hash(self.path_in_repo)

    def __eq__(self, other):
        return (
            self.__class__ == other.__class__
            and self.repo is other.repo
            and self.path_in_repo == other.path_in_repo
        )

    @property
    def path_in_repo(self):
        return relpath(self.path, self.repo.root_dir)

    @property
    def relpath(self):
        return relpath(self.path)

    @property
    def is_data_source(self):
        """Whether the DVC-file was created with `dvc add` or `dvc import`"""
        return self.cmd is None

    @property
    def is_callback(self):
        """
        A callback stage is always considered as changed,
        so it runs on every `dvc repro` call.
        """
        return not self.is_data_source and len(self.deps) == 0

    @property
    def is_import(self):
        """Whether the DVC-file was created with `dvc import`."""
        return not self.cmd and len(self.deps) == 1 and len(self.outs) == 1

    @property
    def is_repo_import(self):
        if not self.is_import:
            return False

        return isinstance(self.deps[0], dependency.RepoDependency)

    def changed_deps(self):
        if self.locked:
            return False

        if self.is_callback:
            logger.warning(
                '{stage} is a "callback" stage '
                "(has a command and no dependencies) and thus always "
                "considered as changed.".format(stage=self)
            )
            return True

        if self.always_changed:
            return True

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

    def changed_stage(self, warn=False):
        changed = self.md5 != self.compute_md5()
        if changed and warn:
            logger.debug("DVC-file '{}' changed.".format(self.relpath))
        return changed

    @rwlocked(read=["deps", "outs"])
    def changed(self):
        if self._changed():
            logger.debug("{} changed.".format(self))
            return True

        logger.debug("{} didn't change.".format(self))
        return False

    def _changed(self):
        # Short-circuit order: stage md5 is fast, deps are expected to change
        return (
            self.changed_stage(warn=True)
            or self.changed_deps()
            or self.changed_outs()
        )

    @rwlocked(write=["outs"])
    def remove_outs(self, ignore_remove=False, force=False):
        """Used mainly for `dvc remove --outs` and :func:`Stage.reproduce`."""
        for out in self.outs:
            if out.persist and not force:
                out.unprotect()
            else:
                logger.debug(
                    "Removing output '{out}' of {stage}.".format(
                        out=out, stage=self
                    )
                )
                out.remove(ignore_remove=ignore_remove)

    def unprotect_outs(self):
        for out in self.outs:
            out.unprotect()

    @rwlocked(write=["outs"])
    def remove(self, force=False, remove_outs=True):
        if remove_outs:
            self.remove_outs(ignore_remove=True, force=force)
        else:
            self.unprotect_outs()
        self.dvcfile.remove()

    @rwlocked(read=["deps"], write=["outs"])
    def reproduce(self, interactive=False, **kwargs):

        if not (kwargs.get("force", False) or self.changed()):
            return None

        msg = (
            "Going to reproduce {stage}. "
            "Are you sure you want to continue?".format(stage=self)
        )

        if interactive and not prompt.confirm(msg):
            raise DvcException("reproduction aborted by the user")

        self.run(**kwargs)

        logger.debug("{stage} was reproduced".format(stage=self))

        return self

    def update(self, rev=None):
        if not (self.is_repo_import or self.is_import):
            raise StageUpdateError(self.relpath)

        self.deps[0].update(rev=rev)
        locked = self.locked
        self.locked = False
        try:
            self.reproduce()
        finally:
            self.locked = locked

    @property
    def can_be_skipped(self):
        return (
            self.is_cached and not self.is_callback and not self.always_changed
        )

    def reload(self):
        return self.dvcfile.stage

    @property
    def is_cached(self):
        """Checks if this stage has been already ran and stored"""
        old = self.reload()
        if old.changed_outs():
            return False

        # NOTE: need to save checksums for deps in order to compare them
        # with what is written in the old stage.
        self.save_deps()
        if not stage_dump_eq(Stage, old.dumpd(), self.dumpd()):
            return False

        # NOTE: committing to prevent potential data duplication. For example
        #
        #    $ dvc config cache.type hardlink
        #    $ echo foo > foo
        #    $ dvc add foo
        #    $ rm -f foo
        #    $ echo foo > foo
        #    $ dvc add foo # should replace foo with a link to cache
        #
        old.commit()

        return True

    def resolve_wdir(self):
        rel_wdir = relpath(self.wdir, os.path.dirname(self.path))
        return (
            pathlib.PurePath(rel_wdir).as_posix() if rel_wdir != "." else None
        )

    def dumpd(self):
        return {
            key: value
            for key, value in {
                Stage.PARAM_MD5: self.md5,
                Stage.PARAM_CMD: self.cmd,
                Stage.PARAM_WDIR: self.resolve_wdir(),
                Stage.PARAM_LOCKED: self.locked,
                Stage.PARAM_DEPS: [d.dumpd() for d in self.deps],
                Stage.PARAM_OUTS: [o.dumpd() for o in self.outs],
                Stage.PARAM_ALWAYS_CHANGED: self.always_changed,
            }.items()
            if value
        }

    def compute_md5(self):
        m = compute_md5(self)
        logger.debug("Computed {} md5: '{}'".format(self, m))
        return m

    def save_deps(self):
        for dep in self.deps:
            dep.save()

    def save(self):
        self.save_deps()

        for out in self.outs:
            out.save()

        self.md5 = self.compute_md5()

        self.repo.stage_cache.save(self)

    @staticmethod
    def _changed_entries(entries):
        return [
            str(entry)
            for entry in entries
            if entry.checksum and entry.changed_checksum()
        ]

    def check_can_commit(self, force):
        changed_deps = self._changed_entries(self.deps)
        changed_outs = self._changed_entries(self.outs)

        if changed_deps or changed_outs or self.changed_stage():
            msg = (
                "dependencies {}".format(changed_deps) if changed_deps else ""
            )
            msg += " and " if (changed_deps and changed_outs) else ""
            msg += "outputs {}".format(changed_outs) if changed_outs else ""
            msg += "md5" if not (changed_deps or changed_outs) else ""
            msg += " of {} changed. ".format(self)
            msg += "Are you sure you want to commit it?"
            if not (force or prompt.confirm(msg)):
                raise StageCommitError(
                    "unable to commit changed {}. Use `-f|--force` to "
                    "force.".format(self)
                )
            self.save()

    @rwlocked(write=["outs"])
    def commit(self):
        for out in self.outs:
            out.commit()

    def _import_sync(self, dry=False, force=False):
        """Synchronize import's outs to the workspace."""
        logger.info(
            "Importing '{dep}' -> '{out}'".format(
                dep=self.deps[0], out=self.outs[0]
            )
        )
        if dry:
            return

        if (
            not force
            and not self.changed_stage(warn=True)
            and self.already_cached()
        ):
            self.outs[0].checkout()
        else:
            self.deps[0].download(self.outs[0])

    @rwlocked(read=["deps"], write=["outs"])
    def run(self, dry=False, no_commit=False, force=False, run_cache=True):
        if (self.cmd or self.is_import) and not self.locked and not dry:
            self.remove_outs(ignore_remove=False, force=False)

        if not self.locked and self.is_import:
            self._import_sync(dry, force)
        elif not self.locked and self.cmd:
            self.run_stage(dry, force, run_cache)
        else:
            logger.info(
                "Verifying %s in %s%s",
                "outputs" if self.locked else "data sources",
                "locked " if self.locked else "",
                self,
            )
            if not dry:
                check_missing_outputs(self)

        if dry:
            return

        self.save()
        if not no_commit:
            self.commit()

    def _filter_outs(self, path_info):
        def _func(o):
            return path_info.isin_or_eq(o.path_info)

        return filter(_func, self.outs) if path_info else self.outs

    @rwlocked(write=["outs"])
    def checkout(
        self,
        force=False,
        progress_callback=None,
        relink=False,
        filter_info=None,
    ):
        checkouts = {"failed": [], "added": [], "modified": []}
        for out in self._filter_outs(filter_info):
            try:
                result = out.checkout(
                    force=force,
                    progress_callback=progress_callback,
                    relink=relink,
                    filter_info=filter_info,
                )
                added, modified = result or (None, None)
                if modified:
                    checkouts["modified"].append(out.path_info)
                elif added:
                    checkouts["added"].append(out.path_info)
            except CheckoutError as exc:
                checkouts["failed"].extend(exc.target_infos)

        return checkouts

    @staticmethod
    def _status(entries):
        ret = {}

        for entry in entries:
            ret.update(entry.status())

        return ret

    def status_stage(self):
        return ["changed checksum"] if self.changed_stage() else []

    @rwlocked(read=["deps", "outs"])
    def status(self, check_updates=False):
        ret = []

        show_import = self.is_repo_import and check_updates

        if not self.locked or show_import:
            deps_status = self._status(self.deps)
            if deps_status:
                ret.append({"changed deps": deps_status})

        outs_status = self._status(self.outs)
        if outs_status:
            ret.append({"changed outs": outs_status})

        ret.extend(self.status_stage())
        if self.is_callback or self.always_changed:
            ret.append("always changed")

        if ret:
            return {self.addressing: ret}

        return {}

    def already_cached(self):
        return self.deps_cached() and self.outs_cached()

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
            for out in self._filter_outs(filter_info)
        )

    def get_used_cache(self, *args, **kwargs):
        from dvc.cache import NamedCache

        cache = NamedCache()
        for out in self._filter_outs(kwargs.get("filter_info")):
            cache.update(out.get_used_cache(*args, **kwargs))

        return cache


class PipelineStage(Stage):
    def __init__(self, *args, name=None, meta=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        self.cmd_changed = False
        # This is how the Stage will discover any discrepancies
        self.meta = meta or {}

    def __eq__(self, other):
        return super().__eq__(other) and self.name == other.name

    def __hash__(self):
        return hash((self.path_in_repo, self.name))

    def __repr__(self):
        return "Stage: '{path}:{name}'".format(
            path=self.relpath if self.path else "No path", name=self.name
        )

    def __str__(self):
        return "stage: '{path}:{name}'".format(
            path=self.relpath if self.path else "No path", name=self.name
        )

    @property
    def addressing(self):
        return super().addressing + ":" + self.name

    def reload(self):
        return self.dvcfile.stages[self.name]

    @property
    def is_cached(self):
        return self.name in self.dvcfile.stages and super().is_cached

    def status_stage(self):
        return ["changed command"] if self.cmd_changed else []

    def changed_stage(self, warn=False):
        if self.cmd_changed and warn:
            logger.debug("'cmd' of {} has changed.".format(self))
        return self.cmd_changed
