import os
from collections import defaultdict
from typing import TYPE_CHECKING, Dict, Optional, Set, Tuple

from funcy import first
from voluptuous import Required

from dvc.path_info import PathInfo

from .base import Dependency

if TYPE_CHECKING:
    from dvc.objects.db.base import ObjectDB
    from dvc.objects.file import HashFile


class RepoDependency(Dependency):
    PARAM_REPO = "repo"
    PARAM_URL = "url"
    PARAM_REV = "rev"
    PARAM_REV_LOCK = "rev_lock"

    REPO_SCHEMA = {
        PARAM_REPO: {
            Required(PARAM_URL): str,
            PARAM_REV: str,
            PARAM_REV_LOCK: str,
        }
    }

    def __init__(self, def_repo, stage, *args, **kwargs):
        self.def_repo = def_repo
        self._staged_objs: Dict[str, "HashFile"] = {}
        super().__init__(stage, *args, **kwargs)

    def _parse_path(self, fs, path_info):
        return None

    @property
    def is_in_repo(self):
        return False

    def __str__(self):
        return f"{self.def_path} ({self.def_repo[self.PARAM_URL]})"

    def workspace_status(self):
        current = self.get_obj(locked=True).hash_info
        updated = self.get_obj(locked=False).hash_info

        if current != updated:
            return {str(self): "update available"}

        return {}

    def status(self):
        return self.workspace_status()

    def save(self):
        pass

    def dumpd(self):
        return {self.PARAM_PATH: self.def_path, self.PARAM_REPO: self.def_repo}

    def download(self, to, jobs=None):
        from dvc.checkout import checkout
        from dvc.fs.memory import MemoryFileSystem
        from dvc.objects import save
        from dvc.repo.fetch import fetch_from_odb

        for odb, objs in self.get_used_objs().items():
            if isinstance(odb.fs, MemoryFileSystem):
                for obj in objs:
                    save(self.repo.odb.local, obj, jobs=jobs)
            else:
                fetch_from_odb(self.repo, odb, objs, jobs=jobs)

        obj = self.get_obj()
        checkout(
            to.path_info,
            to.fs,
            obj,
            self.repo.odb.local,
            dvcignore=None,
            state=self.repo.state,
        )

    def update(self, rev=None):
        if rev:
            self.def_repo[self.PARAM_REV] = rev
        with self._make_repo(locked=False) as repo:
            self.def_repo[self.PARAM_REV_LOCK] = repo.get_rev()

    def changed_checksum(self):
        # From current repo point of view what describes RepoDependency is its
        # origin project url and rev_lock, and it makes RepoDependency
        # immutable, hence its impossible for checksum to change.
        return False

    def get_used_objs(
        self, **kwargs
    ) -> Dict[Optional["ObjectDB"], Set["HashFile"]]:
        used, _ = self._get_used_and_obj(**kwargs)
        return used

    def _get_used_and_obj(
        self, obj_only=False, **kwargs
    ) -> Tuple[Dict[Optional["ObjectDB"], Set["HashFile"]], "HashFile"]:
        from dvc.config import NoRemoteError
        from dvc.exceptions import NoOutputOrStageError, PathMissingError
        from dvc.objects.stage import get_staging, stage

        local_odb = self.repo.odb.local
        locked = kwargs.pop("locked", True)
        with self._make_repo(
            locked=locked, cache_dir=local_odb.cache_dir
        ) as repo:
            used_objs = defaultdict(set)
            rev = repo.get_rev()
            if locked and self.def_repo.get(self.PARAM_REV_LOCK) is None:
                self.def_repo[self.PARAM_REV_LOCK] = rev

            path_info = PathInfo(repo.root_dir) / str(self.def_path)
            if not obj_only:
                try:
                    for odb, objs in repo.used_objs(
                        [os.fspath(path_info)],
                        force=True,
                        jobs=kwargs.get("jobs"),
                        recursive=True,
                    ).items():
                        if odb is None:
                            odb = repo.cloud.get_remote().odb
                            odb.read_only = True
                        self._check_circular_import(objs)
                        used_objs[odb].update(objs)
                except (NoRemoteError, NoOutputOrStageError):
                    pass

            try:
                staged_obj = stage(
                    None,
                    path_info,
                    repo.repo_fs,
                    local_odb.fs.PARAM_CHECKSUM,
                )
            except FileNotFoundError as exc:
                raise PathMissingError(
                    self.def_path, self.def_repo[self.PARAM_URL]
                ) from exc
            staging = get_staging()
            staging.read_only = True

            self._staged_objs[rev] = staged_obj
            used_objs[staging].add(staged_obj)
            return used_objs, staged_obj

    def _check_circular_import(self, objs):
        from dvc.exceptions import CircularImportError
        from dvc.fs.repo import RepoFileSystem
        from dvc.objects.tree import Tree

        obj = first(objs)
        if isinstance(obj, Tree):
            _, obj = first(obj)
        if not isinstance(obj.fs, RepoFileSystem):
            return

        self_url = self.repo.url or self.repo.root_dir
        if obj.fs.repo_url is not None and obj.fs.repo_url == self_url:
            raise CircularImportError(self, obj.fs.repo_url, self_url)

    def get_obj(self, filter_info=None, **kwargs):
        locked = kwargs.get("locked", True)
        rev = self._get_rev(locked=locked)
        if rev in self._staged_objs:
            return self._staged_objs[rev]
        _, obj = self._get_used_and_obj(
            obj_only=True, filter_info=filter_info, **kwargs
        )
        return obj

    def _make_repo(self, locked=True, **kwargs):
        from dvc.external_repo import external_repo

        d = self.def_repo
        rev = self._get_rev(locked=locked)
        return external_repo(d[self.PARAM_URL], rev=rev, **kwargs)

    def _get_rev(self, locked=True):
        d = self.def_repo
        return (d.get(self.PARAM_REV_LOCK) if locked else None) or d.get(
            self.PARAM_REV
        )
