from typing import NamedTuple, Optional

from voluptuous import Required

from dvc.path_info import PathInfo

from .base import Dependency


class RepoPair(NamedTuple):
    url: str
    rev: Optional[str] = None


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
        super().__init__(stage, *args, **kwargs)

    def _parse_path(self, fs, path_info):
        return None

    @property
    def is_in_repo(self):
        return False

    @property
    def repo_pair(self) -> RepoPair:
        d = self.def_repo
        rev = d.get(self.PARAM_REV_LOCK) or d.get(self.PARAM_REV)
        return RepoPair(d[self.PARAM_URL], rev)

    def __str__(self):
        return "{} ({})".format(self.def_path, self.def_repo[self.PARAM_URL])

    def _make_repo(self, *, locked=True, **kwargs):
        from dvc.external_repo import external_repo

        d = self.def_repo
        rev = (d.get("rev_lock") if locked else None) or d.get("rev")
        return external_repo(d["url"], rev=rev, **kwargs)

    def _get_hash(self, locked=True):
        from dvc.objects.stage import stage

        with self._make_repo(locked=locked) as repo:
            path_info = PathInfo(repo.root_dir) / self.def_path
            return stage(
                self.repo.odb.local,
                path_info,
                repo.repo_fs,
                self.repo.odb.local.fs.PARAM_CHECKSUM,
            ).hash_info

    def workspace_status(self):
        current = self._get_hash(locked=True)
        updated = self._get_hash(locked=False)

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
        from dvc.config import NoRemoteError
        from dvc.exceptions import NoOutputOrStageError
        from dvc.objects import save
        from dvc.objects.stage import stage

        odb = self.repo.odb.local

        with self._make_repo(cache_dir=odb.cache_dir) as repo:
            if self.def_repo.get(self.PARAM_REV_LOCK) is None:
                self.def_repo[self.PARAM_REV_LOCK] = repo.get_rev()
            path_info = PathInfo(repo.root_dir) / self.def_path
            try:
                repo.fetch([path_info.fspath], jobs=jobs, recursive=True)
            except (NoOutputOrStageError, NoRemoteError):
                pass
            obj = stage(
                odb, path_info, repo.repo_fs, odb.fs.PARAM_CHECKSUM, jobs=jobs
            )
            save(odb, obj, jobs=jobs)

        checkout(
            to.path_info,
            to.fs,
            obj,
            odb,
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
