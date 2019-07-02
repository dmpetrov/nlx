from __future__ import unicode_literals

import copy

from funcy import merge
from schema import Optional
from contextlib import contextmanager

from dvc.external_repo import external_repo
from dvc.utils.compat import str

from .local import DependencyLOCAL


class DependencyREPO(DependencyLOCAL):
    PARAM_REPO = "repo"
    PARAM_URL = "url"
    PARAM_REV = "rev"
    PARAM_REV_LOCK = "rev_lock"

    REPO_SCHEMA = {
        Optional(PARAM_URL): str,
        Optional(PARAM_REV): str,
        Optional(PARAM_REV_LOCK): str,
    }

    def __init__(self, def_repo, stage, *args, **kwargs):
        self.def_repo = def_repo
        super(DependencyREPO, self).__init__(stage, *args, **kwargs)

    def _parse_path(self, remote, path):
        return None

    @property
    def is_in_repo(self):
        return False

    def __str__(self):
        return "{} ({})".format(self.def_path, self.def_repo[self.PARAM_URL])

    @contextmanager
    def make_repo(self, **overrides):
        with external_repo(**merge(self.def_repo, overrides)) as repo:
            yield repo

    def status(self):
        with self.make_repo() as repo:
            current = repo.find_out_by_relpath(self.def_path).info

        with self.make_repo(rev_lock=None) as repo:
            updated = repo.find_out_by_relpath(self.def_path).info

        if current != updated:
            return {str(self): "update available"}

        return {}

    def save(self):
        pass

    def dumpd(self):
        return {self.PARAM_PATH: self.def_path, self.PARAM_REPO: self.def_repo}

    def download(self, to, resume=False):
        with self.make_repo(cache_dir=self.repo.cache.local.cache_dir) as repo:
            self.def_repo[self.PARAM_REV_LOCK] = repo.scm.get_rev()

            out = repo.find_out_by_relpath(self.def_path)
            repo.fetch(out.stage.path)
            to.info = copy.copy(out.info)
            to.checkout()

    def update(self):
        with self.make_repo(rev_lock=None) as repo:
            self.def_repo[self.PARAM_REV_LOCK] = repo.scm.get_rev()
