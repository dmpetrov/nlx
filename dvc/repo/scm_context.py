from functools import wraps
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dvc.repo import Repo


def scm_context(method):
    @wraps(method)
    def run(repo: "Repo", *args, **kw):
        scm = repo.scm

        with scm.track_file_changes(config=repo.config):
            return method(repo, *args, **kw)

    return run
