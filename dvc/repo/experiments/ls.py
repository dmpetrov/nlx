import logging
from collections import defaultdict

from dvc.repo import locked
from dvc.repo.scm_context import scm_context
from dvc.scm.base import RevError

from .utils import (
    exp_refs,
    exp_refs_by_baseline,
    remote_exp_refs,
    remote_exp_refs_by_baseline,
)

logger = logging.getLogger(__name__)


@locked
@scm_context
def ls(repo, *args, rev=None, git_remote=None, all_=False, **kwargs):
    from dvc.scm.git import Git

    if rev:
        try:
            rev = repo.scm.resolve_rev(rev)
        except RevError:
            if not (git_remote and Git.is_sha(rev)):
                # This could be a remote rev that has not been fetched yet
                raise
    elif not all_:
        rev = repo.scm.get_rev()

    results = defaultdict(list)

    if rev:
        if git_remote:
            gen = remote_exp_refs_by_baseline(repo.scm, git_remote, rev)
        else:
            gen = exp_refs_by_baseline(repo.scm, rev)
        for info in gen:
            results[rev].append(info.name)
    elif all_:
        if git_remote:
            gen = remote_exp_refs(repo.scm, git_remote)
        else:
            gen = exp_refs(repo.scm)
        for info in gen:
            results[info.baseline_sha].append(info.name)

    return results
