import logging

from dvc.cache import NamedCache
from dvc.exceptions import InvalidArgumentError

from ..scheme import Schemes
from . import locked

logger = logging.getLogger(__name__)


def _raise_error_if_all_disabled(**kwargs):
    if not any(kwargs.values()):
        raise InvalidArgumentError(
            "Either of `-w|--workspace`, `-a|--all-branches`, `-T|--all-tags` "
            "or `--all-commits` needs to be set."
        )


@locked
def gc(
    self,
    all_branches=False,
    cloud=False,
    remote=None,
    with_deps=False,
    all_tags=False,
    all_commits=False,
    force=False,
    jobs=None,
    repos=None,
    workspace=False,
    skip_failing_collect=False,
):

    # require `workspace` to be true to come into effect.
    # assume `workspace` to be enabled if any of `all_tags`, `all_commits`,
    # or `all_branches` are enabled.
    _raise_error_if_all_disabled(
        workspace=workspace,
        all_tags=all_tags,
        all_commits=all_commits,
        all_branches=all_branches,
    )

    from contextlib import ExitStack

    from dvc.repo import Repo

    if not repos:
        repos = []
    all_repos = [Repo(path) for path in repos]

    with ExitStack() as stack:
        for repo in all_repos:
            stack.enter_context(repo.lock)
            stack.enter_context(repo.state)

        used = NamedCache()
        for repo in all_repos + [self]:
            used.update(
                repo.used_cache(
                    all_branches=all_branches,
                    with_deps=with_deps,
                    all_tags=all_tags,
                    all_commits=all_commits,
                    remote=remote,
                    force=force,
                    jobs=jobs,
                    ignore_dvc_exceptions=skip_failing_collect,
                )
            )

    for scheme, cache in self.cache.by_scheme():
        if not cache:
            continue

        removed = cache.gc(set(used.scheme_keys(scheme)), jobs=jobs)
        if not removed:
            logger.info(f"No unused '{scheme}' cache to remove.")

    if not cloud:
        return

    remote = self.cloud.get_remote(remote, "gc -c")
    removed = remote.gc(set(used.scheme_keys(Schemes.LOCAL)), jobs=jobs)
    if not removed:
        logger.info("No unused cache to remove from remote.")
