import logging
from typing import TYPE_CHECKING, Set

from dvc.exceptions import InvalidArgumentError

from ..scheme import Schemes
from . import locked

if TYPE_CHECKING:
    from dvc.objects.file import HashFile

logger = logging.getLogger(__name__)


def _raise_error_if_all_disabled(**kwargs):
    if not any(kwargs.values()):
        raise InvalidArgumentError(
            "Either of `-w|--workspace`, `-a|--all-branches`, `-T|--all-tags` "
            "`--all-experiments` or `--all-commits` needs to be set."
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
    all_experiments=False,
    force=False,
    jobs=None,
    repos=None,
    workspace=False,
):

    # require `workspace` to be true to come into effect.
    # assume `workspace` to be enabled if any of `all_tags`, `all_commits`,
    # `all_experiments` or `all_branches` are enabled.
    _raise_error_if_all_disabled(
        workspace=workspace,
        all_tags=all_tags,
        all_commits=all_commits,
        all_branches=all_branches,
        all_experiments=all_experiments,
    )

    from contextlib import ExitStack

    from dvc.repo import Repo

    if not repos:
        repos = []
    all_repos = [Repo(path) for path in repos]

    used_objs: Set["HashFile"] = set()
    with ExitStack() as stack:
        for repo in all_repos:
            stack.enter_context(repo.lock)

        for repo in all_repos + [self]:
            objs, _ = repo.used_cache(
                all_branches=all_branches,
                with_deps=with_deps,
                all_tags=all_tags,
                all_commits=all_commits,
                all_experiments=all_experiments,
                remote=remote,
                force=force,
                jobs=jobs,
            )
            used_objs.update(objs)

    for scheme, odb in self.odb.by_scheme():
        if not odb:
            continue

        removed = odb.gc(
            {obj for obj in used_objs if obj.fs.scheme == scheme},
            jobs=jobs,
        )
        if not removed:
            logger.info(f"No unused '{scheme}' cache to remove.")

    if not cloud:
        return

    remote = self.cloud.get_remote(remote, "gc -c")
    removed = remote.gc(
        {obj for obj in used_objs if obj.fs.scheme == Schemes.LOCAL},
        jobs=jobs,
    )
    if not removed:
        logger.info("No unused cache to remove from remote.")
