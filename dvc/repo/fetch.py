import logging
import os

from dvc.config import NoRemoteError
from dvc.exceptions import DownloadError, NoOutputOrStageError

from ..scheme import Schemes
from . import locked

logger = logging.getLogger(__name__)


@locked
def fetch(
    self,
    targets=None,
    jobs=None,
    remote=None,
    all_branches=False,
    show_checksums=False,
    with_deps=False,
    all_tags=False,
    recursive=False,
    all_commits=False,
    run_cache=False,
    revs=None,
):
    """Download data items from a cloud and imported repositories

    Returns:
        int: number of successfully downloaded files

    Raises:
        DownloadError: thrown when there are failed downloads, either
            during `cloud.pull` or trying to fetch imported files

        config.NoRemoteError: thrown when downloading only local files and no
            remote is configured
    """

    if isinstance(targets, str):
        targets = [targets]

    used_objs, used_external = self.used_cache(
        targets,
        all_branches=all_branches,
        all_tags=all_tags,
        all_commits=all_commits,
        with_deps=with_deps,
        force=True,
        remote=remote,
        jobs=jobs,
        recursive=recursive,
        revs=revs,
    )

    downloaded = 0
    failed = 0

    try:
        if run_cache:
            self.stage_cache.pull(remote)
        downloaded += self.cloud.pull(
            used_objs,
            jobs,
            remote=remote,
            show_checksums=show_checksums,
        )
    except NoRemoteError:
        if not used_external and any(
            obj.fs.scheme == Schemes.LOCAL for obj in used_objs
        ):
            raise
    except DownloadError as exc:
        failed += exc.amount

    for repo_pair, files in used_external.items():
        d, f = _fetch_external(self, repo_pair.url, repo_pair.rev, files, jobs)
        downloaded += d
        failed += f

    if failed:
        raise DownloadError(failed)

    return downloaded


def _fetch_external(self, repo_url, repo_rev, files, jobs):
    from dvc.external_repo import external_repo
    from dvc.objects import save
    from dvc.objects.stage import stage
    from dvc.path_info import PathInfo
    from dvc.scm.base import CloneError

    failed = 0

    results = []

    def cb(result):
        results.append(result)

    odb = self.odb.local
    try:
        with external_repo(
            repo_url, repo_rev, cache_dir=odb.cache_dir
        ) as repo:
            root = PathInfo(repo.root_dir)
            for path in files:
                path_info = root / path
                try:
                    used_objs, _ = repo.used_cache(
                        [os.fspath(path_info)],
                        force=True,
                        jobs=jobs,
                        recursive=True,
                    )
                    cb(repo.cloud.pull(used_objs, jobs))
                except (NoOutputOrStageError, NoRemoteError):
                    pass
                obj = stage(odb, path_info, repo.repo_fs, "md5", jobs=jobs)
                save(odb, obj, jobs=jobs, download_callback=cb)
    except CloneError:
        failed += 1
        logger.exception(
            "failed to fetch data for '{}'".format(", ".join(files))
        )

    return sum(results), failed
