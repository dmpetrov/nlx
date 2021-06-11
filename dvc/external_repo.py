import logging
import os
import tempfile
import threading
from contextlib import contextmanager
from typing import Dict

from funcy import retry, wrap_with

from dvc.exceptions import (
    FileMissingError,
    NoOutputInExternalRepoError,
    NoRemoteInExternalRepoError,
    NotDvcRepoError,
    OutputNotFoundError,
    PathMissingError,
)
from dvc.repo import Repo
from dvc.utils import relpath

logger = logging.getLogger(__name__)


@contextmanager
def external_repo(
    url, rev=None, for_write=False, cache_dir=None, cache_types=None, **kwargs
):
    from dvc.config import NoRemoteError
    from dvc.scm.git import Git

    logger.debug("Creating external repo %s@%s", url, rev)
    path = _cached_clone(url, rev, for_write=for_write)
    # Local HEAD points to the tip of whatever branch we first cloned from
    # (which may not be the default branch), use origin/HEAD here to get
    # the tip of the default branch
    rev = rev or "refs/remotes/origin/HEAD"

    cache_config = {
        "cache": {"dir": cache_dir or _get_cache_dir(url), "type": cache_types}
    }

    config = _get_remote_config(url) if os.path.isdir(url) else {}
    config.update(cache_config)

    def make_repo(path, **_kwargs):
        _config = cache_config.copy()
        if os.path.isdir(url):
            rel = os.path.relpath(path, _kwargs["scm"].root_dir)
            repo_path = os.path.join(url, rel)
            _config.update(_get_remote_config(repo_path))
        return Repo(path, config=_config, **_kwargs)

    root_dir = path if for_write else os.path.realpath(path)
    repo_kwargs = dict(
        root_dir=root_dir,
        url=url,
        scm=None if for_write else Git(root_dir),
        rev=None if for_write else rev,
        config=config,
        repo_factory=make_repo,
        **kwargs,
    )

    if "subrepos" not in repo_kwargs:
        repo_kwargs["subrepos"] = True

    if "uninitialized" not in repo_kwargs:
        repo_kwargs["uninitialized"] = True

    repo = Repo(**repo_kwargs)

    try:
        yield repo
    except NoRemoteError as exc:
        raise NoRemoteInExternalRepoError(url) from exc
    except OutputNotFoundError as exc:
        if exc.repo is repo:
            raise NoOutputInExternalRepoError(
                exc.output, repo.root_dir, url
            ) from exc
        raise
    except FileMissingError as exc:
        raise PathMissingError(exc.path, url) from exc
    finally:
        repo.close()
        if for_write:
            _remove(path)


CLONES: Dict[str, str] = {}
CACHE_DIRS: Dict[str, str] = {}


@wrap_with(threading.Lock())
def _get_cache_dir(url):
    try:
        cache_dir = CACHE_DIRS[url]
    except KeyError:
        cache_dir = CACHE_DIRS[url] = tempfile.mkdtemp("dvc-cache")
    return cache_dir


def clean_repos():
    # Outside code should not see cache while we are removing
    paths = [path for path, _ in CLONES.values()] + list(CACHE_DIRS.values())
    CLONES.clear()
    CACHE_DIRS.clear()

    for path in paths:
        _remove(path)


def _get_remote_config(url):
    try:
        repo = Repo(url)
    except NotDvcRepoError:
        return {}

    try:
        name = repo.config["core"].get("remote")
        if not name:
            # Fill the empty upstream entry with a new remote pointing to the
            # original repo's cache location.
            name = "auto-generated-upstream"
            return {
                "core": {"remote": name},
                "remote": {name: {"url": repo.odb.local.cache_dir}},
            }

        # Use original remote to make sure that we are using correct url,
        # credential paths, etc if they are relative to the config location.
        return {"remote": {name: repo.config["remote"][name]}}
    finally:
        repo.close()


def _cached_clone(url, rev, for_write=False):
    """Clone an external git repo to a temporary directory.

    Returns the path to a local temporary directory with the specified
    revision checked out. If for_write is set prevents reusing this dir via
    cache.
    """
    from shutil import copytree

    # even if we have already cloned this repo, we may need to
    # fetch/fast-forward to get specified rev
    clone_path, shallow = _clone_default_branch(url, rev, for_write=for_write)

    if not for_write and (url) in CLONES:
        return CLONES[url][0]

    # Copy to a new dir to keep the clone clean
    repo_path = tempfile.mkdtemp("dvc-erepo")
    logger.debug("erepo: making a copy of %s clone", url)
    copytree(clone_path, repo_path)

    # Check out the specified revision
    if for_write:
        _git_checkout(repo_path, rev)
    else:
        CLONES[url] = (repo_path, shallow)
    return repo_path


@wrap_with(threading.Lock())
def _clone_default_branch(url, rev, for_write=False):
    """Get or create a clean clone of the url.

    The cloned is reactualized with git pull unless rev is a known sha.
    """
    from dvc.scm.git import Git

    clone_path, shallow = CLONES.get(url, (None, False))

    git = None
    try:
        if clone_path:
            git = Git(clone_path)
            # Do not pull for known shas, branches and tags might move
            if not Git.is_sha(rev) or not git.has_rev(rev):
                if shallow:
                    # If we are missing a rev in a shallow clone, fallback to
                    # a full (unshallowed) clone. Since fetching specific rev
                    # SHAs is only available in certain git versions, if we
                    # have need to reference multiple specific revs for a
                    # given repo URL it is easier/safer for us to work with
                    # full clones in this case.
                    logger.debug("erepo: unshallowing clone for '%s'", url)
                    _unshallow(git)
                    shallow = False
                    CLONES[url] = (clone_path, shallow)
                else:
                    logger.debug("erepo: git pull '%s'", url)
                    git.pull()
        else:
            logger.debug("erepo: git clone '%s' to a temporary dir", url)
            clone_path = tempfile.mkdtemp("dvc-clone")
            if not for_write and rev and not Git.is_sha(rev):
                # If rev is a tag or branch name try shallow clone first
                from dvc.scm.base import CloneError

                try:
                    git = Git.clone(url, clone_path, shallow_branch=rev)
                    shallow = True
                    logger.debug(
                        "erepo: using shallow clone for branch '%s'", rev
                    )
                except CloneError:
                    pass
            if not git:
                git = Git.clone(url, clone_path)
                shallow = False
            CLONES[url] = (clone_path, shallow)
    finally:
        if git:
            git.close()

    return clone_path, shallow


def _unshallow(git):
    if git.gitpython.repo.head.is_detached:
        # If this is a detached head (i.e. we shallow cloned a tag) switch to
        # the default branch
        origin_refs = git.gitpython.repo.remotes["origin"].refs
        ref = origin_refs["HEAD"].reference
        branch_name = ref.name.split("/")[-1]
        branch = git.gitpython.repo.create_head(branch_name, ref)
        branch.set_tracking_branch(ref)
        branch.checkout()
    git.pull(unshallow=True)


def _git_checkout(repo_path, rev):
    from dvc.scm.git import Git

    logger.debug("erepo: git checkout %s@%s", repo_path, rev)
    git = Git(repo_path)
    try:
        git.checkout(rev)
    finally:
        git.close()


def _remove(path):
    from dvc.utils.fs import remove

    if os.name == "nt":
        # git.exe may hang for a while not permitting to remove temp dir
        os_retry = retry(5, errors=OSError, timeout=0.1)
        try:
            os_retry(remove)(path)
        except PermissionError:
            logger.warning(
                "Failed to remove '%s'", relpath(path), exc_info=True
            )
    else:
        remove(path)
