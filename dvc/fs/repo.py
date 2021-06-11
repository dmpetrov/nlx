import logging
import os
import stat
import threading
from contextlib import suppress
from itertools import takewhile
from typing import TYPE_CHECKING, Callable, Optional, Tuple, Type, Union

from funcy import lfilter, wrap_with

from dvc.path_info import PathInfo

from .base import BaseFileSystem
from .dvc import DvcFileSystem

if TYPE_CHECKING:
    from dvc.repo import Repo

logger = logging.getLogger(__name__)

RepoFactory = Union[Callable[[str], "Repo"], Type["Repo"]]


class RepoFileSystem(BaseFileSystem):  # pylint:disable=abstract-method
    """DVC + git-tracked files fs.

    Args:
        repo: DVC or git repo.
        subrepos: traverse to subrepos (by default, it ignores subrepos)
        repo_factory: A function to initialize subrepo with, default is Repo.
        kwargs: Additional keyword arguments passed to the `DvcFileSystem()`.
    """

    scheme = "local"
    PARAM_CHECKSUM = "md5"

    def __init__(
        self, repo=None, subrepos=False, repo_factory: RepoFactory = None
    ):
        super().__init__()

        from dvc.utils.collections import PathStringTrie

        if not repo_factory:
            from dvc.repo import Repo

            self.repo_factory: RepoFactory = Repo
        else:
            self.repo_factory = repo_factory

        self._main_repo = repo
        self.hash_jobs = repo.fs.hash_jobs
        self.root_dir = repo.root_dir
        self._traverse_subrepos = subrepos

        self._subrepos_trie = PathStringTrie()
        """Keeps track of each and every path with the corresponding repo."""

        self._subrepos_trie[self.root_dir] = repo

        self._dvcfss = {}
        """Keep a dvcfs instance of each repo."""

        if hasattr(repo, "dvc_dir"):
            self._dvcfss[repo.root_dir] = DvcFileSystem(repo=repo)

    def _get_repo(self, path: str) -> Optional["Repo"]:
        """Returns repo that the path falls in, using prefix.

        If the path is already tracked/collected, it just returns the repo.

        Otherwise, it collects the repos that might be in the path's parents
        and then returns the appropriate one.
        """
        repo = self._subrepos_trie.get(path)
        if repo:
            return repo

        prefix, repo = self._subrepos_trie.longest_prefix(path)
        if not prefix:
            return None

        parents = (parent.fspath for parent in PathInfo(path).parents)
        dirs = [path] + list(takewhile(lambda p: p != prefix, parents))
        dirs.reverse()
        self._update(dirs, starting_repo=repo)
        return self._subrepos_trie.get(path)

    @wrap_with(threading.Lock())
    def _update(self, dirs, starting_repo):
        """Checks for subrepo in directories and updates them."""
        repo = starting_repo
        for d in dirs:
            if self._is_dvc_repo(d):
                repo = self.repo_factory(
                    d,
                    scm=self._main_repo.scm,
                    rev=self._main_repo.get_rev(),
                    repo_factory=self.repo_factory,
                )
                self._dvcfss[repo.root_dir] = DvcFileSystem(repo=repo)
            self._subrepos_trie[d] = repo

    def _is_dvc_repo(self, dir_path):
        """Check if the directory is a dvc repo."""
        if not self._traverse_subrepos:
            return False

        from dvc.repo import Repo

        repo_path = os.path.join(dir_path, Repo.DVC_DIR)
        return self._main_repo.fs.isdir(repo_path)

    def _get_fs_pair(
        self, path
    ) -> Tuple[BaseFileSystem, Optional[DvcFileSystem]]:
        """
        Returns a pair of fss based on repo the path falls in, using prefix.
        """
        path = os.path.abspath(path)

        # fallback to the top-level repo if repo was not found
        # this can happen if the path is outside of the repo
        repo = self._get_repo(path) or self._main_repo

        dvc_fs = self._dvcfss.get(repo.root_dir)
        return repo.fs, dvc_fs

    def open(
        self, path, mode="r", encoding="utf-8", **kwargs
    ):  # pylint: disable=arguments-differ
        if "b" in mode:
            encoding = None

        fs, dvc_fs = self._get_fs_pair(path)
        path_info = PathInfo(path)
        try:
            return fs.open(path_info, mode=mode, encoding=encoding)
        except FileNotFoundError:
            if not dvc_fs:
                raise

        return dvc_fs.open(path_info, mode=mode, encoding=encoding, **kwargs)

    def exists(self, path_info) -> bool:
        fs, dvc_fs = self._get_fs_pair(path_info)

        if not dvc_fs:
            return fs.exists(path_info)

        if dvc_fs.repo.dvcignore.is_ignored(fs, path_info):
            return False

        if fs.exists(path_info):
            return True

        try:
            meta = dvc_fs.metadata(path_info)
        except FileNotFoundError:
            return False

        (out,) = meta.outs
        assert len(meta.outs) == 1
        if fs.exists(out.path_info):
            return False
        return True

    def isdir(self, path):  # pylint: disable=arguments-differ
        fs, dvc_fs = self._get_fs_pair(path)

        if dvc_fs and dvc_fs.repo.dvcignore.is_ignored_dir(path):
            return False

        try:
            st = fs.stat(path)
            return stat.S_ISDIR(st.st_mode)
        except (OSError, ValueError):
            # from CPython's os.path.isdir()
            pass

        if not dvc_fs:
            return False

        try:
            meta = dvc_fs.metadata(path)
        except FileNotFoundError:
            return False

        (out,) = meta.outs
        assert len(meta.outs) == 1
        if fs.exists(out.path_info):
            return False
        return meta.isdir

    def isdvc(self, path, **kwargs):
        _, dvc_fs = self._get_fs_pair(path)
        return dvc_fs is not None and dvc_fs.isdvc(path, **kwargs)

    def isfile(self, path):  # pylint: disable=arguments-differ
        fs, dvc_fs = self._get_fs_pair(path)

        if dvc_fs and dvc_fs.repo.dvcignore.is_ignored_file(path):
            return False

        try:
            st = fs.stat(path)
            return stat.S_ISREG(st.st_mode)
        except (OSError, ValueError):
            # from CPython's os.path.isfile()
            pass

        if not dvc_fs:
            return False

        try:
            meta = dvc_fs.metadata(path)
        except FileNotFoundError:
            return False

        (out,) = meta.outs
        assert len(meta.outs) == 1
        if fs.exists(out.path_info):
            return False
        return meta.isfile

    def isexec(self, path_info):
        fs, dvc_fs = self._get_fs_pair(path_info)
        if dvc_fs and dvc_fs.exists(path_info):
            return dvc_fs.isexec(path_info)
        return fs.isexec(path_info)

    def stat(self, path):
        fs, _ = self._get_fs_pair(path)
        return fs.stat(path)

    def _dvc_walk(self, walk):
        try:
            root, dirs, files = next(walk)
        except StopIteration:
            return
        yield root, dirs, files
        for _ in dirs:
            yield from self._dvc_walk(walk)

    def _subrepo_walk(self, dir_path, **kwargs):
        """Walk into a new repo.

        NOTE: subrepo will only be discovered when walking if
        ignore_subrepos is set to False.
        """
        fs, dvc_fs = self._get_fs_pair(dir_path)
        fs_walk = fs.walk(dir_path, topdown=True)
        if dvc_fs:
            dvc_walk = dvc_fs.walk(dir_path, topdown=True, **kwargs)
        else:
            dvc_walk = None
        yield from self._walk(fs_walk, dvc_walk, **kwargs)

    def _walk(self, repo_walk, dvc_walk=None, dvcfiles=False):
        from dvc.dvcfile import is_valid_filename
        from dvc.ignore import DvcIgnore

        assert repo_walk
        try:
            _, dvc_dirs, dvc_fnames = (
                next(dvc_walk) if dvc_walk else (None, [], [])
            )
            repo_root, repo_dirs, repo_fnames = next(repo_walk)
        except StopIteration:
            return

        # separate subdirs into shared dirs, dvc-only dirs, repo-only dirs
        dvc_set = set(dvc_dirs)
        repo_set = set(repo_dirs)
        dvc_only = list(dvc_set - repo_set)
        repo_only = list(repo_set - dvc_set)
        shared = list(dvc_set & repo_set)
        dirs = shared + dvc_only + repo_only

        def _func(fname):
            if dvcfiles:
                return True

            return not (
                is_valid_filename(fname) or fname == DvcIgnore.DVCIGNORE_FILE
            )

        # merge file lists
        files = set(filter(_func, dvc_fnames + repo_fnames))

        yield repo_root, dirs, list(files)

        def is_dvc_repo(d):
            return self._is_dvc_repo(os.path.join(repo_root, d))

        # remove subrepos to prevent it from being traversed
        subrepos = set(filter(is_dvc_repo, repo_only))
        # set dir order for next recursion level - shared dirs first so that
        # next() for both generators recurses into the same shared directory
        dvc_dirs[:] = [dirname for dirname in dirs if dirname in dvc_set]
        repo_dirs[:] = lfilter(lambda d: d in (repo_set - subrepos), dirs)

        for dirname in dirs:
            if dirname in subrepos:
                dir_path = os.path.join(repo_root, dirname)
                yield from self._subrepo_walk(dir_path, dvcfiles=dvcfiles)
            elif dirname in shared:
                yield from self._walk(repo_walk, dvc_walk, dvcfiles=dvcfiles)
            elif dirname in dvc_set:
                yield from self._dvc_walk(dvc_walk)
            elif dirname in repo_set:
                yield from self._walk(repo_walk, None, dvcfiles=dvcfiles)

    def walk(self, top, topdown=True, onerror=None, **kwargs):
        """Walk and merge both DVC and repo fss.

        Args:
            top: path to walk from
            topdown: if True, fs will be walked from top down.
            onerror: if set, onerror function will be called if an error
                occurs (by default errors are ignored).
            dvcfiles: if True, dvcfiles will be included in the files list
                for walked directories.

        Any kwargs will be passed into methods used for fetching and/or
        streaming DVC outs from remotes.
        """
        assert topdown

        if not self.exists(top):
            if onerror is not None:
                onerror(FileNotFoundError(top))
            return

        if not self.isdir(top):
            if onerror is not None:
                onerror(NotADirectoryError(top))
            return

        repo = self._get_repo(os.path.abspath(top))
        dvcfiles = kwargs.pop("dvcfiles", False)

        fs, dvc_fs = self._get_fs_pair(top)
        repo_exists = fs.exists(top)

        repo_walk = repo.dvcignore.walk(
            fs, top, topdown=topdown, onerror=onerror, **kwargs
        )

        if not dvc_fs or (repo_exists and dvc_fs.isdvc(top)):
            yield from self._walk(repo_walk, None, dvcfiles=dvcfiles)
            return

        if not repo_exists:
            yield from dvc_fs.walk(
                top, topdown=topdown, onerror=onerror, **kwargs
            )

        dvc_walk = None
        if dvc_fs.exists(top):
            dvc_walk = dvc_fs.walk(
                top, topdown=topdown, onerror=onerror, **kwargs
            )

        yield from self._walk(repo_walk, dvc_walk, dvcfiles=dvcfiles)

    def walk_files(self, path_info, **kwargs):
        for root, _, files in self.walk(path_info, **kwargs):
            for fname in files:
                yield PathInfo(root) / fname

    def _download(
        self, from_info, to_file, name=None, no_progress_bar=False, **kwargs
    ):
        import shutil

        from dvc.progress import Tqdm

        with open(to_file, "wb+") as to_fobj:
            with Tqdm.wrapattr(
                to_fobj, "write", desc=name, disable=no_progress_bar
            ) as wrapped:
                with self.open(from_info, "rb", **kwargs) as from_fobj:
                    shutil.copyfileobj(from_fobj, wrapped)

    def metadata(self, path):
        abspath = os.path.abspath(path)
        path_info = PathInfo(abspath)
        fs, dvc_fs = self._get_fs_pair(path_info)

        dvc_meta = None
        if dvc_fs:
            with suppress(FileNotFoundError):
                dvc_meta = dvc_fs.metadata(path_info)

        stat_result = None
        with suppress(FileNotFoundError):
            stat_result = fs.stat(path_info)

        if not stat_result and not dvc_meta:
            raise FileNotFoundError

        from ._metadata import Metadata

        meta = dvc_meta or Metadata(
            path_info=path_info,
            repo=self._get_repo(abspath) or self._main_repo,
        )

        isdir = bool(stat_result) and stat.S_ISDIR(stat_result.st_mode)
        meta.isdir = meta.isdir or isdir

        if not dvc_meta:
            from dvc.utils import is_exec

            meta.is_exec = bool(stat_result) and is_exec(stat_result.st_mode)
        return meta

    def info(self, path_info):
        fs, dvc_fs = self._get_fs_pair(path_info)

        try:
            return fs.info(path_info)
        except FileNotFoundError:
            return dvc_fs.info(path_info)
