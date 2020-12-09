import logging
import os
from contextlib import contextmanager
from functools import wraps
from typing import TYPE_CHECKING

from funcy import cached_property, cat
from git import InvalidGitRepositoryError

from dvc.config import Config
from dvc.exceptions import DvcException, FileMissingError
from dvc.exceptions import IsADirectoryError as DvcIsADirectoryError
from dvc.exceptions import NotDvcRepoError, OutputNotFoundError
from dvc.path_info import PathInfo
from dvc.scm import Base
from dvc.scm.base import SCMError
from dvc.tree.repo import RepoTree
from dvc.utils.fs import path_isin

from .graph import build_graph, build_outs_graph, get_pipelines
from .trie import build_outs_trie

if TYPE_CHECKING:
    from dvc.tree.base import BaseTree


logger = logging.getLogger(__name__)


@contextmanager
def lock_repo(repo: "Repo"):
    # pylint: disable=protected-access
    depth = repo._lock_depth
    repo._lock_depth += 1

    try:
        if depth > 0:
            yield
        else:
            with repo.lock, repo.state:
                repo._reset()
                yield
                # Graph cache is no longer valid after we release the repo.lock
                repo._reset()
    finally:
        repo._lock_depth = depth


def locked(f):
    @wraps(f)
    def wrapper(repo, *args, **kwargs):
        with lock_repo(repo):
            return f(repo, *args, **kwargs)

    return wrapper


class Repo:
    DVC_DIR = ".dvc"

    from dvc.repo.add import add
    from dvc.repo.brancher import brancher
    from dvc.repo.checkout import checkout
    from dvc.repo.commit import commit
    from dvc.repo.destroy import destroy
    from dvc.repo.diff import diff
    from dvc.repo.fetch import fetch
    from dvc.repo.freeze import freeze, unfreeze
    from dvc.repo.gc import gc
    from dvc.repo.get import get as _get
    from dvc.repo.get_url import get_url as _get_url
    from dvc.repo.imp import imp
    from dvc.repo.imp_url import imp_url
    from dvc.repo.install import install
    from dvc.repo.ls import ls as _ls
    from dvc.repo.move import move
    from dvc.repo.pull import pull
    from dvc.repo.push import push
    from dvc.repo.remove import remove
    from dvc.repo.reproduce import reproduce
    from dvc.repo.run import run
    from dvc.repo.status import status
    from dvc.repo.update import update

    ls = staticmethod(_ls)
    get = staticmethod(_get)
    get_url = staticmethod(_get_url)

    def _get_repo_dirs(
        self,
        root_dir: str = None,
        scm: Base = None,
        rev: str = None,
        uninitialized: bool = False,
    ):
        assert bool(scm) == bool(rev)

        from dvc.scm import SCM
        from dvc.scm.git import Git
        from dvc.utils.fs import makedirs

        dvc_dir = None
        tmp_dir = None
        try:
            tree = scm.get_tree(rev) if isinstance(scm, Git) and rev else None
            root_dir = self.find_root(root_dir, tree)
            dvc_dir = os.path.join(root_dir, self.DVC_DIR)
            tmp_dir = os.path.join(dvc_dir, "tmp")
            makedirs(tmp_dir, exist_ok=True)
        except NotDvcRepoError:
            if not uninitialized:
                raise

            try:
                scm = SCM(root_dir or os.curdir)
            except (SCMError, InvalidGitRepositoryError):
                scm = SCM(os.curdir, no_scm=True)

            assert isinstance(scm, Base)
            root_dir = scm.root_dir

        return root_dir, dvc_dir, tmp_dir

    def __init__(
        self,
        root_dir=None,
        scm=None,
        rev=None,
        subrepos=False,
        uninitialized=False,
    ):
        from dvc.cache import Cache
        from dvc.data_cloud import DataCloud
        from dvc.lock import LockNoop, make_lock
        from dvc.repo.metrics import Metrics
        from dvc.repo.params import Params
        from dvc.repo.plots import Plots
        from dvc.repo.stage import StageLoad
        from dvc.stage.cache import StageCache
        from dvc.state import State, StateNoop
        from dvc.tree.local import LocalTree

        self.root_dir, self.dvc_dir, self.tmp_dir = self._get_repo_dirs(
            root_dir=root_dir, scm=scm, rev=rev, uninitialized=uninitialized
        )

        tree_kwargs = {"use_dvcignore": True, "dvcignore_root": self.root_dir}
        if scm:
            self.tree = scm.get_tree(rev, **tree_kwargs)
        else:
            self.tree = LocalTree(self, {"url": self.root_dir}, **tree_kwargs)

        self.config = Config(self.dvc_dir, tree=self.tree)
        self._scm = scm

        # used by RepoTree to determine if it should traverse subrepos
        self.subrepos = subrepos

        self.cache = Cache(self)
        self.cloud = DataCloud(self)
        self.stage = StageLoad(self)

        if scm or not self.dvc_dir:
            self.lock = LockNoop()
            self.state = StateNoop()
        else:
            self.lock = make_lock(
                os.path.join(self.tmp_dir, "lock"),
                tmp_dir=self.tmp_dir,
                hardlink_lock=self.config["core"].get("hardlink_lock", False),
                friendly=True,
            )

            # NOTE: storing state and link_state in the repository itself to
            # avoid any possible state corruption in 'shared cache dir'
            # scenario.
            self.state = State(self)
            self.stage_cache = StageCache(self)

            self._ignore()

        self.metrics = Metrics(self)
        self.plots = Plots(self)
        self.params = Params(self)
        self.stage_collection_error_handler = None
        self._lock_depth = 0

    @cached_property
    def scm(self):
        from dvc.scm import SCM

        no_scm = self.config["core"].get("no_scm", False)
        return self._scm if self._scm else SCM(self.root_dir, no_scm=no_scm)

    @cached_property
    def experiments(self):
        from dvc.repo.experiments import Experiments

        return Experiments(self)

    @property
    def tree(self) -> "BaseTree":
        return self._tree

    @tree.setter
    def tree(self, tree: "BaseTree"):
        self._tree = tree
        # Our graph cache is no longer valid, as it was based on the previous
        # tree.
        self._reset()

    def __repr__(self):
        return f"{self.__class__.__name__}: '{self.root_dir}'"

    @classmethod
    def find_root(cls, root=None, tree=None) -> str:
        root_dir = os.path.realpath(root or os.curdir)

        if tree:
            if tree.isdir(os.path.join(root_dir, cls.DVC_DIR)):
                return root_dir
            raise NotDvcRepoError(f"'{root}' does not contain DVC directory")

        if not os.path.isdir(root_dir):
            raise NotDvcRepoError(f"directory '{root}' does not exist")

        while True:
            dvc_dir = os.path.join(root_dir, cls.DVC_DIR)
            if os.path.isdir(dvc_dir):
                return root_dir
            if os.path.ismount(root_dir):
                break
            root_dir = os.path.dirname(root_dir)

        message = (
            "you are not inside of a DVC repository "
            "(checked up to mount point '{}')"
        ).format(root_dir)
        raise NotDvcRepoError(message)

    @classmethod
    def find_dvc_dir(cls, root=None):
        root_dir = cls.find_root(root)
        return os.path.join(root_dir, cls.DVC_DIR)

    @staticmethod
    def init(root_dir=os.curdir, no_scm=False, force=False, subdir=False):
        from dvc.repo.init import init

        init(root_dir=root_dir, no_scm=no_scm, force=force, subdir=subdir)
        return Repo(root_dir)

    def unprotect(self, target):
        return self.cache.local.tree.unprotect(PathInfo(target))

    def _ignore(self):
        flist = [
            self.config.files["local"],
            self.tmp_dir,
        ]

        if path_isin(self.cache.local.cache_dir, self.root_dir):
            flist += [self.cache.local.cache_dir]

        self.scm.ignore_list(flist)

    def check_modified_graph(self, new_stages):
        """Generate graph including the new stage to check for errors"""
        # Building graph might be costly for the ones with many DVC-files,
        # so we provide this undocumented hack to skip it. See [1] for
        # more details. The hack can be used as:
        #
        #     repo = Repo(...)
        #     repo._skip_graph_checks = True
        #     repo.add(...)
        #
        # A user should care about not duplicating outs and not adding cycles,
        # otherwise DVC might have an undefined behaviour.
        #
        # [1] https://github.com/iterative/dvc/issues/2671
        if not getattr(self, "_skip_graph_checks", False):
            build_graph(self.stages + new_stages)

    def used_cache(
        self,
        targets=None,
        all_branches=False,
        with_deps=False,
        all_tags=False,
        all_commits=False,
        remote=None,
        force=False,
        jobs=None,
        recursive=False,
        used_run_cache=None,
        ignore_dvc_exceptions=False,
    ):
        """Get the stages related to the given target and collect
        the `info` of its outputs.

        This is useful to know what files from the cache are _in use_
        (namely, a file described as an output on a stage).

        The scope is, by default, the working directory, but you can use
        `all_branches`/`all_tags`/`all_commits` to expand the scope.

        Returns:
            A dictionary with Schemes (representing output's location) mapped
            to items containing the output's `dumpd` names and the output's
            children (if the given output is a directory).
        """
        from dvc.cache import NamedCache

        cache = NamedCache()

        for branch in self.brancher(
            all_branches=all_branches,
            all_tags=all_tags,
            all_commits=all_commits,
        ):
            targets = targets or [None]

            try:
                pairs = cat(
                    self.stage.collect_granular(
                        target, recursive=recursive, with_deps=with_deps
                    )
                    for target in targets
                )

                suffix = f"({branch})" if branch else ""
                for stage, filter_info in pairs:
                    used_cache = stage.get_used_cache(
                        remote=remote,
                        force=force,
                        jobs=jobs,
                        filter_info=filter_info,
                    )
                    cache.update(used_cache, suffix=suffix)
            except DvcException as e:
                logger.error(f"Cache for '{branch}' could not be collected")
                if ignore_dvc_exceptions:
                    logger.debug(e)
                    continue

                raise e

        if used_run_cache:
            used_cache = self.stage_cache.get_used_cache(
                used_run_cache, remote=remote, force=force, jobs=jobs,
            )
            cache.update(used_cache)

        return cache

    @cached_property
    def outs_trie(self):
        return build_outs_trie(self.stages)

    @cached_property
    def graph(self):
        return build_graph(self.stages, self.outs_trie)

    @cached_property
    def outs_graph(self):
        return build_outs_graph(self.graph, self.outs_trie)

    @cached_property
    def pipelines(self):
        return get_pipelines(self.graph)

    @cached_property
    def stages(self):
        """
        Walks down the root directory looking for Dvcfiles,
        skipping the directories that are related with
        any SCM (e.g. `.git`), DVC itself (`.dvc`), or directories
        tracked by DVC (e.g. `dvc add data` would skip `data/`)

        NOTE: For large repos, this could be an expensive
              operation. Consider using some memoization.
        """
        error_handler = self.stage_collection_error_handler
        return self.stage.collect_repo(onerror=error_handler)

    def find_outs_by_path(self, path, outs=None, recursive=False, strict=True):
        if not outs:
            outs = [out for stage in self.stages for out in stage.outs]

        abs_path = os.path.abspath(path)
        path_info = PathInfo(abs_path)
        match = path_info.__eq__ if strict else path_info.isin_or_eq

        def func(out):
            if out.scheme == "local" and match(out.path_info):
                return True

            if recursive and out.path_info.isin(path_info):
                return True

            return False

        matched = list(filter(func, outs))
        if not matched:
            raise OutputNotFoundError(path, self)

        return matched

    def find_out_by_relpath(self, relpath):
        path = os.path.join(self.root_dir, relpath)
        (out,) = self.find_outs_by_path(path)
        return out

    def is_dvc_internal(self, path):
        path_parts = os.path.normpath(path).split(os.path.sep)
        return self.DVC_DIR in path_parts

    @cached_property
    def repo_tree(self):
        return RepoTree(self, subrepos=self.subrepos, fetch=True)

    @contextmanager
    def open_by_relpath(self, path, remote=None, mode="r", encoding=None):
        """Opens a specified resource as a file descriptor"""

        tree = RepoTree(self, stream=True, subrepos=True)
        path = PathInfo(self.root_dir) / path
        try:
            with self.state:
                with tree.open(
                    path, mode=mode, encoding=encoding, remote=remote,
                ) as fobj:
                    yield fobj
        except FileNotFoundError as exc:
            raise FileMissingError(path) from exc
        except IsADirectoryError as exc:
            raise DvcIsADirectoryError(f"'{path}' is a directory") from exc

    def close(self):
        self.scm.close()

    def _reset(self):
        self.__dict__.pop("outs_trie", None)
        self.__dict__.pop("outs_graph", None)
        self.__dict__.pop("graph", None)
        self.__dict__.pop("stages", None)
        self.__dict__.pop("pipelines", None)
