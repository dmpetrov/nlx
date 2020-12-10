import logging
import os
from copy import copy
from typing import Type
from urllib.parse import urlparse

from voluptuous import Any

import dvc.prompt as prompt
from dvc.cache import NamedCache
from dvc.exceptions import (
    CheckoutError,
    CollectCacheError,
    DvcException,
    MergeError,
    RemoteCacheRequiredError,
)
from dvc.hash_info import HashInfo

from ..tree.base import BaseTree

logger = logging.getLogger(__name__)


class OutputDoesNotExistError(DvcException):
    def __init__(self, path):
        msg = f"output '{path}' does not exist"
        super().__init__(msg)


class OutputIsNotFileOrDirError(DvcException):
    def __init__(self, path):
        msg = f"output '{path}' is not a file or directory"
        super().__init__(msg)


class OutputAlreadyTrackedError(DvcException):
    def __init__(self, path):
        msg = f""" output '{path}' is already tracked by SCM (e.g. Git).
    You can remove it from Git, then add to DVC.
        To stop tracking from Git:
            git rm -r --cached '{path}'
            git commit -m "stop tracking {path}" """
        super().__init__(msg)


class OutputIsStageFileError(DvcException):
    def __init__(self, path):
        super().__init__(f"Stage file '{path}' cannot be an output.")


class OutputIsIgnoredError(DvcException):
    def __init__(self, match):
        lines = "\n".join(match.patterns)
        super().__init__(f"Path '{match.file}' is ignored by\n{lines}")


class OutputStoreCacheIncompatible(DvcException):
    def __init__(self, path):
        msg = f"""Incompatible options for output '{path}'.
    The .dvc file specifies an output with both `cache == False` and
    `store == True` which is not possible to satisfy.
    """
        super().__init__(msg)


class BaseOutput:
    IS_DEPENDENCY = False

    TREE_CLS = BaseTree

    PARAM_PATH = "path"
    PARAM_CACHE = "cache"
    PARAM_CHECKPOINT = "checkpoint"
    PARAM_METRIC = "metric"
    PARAM_METRIC_TYPE = "type"
    PARAM_METRIC_XPATH = "xpath"
    PARAM_PLOT = "plot"
    PARAM_PLOT_TEMPLATE = "template"
    PARAM_PLOT_X = "x"
    PARAM_PLOT_Y = "y"
    PARAM_PLOT_X_LABEL = "x_label"
    PARAM_PLOT_Y_LABEL = "y_label"
    PARAM_PLOT_TITLE = "title"
    PARAM_PLOT_HEADER = "header"
    PARAM_PERSIST = "persist"
    PARAM_DESC = "desc"
    PARAM_STORE = "store"

    METRIC_SCHEMA = Any(
        None,
        bool,
        {
            PARAM_METRIC_TYPE: Any(str, None),
            PARAM_METRIC_XPATH: Any(str, None),
        },
    )

    DoesNotExistError = OutputDoesNotExistError  # type: Type[DvcException]
    IsNotFileOrDirError = OutputIsNotFileOrDirError  # type: Type[DvcException]
    IsStageFileError = OutputIsStageFileError  # type: Type[DvcException]
    IsIgnoredError = OutputIsIgnoredError  # type: Type[DvcException]

    sep = "/"

    def __init__(
        self,
        stage,
        path,
        info=None,
        tree=None,
        cache=True,
        metric=False,
        plot=False,
        persist=False,
        checkpoint=False,
        desc=None,
        store=True,
    ):
        self._validate_output_path(path, stage)
        # This output (and dependency) objects have too many paths/urls
        # here is a list and comments:
        #
        #   .def_path - path from definition in stage file
        #   .path_info - PathInfo/URLInfo structured resolved path
        #   .fspath - local only, resolved
        #   .__str__ - for presentation purposes, def_path/relpath
        #
        # By resolved path, which contains actual location,
        # should be absolute and don't contain remote:// refs.
        self.stage = stage
        self.repo = stage.repo if stage else None
        self.def_path = path
        self.hash_info = HashInfo.from_dict(info)
        if tree:
            self.tree = tree
        else:
            self.tree = self.TREE_CLS(self.repo, {})
        self.use_cache = False if self.IS_DEPENDENCY else cache
        self.metric = False if self.IS_DEPENDENCY else metric
        self.plot = False if self.IS_DEPENDENCY else plot
        self.store = True if self.IS_DEPENDENCY else store

        self.persist = persist
        self.checkpoint = checkpoint
        self.desc = desc

        self.path_info = self._parse_path(tree, path)
        if self.use_cache and self.cache is None:
            raise RemoteCacheRequiredError(self.path_info)

    def _parse_path(self, tree, path):
        if tree:
            parsed = urlparse(path)
            return tree.path_info / parsed.path.lstrip("/")
        return self.TREE_CLS.PATH_CLS(path)

    def __repr__(self):
        return "{class_name}: '{def_path}'".format(
            class_name=type(self).__name__, def_path=self.def_path
        )

    def __str__(self):
        return self.def_path

    @property
    def scheme(self):
        return self.TREE_CLS.scheme

    @property
    def is_in_repo(self):
        return False

    @property
    def use_scm_ignore(self):
        if not self.is_in_repo:
            return False

        return self.use_cache or self.stage.is_repo_import

    @property
    def cache(self):
        return getattr(self.repo.cache, self.scheme)

    @property
    def dir_cache(self):
        return self.cache.get_dir_cache(self.hash_info)

    @classmethod
    def supported(cls, url):
        return cls.TREE_CLS.supported(url)

    @property
    def cache_path(self):
        return self.cache.tree.hash_to_path_info(self.hash_info.value).url

    def get_hash(self):
        if not self.use_cache:
            return self.tree.get_hash(self.path_info)
        return self.cache.get_hash(self.tree, self.path_info)

    @property
    def is_dir_checksum(self):
        return self.hash_info.isdir

    @property
    def exists(self):
        return self.tree.exists(self.path_info)

    def changed_checksum(self):
        return self.hash_info != self.get_hash()

    def changed_cache(self, filter_info=None):
        if not self.use_cache or not self.hash_info:
            return True

        return self.cache.changed_cache(
            self.hash_info, filter_info=filter_info
        )

    def workspace_status(self):
        if not self.exists:
            return {str(self): "deleted"}

        if self.changed_checksum():
            return {str(self): "modified"}

        if not self.hash_info:
            return {str(self): "new"}

        return {}

    def status(self):
        if self.hash_info and self.use_cache and self.changed_cache():
            return {str(self): "not in cache"}

        return self.workspace_status()

    def changed(self):
        status = self.status()
        logger.debug(str(status))
        return bool(status)

    @property
    def is_empty(self):
        return self.tree.is_empty(self.path_info)

    def isdir(self):
        return self.tree.isdir(self.path_info)

    def isfile(self):
        return self.tree.isfile(self.path_info)

    # pylint: disable=no-member

    def ignore(self):
        if not self.use_scm_ignore:
            return

        if self.repo.scm.is_tracked(self.fspath):
            raise OutputAlreadyTrackedError(self)

        self.repo.scm.ignore(self.fspath)

    def ignore_remove(self):
        if not self.use_scm_ignore:
            return

        self.repo.scm.ignore_remove(self.fspath)

    # pylint: enable=no-member

    def save(self):
        if not self.exists:
            raise self.DoesNotExistError(self)

        if not self.isfile and not self.isdir:
            raise self.IsNotFileOrDirError(self)

        if self.is_empty:
            logger.warning(f"'{self}' is empty.")

        self.ignore()

        if self.metric or self.plot:
            self.verify_metric()

        if not self.use_cache:
            self.hash_info = self.get_hash()
            if not self.IS_DEPENDENCY:
                logger.debug(
                    "Output '%s' doesn't use cache. Skipping saving.", self
                )
            return

        assert not self.IS_DEPENDENCY

        if not self.changed():
            logger.debug("Output '%s' didn't change. Skipping saving.", self)
            return

        self.hash_info = self.get_hash()

    def commit(self):
        if not self.exists:
            raise self.DoesNotExistError(self)

        assert self.hash_info
        if self.use_cache:
            self.cache.save(self.path_info, self.cache.tree, self.hash_info)

    def dumpd(self):
        ret = copy(self.hash_info.to_dict())
        ret[self.PARAM_PATH] = self.def_path

        if self.IS_DEPENDENCY:
            return ret

        if self.desc:
            ret[self.PARAM_DESC] = self.desc

        if not self.use_cache:
            ret[self.PARAM_CACHE] = self.use_cache

        if isinstance(self.metric, dict):
            if (
                self.PARAM_METRIC_XPATH in self.metric
                and not self.metric[self.PARAM_METRIC_XPATH]
            ):
                del self.metric[self.PARAM_METRIC_XPATH]

        if self.metric:
            ret[self.PARAM_METRIC] = self.metric

        if self.plot:
            ret[self.PARAM_PLOT] = self.plot

        if self.persist:
            ret[self.PARAM_PERSIST] = self.persist

        if self.checkpoint:
            ret[self.PARAM_CHECKPOINT] = self.checkpoint

        if self.store == self.stage.is_repo_import:
            ret[self.PARAM_STORE] = self.store

        return ret

    def verify_metric(self):
        raise DvcException(f"verify metric is not supported for {self.scheme}")

    def download(self, to):
        self.tree.download(self.path_info, to.path_info)

    def checkout(
        self,
        force=False,
        progress_callback=None,
        relink=False,
        filter_info=None,
        allow_missing=False,
        **kwargs,
    ):
        if not self.use_cache:
            if progress_callback:
                progress_callback(
                    str(self.path_info), self.get_files_number(filter_info)
                )
            return None

        try:
            return self.cache.checkout(
                self.path_info,
                self.hash_info,
                force=force,
                progress_callback=progress_callback,
                relink=relink,
                filter_info=filter_info,
                **kwargs,
            )
        except CheckoutError:
            if allow_missing or self.checkpoint:
                return None
            raise

    def remove(self, ignore_remove=False):
        self.tree.remove(self.path_info)
        if self.scheme != "local":
            return

        if ignore_remove:
            self.ignore_remove()

    def move(self, out):
        # pylint: disable=no-member
        if self.scheme == "local" and self.use_scm_ignore:
            self.repo.scm.ignore_remove(self.fspath)

        self.tree.move(self.path_info, out.path_info)
        self.def_path = out.def_path
        self.path_info = out.path_info
        self.save()
        self.commit()

        if self.scheme == "local" and self.use_scm_ignore:
            self.repo.scm.ignore(self.fspath)

    def get_files_number(self, filter_info=None):
        if not self.use_cache:
            return 0

        return self.cache.get_files_number(
            self.path_info, self.hash_info, filter_info
        )

    def unprotect(self):
        if self.exists:
            self.tree.unprotect(self.path_info)

    def get_dir_cache(self, **kwargs):
        if not self.is_dir_checksum:
            raise DvcException("cannot get dir cache for file checksum")
        if self.cache.changed_cache_file(self.hash_info):
            self.repo.cloud.pull(
                NamedCache.make("local", self.hash_info.value, str(self)),
                show_checksums=False,
                **kwargs,
            )
            self.hash_info.dir_info = None
        if not self.hash_info.dir_info:
            self.cache.set_dir_info(self.hash_info)
        return self.dir_cache

    def collect_used_dir_cache(
        self, remote=None, force=False, jobs=None, filter_info=None
    ):
        """Get a list of `info`s related to the given directory.

        - Pull the directory entry from the remote cache if it was changed.

        Example:

            Given the following commands:

            $ echo "foo" > directory/foo
            $ echo "bar" > directory/bar
            $ dvc add directory

            It will return a NamedCache like:

            nc = NamedCache()
            nc.add(self.scheme, 'c157a79031e1', 'directory/foo')
            nc.add(self.scheme, 'd3b07384d113', 'directory/bar')
        """

        cache = NamedCache()

        try:
            self.get_dir_cache(jobs=jobs, remote=remote)
        except DvcException:
            logger.debug(f"failed to pull cache for '{self}'")

        if self.cache.changed_cache_file(self.hash_info):
            msg = (
                "Missing cache for directory '{}'. "
                "Cache for files inside will be lost. "
                "Would you like to continue? Use '-f' to force."
            )
            if not force and not prompt.confirm(msg.format(self.path_info)):
                raise CollectCacheError(
                    "unable to fully collect used cache"
                    " without cache for directory '{}'".format(self)
                )
            else:
                return cache

        path = str(self.path_info)
        filter_path = str(filter_info) if filter_info else None
        is_win = os.name == "nt"
        for entry_relpath, entry_hash_info in self.dir_cache.items():
            if is_win:
                entry_relpath = entry_relpath.replace("/", os.sep)
            entry_path = os.path.join(path, entry_relpath)
            if (
                not filter_path
                or entry_path == filter_path
                or entry_path.startswith(filter_path + os.sep)
            ):
                cache.add(self.scheme, entry_hash_info.value, entry_path)

        return cache

    def get_used_cache(self, **kwargs):
        """Get a dumpd of the given `out`, with an entry including the branch.

        The `used_cache` of an output is no more than its `info`.

        In case that the given output is a directory, it will also
        include the `info` of its files.
        """

        if not self.use_cache:
            if self.store:
                raise OutputStoreCacheIncompatible(self)
            return NamedCache()

        if not self.store:
            cache = NamedCache()
            if not self.stage.is_repo_import:
                return cache
            # If this out isn't backed up but it is an import, then it's an
            # external dependancy and can be retrieved from its source
            (dep,) = self.stage.deps
            cache.external[dep.repo_pair].add(dep.def_path)
            return cache

        if not self.hash_info:
            msg = (
                "Output '{}'({}) is missing version info. "
                "Cache for it will not be collected. "
                "Use `dvc repro` to get your pipeline up to date.".format(
                    self, self.stage
                )
            )
            if self.exists:
                msg += (
                    "\n"
                    "You can also use `dvc commit {stage.addressing}` "
                    "to associate existing '{out}' with {stage}.".format(
                        out=self, stage=self.stage
                    )
                )
            logger.warning(msg)
            return NamedCache()

        ret = NamedCache.make(self.scheme, self.hash_info.value, str(self))

        if not self.is_dir_checksum:
            return ret

        ret.add_child_cache(
            self.hash_info.value, self.collect_used_dir_cache(**kwargs),
        )

        return ret

    @classmethod
    def _validate_output_path(cls, path, stage=None):
        from dvc.dvcfile import is_valid_filename

        if is_valid_filename(path):
            raise cls.IsStageFileError(path)

        if stage:
            check = stage.repo.tree.dvcignore.check_ignore(path)
            if check.match:
                raise cls.IsIgnoredError(check)

    def _check_can_merge(self, out):
        if self.scheme != out.scheme:
            raise MergeError("unable to auto-merge outputs of different types")

        my = self.dumpd()
        other = out.dumpd()

        ignored = [
            self.tree.PARAM_CHECKSUM,
            HashInfo.PARAM_SIZE,
            HashInfo.PARAM_NFILES,
        ]

        for opt in ignored:
            my.pop(opt, None)
            other.pop(opt, None)

        if my != other:
            raise MergeError(
                "unable to auto-merge outputs with different options"
            )

        if not out.is_dir_checksum:
            raise MergeError(
                "unable to auto-merge outputs that are not directories"
            )

    def merge(self, ancestor, other):
        assert other

        if ancestor:
            self._check_can_merge(ancestor)
            ancestor_info = ancestor.hash_info
        else:
            ancestor_info = None

        self._check_can_merge(self)
        self._check_can_merge(other)

        self.hash_info = self.cache.merge(
            ancestor_info, self.hash_info, other.hash_info
        )
