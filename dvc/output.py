import logging
import os
from collections import defaultdict
from copy import copy
from typing import TYPE_CHECKING, Dict, Set, Type
from urllib.parse import urlparse

from funcy import collecting, project
from voluptuous import And, Any, Coerce, Length, Lower, Required, SetTo

from dvc import objects, prompt
from dvc.checkout import checkout
from dvc.exceptions import (
    CheckoutError,
    CollectCacheError,
    DvcException,
    MergeError,
    RemoteCacheRequiredError,
)

from .fs import get_cloud_fs
from .fs.hdfs import HDFSFileSystem
from .fs.local import LocalFileSystem
from .fs.s3 import S3FileSystem
from .hash_info import HashInfo
from .istextfile import istextfile
from .objects import Tree
from .objects import save as osave
from .objects.errors import ObjectFormatError
from .objects.stage import stage as ostage
from .scheme import Schemes
from .utils import relpath
from .utils.fs import path_isin

if TYPE_CHECKING:
    from dvc.dependency.repo import RepoPair

    from .objects.file import HashFile

logger = logging.getLogger(__name__)


CHECKSUM_SCHEMA = Any(
    None,
    And(str, Length(max=0), SetTo(None)),
    And(Any(str, And(int, Coerce(str))), Length(min=3), Lower),
)

# NOTE: currently there are only 3 possible checksum names:
#
#    1) md5 (LOCAL, SSH);
#    2) etag (S3);
#    3) checksum (HDFS);
#
# so when a few types of outputs share the same name, we only need
# specify it once.
CHECKSUMS_SCHEMA = {
    LocalFileSystem.PARAM_CHECKSUM: CHECKSUM_SCHEMA,
    S3FileSystem.PARAM_CHECKSUM: CHECKSUM_SCHEMA,
    HDFSFileSystem.PARAM_CHECKSUM: CHECKSUM_SCHEMA,
}


def _get(stage, path, **kwargs):
    return Output(stage, path, **kwargs)


def loadd_from(stage, d_list):
    ret = []
    for d in d_list:
        p = d.pop(Output.PARAM_PATH)
        cache = d.pop(Output.PARAM_CACHE, True)
        metric = d.pop(Output.PARAM_METRIC, False)
        plot = d.pop(Output.PARAM_PLOT, False)
        persist = d.pop(Output.PARAM_PERSIST, False)
        checkpoint = d.pop(Output.PARAM_CHECKPOINT, False)
        desc = d.pop(Output.PARAM_DESC, False)
        isexec = d.pop(Output.PARAM_ISEXEC, False)
        live = d.pop(Output.PARAM_LIVE, False)
        ret.append(
            _get(
                stage,
                p,
                info=d,
                cache=cache,
                metric=metric,
                plot=plot,
                persist=persist,
                checkpoint=checkpoint,
                desc=desc,
                isexec=isexec,
                live=live,
            )
        )
    return ret


def loads_from(
    stage,
    s_list,
    use_cache=True,
    metric=False,
    plot=False,
    persist=False,
    checkpoint=False,
    isexec=False,
    live=False,
):
    return [
        _get(
            stage,
            s,
            info={},
            cache=use_cache,
            metric=metric,
            plot=plot,
            persist=persist,
            checkpoint=checkpoint,
            isexec=isexec,
            live=live,
        )
        for s in s_list
    ]


def _split_dict(d, keys):
    return project(d, keys), project(d, d.keys() - keys)


def _merge_data(s_list):
    d = defaultdict(dict)
    for key in s_list:
        if isinstance(key, str):
            d[key].update({})
            continue
        if not isinstance(key, dict):
            raise ValueError(f"'{type(key).__name__}' not supported.")

        for k, flags in key.items():
            if not isinstance(flags, dict):
                raise ValueError(
                    f"Expected dict for '{k}', got: '{type(flags).__name__}'"
                )
            d[k].update(flags)
    return d


@collecting
def load_from_pipeline(stage, data, typ="outs"):
    if typ not in (
        stage.PARAM_OUTS,
        stage.PARAM_METRICS,
        stage.PARAM_PLOTS,
        stage.PARAM_LIVE,
    ):
        raise ValueError(f"'{typ}' key is not allowed for pipeline files.")

    metric = typ == stage.PARAM_METRICS
    plot = typ == stage.PARAM_PLOTS
    live = typ == stage.PARAM_LIVE

    if live:
        # `live` is single object
        data = [data]

    d = _merge_data(data)

    for path, flags in d.items():
        plt_d, live_d = {}, {}
        if plot:
            from dvc.schema import PLOT_PROPS

            plt_d, flags = _split_dict(flags, keys=PLOT_PROPS.keys())
        if live:
            from dvc.schema import LIVE_PROPS

            live_d, flags = _split_dict(flags, keys=LIVE_PROPS.keys())
        extra = project(
            flags,
            [
                Output.PARAM_CACHE,
                Output.PARAM_PERSIST,
                Output.PARAM_CHECKPOINT,
            ],
        )

        yield _get(
            stage,
            path,
            info={},
            plot=plt_d or plot,
            metric=metric,
            live=live_d or live,
            **extra,
        )


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
        super().__init__(f"DVC file '{path}' cannot be an output.")


class OutputIsIgnoredError(DvcException):
    def __init__(self, match):
        lines = "\n".join(match.patterns)
        super().__init__(f"Path '{match.file}' is ignored by\n{lines}")


class Output:
    IS_DEPENDENCY = False

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
    PARAM_ISEXEC = "isexec"
    PARAM_LIVE = "live"
    PARAM_LIVE_SUMMARY = "summary"
    PARAM_LIVE_HTML = "html"

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
        cache=True,
        metric=False,
        plot=False,
        persist=False,
        checkpoint=False,
        live=False,
        desc=None,
        isexec=False,
    ):
        self.repo = stage.repo if stage else None

        fs_cls, fs_config, path_info = get_cloud_fs(self.repo, url=path)
        self.fs = fs_cls(**fs_config)

        if (
            self.fs.scheme == "local"
            and stage
            and path_isin(path, stage.repo.root_dir)
        ):
            self.def_path = relpath(path, stage.wdir)
        else:
            self.def_path = path

        self._validate_output_path(path, stage)
        # This output (and dependency) objects have too many paths/urls
        # here is a list and comments:
        #
        #   .def_path - path from definition in DVC file
        #   .path_info - PathInfo/URLInfo structured resolved path
        #   .fspath - local only, resolved
        #   .__str__ - for presentation purposes, def_path/relpath
        #
        # By resolved path, which contains actual location,
        # should be absolute and don't contain remote:// refs.
        self.stage = stage
        self.hash_info = HashInfo.from_dict(info)
        self.use_cache = False if self.IS_DEPENDENCY else cache
        self.metric = False if self.IS_DEPENDENCY else metric
        self.plot = False if self.IS_DEPENDENCY else plot
        self.persist = persist
        self.checkpoint = checkpoint
        self.live = live
        self.desc = desc

        self.path_info = self._parse_path(self.fs, path_info)
        if self.use_cache and self.odb is None:
            raise RemoteCacheRequiredError(self.path_info)

        self.obj = None
        self.isexec = False if self.IS_DEPENDENCY else isexec

    def _parse_path(self, fs, path_info):
        if fs.scheme != "local":
            return path_info

        parsed = urlparse(self.def_path)
        if parsed.scheme != "remote":
            # NOTE: we can path either from command line or .dvc file,
            # so we should expect both posix and windows style paths.
            # PathInfo accepts both, i.e. / works everywhere, \ only on win.
            #
            # FIXME: if we have Windows path containing / or posix one with \
            # then we have #2059 bug and can't really handle that.
            if self.stage and not path_info.is_absolute():
                path_info = self.stage.wdir / path_info

        abs_p = os.path.abspath(os.path.normpath(path_info))
        return fs.PATH_CLS(abs_p)

    def __repr__(self):
        return "{class_name}: '{def_path}'".format(
            class_name=type(self).__name__, def_path=self.def_path
        )

    def __str__(self):
        if self.fs.scheme != "local":
            return self.def_path

        if (
            not self.repo
            or urlparse(self.def_path).scheme == "remote"
            or os.path.isabs(self.def_path)
        ):
            return str(self.def_path)

        cur_dir = os.getcwd()
        if path_isin(cur_dir, self.repo.root_dir):
            return relpath(self.path_info, cur_dir)

        return relpath(self.path_info, self.repo.root_dir)

    @property
    def scheme(self):
        return self.fs.scheme

    @property
    def is_in_repo(self):
        if self.fs.scheme != "local":
            return False

        if urlparse(self.def_path).scheme == "remote":
            return False

        if os.path.isabs(self.def_path):
            return False

        return self.repo and path_isin(
            os.path.realpath(self.path_info), self.repo.root_dir
        )

    @property
    def use_scm_ignore(self):
        if not self.is_in_repo:
            return False

        return self.use_cache or self.stage.is_repo_import

    @property
    def odb(self):
        return getattr(self.repo.odb, self.scheme)

    @property
    def cache_path(self):
        return self.odb.hash_to_path_info(self.hash_info.value).url

    def get_hash(self):
        if not self.use_cache:
            return ostage(
                self.repo.odb.local,
                self.path_info,
                self.fs,
                self.fs.PARAM_CHECKSUM,
                dvcignore=self.dvcignore,
            ).hash_info
        return ostage(
            self.odb,
            self.path_info,
            self.fs,
            self.odb.fs.PARAM_CHECKSUM,
            dvcignore=self.dvcignore,
        ).hash_info

    @property
    def is_dir_checksum(self):
        return self.hash_info.isdir

    def _is_path_dvcignore(self, path) -> bool:
        if not self.IS_DEPENDENCY and self.dvcignore:
            if self.dvcignore.is_ignored(self.fs, path, ignore_subrepos=False):
                return True
        return False

    @property
    def exists(self):
        if self._is_path_dvcignore(self.path_info):
            return False

        return self.fs.exists(self.path_info)

    def changed_checksum(self):
        return self.hash_info != self.get_hash()

    def changed_cache(self, filter_info=None):
        if not self.use_cache or not self.hash_info:
            return True

        obj = self.get_obj(filter_info=filter_info)
        if not obj:
            return True

        try:
            objects.check(self.odb, obj)
            return False
        except (FileNotFoundError, ObjectFormatError):
            return True

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
    def dvcignore(self):
        if self.fs.scheme == "local":
            return self.repo.dvcignore
        return None

    @property
    def is_empty(self):
        return self.fs.is_empty(self.path_info)

    def isdir(self):
        if self._is_path_dvcignore(self.path_info):
            return False
        return self.fs.isdir(self.path_info)

    def isfile(self):
        if self._is_path_dvcignore(self.path_info):
            return False
        return self.fs.isfile(self.path_info)

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

        self.obj = ostage(
            self.odb,
            self.path_info,
            self.fs,
            self.odb.fs.PARAM_CHECKSUM,
            dvcignore=self.dvcignore,
        )
        self.hash_info = self.obj.hash_info
        self.isexec = self.isfile() and self.fs.isexec(self.path_info)

    def set_exec(self):
        if self.isfile() and self.isexec:
            self.odb.set_exec(self.path_info)

    def commit(self, filter_info=None):
        if not self.exists:
            raise self.DoesNotExistError(self)

        assert self.hash_info

        if self.use_cache:
            obj = ostage(
                self.odb,
                filter_info or self.path_info,
                self.fs,
                self.odb.fs.PARAM_CHECKSUM,
                dvcignore=self.dvcignore,
            )
            objects.save(self.odb, obj)
            checkout(
                filter_info or self.path_info,
                self.fs,
                obj,
                self.odb,
                relink=True,
                dvcignore=self.dvcignore,
                state=self.repo.state,
            )
            self.set_exec()

    def dumpd(self):
        ret = copy(self.hash_info.to_dict())

        if self.is_in_repo:
            path = self.path_info.relpath(self.stage.wdir).as_posix()
        else:
            path = self.def_path

        ret[self.PARAM_PATH] = path

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

        if self.isexec:
            ret[self.PARAM_ISEXEC] = self.isexec

        if self.live:
            ret[self.PARAM_LIVE] = self.live

        return ret

    def verify_metric(self):
        if self.fs.scheme != "local":
            raise DvcException(
                f"verify metric is not supported for {self.scheme}"
            )

        if not self.metric or self.plot:
            return

        path = os.fspath(self.path_info)
        if not os.path.exists(path):
            return

        name = "metrics" if self.metric else "plot"
        if os.path.isdir(path):
            msg = "directory '%s' cannot be used as %s."
            logger.debug(msg, str(self.path_info), name)
            return

        if not istextfile(path, self.fs):
            msg = "binary file '{}' cannot be used as {}."
            raise DvcException(msg.format(self.path_info, name))

    def download(self, to, jobs=None):
        self.fs.download(self.path_info, to.path_info, jobs=jobs)

    def get_obj(self, filter_info=None, **kwargs):
        if self.obj:
            obj = self.obj
        elif self.hash_info:
            try:
                obj = objects.load(self.odb, self.hash_info)
            except FileNotFoundError:
                return None
        else:
            return None

        if filter_info and filter_info != self.path_info:
            prefix = filter_info.relative_to(self.path_info).parts
            obj = obj.filter(self.odb, prefix, **kwargs)

        return obj

    def checkout(
        self,
        force=False,
        progress_callback=None,
        relink=False,
        filter_info=None,
        allow_missing=False,
        checkpoint_reset=False,
        **kwargs,
    ):
        if not self.use_cache:
            if progress_callback:
                progress_callback(
                    str(self.path_info), self.get_files_number(filter_info)
                )
            return None

        obj = self.get_obj(filter_info=filter_info)
        if not obj and (filter_info and filter_info != self.path_info):
            # backward compatibility
            return None

        if self.checkpoint and checkpoint_reset:
            if self.exists:
                self.remove()
            return None

        added = not self.exists

        try:
            modified = checkout(
                filter_info or self.path_info,
                self.fs,
                obj,
                self.odb,
                force=force,
                progress_callback=progress_callback,
                relink=relink,
                state=self.repo.state,
                **kwargs,
            )
        except CheckoutError:
            if allow_missing or self.checkpoint:
                return None
            raise
        self.set_exec()
        return added, False if added else modified

    def remove(self, ignore_remove=False):
        self.fs.remove(self.path_info)
        if self.scheme != Schemes.LOCAL:
            return

        if ignore_remove:
            self.ignore_remove()

    def move(self, out):
        # pylint: disable=no-member
        if self.scheme == "local" and self.use_scm_ignore:
            self.repo.scm.ignore_remove(self.fspath)

        self.fs.move(self.path_info, out.path_info)
        self.def_path = out.def_path
        self.path_info = out.path_info
        self.save()
        self.commit()

        if self.scheme == "local" and self.use_scm_ignore:
            self.repo.scm.ignore(self.fspath)

    def transfer(
        self, source, odb=None, jobs=None, update=False, no_progress_bar=False
    ):
        if odb is None:
            odb = self.odb

        cls, config, from_info = get_cloud_fs(self.repo, url=source)
        from_fs = cls(**config)

        # When running import-url --to-remote / add --to-remote/-o ... we
        # assume that it is unlikely that the odb will contain majority of the
        # hashes, so we transfer everything as is (even if that file might
        # already be in the cache) and don't waste an upload to scan the layout
        # of the source location. But when doing update --to-remote, there is
        # a high probability that the odb might contain some of the hashes, so
        # we first calculate all the hashes (but don't transfer anything) and
        # then only update the missing cache files.

        upload = not (update and from_fs.isdir(from_info))
        jobs = jobs or min((from_fs.jobs, odb.fs.jobs))
        obj = ostage(
            odb,
            from_info,
            from_fs,
            "md5",
            upload=upload,
            jobs=jobs,
            no_progress_bar=no_progress_bar,
        )
        osave(odb, obj, jobs=jobs, move=upload)

        self.hash_info = obj.hash_info
        return obj

    def get_files_number(self, filter_info=None):
        if not self.use_cache or not self.hash_info:
            return 0

        if not self.hash_info.isdir:
            return 1

        if not filter_info or filter_info == self.path_info:
            return self.hash_info.nfiles or 0

        obj = self.get_obj(filter_info=filter_info)
        return len(obj) if obj else 0

    def unprotect(self):
        if self.exists:
            self.odb.unprotect(self.path_info)

    def get_dir_cache(self, **kwargs):
        if not self.is_dir_checksum:
            raise DvcException("cannot get dir cache for file checksum")

        obj = self.odb.get(self.hash_info)
        try:
            objects.check(self.odb, obj)
        except (FileNotFoundError, ObjectFormatError):
            self.repo.cloud.pull([obj], show_checksums=False, **kwargs)

        try:
            self.obj = objects.load(self.odb, self.hash_info)
        except (FileNotFoundError, ObjectFormatError):
            self.obj = None

        return self.obj

    def collect_used_dir_cache(
        self, remote=None, force=False, jobs=None, filter_info=None
    ) -> Set["HashFile"]:
        """Fetch dir cache and return used objects for this out."""

        try:
            self.get_dir_cache(jobs=jobs, remote=remote)
        except DvcException:
            logger.debug(f"failed to pull cache for '{self}'")

        try:
            objects.check(self.odb, self.odb.get(self.hash_info))
        except (FileNotFoundError, ObjectFormatError):
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
            return set()

        obj = self.get_obj(filter_info=filter_info, copy=True)
        self._set_obj_names(obj)
        return {obj}

    def get_used_objs(self, **kwargs) -> Set["HashFile"]:
        """Return filtered set of used objects for this out."""

        if not self.use_cache:
            return set()

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
            return set()

        if self.is_dir_checksum:
            return self.collect_used_dir_cache(**kwargs)

        obj = self.get_obj(filter_info=kwargs.get("filter_info"))
        if not obj:
            obj = self.odb.get(self.hash_info)
        self._set_obj_names(obj)

        return {obj}

    def _set_obj_names(self, obj):
        obj.name = str(self)
        if isinstance(obj, Tree):
            for key, entry_obj in obj:
                entry_obj.name = os.path.join(str(self), *key)

    def get_used_external(self, **kwargs) -> Dict["RepoPair", str]:
        if not self.use_cache or not self.stage.is_repo_import:
            return {}

        (dep,) = self.stage.deps
        return {dep.repo_pair: dep.def_path}

    def _validate_output_path(self, path, stage=None):
        from dvc.dvcfile import is_valid_filename

        if is_valid_filename(path):
            raise self.IsStageFileError(path)

        if stage:
            abs_path = os.path.join(stage.wdir, path)
            if self._is_path_dvcignore(abs_path):
                check = stage.repo.dvcignore.check_ignore(abs_path)
                raise self.IsIgnoredError(check)

    def _check_can_merge(self, out):
        if self.scheme != out.scheme:
            raise MergeError("unable to auto-merge outputs of different types")

        my = self.dumpd()
        other = out.dumpd()

        ignored = [
            self.fs.PARAM_CHECKSUM,
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
        from dvc.objects.tree import merge

        assert other

        if ancestor:
            self._check_can_merge(ancestor)
            ancestor_info = ancestor.hash_info
        else:
            ancestor_info = None

        self._check_can_merge(self)
        self._check_can_merge(other)

        self.hash_info = merge(
            self.odb, ancestor_info, self.hash_info, other.hash_info
        )

    @property
    def fspath(self):
        return self.path_info.fspath


ARTIFACT_SCHEMA = {
    **CHECKSUMS_SCHEMA,
    Required(Output.PARAM_PATH): str,
    Output.PARAM_PLOT: bool,
    Output.PARAM_PERSIST: bool,
    Output.PARAM_CHECKPOINT: bool,
    HashInfo.PARAM_SIZE: int,
    HashInfo.PARAM_NFILES: int,
    Output.PARAM_ISEXEC: bool,
}

SCHEMA = {
    **ARTIFACT_SCHEMA,
    Output.PARAM_CACHE: bool,
    Output.PARAM_METRIC: Output.METRIC_SCHEMA,
    Output.PARAM_DESC: str,
}
