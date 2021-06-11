import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial, partialmethod
from multiprocessing import cpu_count
from typing import Any, ClassVar, Dict, FrozenSet, Optional

from dvc.exceptions import DvcException
from dvc.path_info import URLInfo
from dvc.progress import Tqdm
from dvc.utils import tmp_fname
from dvc.utils.fs import makedirs, move
from dvc.utils.http import open_url

logger = logging.getLogger(__name__)


class RemoteCmdError(DvcException):
    def __init__(self, remote, cmd, ret, err):
        super().__init__(
            "{remote} command '{cmd}' finished with non-zero return code"
            " {ret}': {err}".format(remote=remote, cmd=cmd, ret=ret, err=err)
        )


class RemoteActionNotImplemented(DvcException):
    def __init__(self, action, scheme):
        m = f"{action} is not supported for {scheme} remotes"
        super().__init__(m)


class RemoteMissingDepsError(DvcException):
    pass


class BaseFileSystem:
    scheme = "base"
    REQUIRES: ClassVar[Dict[str, str]] = {}
    PATH_CLS = URLInfo  # type: Any
    _JOBS = 4 * cpu_count()

    CHECKSUM_DIR_SUFFIX = ".dir"
    HASH_JOBS = max(1, min(4, cpu_count() // 2))
    LIST_OBJECT_PAGE_SIZE = 1000
    TRAVERSE_WEIGHT_MULTIPLIER = 5
    TRAVERSE_PREFIX_LEN = 3
    TRAVERSE_THRESHOLD_SIZE = 500000
    CAN_TRAVERSE = True

    # Needed for some providers, and http open()
    CHUNK_SIZE = 64 * 1024 * 1024  # 64 MiB

    PARAM_CHECKSUM: ClassVar[Optional[str]] = None
    DETAIL_FIELDS: FrozenSet[str] = frozenset()

    def __init__(self, **kwargs):
        self._check_requires(**kwargs)

        self.jobs = kwargs.get("jobs") or self._JOBS
        self.hash_jobs = kwargs.get("checksum_jobs") or self.HASH_JOBS

    @classmethod
    def _strip_protocol(cls, path: str):
        return path

    @staticmethod
    def _get_kwargs_from_urls(urlpath):  # pylint:disable=unused-argument
        return {}

    @classmethod
    def get_missing_deps(cls):
        import importlib

        missing = []
        for package, module in cls.REQUIRES.items():
            try:
                importlib.import_module(module)
            except ImportError:
                missing.append(package)

        return missing

    def _check_requires(self, **kwargs):
        from ..scheme import Schemes
        from ..utils import format_link
        from ..utils.pkg import PKG

        missing = self.get_missing_deps()
        if not missing:
            return

        url = kwargs.get("url", f"{self.scheme}://")

        scheme = self.scheme
        if scheme == Schemes.WEBDAVS:
            scheme = Schemes.WEBDAV

        by_pkg = {
            "pip": f"pip install 'dvc[{scheme}]'",
            "conda": f"conda install -c conda-forge dvc-{scheme}",
        }

        cmd = by_pkg.get(PKG)
        if cmd:
            link = format_link("https://dvc.org/doc/install")
            hint = (
                f"To install dvc with those dependencies, run:\n"
                "\n"
                f"\t{cmd}\n"
                "\n"
                f"See {link} for more info."
            )
        else:
            link = format_link("https://github.com/iterative/dvc/issues")
            hint = f"Please report this bug to {link}. Thank you!"

        raise RemoteMissingDepsError(
            f"URL '{url}' is supported but requires these missing "
            f"dependencies: {missing}. {hint}"
        )

    def open(self, path_info, mode: str = "r", encoding: str = None, **kwargs):
        if hasattr(self, "_generate_download_url"):
            # pylint:disable=no-member
            func = self._generate_download_url  # type: ignore[attr-defined]
            get_url = partial(func, path_info)
            return open_url(get_url, mode=mode, encoding=encoding, **kwargs)

        raise RemoteActionNotImplemented("open", self.scheme)

    def exists(self, path_info) -> bool:
        raise NotImplementedError

    # pylint: disable=unused-argument

    def isdir(self, path_info):
        """Optional: Overwrite only if the remote has a way to distinguish
        between a directory and a file.
        """
        return False

    def isfile(self, path_info):
        """Optional: Overwrite only if the remote has a way to distinguish
        between a directory and a file.
        """
        return True

    def isexec(self, path_info):
        """Optional: Overwrite only if the remote has a way to distinguish
        between executable and non-executable file.
        """
        return False

    def iscopy(self, path_info):
        """Check if this file is an independent copy."""
        return False  # We can't be sure by default

    def walk(self, top, topdown=True, onerror=None, **kwargs):
        """Return a generator with (root, dirs, files)."""
        raise NotImplementedError

    def walk_files(self, path_info, **kwargs):
        """Return a generator with `PathInfo`s to all the files.

        Optional kwargs:
            prefix (bool): If true `path_info` will be treated as a prefix
                rather than directory path.
        """
        raise NotImplementedError

    def ls(self, path_info, detail=False):
        raise RemoteActionNotImplemented("ls", self.scheme)

    def find(self, path_info, detail=False):
        raise RemoteActionNotImplemented("find", self.scheme)

    def is_empty(self, path_info):
        return False

    def info(self, path_info):
        raise NotImplementedError

    def getsize(self, path_info):
        return self.info(path_info).get("size")

    def remove(self, path_info):
        raise RemoteActionNotImplemented("remove", self.scheme)

    def makedirs(self, path_info, **kwargs):
        """Optional: Implement only if the remote needs to create
        directories before copying/linking/moving data
        """

    def move(self, from_info, to_info):
        self.copy(from_info, to_info)
        self.remove(from_info)

    def copy(self, from_info, to_info):
        raise RemoteActionNotImplemented("copy", self.scheme)

    def symlink(self, from_info, to_info):
        raise RemoteActionNotImplemented("symlink", self.scheme)

    def hardlink(self, from_info, to_info):
        raise RemoteActionNotImplemented("hardlink", self.scheme)

    def reflink(self, from_info, to_info):
        raise RemoteActionNotImplemented("reflink", self.scheme)

    # pylint: enable=unused-argument

    @classmethod
    def is_dir_hash(cls, hash_):
        if not hash_:
            return False
        return hash_.endswith(cls.CHECKSUM_DIR_SUFFIX)

    def upload(self, from_info, to_info, name=None, no_progress_bar=False):
        if not hasattr(self, "_upload"):
            raise RemoteActionNotImplemented("upload", self.scheme)

        if to_info.scheme != self.scheme:
            raise NotImplementedError

        if from_info.scheme != "local":
            raise NotImplementedError

        logger.debug("Uploading '%s' to '%s'", from_info, to_info)

        name = name or from_info.name

        self._upload(  # noqa, pylint: disable=no-member
            from_info.fspath,
            to_info,
            name=name,
            no_progress_bar=no_progress_bar,
        )

    def upload_fobj(self, fobj, to_info, no_progress_bar=False, **pbar_args):
        if not hasattr(self, "_upload_fobj"):
            raise RemoteActionNotImplemented("upload_fobj", self.scheme)

        with Tqdm.wrapattr(
            fobj, "read", disable=no_progress_bar, bytes=True, **pbar_args
        ) as wrapped:
            self._upload_fobj(wrapped, to_info)  # pylint: disable=no-member

    def download(
        self,
        from_info,
        to_info,
        name=None,
        no_progress_bar=False,
        jobs=None,
        _only_file=False,
        **kwargs,
    ):
        if not hasattr(self, "_download"):
            raise RemoteActionNotImplemented("download", self.scheme)

        if from_info.scheme != self.scheme:
            raise NotImplementedError

        if to_info.scheme == self.scheme != "local":
            self.copy(from_info, to_info)
            return 0

        if to_info.scheme != "local":
            raise NotImplementedError

        if not _only_file and self.isdir(from_info):
            return self._download_dir(
                from_info, to_info, name, no_progress_bar, jobs, **kwargs
            )
        return self._download_file(from_info, to_info, name, no_progress_bar)

    download_file = partialmethod(download, _only_file=True)

    def _download_dir(
        self, from_info, to_info, name, no_progress_bar, jobs, **kwargs
    ):
        from_infos = list(self.walk_files(from_info, **kwargs))
        if not from_infos:
            makedirs(to_info, exist_ok=True)
            return None
        to_infos = (
            to_info / info.relative_to(from_info) for info in from_infos
        )

        with Tqdm(
            total=len(from_infos),
            desc="Downloading directory",
            unit="Files",
            disable=no_progress_bar,
        ) as pbar:
            download_files = pbar.wrap_fn(
                partial(self._download_file, name=name, no_progress_bar=True)
            )
            max_workers = jobs or self.jobs
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(download_files, from_info, to_info)
                    for from_info, to_info in zip(from_infos, to_infos)
                ]

                # NOTE: unlike pulling/fetching cache, where we need to
                # download everything we can, not raising an error here might
                # turn very ugly, as the user might think that he has
                # downloaded a complete directory, while having a partial one,
                # which might cause unexpected results in his pipeline.
                for future in as_completed(futures):
                    # NOTE: executor won't let us raise until all futures that
                    # it has are finished, so we need to cancel them ourselves
                    # before re-raising.
                    exc = future.exception()
                    if exc:
                        for entry in futures:
                            entry.cancel()
                        raise exc

    def _download_file(self, from_info, to_info, name, no_progress_bar):
        makedirs(to_info.parent, exist_ok=True)

        logger.debug("Downloading '%s' to '%s'", from_info, to_info)
        name = name or to_info.name

        tmp_file = tmp_fname(to_info)

        self._download(  # noqa, pylint: disable=no-member
            from_info, tmp_file, name=name, no_progress_bar=no_progress_bar
        )

        move(tmp_file, to_info)
