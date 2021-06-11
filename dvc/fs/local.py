import logging
import os
import stat

from dvc.path_info import PathInfo
from dvc.scheme import Schemes
from dvc.system import System
from dvc.utils import is_exec, tmp_fname
from dvc.utils.fs import copy_fobj_to_file, copyfile, makedirs, move, remove

from .base import BaseFileSystem

logger = logging.getLogger(__name__)


class LocalFileSystem(BaseFileSystem):
    scheme = Schemes.LOCAL
    PATH_CLS = PathInfo
    PARAM_CHECKSUM = "md5"
    PARAM_PATH = "path"
    TRAVERSE_PREFIX_LEN = 2

    def __init__(self, **config):
        from fsspec.implementations.local import LocalFileSystem as LocalFS

        super().__init__(**config)
        self.fs = LocalFS()

    @staticmethod
    def open(path_info, mode="r", encoding=None, **kwargs):
        return open(path_info, mode=mode, encoding=encoding)

    def exists(self, path_info) -> bool:
        assert isinstance(path_info, str) or path_info.scheme == "local"
        return self.fs.exists(path_info)

    def isfile(self, path_info) -> bool:
        return os.path.isfile(path_info)

    def isdir(self, path_info) -> bool:
        return os.path.isdir(path_info)

    def iscopy(self, path_info):
        return not (
            System.is_symlink(path_info) or System.is_hardlink(path_info)
        )

    def walk(self, top, topdown=True, onerror=None, **kwargs):
        """Directory fs generator.

        See `os.walk` for the docs. Differences:
        - no support for symlinks
        """
        for root, dirs, files in os.walk(
            top, topdown=topdown, onerror=onerror
        ):
            yield os.path.normpath(root), dirs, files

    def walk_files(self, path_info, **kwargs):
        for root, _, files in self.walk(path_info):
            for file in files:
                # NOTE: os.path.join is ~5.5 times slower
                yield PathInfo(f"{root}{os.sep}{file}")

    def is_empty(self, path_info):
        if self.isfile(path_info) and os.path.getsize(path_info) == 0:
            return True

        if self.isdir(path_info) and len(os.listdir(path_info)) == 0:
            return True

        return False

    def remove(self, path_info):
        if isinstance(path_info, PathInfo):
            if path_info.scheme != "local":
                raise NotImplementedError
        remove(path_info)

    def makedirs(self, path_info):
        makedirs(path_info, exist_ok=True)

    def isexec(self, path_info):
        mode = self.stat(path_info).st_mode
        return is_exec(mode)

    def stat(self, path):
        return os.stat(path)

    def move(self, from_info, to_info):
        if from_info.scheme != "local" or to_info.scheme != "local":
            raise NotImplementedError

        self.makedirs(to_info.parent)
        move(from_info, to_info)

    def copy(self, from_info, to_info):
        tmp_info = to_info.parent / tmp_fname("")
        try:
            copyfile(from_info, tmp_info)
            os.rename(tmp_info, to_info)
        except Exception:
            self.remove(tmp_info)
            raise

    def _upload_fobj(self, fobj, to_info):
        self.makedirs(to_info.parent)
        tmp_info = to_info.parent / tmp_fname("")
        try:
            copy_fobj_to_file(fobj, tmp_info)
            os.rename(tmp_info, to_info)
        except Exception:
            self.remove(tmp_info)
            raise

    @staticmethod
    def symlink(from_info, to_info):
        System.symlink(from_info, to_info)

    @staticmethod
    def is_symlink(path_info):
        return System.is_symlink(path_info)

    def hardlink(self, from_info, to_info):
        # If there are a lot of empty files (which happens a lot in datasets),
        # and the cache type is `hardlink`, we might reach link limits and
        # will get something like: `too many links error`
        #
        # This is because all those empty files will have the same hash
        # (i.e. 68b329da9893e34099c7d8ad5cb9c940), therefore, they will be
        # linked to the same file in the cache.
        #
        # From https://en.wikipedia.org/wiki/Hard_link
        #   * ext4 limits the number of hard links on a file to 65,000
        #   * Windows with NTFS has a limit of 1024 hard links on a file
        #
        # That's why we simply create an empty file rather than a link.
        if self.getsize(from_info) == 0:
            self.open(to_info, "w").close()

            logger.debug("Created empty file: %s -> %s", from_info, to_info)
            return

        System.hardlink(from_info, to_info)

    @staticmethod
    def is_hardlink(path_info):
        return System.is_hardlink(path_info)

    def reflink(self, from_info, to_info):
        System.reflink(from_info, to_info)

    @staticmethod
    def info(path_info):
        st = os.stat(path_info)
        return {
            "size": st.st_size,
            "type": "dir" if stat.S_ISDIR(st.st_mode) else "file",
        }

    def _upload(
        self, from_file, to_info, name=None, no_progress_bar=False, **_kwargs
    ):
        makedirs(to_info.parent, exist_ok=True)

        tmp_file = tmp_fname(to_info)
        copyfile(
            from_file, tmp_file, name=name, no_progress_bar=no_progress_bar
        )
        os.replace(tmp_file, to_info)

    @staticmethod
    def _download(
        from_info, to_file, name=None, no_progress_bar=False, **_kwargs
    ):
        copyfile(
            from_info, to_file, no_progress_bar=no_progress_bar, name=name
        )
