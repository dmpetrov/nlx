import logging
import os
import stat

from funcy import cached_property
from shortuuid import uuid

from dvc.hash_info import HashInfo
from dvc.objects.errors import ObjectFormatError
from dvc.path_info import PathInfo
from dvc.progress import Tqdm
from dvc.utils import relpath
from dvc.utils.fs import copyfile, remove, umask, walk_files

from .base import ObjectDB

logger = logging.getLogger(__name__)


class LocalObjectDB(ObjectDB):
    DEFAULT_CACHE_TYPES = ["reflink", "copy"]
    CACHE_MODE = 0o444
    UNPACKED_DIR_SUFFIX = ".unpacked"

    def __init__(self, fs, path_info, **config):
        super().__init__(fs, path_info, **config)
        self.cache_dir = path_info.fspath

        shared = config.get("shared")
        if shared:
            self._file_mode = 0o664
            self._dir_mode = 0o2775
        else:
            self._file_mode = 0o666 & ~umask
            self._dir_mode = 0o777 & ~umask

    @property
    def cache_dir(self):
        return self.path_info.fspath if self.path_info else None

    @cache_dir.setter
    def cache_dir(self, value):
        self.path_info = PathInfo(value) if value else None

    @cached_property
    def cache_path(self):
        return os.path.abspath(self.cache_dir)

    def move(self, from_info, to_info):
        super().move(from_info, to_info)
        os.chmod(to_info, self._file_mode)

    def makedirs(self, path_info):
        from dvc.utils.fs import makedirs

        makedirs(path_info, exist_ok=True, mode=self._dir_mode)

    def hash_to_path(self, hash_):
        # NOTE: `self.cache_path` is already normalized so we can simply use
        # `os.sep` instead of `os.path.join`. This results in this helper
        # being ~5.5 times faster.
        return f"{self.cache_path}{os.sep}{hash_[0:2]}{os.sep}{hash_[2:]}"

    def hashes_exist(
        self, hashes, jobs=None, name=None
    ):  # pylint: disable=unused-argument
        ret = []

        for hash_ in Tqdm(
            hashes,
            unit="file",
            desc="Querying " + ("cache in " + name if name else "local cache"),
        ):
            hash_info = HashInfo(self.fs.PARAM_CHECKSUM, hash_)
            try:
                self.check(hash_info)
                ret.append(hash_)
            except (FileNotFoundError, ObjectFormatError):
                pass

        return ret

    def _list_paths(self, prefix=None, progress_callback=None):
        assert self.path_info is not None
        if prefix:
            path_info = self.path_info / prefix[:2]
            if not self.fs.exists(path_info):
                return
        else:
            path_info = self.path_info
        # NOTE: use utils.fs walk_files since fs.walk_files will not follow
        # symlinks
        if progress_callback:
            for path in walk_files(path_info):
                progress_callback()
                yield path
        else:
            yield from walk_files(path_info)

    def _remove_unpacked_dir(self, hash_):
        info = self.hash_to_path_info(hash_)
        path_info = info.with_name(info.name + self.UNPACKED_DIR_SUFFIX)
        self.fs.remove(path_info)

    def _unprotect_file(self, path):
        if self.fs.is_symlink(path) or self.fs.is_hardlink(path):
            logger.debug("Unprotecting '%s'", path)
            tmp = os.path.join(os.path.dirname(path), "." + uuid())

            # The operations order is important here - if some application
            # would access the file during the process of copyfile then it
            # would get only the part of file. So, at first, the file should be
            # copied with the temporary name, and then original file should be
            # replaced by new.
            copyfile(path, tmp, name=f"Unprotecting '{relpath(path)}'")
            remove(path)
            os.rename(tmp, path)

        else:
            logger.debug(
                "Skipping copying for '%s', since it is not "
                "a symlink or a hardlink.",
                path,
            )

        os.chmod(path, self._file_mode)

    def _unprotect_dir(self, path):
        for fname in self.fs.walk_files(path):
            self._unprotect_file(fname)

    def unprotect(self, path_info):
        if not os.path.exists(path_info):
            from dvc.exceptions import DvcException

            raise DvcException(
                f"can't unprotect non-existing data '{path_info}'"
            )

        if os.path.isdir(path_info):
            self._unprotect_dir(path_info)
        else:
            self._unprotect_file(path_info)

    def protect(self, path_info):
        try:
            os.chmod(path_info, self.CACHE_MODE)
        except OSError:
            # NOTE: not being able to protect cache file is not fatal, it
            # might happen on funky filesystems (e.g. Samba, see #5255),
            # read-only filesystems or in a shared cache scenario.
            logger.trace("failed to protect '%s'", path_info, exc_info=True)

    def is_protected(self, path_info):
        try:
            mode = os.stat(path_info).st_mode
        except FileNotFoundError:
            return False

        return stat.S_IMODE(mode) == self.CACHE_MODE

    def set_exec(self, path_info):
        mode = os.stat(path_info).st_mode | stat.S_IEXEC
        try:
            os.chmod(path_info, mode)
        except OSError:
            logger.trace(
                "failed to chmod '%s' '%s'",
                oct(mode),
                path_info,
                exc_info=True,
            )
