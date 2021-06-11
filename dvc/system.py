import errno
import logging
import os
import platform
import sys

from dvc.exceptions import DvcException

logger = logging.getLogger(__name__)

if (
    sys.platform == "win32"
    and sys.version_info < (3, 8)
    and sys.getwindowsversion() >= (6, 2)
):
    try:
        import speedcopy

        speedcopy.patch_copyfile()
    except ImportError:
        pass


class System:
    @staticmethod
    def is_unix():
        return os.name != "nt"

    @staticmethod
    def hardlink(source, link_name):
        try:
            os.link(source, link_name)
        except OSError as exc:
            raise DvcException("failed to link") from exc

    @staticmethod
    def symlink(source, link_name):
        try:
            os.symlink(source, link_name)
        except OSError as exc:
            raise DvcException("failed to symlink") from exc

    @staticmethod
    def _reflink_darwin(src, dst):
        import ctypes

        LIBC = "libc.dylib"
        LIBC_FALLBACK = "/usr/lib/libSystem.dylib"
        try:
            clib = ctypes.CDLL(LIBC)
        except OSError as exc:
            logger.debug(
                "unable to access '{}' (errno '{}'). "
                "Falling back to '{}'.".format(LIBC, exc.errno, LIBC_FALLBACK)
            )
            if exc.errno != errno.ENOENT:
                raise
            # NOTE: trying to bypass System Integrity Protection (SIP)
            clib = ctypes.CDLL(LIBC_FALLBACK)

        if not hasattr(clib, "clonefile"):
            return -1

        clonefile = clib.clonefile
        clonefile.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int]
        clonefile.restype = ctypes.c_int

        return clonefile(
            ctypes.c_char_p(os.fsencode(src)),
            ctypes.c_char_p(os.fsencode(dst)),
            ctypes.c_int(0),
        )

    @staticmethod
    def _reflink_windows(_src, _dst):
        return -1

    @staticmethod
    def _reflink_linux(src, dst):
        import fcntl

        FICLONE = 0x40049409

        try:
            ret = 255
            with open(src) as s, open(dst, "w+") as d:
                ret = fcntl.ioctl(d.fileno(), FICLONE, s.fileno())
        finally:
            if ret != 0:
                os.unlink(dst)

        return ret

    @staticmethod
    def reflink(source, link_name):
        from dvc.utils.fs import umask

        source, link_name = os.fspath(source), os.fspath(link_name)

        system = platform.system()
        try:
            if system == "Windows":
                ret = System._reflink_windows(source, link_name)
            elif system == "Darwin":
                ret = System._reflink_darwin(source, link_name)
            elif system == "Linux":
                ret = System._reflink_linux(source, link_name)
            else:
                ret = -1
        except OSError:
            ret = -1

        if ret != 0:
            raise DvcException("reflink is not supported")

        # NOTE: reflink has a new inode, but has the same mode as the src,
        # so we need to chmod it to look like a normal copy.
        os.chmod(link_name, 0o666 & ~umask)

    @staticmethod
    def _getdirinfo(path):
        from collections import namedtuple

        from win32file import (  # pylint: disable=import-error
            FILE_FLAG_BACKUP_SEMANTICS,
            FILE_FLAG_OPEN_REPARSE_POINT,
            FILE_SHARE_READ,
            OPEN_EXISTING,
            CreateFileW,
            GetFileInformationByHandle,
        )

        # NOTE: use FILE_FLAG_OPEN_REPARSE_POINT to open symlink itself and not
        # the target See https://docs.microsoft.com/en-us/windows/desktop/api/
        # fileapi/nf-fileapi-createfilew#symbolic-link-behavior
        flags = FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT

        hfile = CreateFileW(
            path, 0, FILE_SHARE_READ, None, OPEN_EXISTING, flags, None
        )

        # See BY_HANDLE_FILE_INFORMATION structure from fileapi.h
        Info = namedtuple(
            "BY_HANDLE_FILE_INFORMATION",
            [
                "dwFileAttributes",
                "ftCreationTime",
                "ftLastAccessTime",
                "ftLastWriteTime",
                "dwVolumeSerialNumber",
                "nFileSizeHigh",
                "nFileSizeLow",
                "nNumberOfLinks",
                "nFileIndexHigh",
                "nFileIndexLow",
            ],
        )

        return Info(*GetFileInformationByHandle(hfile))

    @staticmethod
    def inode(path):
        path = os.fspath(path)

        if System.is_unix():
            import ctypes

            inode = os.lstat(path).st_ino
            # NOTE: See https://bugs.python.org/issue29619 and
            # https://stackoverflow.com/questions/34643289/
            # pythons-os-stat-is-returning-wrong-inode-value
            inode = ctypes.c_ulong(inode).value
        else:
            # getdirinfo from ntfsutils works on both files and dirs
            info = System._getdirinfo(path)
            inode = abs(
                hash(
                    (
                        info.dwVolumeSerialNumber,
                        info.nFileIndexHigh,
                        info.nFileIndexLow,
                    )
                )
            )
        assert inode >= 0
        assert inode < 2 ** 64
        return inode

    @staticmethod
    def is_symlink(path):
        path = os.fspath(path)

        if System.is_unix():
            return os.path.islink(path)

        # https://docs.microsoft.com/en-us/windows/desktop/fileio/
        # file-attribute-constants
        from winnt import (  # pylint: disable=import-error
            FILE_ATTRIBUTE_REPARSE_POINT,
        )

        if os.path.lexists(path):
            info = System._getdirinfo(path)
            return info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT
        return False

    @staticmethod
    def is_hardlink(path):
        path = os.fspath(path)

        if System.is_unix():
            return os.stat(path).st_nlink > 1

        info = System._getdirinfo(path)
        return info.nNumberOfLinks > 1
