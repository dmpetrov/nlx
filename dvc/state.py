"""Manages state database used for checksum caching."""

import logging
import os
import re
from abc import ABC, abstractmethod
from urllib.parse import urlencode, urlunparse

from dvc.exceptions import DvcException
from dvc.fs.local import LocalFileSystem
from dvc.hash_info import HashInfo
from dvc.utils import relpath
from dvc.utils.fs import get_inode, get_mtime_and_size, remove

SQLITE_MAX_VARIABLES_NUMBER = 999

logger = logging.getLogger(__name__)


class StateVersionTooNewError(DvcException):
    """Thrown when DVC version is older than the state database version."""

    def __init__(self, dvc_version, expected, actual):
        super().__init__(
            "you are using an old version '{dvc_version}' of DVC that is "
            "using state file version '{expected}', which is not compatible "
            "with the state file version '{actual}', that is used in this "
            "repo. Please upgrade right now!".format(
                dvc_version=dvc_version, expected=expected, actual=actual
            )
        )


class StateBase(ABC):
    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def save(self, path_info, fs, hash_info):
        pass

    @abstractmethod
    def get(self, path_info, fs):
        pass

    @abstractmethod
    def save_link(self, path_info, fs):
        pass


class StateNoop(StateBase):
    def close(self):
        pass

    def save(self, path_info, fs, hash_info):
        pass

    def get(self, path_info, fs):  # pylint: disable=unused-argument
        return None

    def save_link(self, path_info, fs):
        pass


class State(StateBase):  # pylint: disable=too-many-instance-attributes
    def __init__(self, root_dir=None, tmp_dir=None, dvcignore=None):
        from diskcache import Cache

        super().__init__()

        self.tmp_dir = tmp_dir
        self.root_dir = root_dir
        self.dvcignore = dvcignore
        self.fs = LocalFileSystem()

        if not tmp_dir:
            return

        config = {
            "eviction_policy": "least-recently-used",
            "disk_pickle_protocol": 4,
        }
        self.links = Cache(directory=os.path.join(tmp_dir, "links"), **config)
        self.md5s = Cache(directory=os.path.join(tmp_dir, "md5s"), **config)

    def close(self):
        self.md5s.close()
        self.links.close()

    def save(self, path_info, fs, hash_info):
        """Save hash for the specified path info.

        Args:
            path_info (dict): path_info to save hash for.
            hash_info (HashInfo): hash to save.
        """

        if not isinstance(fs, LocalFileSystem):
            return

        assert isinstance(path_info, str) or path_info.scheme == "local"
        assert hash_info
        assert isinstance(hash_info, HashInfo)
        assert os.path.exists(path_info)

        mtime, size = get_mtime_and_size(path_info, self.fs, self.dvcignore)
        inode = get_inode(path_info)

        logger.debug(
            "state save (%s, %s, %s) %s", inode, mtime, size, hash_info.value
        )

        self.md5s[inode] = (mtime, size, hash_info.value)

    def get(self, path_info, fs):
        """Gets the hash for the specified path info. Hash will be
        retrieved from the state database if available.

        Args:
            path_info (dict): path info to get the hash for.

        Returns:
            HashInfo or None: hash for the specified path info or None if it
            doesn't exist in the state database.
        """
        if not isinstance(fs, LocalFileSystem):
            return None

        assert isinstance(path_info, str) or path_info.scheme == "local"
        path = os.fspath(path_info)

        # NOTE: use os.path.exists instead of LocalFileSystem.exists
        # because it uses lexists() and will return True for broken
        # symlinks that we cannot stat() in get_mtime_and_size
        if not os.path.exists(path):
            return None

        mtime, size = get_mtime_and_size(path, self.fs, self.dvcignore)
        inode = get_inode(path)

        value = self.md5s.get(inode)

        if not value or value[0] != mtime or value[1] != size:
            return None

        return HashInfo("md5", value[2], size=int(size))

    def save_link(self, path_info, fs):
        """Adds the specified path to the list of links created by dvc. This
        list is later used on `dvc checkout` to cleanup old links.

        Args:
            path_info (dict): path info to add to the list of links.
        """
        if not isinstance(fs, LocalFileSystem):
            return

        assert isinstance(path_info, str) or path_info.scheme == "local"

        if not self.fs.exists(path_info):
            return

        mtime, _ = get_mtime_and_size(path_info, self.fs, self.dvcignore)
        inode = get_inode(path_info)
        relative_path = relpath(path_info, self.root_dir)

        with self.links as ref:
            ref[relative_path] = (inode, mtime)

    def get_unused_links(self, used, fs):
        """Removes all saved links except the ones that are used.

        Args:
            used (list): list of used links that should not be removed.
        """
        if not isinstance(fs, LocalFileSystem):
            return

        unused = []

        with self.links as ref:
            for relative_path in ref:
                path = os.path.join(self.root_dir, relative_path)

                if path in used or not self.fs.exists(path):
                    continue

                inode = get_inode(path)
                mtime, _ = get_mtime_and_size(path, self.fs, self.dvcignore)

                if ref[relative_path] == (inode, mtime):
                    logger.debug("Removing '%s' as unused link.", path)
                    unused.append(relative_path)

        return unused

    def remove_links(self, unused, fs):
        if not isinstance(fs, LocalFileSystem):
            return

        for path in unused:
            remove(os.path.join(self.root_dir, path))

        with self.links as ref:
            for path in unused:
                del ref[path]


def _connect_sqlite(filename, options):
    # Connect by URI was added in Python 3.4 and sqlite 3.7.7,
    # we ignore options, which should be fine unless repo is on old NFS/CIFS
    import sqlite3

    if sqlite3.sqlite_version_info < (3, 7, 7):
        return sqlite3.connect(filename)

    uri = _build_sqlite_uri(filename, options)
    return sqlite3.connect(uri, uri=True)


def _build_sqlite_uri(filename, options):
    # In the doc mentioned below we only need to replace ? -> %3f and
    # # -> %23, but, if present, we also need to replace % -> %25 first
    # (happens when we are on a weird FS that shows urlencoded filenames
    # instead of proper ones) to not confuse sqlite.
    uri_path = filename.replace("%", "%25")

    # Convert filename to uri according to https://www.sqlite.org/uri.html, 3.1
    uri_path = uri_path.replace("?", "%3f").replace("#", "%23")
    if os.name == "nt":
        uri_path = uri_path.replace("\\", "/")
        uri_path = re.sub(r"^([a-z]:)", "/\\1", uri_path, flags=re.I)
    uri_path = re.sub(r"/+", "/", uri_path)

    # Empty netloc, params and fragment
    return urlunparse(("file", "", uri_path, "", urlencode(options), ""))
