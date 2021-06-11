import logging
import os
import sqlite3
import threading

from funcy import lchunks

from dvc.state import _connect_sqlite
from dvc.utils.fs import makedirs

logger = logging.getLogger(__name__)


class RemoteIndexNoop:
    """No-op class for remotes which are not indexed (i.e. local)."""

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        pass

    def __exit__(self, typ, value, tbck):
        pass

    def __iter__(self):
        return iter([])

    def __contains__(self, hash_):
        return False

    @staticmethod
    def hashes():
        return []

    @staticmethod
    def dir_hashes():
        return []

    def load(self):
        pass

    def dump(self):
        pass

    def clear(self):
        pass

    def update(self, *args):
        pass

    @staticmethod
    def intersection(*args):
        return []


class RemoteIndex:
    """Class for indexing remote hashes in a sqlite3 database.

    Args:
        repo: repo for this remote index.
        name: name for this index. Index db will be loaded from and saved to
            ``.dvc/tmp/index/{name}.idx``.
        dir_suffix: suffix used for naming directory hashes
    """

    INDEX_SUFFIX = ".idx"
    VERSION = 1
    INDEX_TABLE = "remote_index"
    INDEX_TABLE_LAYOUT = "checksum TEXT PRIMARY KEY, dir INTEGER NOT NULL"
    INDEX_DIR = "index"

    def __init__(self, tmp_dir, name, dir_suffix=".dir"):
        self.path = os.path.join(
            tmp_dir, self.INDEX_DIR, f"{name}{self.INDEX_SUFFIX}"
        )

        self.dir_suffix = dir_suffix
        self.database = None
        self.cursor = None
        self.modified = False
        self.lock = threading.Lock()

    def __iter__(self):
        cmd = f"SELECT checksum FROM {self.INDEX_TABLE}"
        for (hash_,) in self._execute(cmd):
            yield hash_

    def __enter__(self):
        self.lock.acquire()
        self.load()

    def __exit__(self, typ, value, tbck):
        self.dump()
        self.lock.release()

    def __contains__(self, hash_):
        cmd = "SELECT checksum FROM {} WHERE checksum = (?)".format(
            self.INDEX_TABLE
        )
        self._execute(cmd, (hash_,))
        return self.cursor.fetchone() is not None

    def hashes(self):
        """Iterate over hashes stored in the index."""
        return iter(self)

    def dir_hashes(self):
        """Iterate over .dir hashes stored in the index."""
        cmd = f"SELECT checksum FROM {self.INDEX_TABLE} WHERE dir = 1"
        for (hash_,) in self._execute(cmd):
            yield hash_

    def is_dir_hash(self, hash_):
        return hash_.endswith(self.dir_suffix)

    def _execute(self, cmd, parameters=()):
        return self.cursor.execute(cmd, parameters)

    def _executemany(self, cmd, seq_of_parameters):
        return self.cursor.executemany(cmd, seq_of_parameters)

    def _prepare_db(self, empty=False):
        if not empty:
            cmd = "PRAGMA user_version;"
            self._execute(cmd)
            ret = self.cursor.fetchall()
            assert len(ret) == 1
            assert len(ret[0]) == 1
            assert isinstance(ret[0][0], int)
            version = ret[0][0]

            if version != self.VERSION:
                logger.error(
                    "Index file version '{}' will be reformatted "
                    "to the current version '{}'.".format(
                        version, self.VERSION
                    )
                )
                cmd = "DROP TABLE IF EXISTS {};"
                self._execute(cmd.format(self.INDEX_TABLE))

        cmd = "CREATE TABLE IF NOT EXISTS {} ({})"
        self._execute(cmd.format(self.INDEX_TABLE, self.INDEX_TABLE_LAYOUT))

        cmd = "PRAGMA user_version = {};"
        self._execute(cmd.format(self.VERSION))

    def load(self):
        """(Re)load this index database."""
        retries = 1
        while True:
            assert self.database is None
            assert self.cursor is None

            empty = not os.path.isfile(self.path)
            makedirs(os.path.dirname(self.path), exist_ok=True)
            self.database = _connect_sqlite(self.path, {"nolock": 1})
            self.cursor = self.database.cursor()

            try:
                self._prepare_db(empty=empty)
                return
            except sqlite3.DatabaseError:
                self.cursor.close()
                self.database.close()
                self.database = None
                self.cursor = None
                if retries > 0:
                    os.unlink(self.path)
                    retries -= 1
                else:
                    raise

    def dump(self):
        """Save this index database."""
        assert self.database is not None

        self.database.commit()
        self.cursor.close()
        self.database.close()
        self.database = None
        self.cursor = None

    def clear(self):
        """Clear this index (to force re-indexing later).

        Changes to the index will not committed until dump() is called.
        """
        cmd = f"DELETE FROM {self.INDEX_TABLE}"
        self._execute(cmd)

    def update(self, dir_hashes, file_hashes):
        """Update this index, adding the specified hashes.

        Changes to the index will not committed until dump() is called.
        """
        cmd = "INSERT OR IGNORE INTO {} (checksum, dir) VALUES (?, ?)".format(
            self.INDEX_TABLE
        )
        self._executemany(cmd, ((hash_, True) for hash_ in dir_hashes))
        self._executemany(cmd, ((hash_, False) for hash_ in file_hashes))

    def intersection(self, hashes):
        """Iterate over values from `hashes` which exist in the index."""
        # sqlite has a compile time limit of 999, see:
        # https://www.sqlite.org/c3ref/c_limit_attached.html#sqlitelimitvariablenumber
        for chunk in lchunks(999, hashes):
            cmd = "SELECT checksum FROM {} WHERE checksum IN ({})".format(
                self.INDEX_TABLE, ",".join("?" for hash_ in chunk)
            )
            for (hash_,) in self._execute(cmd, chunk):
                yield hash_
