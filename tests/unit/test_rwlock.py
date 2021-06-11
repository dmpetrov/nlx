import os

import pytest

from dvc.lock import LockError
from dvc.path_info import PathInfo
from dvc.rwlock import (
    RWLockFileCorruptedError,
    RWLockFileFormatError,
    _edit_rwlock,
    rwlock,
)


def test_rwlock(tmp_path):
    path = os.fspath(tmp_path)
    foo = PathInfo("foo")

    with rwlock(path, "cmd1", [foo], []):
        with pytest.raises(LockError):
            with rwlock(path, "cmd2", [], [foo]):
                pass

    with rwlock(path, "cmd1", [], [foo]):
        with pytest.raises(LockError):
            with rwlock(path, "cmd2", [foo], []):
                pass

    with rwlock(path, "cmd1", [], [foo]):
        with pytest.raises(LockError):
            with rwlock(path, "cmd2", [], [foo]):
                pass


def test_rwlock_reentrant(tmp_path):
    path = os.fspath(tmp_path)
    foo = PathInfo("foo")

    with rwlock(path, "cmd1", [], [foo]):
        with rwlock(path, "cmd1", [], [foo]):
            pass
        with _edit_rwlock(path) as lock:
            assert lock == {
                "read": {},
                "write": {"foo": {"cmd": "cmd1", "pid": os.getpid()}},
            }

    with rwlock(path, "cmd", [foo], []):
        with rwlock(path, "cmd", [foo], []):
            pass
        with _edit_rwlock(path) as lock:
            assert lock == {
                "read": {"foo": [{"cmd": "cmd", "pid": os.getpid()}]},
                "write": {},
            }


def test_rwlock_subdirs(tmp_path):
    path = os.fspath(tmp_path)
    foo = PathInfo("foo")
    subfoo = PathInfo("foo/subfoo")

    with rwlock(path, "cmd1", [foo], []):
        with pytest.raises(LockError, match=r"subfoo(.|\n)*cmd1"):
            with rwlock(path, "cmd2", [], [subfoo]):
                pass

    with rwlock(path, "cmd1", [], [subfoo]):
        with pytest.raises(LockError, match=r"'foo'(.|\n)*cmd1"):
            with rwlock(path, "cmd2", [foo], []):
                pass

    with rwlock(path, "cmd1", [], [subfoo]):
        with pytest.raises(LockError):
            with rwlock(path, "cmd2", [], [foo]):
                pass

    with rwlock(path, "cmd1", [subfoo], []):
        with rwlock(path, "cmd2", [foo], []):
            pass


def test_broken_rwlock(tmp_path):
    dir_path = os.fspath(tmp_path)
    path = tmp_path / "rwlock"

    path.write_text('{"broken": "format"}', encoding="utf-8")
    with pytest.raises(RWLockFileFormatError):
        with _edit_rwlock(dir_path):
            pass

    path.write_text("{broken json", encoding="utf-8")
    with pytest.raises(RWLockFileCorruptedError):
        with _edit_rwlock(dir_path):
            pass
