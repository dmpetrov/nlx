import json
import os
from collections import defaultdict
from contextlib import contextmanager

from voluptuous import Invalid, Optional, Required, Schema

from .exceptions import DvcException
from .utils import relpath

INFO_SCHEMA = {Required("pid"): int, Required("cmd"): str}

SCHEMA = Schema(
    {
        Optional("write", default={}): {str: INFO_SCHEMA},
        Optional("read", default={}): {str: [INFO_SCHEMA]},
    }
)


class RWLockFileCorruptedError(DvcException):
    def __init__(self, path):
        super().__init__(
            "Unable to read RWLock-file '{}'. JSON structure is "
            "corrupted".format(relpath(path))
        )


class RWLockFileFormatError(DvcException):
    def __init__(self, path):
        super().__init__(
            "RWLock-file '{}' format error.".format(relpath(path))
        )


@contextmanager
def _edit_rwlock(lock_dir):
    path = os.path.join(lock_dir, "rwlock")
    try:
        with open(path) as fobj:
            lock = SCHEMA(json.load(fobj))
    except FileNotFoundError:
        lock = SCHEMA({})
    except json.JSONDecodeError as exc:
        raise RWLockFileCorruptedError(path) from exc
    except Invalid as exc:
        raise RWLockFileFormatError(path) from exc
    lock["read"] = defaultdict(list, lock["read"])
    lock["write"] = defaultdict(dict, lock["write"])
    yield lock
    with open(path, "w+") as fobj:
        json.dump(lock, fobj)


def _infos_to_str(infos):
    return "\n".join(
        "  (PID {}): {}".format(info["pid"], info["cmd"]) for info in infos
    )


def _check_blockers(lock, info, *, mode, waiters):
    from .lock import LockError

    for path_info in waiters:
        blockers = [
            blocker
            for path, infos in lock[mode].items()
            if path_info.overlaps(path)
            for blocker in (infos if isinstance(infos, list) else [infos])
            if blocker != info
        ]

        if not blockers:
            continue

        raise LockError(
            "'{path}' is busy, it is being blocked by:\n"
            "{blockers}\n"
            "\n"
            "If there are no processes with such PIDs, you can manually "
            "remove '.dvc/tmp/rwlock' and try again.".format(
                path=str(path_info), blockers=_infos_to_str(blockers)
            )
        )


def _acquire_read(lock, info, path_infos):
    changes = []

    for path_info in path_infos:
        url = path_info.url

        readers = lock["read"][url]
        if info in readers:
            continue

        changes.append(url)
        readers.append(info)

    return changes


def _acquire_write(lock, info, path_infos):
    changes = []

    for path_info in path_infos:
        url = path_info.url

        if lock["write"][url] == info:
            continue

        changes.append(url)
        lock["write"][url] = info

    return changes


def _release_write(lock, info, changes):
    for url in changes:
        assert "write" in lock
        assert url in lock["write"]
        assert lock["write"][url] == info
        del lock["write"][url]
        if not lock["write"]:
            del lock["write"]


def _release_read(lock, info, changes):
    for url in changes:
        assert "read" in lock
        assert url in lock["read"]
        assert info in lock["read"][url]
        lock["read"][url].remove(info)
        if not lock["read"][url]:
            del lock["read"][url]
        if not lock["read"]:
            del lock["read"]


@contextmanager
def rwlock(tmp_dir, cmd, read, write):
    """Create non-thread-safe RWLock for PathInfos.

    Args:
        tmp_dir (str): existing directory where to create the rwlock file.
        cmd (str): command that will be working on these PathInfos.
        read ([PathInfo]): PathInfos that are going to be read.
        write ([PathInfo]): PathInfos that are going to be written.

    Raises:
        LockError: raised if PathInfo we want to read is being written to by
            another command or if PathInfo we want to write is being written
            to or read from by another command.
        RWLockFileCorruptedError: raised if rwlock file is not a valid JSON.
        RWLockFileFormatError: raised if rwlock file is a valid JSON, but
            has internal format that doesn't pass our schema validation.
    """
    info = {"pid": os.getpid(), "cmd": cmd}

    with _edit_rwlock(tmp_dir) as lock:

        _check_blockers(lock, info, mode="write", waiters=read + write)
        _check_blockers(lock, info, mode="read", waiters=write)

        rchanges = _acquire_read(lock, info, read)
        wchanges = _acquire_write(lock, info, write)

    try:
        yield
    finally:
        with _edit_rwlock(tmp_dir) as lock:
            _release_write(lock, info, wchanges)
            _release_read(lock, info, rchanges)
