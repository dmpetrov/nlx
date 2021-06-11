from functools import wraps

from funcy import decorator


@decorator
def rwlocked(call, read=None, write=None):
    import sys

    from dvc.dependency.repo import RepoDependency
    from dvc.rwlock import rwlock

    if read is None:
        read = []

    if write is None:
        write = []

    stage = call._args[0]  # pylint: disable=protected-access

    assert stage.repo.lock.is_locked

    def _chain(names):
        return [
            item.path_info
            for attr in names
            for item in getattr(stage, attr)
            # There is no need to lock RepoDependency deps, as there is no
            # corresponding OutputREPO, so we can't even write it.
            if not isinstance(item, RepoDependency)
        ]

    cmd = " ".join(sys.argv)

    with rwlock(stage.repo.tmp_dir, cmd, _chain(read), _chain(write)):
        return call()


def unlocked_repo(f):
    @wraps(f)
    def wrapper(stage, *args, **kwargs):
        stage.repo.lock.unlock()
        stage.repo._reset()  # pylint: disable=protected-access
        try:
            ret = f(stage, *args, **kwargs)
        finally:
            stage.repo.lock.lock()
        return ret

    return wrapper


def relock_repo(f):
    @wraps(f)
    def wrapper(stage, *args, **kwargs):
        stage.repo.lock.lock()
        try:
            ret = f(stage, *args, **kwargs)
        finally:
            stage.repo.lock.unlock()
            stage.repo._reset()  # pylint: disable=protected-access
        return ret

    return wrapper
