import threading
from collections import deque
from contextlib import contextmanager

from funcy import memoize, wrap_with


@contextmanager
def get_connection(conn_func, *args, **kwargs):
    pool = get_pool(conn_func, *args, **kwargs)
    conn = pool.get_connection()
    try:
        yield conn
    except Exception:
        conn.close()
        raise
    else:
        pool.release(conn)


@wrap_with(threading.Lock())
@memoize
def get_pool(conn_func, *args, **kwargs):
    return Pool(conn_func, *args, **kwargs)


def close_pools():
    for pool in get_pool.memory.values():
        pool.close()
    get_pool.memory.clear()


class Pool:
    def __init__(self, conn_func, *conn_args, **conn_kwargs):
        self._conn_func = conn_func
        self._conn_args = conn_args
        self._conn_kwargs = conn_kwargs
        self._conns = deque()
        self._closed = False

    def __del__(self):
        self.close()

    def close(self):
        while self._conns:
            self._conns.pop().close()
        self._closed = True

    def get_connection(self):
        try:
            return self._conns.popleft()
        except IndexError:
            return self._conn_func(*self._conn_args, **self._conn_kwargs)

    def release(self, conn):
        if self._closed:
            conn.close()
        else:
            self._conns.append(conn)
