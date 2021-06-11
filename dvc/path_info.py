# pylint: disable=protected-access
import os
import pathlib
import posixpath
from typing import Callable
from urllib.parse import urlparse

from funcy import cached_property

from dvc.utils import relpath


class _BasePath:
    def overlaps(self, other):
        if isinstance(other, (str, bytes)):
            other = self.__class__(other)
        elif self.__class__ != other.__class__:
            return False
        return self.isin_or_eq(other) or other.isin(self)

    def isin_or_eq(self, other):
        return self == other or self.isin(other)  # pylint: disable=no-member


class PathInfo(pathlib.PurePath, _BasePath):
    # Use __slots__ in PathInfo objects following PurePath implementation.
    # This makes objects smaller and speeds up attribute access.
    # We don't add any fields so it's empty.
    __slots__ = ()
    scheme = "local"

    def __new__(cls, *args):
        # Construct a proper subclass depending on current os
        if cls is PathInfo:
            cls = (  # pylint: disable=self-cls-assignment
                WindowsPathInfo if os.name == "nt" else PosixPathInfo
            )

        return cls._from_parts(args)

    def as_posix(self):
        f = self._flavour  # pylint: disable=no-member
        # Unlike original implementation [1] that uses `str()` we actually need
        # to use `fspath`, because we've overridden `__str__` method to return
        # relative paths, which will break original `as_posix`.
        #
        # [1] https://github.com/python/cpython/blob/v3.7.0/Lib/pathlib.py#L692
        return self.fspath.replace(f.sep, "/")

    def __str__(self):
        path = self.__fspath__()
        return relpath(path)

    def __repr__(self):
        return "{}: '{}'".format(type(self).__name__, self)

    # This permits passing it to file utils directly in Python 3.6+
    def __fspath__(self):
        return pathlib.PurePath.__str__(self)

    @property
    def fspath(self):
        return self.__fspath__()

    url = fspath

    def relpath(self, other):
        return self.__class__(relpath(self, other))

    def isin(self, other):
        if isinstance(other, (str, bytes)):
            other = self.__class__(other)
        elif self.__class__ != other.__class__:
            return False
        # Use cached casefolded parts to compare paths
        n = len(other._cparts)
        return len(self._cparts) > n and self._cparts[:n] == other._cparts

    def relative_to(self, other):  # pylint: disable=arguments-differ
        # pathlib relative_to raises exception when one path is not a direct
        # descendant of the other when os.path.relpath would return abspath.
        # For DVC PathInfo we only need the relpath behavior.
        # See: https://bugs.python.org/issue40358
        try:
            path = super().relative_to(other)
        except ValueError:
            path = relpath(self, other)
        return self.__class__(path)


class WindowsPathInfo(PathInfo, pathlib.PureWindowsPath):
    pass


class PosixPathInfo(PathInfo, pathlib.PurePosixPath):
    pass


class _URLPathInfo(PosixPathInfo):
    def __str__(self):
        return self.__fspath__()

    __unicode__ = __str__


class _URLPathParents:
    def __init__(self, src):
        self.src = src
        self._parents = self.src._path.parents

    def __len__(self):
        return len(self._parents)

    def __getitem__(self, idx):
        return self.src.replace(path=self._parents[idx])

    def __repr__(self):
        return f"<{self.src}.parents>"


class URLInfo(_BasePath):
    DEFAULT_PORTS = {"http": 80, "https": 443, "ssh": 22, "hdfs": 0}

    def __init__(self, url):
        p = urlparse(url)
        assert not p.query and not p.params and not p.fragment
        assert p.password is None
        self._fill_parts(p.scheme, p.hostname, p.username, p.port, p.path)

    @classmethod
    def from_parts(
        cls, scheme=None, host=None, user=None, port=None, path="", netloc=None
    ):
        assert bool(host) ^ bool(netloc)

        if netloc is not None:
            return cls(f"{scheme}://{netloc}{path}")

        obj = cls.__new__(cls)
        obj._fill_parts(scheme, host, user, port, path)
        return obj

    def _fill_parts(self, scheme, host, user, port, path):
        assert scheme != "remote"
        assert isinstance(path, (str, bytes, _URLPathInfo))

        self.scheme, self.host, self.user = scheme, host, user
        self.port = int(port) if port else self.DEFAULT_PORTS.get(self.scheme)

        if isinstance(path, _URLPathInfo):
            self._spath = str(path)
            self._path = path
        else:
            if path and path[0] != "/":
                path = "/" + path
            self._spath = path

    @property
    def _base_parts(self):
        return (self.scheme, self.host, self.user, self.port)

    @property
    def parts(self):
        return self._base_parts + self._path.parts

    def replace(self, path=None):
        return self.from_parts(*self._base_parts, path=path)

    @cached_property
    def url(self):
        return f"{self.scheme}://{self.netloc}{self._spath}"

    def __str__(self):
        return self.url

    def __repr__(self):
        return "{}: '{}'".format(type(self).__name__, self)

    def __eq__(self, other):
        if isinstance(other, (str, bytes)):
            other = self.__class__(other)
        return (
            self.__class__ == other.__class__
            and self._base_parts == other._base_parts
            and self._path == other._path
        )

    def __hash__(self):
        return hash(self.parts)

    def __div__(self, other):
        return self.replace(path=posixpath.join(self._spath, other))

    def joinpath(self, *args):
        return self.replace(path=posixpath.join(self._spath, *args))

    __truediv__ = __div__

    @property
    def path(self):
        return self._spath

    @cached_property
    def _path(self):  # false-positive, pylint: disable=method-hidden
        return _URLPathInfo(self._spath)

    @property
    def name(self):
        return self._path.name

    @cached_property
    def netloc(self):
        netloc = self.host
        if self.user:
            netloc = self.user + "@" + netloc
        if self.port and int(self.port) != self.DEFAULT_PORTS.get(self.scheme):
            netloc += ":" + str(self.port)
        return netloc

    @property
    def bucket(self):
        return self.netloc

    @property
    def parent(self):
        return self.replace(path=self._path.parent)

    @property
    def parents(self):
        return _URLPathParents(self)

    def relative_to(self, other):
        if isinstance(other, (str, bytes)):
            other = self.__class__(other)
        if self.__class__ != other.__class__:
            msg = f"'{self}' has incompatible class with '{other}'"
            raise ValueError(msg)
        if self._base_parts != other._base_parts:
            msg = f"'{self}' does not start with '{other}'"
            raise ValueError(msg)
        return self._path.relative_to(other._path)

    def isin(self, other):
        if isinstance(other, (str, bytes)):
            other = self.__class__(other)
        elif self.__class__ != other.__class__:
            return False
        return self._base_parts == other._base_parts and self._path.isin(
            other._path
        )


class CloudURLInfo(URLInfo):
    @property
    def path(self):
        return self._spath.lstrip("/")


class HTTPURLInfo(URLInfo):
    __hash__: Callable[["HTTPURLInfo"], int] = URLInfo.__hash__

    def __init__(self, url):
        p = urlparse(url)
        stripped = p._replace(params=None, query=None, fragment=None)
        super().__init__(stripped.geturl())
        self.params = p.params
        self.query = p.query
        self.fragment = p.fragment

    def replace(self, path=None):
        return self.from_parts(
            *self._base_parts,
            params=self.params,
            query=self.query,
            fragment=self.fragment,
            path=path,
        )

    @classmethod
    def from_parts(
        cls,
        scheme=None,
        host=None,
        user=None,
        port=None,
        path="",
        netloc=None,
        params=None,
        query=None,
        fragment=None,
    ):  # pylint: disable=arguments-differ
        assert bool(host) ^ bool(netloc)

        if netloc is not None:
            return cls(
                "{}://{}{}{}{}{}".format(
                    scheme,
                    netloc,
                    path,
                    (";" + params) if params else "",
                    ("?" + query) if query else "",
                    ("#" + fragment) if fragment else "",
                )
            )

        obj = cls.__new__(cls)
        obj._fill_parts(scheme, host, user, port, path)
        obj.params = params
        obj.query = query
        obj.fragment = fragment
        return obj

    @property
    def _extra_parts(self):
        return (self.params, self.query, self.fragment)

    @property
    def parts(self):
        return self._base_parts + self._path.parts + self._extra_parts

    @cached_property
    def url(self):
        return "{}://{}{}{}{}{}".format(
            self.scheme,
            self.netloc,
            self._spath,
            (";" + self.params) if self.params else "",
            ("?" + self.query) if self.query else "",
            ("#" + self.fragment) if self.fragment else "",
        )

    def __eq__(self, other):
        if isinstance(other, (str, bytes)):
            other = self.__class__(other)
        return (
            self.__class__ == other.__class__
            and self._base_parts == other._base_parts
            and self._path == other._path
            and self._extra_parts == other._extra_parts
        )


class WebDAVURLInfo(URLInfo):
    @cached_property
    def url(self):
        return "{}://{}{}".format(
            self.scheme.replace("webdav", "http"), self.netloc, self._spath
        )
