import locale
import os
import uuid
from contextlib import contextmanager
from pathlib import Path

import pytest

from dvc.path_info import URLInfo

from .base import Base
from .hdfs import _hdfs_root, md5md5crc32c


class WebHDFS(Base, URLInfo):  # pylint: disable=abstract-method
    @contextmanager
    def _webhdfs(self):
        from hdfs import InsecureClient

        client = InsecureClient(f"http://{self.host}:{self.port}", self.user)
        yield client

    def is_file(self):
        with self._webhdfs() as _hdfs:
            return _hdfs.status(self.path)["type"] == "FILE"

    def is_dir(self):
        with self._webhdfs() as _hdfs:
            return _hdfs.status(self.path)["type"] == "DIRECTORY"

    def exists(self):
        with self._webhdfs() as _hdfs:
            return _hdfs.status(self.path, strict=False) is not None

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        assert mode == 0o777
        assert parents

        with self._webhdfs() as _hdfs:
            # NOTE: hdfs.makekdirs always creates parents
            _hdfs.makedirs(self.path, permission=mode)

    def write_bytes(self, contents):
        with self._webhdfs() as _hdfs:
            with _hdfs.write(self.path, overwrite=True) as writer:
                writer.write(contents)

    def read_bytes(self):
        with self._webhdfs() as _hdfs:
            with _hdfs.read(self.path) as reader:
                return reader.read()

    def read_text(self, encoding=None, errors=None):
        if not encoding:
            encoding = locale.getpreferredencoding(False)
        assert errors is None
        return self.read_bytes().decode(encoding)


@pytest.fixture
def real_webhdfs(hdfs_server):
    port = hdfs_server["webhdfs"]
    url = f"webhdfs://127.0.0.1:{port}/{uuid.uuid4()}"
    yield WebHDFS(url)


class FakeClient:
    def __init__(self, *args, **kwargs):
        self._root = Path(_hdfs_root.name)

    def _path(self, path):
        return self._root / path.lstrip("/")

    def makedirs(self, path, permission=None):
        self._path(path).mkdir(
            mode=permission or 0o777, exist_ok=True, parents=True
        )

    def write(self, hdfs_path, overwrite=False):
        from hdfs.util import HdfsError

        path = self._path(hdfs_path)

        if not overwrite and path.exists():
            raise HdfsError(f"Remote path {hdfs_path} already exists.")

        path.parent.mkdir(parents=True, exist_ok=True)
        return path.open("wb")

    @contextmanager
    def read(
        self,
        hdfs_path,
        encoding=None,
        chunk_size=0,
        delimiter=None,
        progress=None,
    ):
        pobj = self._path(hdfs_path)
        if not chunk_size and not delimiter:
            if encoding:
                yield pobj.open("r", encoding=encoding)
            else:
                yield pobj.open("rb")
        else:
            if delimiter:
                data = pobj.open("r", encoding=encoding, newline=delimiter)
            else:

                def read_chunks(fobj, _chunk_size):
                    while True:
                        chunk = fobj.read(_chunk_size)
                        if not chunk:
                            break
                        yield chunk

                data = read_chunks(
                    pobj.open("rb", encoding=encoding), chunk_size
                )

            if progress:

                def reader(_hdfs_path, _progress):
                    nbytes = 0
                    for chunk in data:
                        nbytes += len(chunk)
                        _progress(_hdfs_path, nbytes)
                        yield chunk
                    _progress(_hdfs_path, -1)

                yield reader(hdfs_path, progress)
            else:
                yield data

    def walk(self, hdfs_path):
        import posixpath

        local_path = self._path(hdfs_path)
        for local_root, dnames, fnames in os.walk(local_path):
            if local_root == os.fspath(local_path):
                root = hdfs_path
            else:
                root = posixpath.join(
                    hdfs_path, os.path.relpath(local_root, local_path)
                )
            yield (root, dnames, fnames)

    def delete(self, hdfs_path):
        return self._path(hdfs_path).unlink()

    def status(self, hdfs_path, strict=True):
        from hdfs.util import HdfsError

        try:
            return {"length": self._path(hdfs_path).stat().st_size}
        except FileNotFoundError:
            if not strict:
                return None
            raise HdfsError(
                f"File does not exist: {hdfs_path}",
                exception="FileNotFoundException",
            )

    def checksum(self, hdfs_path):
        return {
            "algorithm": "MD5-of-0MD5-of-512CRC32",
            "bytes": md5md5crc32c(self._path(hdfs_path)) + "00000000",
            "size": 28,
        }

    def rename(self, from_path, to_path):
        from dvc.utils.fs import move

        move(self._path(from_path), self._path(to_path))

    def upload(
        self,
        hdfs_path,
        local_path,
        chunk_size=2 ** 16,
        progress=None,
        **kwargs,
    ):
        with open(local_path, "rb") as from_fobj:
            with self.write(hdfs_path, **kwargs) as to_fobj:
                nbytes = 0
                while True:
                    chunk = from_fobj.read(chunk_size)
                    if not chunk:
                        break
                    if progress:
                        nbytes += len(chunk)
                        progress(local_path, nbytes)
                    to_fobj.write(chunk)

    def download(self, hdfs_path, local_path, **kwargs):
        from dvc.utils.fs import makedirs

        kwargs.setdefault("chunk_size", 2 ** 16)

        makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as writer:
            with self.read(hdfs_path, **kwargs) as reader:
                for chunk in reader:
                    writer.write(chunk)


@pytest.fixture
def webhdfs(mocker):
    mocker.patch("hdfs.InsecureClient", FakeClient)
    url = f"webhdfs://example.com:12345/{uuid.uuid4()}"
    yield WebHDFS(url)
