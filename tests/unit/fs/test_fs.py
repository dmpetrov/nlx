import pytest

from dvc.fs import get_fs_cls
from dvc.fs.hdfs import HDFSFileSystem
from dvc.fs.http import HTTPFileSystem
from dvc.fs.https import HTTPSFileSystem
from dvc.fs.local import LocalFileSystem
from dvc.fs.s3 import S3FileSystem
from dvc.fs.ssh import SSHFileSystem


@pytest.mark.parametrize(
    "url, cls",
    [
        ("s3://bucket/path", S3FileSystem),
        ("ssh://example.com:/dir/path", SSHFileSystem),
        ("hdfs://example.com/dir/path", HDFSFileSystem),
        ("http://example.com/path/to/file", HTTPFileSystem),
        ("https://example.com/path/to/file", HTTPSFileSystem),
        ("path/to/file", LocalFileSystem),
        ("path\\to\\file", LocalFileSystem),
        ("file", LocalFileSystem),
        ("./file", LocalFileSystem),
        (".\\file", LocalFileSystem),
        ("../file", LocalFileSystem),
        ("..\\file", LocalFileSystem),
        ("unknown://path", LocalFileSystem),
    ],
)
def test_get_fs_cls(url, cls):
    assert get_fs_cls({"url": url}) == cls
