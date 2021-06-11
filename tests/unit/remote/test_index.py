import os

import pytest
from funcy import first

from dvc.remote.index import RemoteIndex


@pytest.fixture(scope="function")
def index(dvc):
    idx = RemoteIndex(dvc.tmp_dir, "foo")
    idx.load()
    yield idx
    idx.dump()
    os.unlink(idx.path)


def test_init(dvc, index):
    assert str(index.path) == os.path.join(dvc.tmp_dir, "index", "foo.idx")


def test_is_dir_hash(dvc, index):
    assert index.is_dir_hash("foo.dir")
    assert not index.is_dir_hash("foo")


def test_roundtrip(dvc, index):
    expected_dir = {"1234.dir"}
    expected_file = {"5678"}
    index.update(expected_dir, expected_file)
    index.dump()
    index.load()
    assert set(index.dir_hashes()) == expected_dir
    assert set(index.hashes()) == expected_dir | expected_file


def test_clear(dvc, index):
    index.update(["1234.dir"], ["5678"])
    index.clear()
    assert first(index.hashes()) is None


def test_update(dvc, index):
    expected_dir = {"1234.dir"}
    expected_file = {"5678"}
    index.update(expected_dir, expected_file)
    assert set(index.dir_hashes()) == expected_dir
    assert set(index.hashes()) == expected_dir | expected_file


def test_intersection(dvc, index):
    hashes = (str(i) for i in range(2000))
    expected = {str(i) for i in range(1000)}
    index.update([], hashes)
    assert set(index.intersection(expected)) == expected
