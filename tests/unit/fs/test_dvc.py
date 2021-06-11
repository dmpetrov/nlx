import os
import shutil

import pytest

from dvc.config import NoRemoteError
from dvc.fs.dvc import DvcFileSystem
from dvc.hash_info import HashInfo
from dvc.objects.stage import stage
from dvc.path_info import PathInfo
from dvc.utils.fs import remove


def test_exists(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    dvc.add("foo")
    (tmp_dir / "foo").unlink()

    fs = DvcFileSystem(repo=dvc)
    assert fs.exists("foo")


def test_open(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    dvc.add("foo")
    (tmp_dir / "foo").unlink()

    fs = DvcFileSystem(repo=dvc)
    with fs.open(PathInfo(tmp_dir) / "foo", "r") as fobj:
        assert fobj.read() == "foo"


def test_open_dirty_hash(tmp_dir, dvc):
    tmp_dir.dvc_gen("file", "file")
    (tmp_dir / "file").write_text("something")

    fs = DvcFileSystem(repo=dvc)
    with fs.open(PathInfo(tmp_dir) / "file", "r") as fobj:
        # NOTE: Unlike RepoFileSystem, DvcFileSystem should not
        # be affected by a dirty workspace.
        assert fobj.read() == "file"


def test_open_no_remote(tmp_dir, dvc):
    tmp_dir.dvc_gen("file", "file")
    (tmp_dir / "file").unlink()
    remove(dvc.odb.local.cache_dir)

    fs = DvcFileSystem(repo=dvc)
    with pytest.raises(FileNotFoundError) as exc_info:
        with fs.open(PathInfo(tmp_dir) / "file", "r"):
            pass
    assert isinstance(exc_info.value.__cause__, NoRemoteError)


def test_open_dirty_no_hash(tmp_dir, dvc):
    tmp_dir.gen("file", "file")
    (tmp_dir / "file.dvc").write_text("outs:\n- path: file\n")

    fs = DvcFileSystem(repo=dvc)
    # NOTE: Unlike RepoFileSystem, DvcFileSystem should not
    # be affected by a dirty workspace.
    with pytest.raises(FileNotFoundError):
        with fs.open(PathInfo(tmp_dir) / "file", "r"):
            pass


def test_open_in_history(tmp_dir, scm, dvc):
    tmp_dir.gen("foo", "foo")
    dvc.add("foo")
    dvc.scm.add(["foo.dvc", ".gitignore"])
    dvc.scm.commit("foo")

    tmp_dir.gen("foo", "foofoo")
    dvc.add("foo")
    dvc.scm.add(["foo.dvc", ".gitignore"])
    dvc.scm.commit("foofoo")

    for rev in dvc.brancher(revs=["HEAD~1"]):
        if rev == "workspace":
            continue

        fs = DvcFileSystem(repo=dvc)
        with fs.open(PathInfo(tmp_dir) / "foo", "r") as fobj:
            assert fobj.read() == "foo"


def test_isdir_isfile(tmp_dir, dvc):
    tmp_dir.gen({"datafile": "data", "datadir": {"foo": "foo", "bar": "bar"}})

    fs = DvcFileSystem(repo=dvc)
    assert not fs.isdir("datadir")
    assert not fs.isfile("datadir")
    assert not fs.isdir("datafile")
    assert not fs.isfile("datafile")

    dvc.add(["datadir", "datafile"])
    shutil.rmtree(tmp_dir / "datadir")
    (tmp_dir / "datafile").unlink()

    assert fs.isdir("datadir")
    assert not fs.isfile("datadir")
    assert not fs.isdir("datafile")
    assert fs.isfile("datafile")


def test_isdir_mixed(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"foo": "foo", "bar": "bar"}})

    dvc.add(str(tmp_dir / "dir" / "foo"))

    fs = DvcFileSystem(repo=dvc)
    assert fs.isdir("dir")
    assert not fs.isfile("dir")


def test_walk(tmp_dir, dvc):
    tmp_dir.gen(
        {
            "dir": {
                "subdir1": {"foo1": "foo1", "bar1": "bar1"},
                "subdir2": {"foo2": "foo2"},
                "foo": "foo",
                "bar": "bar",
            }
        }
    )

    dvc.add("dir", recursive=True)
    fs = DvcFileSystem(repo=dvc)

    expected = [
        str(tmp_dir / "dir" / "subdir1"),
        str(tmp_dir / "dir" / "subdir2"),
        str(tmp_dir / "dir" / "subdir1" / "foo1"),
        str(tmp_dir / "dir" / "subdir1" / "bar1"),
        str(tmp_dir / "dir" / "subdir2" / "foo2"),
        str(tmp_dir / "dir" / "foo"),
        str(tmp_dir / "dir" / "bar"),
    ]

    actual = []
    for root, dirs, files in fs.walk("dir"):
        for entry in dirs + files:
            actual.append(os.path.join(root, entry))

    assert set(actual) == set(expected)
    assert len(actual) == len(expected)


def test_walk_dir(tmp_dir, dvc):
    tmp_dir.gen(
        {
            "dir": {
                "subdir1": {"foo1": "foo1", "bar1": "bar1"},
                "subdir2": {"foo2": "foo2"},
                "foo": "foo",
                "bar": "bar",
            }
        }
    )

    dvc.add("dir")
    fs = DvcFileSystem(repo=dvc)

    expected = [
        str(tmp_dir / "dir" / "subdir1"),
        str(tmp_dir / "dir" / "subdir2"),
        str(tmp_dir / "dir" / "subdir1" / "foo1"),
        str(tmp_dir / "dir" / "subdir1" / "bar1"),
        str(tmp_dir / "dir" / "subdir2" / "foo2"),
        str(tmp_dir / "dir" / "foo"),
        str(tmp_dir / "dir" / "bar"),
    ]

    actual = []
    for root, dirs, files in fs.walk("dir"):
        for entry in dirs + files:
            actual.append(os.path.join(root, entry))

    assert set(actual) == set(expected)
    assert len(actual) == len(expected)


def test_walk_onerror(tmp_dir, dvc):
    def onerror(exc):
        raise exc

    tmp_dir.dvc_gen("foo", "foo")
    fs = DvcFileSystem(repo=dvc)

    # path does not exist
    for _ in fs.walk("dir"):
        pass
    with pytest.raises(OSError):
        for _ in fs.walk("dir", onerror=onerror):
            pass

    # path is not a directory
    for _ in fs.walk("foo"):
        pass
    with pytest.raises(OSError):
        for _ in fs.walk("foo", onerror=onerror):
            pass


def test_isdvc(tmp_dir, dvc):
    tmp_dir.gen({"foo": "foo", "bar": "bar"})
    dvc.add("foo")
    fs = DvcFileSystem(repo=dvc)
    assert fs.isdvc("foo")
    assert not fs.isdvc("bar")


def test_get_hash_file(tmp_dir, dvc):
    tmp_dir.dvc_gen({"foo": "foo"})
    fs = DvcFileSystem(repo=dvc)
    assert (
        fs.info(PathInfo(tmp_dir) / "foo")["md5"]
        == "acbd18db4cc2f85cedef654fccc4a4d8"
    )


def test_get_hash_dir(tmp_dir, dvc, mocker):
    import dvc as dvc_module

    tmp_dir.dvc_gen(
        {"dir": {"foo": "foo", "bar": "bar", "subdir": {"data": "data"}}}
    )
    fs = DvcFileSystem(repo=dvc)
    get_file_hash_spy = mocker.spy(dvc_module.objects.stage, "get_file_hash")
    assert (
        fs.info(PathInfo(tmp_dir) / "dir")["md5"]
        == "8761c4e9acad696bee718615e23e22db.dir"
    )
    assert not get_file_hash_spy.called


def test_get_hash_granular(tmp_dir, dvc):
    tmp_dir.dvc_gen(
        {"dir": {"foo": "foo", "bar": "bar", "subdir": {"data": "data"}}}
    )
    fs = DvcFileSystem(repo=dvc)
    subdir = PathInfo(tmp_dir) / "dir" / "subdir"
    assert fs.info(subdir).get("md5") is None
    assert stage(dvc.odb.local, subdir, fs, "md5").hash_info == HashInfo(
        "md5", "af314506f1622d107e0ed3f14ec1a3b5.dir"
    )
    assert (
        fs.info(subdir / "data")["md5"] == "8d777f385d3dfec8815d20f7496026dc"
    )
    assert stage(
        dvc.odb.local, subdir / "data", fs, "md5"
    ).hash_info == HashInfo("md5", "8d777f385d3dfec8815d20f7496026dc")


def test_get_hash_dirty_file(tmp_dir, dvc):
    tmp_dir.dvc_gen("file", "file")
    (tmp_dir / "file").write_text("something")

    fs = DvcFileSystem(repo=dvc)
    expected = "8c7dd922ad47494fc02c388e12c00eac"
    assert fs.info(PathInfo(tmp_dir) / "file").get("md5") == expected
    assert stage(
        dvc.odb.local, PathInfo(tmp_dir) / "file", fs, "md5"
    ).hash_info == HashInfo("md5", expected)


def test_get_hash_dirty_dir(tmp_dir, dvc):
    tmp_dir.dvc_gen({"dir": {"foo": "foo", "bar": "bar"}})
    (tmp_dir / "dir" / "baz").write_text("baz")

    fs = DvcFileSystem(repo=dvc)
    expected = "5ea40360f5b4ec688df672a4db9c17d1.dir"
    assert fs.info(PathInfo(tmp_dir) / "dir").get("md5") == expected
    assert stage(
        dvc.odb.local, PathInfo(tmp_dir) / "dir", fs, "md5"
    ).hash_info == HashInfo("md5", expected)
