import os
from operator import itemgetter
from os.path import join

import pytest

from dvc.fs import get_cloud_fs
from dvc.fs.local import LocalFileSystem
from dvc.path_info import PathInfo
from dvc.repo import Repo
from dvc.scm import SCM
from tests.basic_env import TestDir, TestGit, TestGitSubmodule


class TestLocalFileSystem(TestDir):
    def setUp(self):
        super().setUp()
        self.fs = LocalFileSystem()

    def test_open(self):
        with self.fs.open(self.FOO) as fd:
            self.assertEqual(fd.read(), self.FOO_CONTENTS)
        with self.fs.open(self.UNICODE, encoding="utf-8") as fd:
            self.assertEqual(fd.read(), self.UNICODE_CONTENTS)

    def test_exists(self):
        self.assertTrue(self.fs.exists(self.FOO))
        self.assertTrue(self.fs.exists(self.UNICODE))
        self.assertFalse(self.fs.exists("not-existing-file"))

    def test_isdir(self):
        self.assertTrue(self.fs.isdir(self.DATA_DIR))
        self.assertFalse(self.fs.isdir(self.FOO))
        self.assertFalse(self.fs.isdir("not-existing-file"))

    def test_isfile(self):
        self.assertTrue(self.fs.isfile(self.FOO))
        self.assertFalse(self.fs.isfile(self.DATA_DIR))
        self.assertFalse(self.fs.isfile("not-existing-file"))


class GitFileSystemTests:
    # pylint: disable=no-member
    def test_open(self):
        self.scm.add([self.FOO, self.UNICODE, self.DATA_DIR])
        self.scm.commit("add")

        fs = self.scm.get_fs("master")
        with fs.open(self.FOO) as fd:
            self.assertEqual(fd.read(), self.FOO_CONTENTS)
        with fs.open(self.UNICODE) as fd:
            self.assertEqual(fd.read(), self.UNICODE_CONTENTS)
        with self.assertRaises(IOError):
            fs.open("not-existing-file")
        with self.assertRaises(IOError):
            fs.open(self.DATA_DIR)

    def test_exists(self):
        fs = self.scm.get_fs("master")
        self.assertFalse(fs.exists(self.FOO))
        self.assertFalse(fs.exists(self.UNICODE))
        self.assertFalse(fs.exists(self.DATA_DIR))
        self.scm.add([self.FOO, self.UNICODE, self.DATA])
        self.scm.commit("add")

        fs = self.scm.get_fs("master")
        self.assertTrue(fs.exists(self.FOO))
        self.assertTrue(fs.exists(self.UNICODE))
        self.assertTrue(fs.exists(self.DATA_DIR))
        self.assertFalse(fs.exists("non-existing-file"))

    def test_isdir(self):
        self.scm.add([self.FOO, self.DATA_DIR])
        self.scm.commit("add")

        fs = self.scm.get_fs("master")
        self.assertTrue(fs.isdir(self.DATA_DIR))
        self.assertFalse(fs.isdir(self.FOO))
        self.assertFalse(fs.isdir("non-existing-file"))

    def test_isfile(self):
        self.scm.add([self.FOO, self.DATA_DIR])
        self.scm.commit("add")

        fs = self.scm.get_fs("master")
        self.assertTrue(fs.isfile(self.FOO))
        self.assertFalse(fs.isfile(self.DATA_DIR))
        self.assertFalse(fs.isfile("not-existing-file"))


class TestGitFileSystem(TestGit, GitFileSystemTests):
    def setUp(self):
        super().setUp()
        self.scm = SCM(self._root_dir)


class TestGitSubmoduleFileSystem(TestGitSubmodule, GitFileSystemTests):
    def setUp(self):
        super().setUp()
        self.scm = SCM(self._root_dir)
        self._pushd(self._root_dir)


class AssertWalkEqualMixin:
    def assertWalkEqual(self, actual, expected, msg=None):
        def convert_to_sets(walk_results):
            return [
                (root, set(dirs), set(nondirs))
                for root, dirs, nondirs in walk_results
            ]

        self.assertEqual(
            convert_to_sets(actual), convert_to_sets(expected), msg=msg
        )


class TestWalkInNoSCM(AssertWalkEqualMixin, TestDir):
    def test(self):
        fs = LocalFileSystem()
        self.assertWalkEqual(
            fs.walk(self._root_dir),
            [
                (
                    self._root_dir,
                    ["data_dir"],
                    ["code.py", "bar", "тест", "foo"],
                ),
                (join(self._root_dir, "data_dir"), ["data_sub_dir"], ["data"]),
                (
                    join(self._root_dir, "data_dir", "data_sub_dir"),
                    [],
                    ["data_sub"],
                ),
            ],
        )

    def test_subdir(self):
        fs = LocalFileSystem()
        self.assertWalkEqual(
            fs.walk(join("data_dir", "data_sub_dir")),
            [(join("data_dir", "data_sub_dir"), [], ["data_sub"])],
        )


class TestWalkInGit(AssertWalkEqualMixin, TestGit):
    def test_nobranch(self):
        fs = LocalFileSystem(url=self._root_dir)
        walk_result = []
        for root, dirs, files in fs.walk("."):
            dirs[:] = [i for i in dirs if i != ".git"]
            walk_result.append((root, dirs, files))
        self.assertWalkEqual(
            walk_result,
            [
                (".", ["data_dir"], ["bar", "тест", "code.py", "foo"]),
                (join("data_dir"), ["data_sub_dir"], ["data"]),
                (join("data_dir", "data_sub_dir"), [], ["data_sub"]),
            ],
        )
        self.assertWalkEqual(
            fs.walk(join("data_dir", "data_sub_dir")),
            [(join("data_dir", "data_sub_dir"), [], ["data_sub"])],
        )

    def test_branch(self):
        scm = SCM(self._root_dir)
        scm.add([self.DATA_SUB_DIR])
        scm.commit("add data_dir/data_sub_dir/data_sub")
        fs = scm.get_fs("master")
        self.assertWalkEqual(
            fs.walk("."),
            [
                (self._root_dir, ["data_dir"], ["code.py"]),
                (join(self._root_dir, "data_dir"), ["data_sub_dir"], []),
                (
                    join(self._root_dir, "data_dir", "data_sub_dir"),
                    [],
                    ["data_sub"],
                ),
            ],
        )
        self.assertWalkEqual(
            fs.walk(join("data_dir", "data_sub_dir")),
            [
                (
                    join(self._root_dir, "data_dir", "data_sub_dir"),
                    [],
                    ["data_sub"],
                )
            ],
        )


def test_cleanfs_subrepo(tmp_dir, dvc, scm, monkeypatch):
    tmp_dir.gen({"subdir": {}})
    subrepo_dir = tmp_dir / "subdir"
    with subrepo_dir.chdir():
        subrepo = Repo.init(subdir=True)
        subrepo_dir.gen({"foo": "foo", "dir": {"bar": "bar"}})

    path = PathInfo(subrepo_dir)

    assert dvc.fs.exists(path / "foo")
    assert dvc.fs.isfile(path / "foo")
    assert dvc.fs.exists(path / "dir")
    assert dvc.fs.isdir(path / "dir")

    assert subrepo.fs.exists(path / "foo")
    assert subrepo.fs.isfile(path / "foo")
    assert subrepo.fs.exists(path / "dir")
    assert subrepo.fs.isdir(path / "dir")


def test_walk_dont_ignore_subrepos(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen({"foo": "foo"}, commit="add foo")
    subrepo_dir = tmp_dir / "subdir"
    subrepo_dir.mkdir()
    with subrepo_dir.chdir():
        Repo.init(subdir=True)
    scm.add(["subdir"])
    scm.commit("Add subrepo")

    dvc_fs = dvc.fs
    dvc._reset()
    scm_fs = scm.get_fs("HEAD")
    path = os.fspath(tmp_dir)
    get_dirs = itemgetter(1)

    assert set(get_dirs(next(dvc_fs.walk(path)))) == {".dvc", "subdir", ".git"}
    assert set(get_dirs(next(scm_fs.walk(path)))) == {".dvc", "subdir"}


@pytest.mark.parametrize(
    "cloud",
    [
        pytest.lazy_fixture("local_cloud"),
        pytest.lazy_fixture("s3"),
        pytest.param(
            pytest.lazy_fixture("gs"), marks=pytest.mark.needs_internet
        ),
        pytest.lazy_fixture("hdfs"),
        pytest.lazy_fixture("http"),
    ],
)
def test_fs_getsize(dvc, cloud):
    cloud.gen({"data": {"foo": "foo"}, "baz": "baz baz"})
    cls, config, path_info = get_cloud_fs(dvc, **cloud.config)
    fs = cls(**config)

    assert fs.getsize(path_info / "baz") == 7
    assert fs.getsize(path_info / "data" / "foo") == 3


@pytest.mark.needs_internet
@pytest.mark.parametrize(
    "cloud",
    [
        pytest.lazy_fixture("azure"),
        pytest.lazy_fixture("gs"),
        pytest.lazy_fixture("gdrive"),
        pytest.lazy_fixture("hdfs"),
        pytest.lazy_fixture("http"),
        pytest.lazy_fixture("local_cloud"),
        pytest.lazy_fixture("oss"),
        pytest.lazy_fixture("s3"),
        pytest.lazy_fixture("ssh"),
        pytest.lazy_fixture("webhdfs"),
    ],
)
def test_fs_upload_fobj(dvc, tmp_dir, cloud):
    tmp_dir.gen("foo", "foo")
    cls, config, path_info = get_cloud_fs(dvc, **cloud.config)
    fs = cls(**config)

    from_info = tmp_dir / "foo"
    to_info = path_info / "foo"

    with open(from_info, "rb") as stream:
        fs.upload_fobj(stream, to_info)

    assert fs.exists(to_info)
    with fs.open(to_info, "rb") as stream:
        assert stream.read() == b"foo"


@pytest.mark.needs_internet
@pytest.mark.parametrize("cloud", [pytest.lazy_fixture("gdrive")])
def test_fs_ls(dvc, cloud):
    cloud.gen(
        {
            "directory": {
                "foo": "foo",
                "bar": "bar",
                "baz": {"quux": "quux", "egg": {"foo": "foo"}},
                "empty": {},
            }
        }
    )
    cls, config, path_info = get_cloud_fs(dvc, **cloud.config)
    fs = cls(**config)
    path_info /= "directory"

    assert {os.path.basename(file_key) for file_key in fs.ls(path_info)} == {
        "foo",
        "bar",
        "baz",
        "empty",
    }
    assert set(fs.ls(path_info / "empty")) == set()
    assert {
        (detail["type"], os.path.basename(detail["name"]))
        for detail in fs.ls(path_info / "baz", detail=True)
    } == {("file", "quux"), ("directory", "egg")}


@pytest.mark.needs_internet
@pytest.mark.parametrize(
    "cloud",
    [
        pytest.lazy_fixture("s3"),
        pytest.lazy_fixture("azure"),
        pytest.lazy_fixture("gs"),
        pytest.lazy_fixture("webdav"),
        pytest.lazy_fixture("gdrive"),
    ],
)
def test_fs_find_recursive(dvc, cloud):
    cloud.gen({"data": {"foo": "foo", "bar": {"baz": "baz"}, "quux": "quux"}})
    cls, config, path_info = get_cloud_fs(dvc, **cloud.config)
    fs = cls(**config)

    assert {
        os.path.basename(file_key) for file_key in fs.find(path_info / "data")
    } == {"foo", "baz", "quux"}


@pytest.mark.parametrize(
    "cloud",
    [
        pytest.lazy_fixture("s3"),
        pytest.lazy_fixture("azure"),
        pytest.param(
            pytest.lazy_fixture("gs"), marks=pytest.mark.needs_internet
        ),
        pytest.lazy_fixture("webdav"),
    ],
)
def test_fs_find_with_etag(dvc, cloud):
    cloud.gen({"data": {"foo": "foo", "bar": {"baz": "baz"}, "quux": "quux"}})
    cls, config, path_info = get_cloud_fs(dvc, **cloud.config)
    fs = cls(**config)

    for details in fs.find(path_info / "data", detail=True):
        assert (
            fs.info(path_info.replace(path=details["name"]))["etag"]
            == details["etag"]
        )


@pytest.mark.parametrize(
    "cloud",
    [
        pytest.lazy_fixture("azure"),
        pytest.param(
            pytest.lazy_fixture("gs"), marks=pytest.mark.needs_internet
        ),
    ],
)
def test_fs_fsspec_path_management(dvc, cloud):
    cloud.gen({"foo": "foo", "data": {"bar": "bar", "baz": {"foo": "foo"}}})
    cls, config, _ = get_cloud_fs(dvc, **cloud.config)
    fs = cls(**config)

    root = cloud.parents[len(cloud.parents) - 1]
    bucket_details = fs.info(root)

    # special conditions: name always points to the bucket name
    assert bucket_details["name"] == root.bucket
    assert bucket_details["type"] == "directory"

    data = cloud / "data"
    data_details = fs.info(data)
    assert data_details["name"].rstrip("/") == data.path
    assert data_details["type"] == "directory"
