import json
import os
import textwrap
from uuid import uuid4

import pytest

from dvc.dependency.base import DependencyDoesNotExistError
from dvc.exceptions import InvalidArgumentError
from dvc.main import main
from dvc.objects.db import ODBManager
from dvc.stage import Stage
from dvc.utils.fs import makedirs
from tests.basic_env import TestDvc


class TestCmdImport(TestDvc):
    def test(self):
        ret = main(["import-url", self.FOO, "import"])
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.exists("import.dvc"))

        ret = main(["import-url", "non-existing-file", "import"])
        self.assertNotEqual(ret, 0)

    def test_unsupported(self):
        ret = main(["import-url", "unsupported://path", "import_unsupported"])
        self.assertNotEqual(ret, 0)


class TestDefaultOutput(TestDvc):
    def test(self):
        tmpdir = self.mkdtemp()
        filename = str(uuid4())
        tmpfile = os.path.join(tmpdir, filename)

        with open(tmpfile, "w") as fd:
            fd.write("content")

        ret = main(["import-url", tmpfile])
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.exists(filename))
        with open(filename) as fd:
            self.assertEqual(fd.read(), "content")


def test_should_remove_outs_before_import(tmp_dir, dvc, mocker, erepo_dir):
    erepo_dir.gen({"foo": "foo"})

    remove_outs_call_counter = mocker.spy(Stage, "remove_outs")
    ret = main(["import-url", os.fspath(erepo_dir / "foo")])

    assert ret == 0
    assert remove_outs_call_counter.mock.call_count == 1


class TestImportFilename(TestDvc):
    def setUp(self):
        super().setUp()
        tmp_dir = self.mkdtemp()
        self.external_source = os.path.join(tmp_dir, "file")
        with open(self.external_source, "w") as fobj:
            fobj.write("content")

    def test(self):
        ret = main(["import-url", "--file", "bar.dvc", self.external_source])
        self.assertEqual(0, ret)
        self.assertTrue(os.path.exists("bar.dvc"))

        os.remove("bar.dvc")
        os.mkdir("sub")

        path = os.path.join("sub", "bar.dvc")
        ret = main(["import-url", "--file", path, self.external_source, "out"])
        self.assertEqual(0, ret)
        self.assertTrue(os.path.exists(path))


@pytest.mark.parametrize("dname", [".", "dir", "dir/subdir"])
def test_import_url_to_dir(dname, tmp_dir, dvc):
    tmp_dir.gen({"data_dir": {"file": "file content"}})
    src = os.path.join("data_dir", "file")

    makedirs(dname, exist_ok=True)

    stage = dvc.imp_url(src, dname)

    dst = tmp_dir / dname / "file"

    assert stage.outs[0].path_info == dst
    assert os.path.isdir(dname)
    assert dst.read_text() == "file content"


def test_import_stage_accompanies_target(tmp_dir, dvc, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("file1", "file1 content", commit="commit file")

    tmp_dir.gen({"dir": {}})
    erepo = {"url": os.fspath(erepo_dir)}
    dvc.imp_url("file1", out=os.path.join("dir", "imported_file"), erepo=erepo)

    assert (tmp_dir / "dir" / "imported_file").exists()
    assert (tmp_dir / "dir" / "imported_file.dvc").exists()


def test_import_url_nonexistent(dvc, erepo_dir):
    with pytest.raises(DependencyDoesNotExistError):
        dvc.imp_url(os.fspath(erepo_dir / "non-existent"))


def test_import_url_with_no_exec(tmp_dir, dvc, erepo_dir):
    tmp_dir.gen({"data_dir": {"file": "file content"}})
    src = os.path.join("data_dir", "file")

    dvc.imp_url(src, ".", no_exec=True)
    dst = tmp_dir / "file"
    assert not dst.exists()


@pytest.mark.parametrize(
    "workspace",
    [
        pytest.lazy_fixture("local_cloud"),
        pytest.lazy_fixture("s3"),
        pytest.lazy_fixture("azure"),
        pytest.param(
            pytest.lazy_fixture("gs"), marks=pytest.mark.needs_internet
        ),
        pytest.lazy_fixture("hdfs"),
        pytest.lazy_fixture("webhdfs"),
        pytest.param(
            pytest.lazy_fixture("ssh"),
            marks=pytest.mark.skipif(
                os.name == "nt", reason="disabled on windows"
            ),
        ),
        pytest.lazy_fixture("http"),
    ],
    indirect=True,
)
def test_import_url(tmp_dir, dvc, workspace):
    workspace.gen("file", "file")
    assert not (tmp_dir / "file").exists()  # sanity check
    dvc.imp_url("remote://workspace/file")
    assert (tmp_dir / "file").read_text() == "file"

    assert dvc.status() == {}


@pytest.mark.parametrize(
    "workspace, stage_md5, dir_md5",
    [
        (
            pytest.lazy_fixture("local_cloud"),
            "dc24e1271084ee317ac3c2656fb8812b",
            "b6dcab6ccd17ca0a8bf4a215a37d14cc.dir",
        ),
        (
            pytest.lazy_fixture("s3"),
            "2aa17f8daa26996b3f7a4cf8888ac9ac",
            "ec602a6ba97b2dd07bd6d2cd89674a60.dir",
        ),
        (
            pytest.lazy_fixture("azure"),
            None,  # ETags for azure are not consistent with the actual
            None,  # content of the object, so they will change every time
            # we create new objects. We'll skip stage content check for Azure.
        ),
        (
            pytest.lazy_fixture("hdfs"),
            "ec0943f83357f702033c98e70b853c8c",
            "e6dcd267966dc628d732874f94ef4280.dir",
        ),
        pytest.param(
            pytest.lazy_fixture("ssh"),
            "dc24e1271084ee317ac3c2656fb8812b",
            "b6dcab6ccd17ca0a8bf4a215a37d14cc.dir",
            marks=pytest.mark.skipif(
                os.name == "nt", reason="disabled on windows"
            ),
        ),
    ],
    indirect=["workspace"],
)
def test_import_url_dir(tmp_dir, dvc, workspace, stage_md5, dir_md5):
    workspace.gen({"dir": {"file": "file", "subdir": {"subfile": "subfile"}}})

    # remove external cache to make sure that we don't need it to import dirs
    with dvc.config.edit() as conf:
        del conf["cache"]
    dvc.odb = ODBManager(dvc)

    assert not (tmp_dir / "dir").exists()  # sanity check
    dvc.imp_url("remote://workspace/dir")
    assert set(os.listdir(tmp_dir / "dir")) == {"file", "subdir"}
    assert (tmp_dir / "dir" / "file").read_text() == "file"
    assert list(os.listdir(tmp_dir / "dir" / "subdir")) == ["subfile"]
    assert (tmp_dir / "dir" / "subdir" / "subfile").read_text() == "subfile"

    assert dvc.status() == {}

    if stage_md5 is not None and dir_md5 is not None:
        assert (tmp_dir / "dir.dvc").read_text() == (
            f"md5: {stage_md5}\n"
            "frozen: true\n"
            "deps:\n"
            f"- md5: {dir_md5}\n"
            "  size: 11\n"
            "  nfiles: 2\n"
            "  path: remote://workspace/dir\n"
            "outs:\n"
            "- md5: b6dcab6ccd17ca0a8bf4a215a37d14cc.dir\n"
            "  size: 11\n"
            "  nfiles: 2\n"
            "  path: dir\n"
        )


def test_import_url_preserve_meta(tmp_dir, dvc):
    text = textwrap.dedent(
        """\
        # top comment
        desc: top desc
        deps:
        - path: foo # dep comment
        outs:
        - path: bar # out comment
          desc: out desc
        meta: some metadata
    """
    )
    tmp_dir.gen("bar.dvc", text)

    tmp_dir.gen("foo", "foo")
    dvc.imp_url("foo", out="bar")
    assert (tmp_dir / "bar.dvc").read_text() == textwrap.dedent(
        """\
        # top comment
        desc: top desc
        deps:
        - path: foo # dep comment
          md5: acbd18db4cc2f85cedef654fccc4a4d8
          size: 3
        outs:
        - path: bar # out comment
          desc: out desc
          md5: acbd18db4cc2f85cedef654fccc4a4d8
          size: 3
        meta: some metadata
        md5: be7ade0aa89cc8d56e320867a9de9740
        frozen: true
    """
    )


@pytest.mark.parametrize(
    "workspace",
    [
        pytest.lazy_fixture("local_cloud"),
        pytest.lazy_fixture("s3"),
        pytest.param(
            pytest.lazy_fixture("gs"), marks=pytest.mark.needs_internet
        ),
        pytest.lazy_fixture("hdfs"),
        pytest.param(
            pytest.lazy_fixture("ssh"),
            marks=pytest.mark.skipif(
                os.name == "nt", reason="disabled on windows"
            ),
        ),
        pytest.lazy_fixture("http"),
    ],
    indirect=True,
)
def test_import_url_to_remote_single_file(
    tmp_dir, dvc, workspace, local_remote
):
    workspace.gen("foo", "foo")

    url = "remote://workspace/foo"
    stage = dvc.imp_url(url, to_remote=True)

    assert stage.deps[0].hash_info.value is not None
    assert not (tmp_dir / "foo").exists()
    assert (tmp_dir / "foo.dvc").exists()

    assert len(stage.deps) == 1
    assert stage.deps[0].def_path == url
    assert len(stage.outs) == 1

    hash_info = stage.outs[0].hash_info
    assert local_remote.hash_to_path_info(hash_info.value).read_text() == "foo"
    assert hash_info.size == len("foo")


@pytest.mark.parametrize(
    "workspace",
    [
        pytest.lazy_fixture("local_cloud"),
        pytest.lazy_fixture("s3"),
        pytest.param(
            pytest.lazy_fixture("gs"), marks=pytest.mark.needs_internet
        ),
        pytest.lazy_fixture("hdfs"),
        pytest.param(
            pytest.lazy_fixture("ssh"),
            marks=pytest.mark.skipif(
                os.name == "nt", reason="disabled on windows"
            ),
        ),
    ],
    indirect=True,
)
def test_import_url_to_remote_directory(tmp_dir, dvc, workspace, local_remote):
    workspace.gen(
        {
            "data": {
                "foo": "foo",
                "bar": "bar",
                "sub_dir": {"baz": "sub_dir/baz"},
            }
        }
    )

    url = "remote://workspace/data"
    stage = dvc.imp_url(url, to_remote=True)

    assert not (tmp_dir / "data").exists()
    assert (tmp_dir / "data.dvc").exists()

    assert len(stage.deps) == 1
    assert stage.deps[0].def_path == url
    assert len(stage.outs) == 1

    hash_info = stage.outs[0].hash_info
    with open(local_remote.hash_to_path_info(hash_info.value)) as stream:
        file_parts = json.load(stream)

    assert len(file_parts) == 3
    assert {file_part["relpath"] for file_part in file_parts} == {
        "foo",
        "bar",
        "sub_dir/baz",
    }

    for file_part in file_parts:
        assert (
            local_remote.hash_to_path_info(file_part["md5"]).read_text()
            == file_part["relpath"]
        )


def test_import_url_to_remote_absolute(
    tmp_dir, make_tmp_dir, dvc, local_remote
):
    tmp_abs_dir = make_tmp_dir("abs")
    tmp_foo = tmp_abs_dir / "foo"
    tmp_foo.write_text("foo")

    stage = dvc.imp_url(str(tmp_foo), to_remote=True)

    foo = tmp_dir / "foo"
    assert stage.deps[0].fspath == str(tmp_foo)
    assert stage.outs[0].fspath == os.fspath(foo)
    assert foo.with_suffix(".dvc").exists()


def test_import_url_to_remote_invalid_combinations(dvc):
    with pytest.raises(InvalidArgumentError, match="--no-exec"):
        dvc.imp_url("s3://bucket/foo", no_exec=True, to_remote=True)


empty_xfail = pytest.mark.xfail(
    reason="https://github.com/iterative/dvc/issues/5521"
)


@pytest.mark.parametrize(
    "workspace",
    [
        pytest.lazy_fixture("hdfs"),
        pytest.param(pytest.lazy_fixture("s3"), marks=empty_xfail),
        pytest.param(pytest.lazy_fixture("gs"), marks=empty_xfail),
        pytest.param(pytest.lazy_fixture("azure"), marks=empty_xfail),
        pytest.param(
            pytest.lazy_fixture("ssh"),
            marks=pytest.mark.skipif(
                os.name == "nt", reason="disabled on windows"
            ),
        ),
    ],
    indirect=True,
)
def test_import_url_empty_directory(tmp_dir, dvc, workspace):
    # prefix based storage services (e.g s3) doesn't have the real concept
    # of directories. So instead we create an empty file that ends with a
    # trailing slash in order to actually support this operation
    if workspace.IS_OBJECT_STORAGE:
        contents = ""
    else:
        contents = {}

    workspace.gen({"empty_dir/": contents})

    dvc.imp_url("remote://workspace/empty_dir/")

    empty_dir = tmp_dir / "empty_dir"
    assert empty_dir.is_dir()
    assert tuple(empty_dir.iterdir()) == ()


def test_import_url_to_remote_status(tmp_dir, dvc, local_cloud, local_remote):
    local_cloud.gen("foo", "foo")

    stage = dvc.imp_url(str(local_cloud / "foo"), to_remote=True)
    assert stage.md5 is not None

    status = dvc.status()
    assert status["foo.dvc"] == [{"changed outs": {"foo": "not in cache"}}]

    dvc.pull()

    status = dvc.status()
    assert len(status) == 0
