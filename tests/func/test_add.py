import errno
import filecmp
import logging
import os
import shutil
import stat
import textwrap
import time

import colorama
import pytest
from mock import call, patch

import dvc as dvc_module
from dvc.dvcfile import DVC_FILE_SUFFIX
from dvc.exceptions import (
    DvcException,
    InvalidArgumentError,
    OutputDuplicationError,
    OverlappingOutputPathsError,
    RecursiveAddingWhileUsingFilename,
)
from dvc.fs.local import LocalFileSystem
from dvc.hash_info import HashInfo
from dvc.main import main
from dvc.objects.db import ODBManager
from dvc.output import OutputAlreadyTrackedError, OutputIsStageFileError
from dvc.stage import Stage
from dvc.stage.exceptions import (
    StageExternalOutputsError,
    StagePathNotFoundError,
)
from dvc.system import System
from dvc.utils import LARGE_DIR_SIZE, file_md5, relpath
from dvc.utils.fs import path_isin
from dvc.utils.serialize import YAMLFileCorruptedError, load_yaml
from tests.basic_env import TestDvc
from tests.utils import get_gitignore_content


def test_add(tmp_dir, dvc):
    (stage,) = tmp_dir.dvc_gen({"foo": "foo"})
    md5 = file_md5("foo", dvc.fs)

    assert stage is not None

    assert isinstance(stage, Stage)
    assert os.path.isfile(stage.path)
    assert len(stage.outs) == 1
    assert len(stage.deps) == 0
    assert stage.cmd is None
    assert stage.outs[0].hash_info == HashInfo("md5", md5)
    assert stage.md5 is None

    assert load_yaml("foo.dvc") == {
        "outs": [
            {
                "md5": "acbd18db4cc2f85cedef654fccc4a4d8",
                "path": "foo",
                "size": 3,
            }
        ]
    }


@pytest.mark.skipif(os.name == "nt", reason="can't set exec bit on Windows")
def test_add_executable(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    st = os.stat("foo")
    os.chmod("foo", st.st_mode | stat.S_IEXEC)
    dvc.add("foo")

    assert load_yaml("foo.dvc") == {
        "outs": [
            {
                "md5": "acbd18db4cc2f85cedef654fccc4a4d8",
                "path": "foo",
                "size": 3,
                "isexec": True,
            }
        ]
    }
    assert os.stat("foo").st_mode & stat.S_IEXEC


def test_add_unicode(tmp_dir, dvc):
    with open("\xe1", "wb") as fd:
        fd.write(b"something")

    (stage,) = dvc.add("\xe1")

    assert os.path.isfile(stage.path)


def test_add_unsupported_file(dvc):
    with pytest.raises(DvcException):
        dvc.add("unsupported://unsupported")


def test_add_directory(tmp_dir, dvc):
    from dvc.objects import load

    (stage,) = tmp_dir.dvc_gen({"dir": {"file": "file"}})

    assert stage is not None
    assert len(stage.deps) == 0
    assert len(stage.outs) == 1

    hash_info = stage.outs[0].hash_info

    obj = load(dvc.odb.local, hash_info)
    for key, _ in obj:
        for part in key:
            assert "\\" not in part


class TestAddDirectoryRecursive(TestDvc):
    def test(self):
        stages = self.dvc.add(self.DATA_DIR, recursive=True)
        self.assertEqual(len(stages), 2)


class TestAddCmdDirectoryRecursive(TestDvc):
    def test(self):
        ret = main(["add", "--recursive", self.DATA_DIR])
        self.assertEqual(ret, 0)

    def test_warn_about_large_directories(self):
        warning = (
            "You are adding a large directory 'large-dir' recursively,"
            " consider tracking it as a whole instead.\n"
            "{purple}HINT:{nc} Remove the generated DVC file and then"
            " run `{cyan}dvc add large-dir{nc}`".format(
                purple=colorama.Fore.MAGENTA,
                cyan=colorama.Fore.CYAN,
                nc=colorama.Style.RESET_ALL,
            )
        )

        os.mkdir("large-dir")

        # Create a lot of files
        for iteration in range(LARGE_DIR_SIZE + 1):
            path = os.path.join("large-dir", str(iteration))
            with open(path, "w") as fobj:
                fobj.write(path)

        with self._caplog.at_level(logging.WARNING, logger="dvc"):
            assert main(["add", "--recursive", "large-dir"]) == 0
            assert warning in self._caplog.messages


class TestAddDirectoryWithForwardSlash(TestDvc):
    def test(self):
        dname = "directory/"
        os.mkdir(dname)
        self.create(os.path.join(dname, "file"), "file")
        stages = self.dvc.add(dname)
        self.assertEqual(len(stages), 1)
        stage = stages[0]
        self.assertTrue(stage is not None)
        self.assertEqual(os.path.abspath("directory.dvc"), stage.path)


def test_add_tracked_file(tmp_dir, scm, dvc):
    path = "tracked_file"
    tmp_dir.scm_gen(path, "...", commit="add tracked file")
    msg = f""" output '{path}' is already tracked by SCM \\(e.g. Git\\).
    You can remove it from Git, then add to DVC.
        To stop tracking from Git:
            git rm -r --cached '{path}'
            git commit -m "stop tracking {path}" """

    with pytest.raises(OutputAlreadyTrackedError, match=msg):
        dvc.add(path)


class TestAddDirWithExistingCache(TestDvc):
    def test(self):
        dname = "a"
        fname = os.path.join(dname, "b")
        os.mkdir(dname)
        shutil.copyfile(self.FOO, fname)

        stages = self.dvc.add(self.FOO)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)
        stages = self.dvc.add(dname)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)


class TestAddModifiedDir(TestDvc):
    def test(self):
        stages = self.dvc.add(self.DATA_DIR)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)
        os.unlink(self.DATA)

        time.sleep(2)

        stages = self.dvc.add(self.DATA_DIR)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)


def test_add_file_in_dir(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"subdir": {"subdata": "subdata content"}}})
    subdir_path = os.path.join("dir", "subdir", "subdata")

    (stage,) = dvc.add(subdir_path)

    assert stage is not None
    assert len(stage.deps) == 0
    assert len(stage.outs) == 1
    assert stage.relpath == subdir_path + ".dvc"

    # Current dir should not be taken into account
    assert stage.wdir == os.path.dirname(stage.path)
    assert stage.outs[0].def_path == "subdata"


@pytest.mark.parametrize(
    "target, expected_def_paths, expected_rel_paths",
    [
        (
            os.path.join("dir", "subdir", "subdata*"),
            ["subdata", "subdata123"],
            [
                os.path.join("dir", "subdir", "subdata") + ".dvc",
                os.path.join("dir", "subdir", "subdata123") + ".dvc",
            ],
        ),
        (
            os.path.join("dir", "subdir", "?subdata"),
            ["esubdata", "isubdata"],
            [
                os.path.join("dir", "subdir", "esubdata") + ".dvc",
                os.path.join("dir", "subdir", "isubdata") + ".dvc",
            ],
        ),
        (
            os.path.join("dir", "subdir", "[aiou]subdata"),
            ["isubdata"],
            [os.path.join("dir", "subdir", "isubdata") + ".dvc"],
        ),
        (
            os.path.join("dir", "**", "subdata*"),
            ["subdata", "subdata123", "subdata4", "subdata5"],
            [
                os.path.join("dir", "subdir", "subdata") + ".dvc",
                os.path.join("dir", "subdir", "subdata123") + ".dvc",
                os.path.join("dir", "anotherdir", "subdata4") + ".dvc",
                os.path.join("dir", "subdata5") + ".dvc",
            ],
        ),
    ],
)
def test_add_filtered_files_in_dir(
    tmp_dir, dvc, target, expected_def_paths, expected_rel_paths
):
    tmp_dir.gen(
        {
            "dir": {
                "subdir": {
                    "subdata": "subdata content",
                    "esubdata": "extra subdata content",
                    "isubdata": "i subdata content",
                    "subdata123": "subdata content 123",
                },
                "anotherdir": {
                    "subdata4": "subdata 4 content",
                    "esubdata": "extra 2 subdata content",
                },
                "subdata5": "subdata 5 content",
            }
        }
    )

    stages = dvc.add(target, glob=True)

    assert len(stages) == len(expected_def_paths)
    for stage in stages:
        assert stage is not None
        assert len(stage.deps) == 0
        assert len(stage.outs) == 1
        assert stage.relpath in expected_rel_paths

        # Current dir should not be taken into account
        assert stage.wdir == os.path.dirname(stage.path)
        assert stage.outs[0].def_path in expected_def_paths


@pytest.mark.parametrize(
    "workspace, hash_name, hash_value",
    [
        (
            pytest.lazy_fixture("local_cloud"),
            "md5",
            "8c7dd922ad47494fc02c388e12c00eac",
        ),
        pytest.param(
            pytest.lazy_fixture("ssh"),
            "md5",
            "8c7dd922ad47494fc02c388e12c00eac",
            marks=pytest.mark.skipif(
                os.name == "nt", reason="disabled on windows"
            ),
        ),
        (
            pytest.lazy_fixture("s3"),
            "etag",
            "8c7dd922ad47494fc02c388e12c00eac",
        ),
        (
            pytest.lazy_fixture("hdfs"),
            "checksum",
            "000002000000000000000000a86fe4d846edc1bf4c355cb6112f141e",
        ),
        (
            pytest.lazy_fixture("webhdfs"),
            "checksum",
            "000002000000000000000000a86fe4d846edc1bf4c355cb6112f141e00000000",
        ),
    ],
    indirect=["workspace"],
)
def test_add_external_file(tmp_dir, dvc, workspace, hash_name, hash_value):
    workspace.gen("file", "file")

    with pytest.raises(StageExternalOutputsError):
        dvc.add(workspace.url)

    dvc.add("remote://workspace/file")
    assert (tmp_dir / "file.dvc").read_text() == (
        "outs:\n"
        f"- {hash_name}: {hash_value}\n"
        "  size: 4\n"
        "  path: remote://workspace/file\n"
    )
    assert (workspace / "file").read_text() == "file"
    assert (
        workspace / "cache" / hash_value[:2] / hash_value[2:]
    ).read_text() == "file"

    assert dvc.status() == {}


def test_add_external_relpath(tmp_dir, dvc, local_cloud):
    (fpath,) = local_cloud.gen("file", "file")
    rel = os.path.relpath(fpath)

    with pytest.raises(StageExternalOutputsError):
        dvc.add(rel)

    dvc.add(rel, external=True)
    assert (tmp_dir / "file.dvc").read_text() == (
        "outs:\n"
        "- md5: 8c7dd922ad47494fc02c388e12c00eac\n"
        "  size: 4\n"
        f"  path: {rel}\n"
    )
    assert fpath.read_text() == "file"
    assert dvc.status() == {}


@pytest.mark.parametrize(
    "workspace, hash_name, hash_value",
    [
        (
            pytest.lazy_fixture("local_cloud"),
            "md5",
            "b6dcab6ccd17ca0a8bf4a215a37d14cc.dir",
        ),
        pytest.param(
            pytest.lazy_fixture("ssh"),
            "md5",
            "b6dcab6ccd17ca0a8bf4a215a37d14cc.dir",
            marks=pytest.mark.skipif(
                os.name == "nt", reason="disabled on windows"
            ),
        ),
        (
            pytest.lazy_fixture("s3"),
            "etag",
            "ec602a6ba97b2dd07bd6d2cd89674a60.dir",
        ),
    ],
    indirect=["workspace"],
)
def test_add_external_dir(tmp_dir, dvc, workspace, hash_name, hash_value):
    workspace.gen({"dir": {"file": "file", "subdir": {"subfile": "subfile"}}})

    dvc.add("remote://workspace/dir")
    assert (tmp_dir / "dir.dvc").read_text() == (
        "outs:\n"
        f"- {hash_name}: {hash_value}\n"
        "  size: 11\n"
        "  nfiles: 2\n"
        "  path: remote://workspace/dir\n"
    )
    assert (workspace / "cache" / hash_value[:2] / hash_value[2:]).is_file()


class TestAddLocalRemoteFile(TestDvc):
    def test(self):
        """
        Making sure that 'remote' syntax is handled properly for local outs.
        """
        cwd = os.getcwd()
        remote = "myremote"

        ret = main(["remote", "add", remote, cwd])
        self.assertEqual(ret, 0)

        self.dvc.config.load()

        foo = f"remote://{remote}/{self.FOO}"
        ret = main(["add", foo])
        self.assertEqual(ret, 0)

        d = load_yaml("foo.dvc")
        self.assertEqual(d["outs"][0]["path"], foo)

        bar = os.path.join(cwd, self.BAR)
        ret = main(["add", bar])
        self.assertEqual(ret, 0)

        d = load_yaml("bar.dvc")
        self.assertEqual(d["outs"][0]["path"], self.BAR)


class TestCmdAdd(TestDvc):
    def test(self):
        ret = main(["add", self.FOO])
        self.assertEqual(ret, 0)

        ret = main(["add", "non-existing-file"])
        self.assertNotEqual(ret, 0)


class TestDoubleAddUnchanged(TestDvc):
    def test_file(self):
        ret = main(["add", self.FOO])
        self.assertEqual(ret, 0)

        ret = main(["add", self.FOO])
        self.assertEqual(ret, 0)

    def test_dir(self):
        ret = main(["add", self.DATA_DIR])
        self.assertEqual(ret, 0)

        ret = main(["add", self.DATA_DIR])
        self.assertEqual(ret, 0)


def test_should_update_state_entry_for_file_after_add(mocker, dvc, tmp_dir):
    file_md5_counter = mocker.spy(dvc_module.objects.stage, "file_md5")
    tmp_dir.gen("foo", "foo")

    ret = main(["config", "cache.type", "copy"])
    assert ret == 0

    ret = main(["add", "foo"])
    assert ret == 0
    assert file_md5_counter.mock.call_count == 1

    ret = main(["status"])
    assert ret == 0
    assert file_md5_counter.mock.call_count == 1

    ret = main(["run", "--single-stage", "-d", "foo", "echo foo"])
    assert ret == 0
    assert file_md5_counter.mock.call_count == 1

    os.rename("foo", "foo.back")
    ret = main(["checkout"])
    assert ret == 0
    assert file_md5_counter.mock.call_count == 1

    ret = main(["status"])
    assert ret == 0
    assert file_md5_counter.mock.call_count == 1


def test_should_update_state_entry_for_directory_after_add(
    mocker, dvc, tmp_dir
):
    file_md5_counter = mocker.spy(dvc_module.objects.stage, "file_md5")

    tmp_dir.gen({"data/data": "foo", "data/data_sub/sub_data": "foo"})

    ret = main(["config", "cache.type", "copy"])
    assert ret == 0

    ret = main(["add", "data"])
    assert ret == 0
    assert file_md5_counter.mock.call_count == 3

    ret = main(["status"])
    assert ret == 0
    assert file_md5_counter.mock.call_count == 3

    ls = "dir" if os.name == "nt" else "ls"
    ret = main(
        ["run", "--single-stage", "-d", "data", "{} {}".format(ls, "data")]
    )
    assert ret == 0
    assert file_md5_counter.mock.call_count == 3

    os.rename("data", "data" + ".back")
    ret = main(["checkout"])
    assert ret == 0
    assert file_md5_counter.mock.call_count == 3

    ret = main(["status"])
    assert ret == 0
    assert file_md5_counter.mock.call_count == 3


class TestAddCommit(TestDvc):
    def test(self):
        ret = main(["add", self.FOO, "--no-commit"])
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile(self.FOO))
        self.assertFalse(os.path.exists(self.dvc.odb.local.cache_dir))

        ret = main(["commit", self.FOO + ".dvc"])
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile(self.FOO))
        self.assertEqual(len(os.listdir(self.dvc.odb.local.cache_dir)), 1)


def test_should_collect_dir_cache_only_once(mocker, tmp_dir, dvc):
    tmp_dir.gen({"data/data": "foo"})
    counter = mocker.spy(dvc_module.objects.stage, "_get_tree_obj")
    ret = main(["add", "data"])
    assert ret == 0

    ret = main(["status"])
    assert ret == 0

    ret = main(["status"])
    assert ret == 0
    assert counter.mock.call_count == 1


class TestShouldPlaceStageInDataDirIfRepositoryBelowSymlink(TestDvc):
    def test(self):
        def is_symlink_true_below_dvc_root(path):
            if path == os.path.dirname(self.dvc.root_dir):
                return True
            return False

        with patch.object(
            System, "is_symlink", side_effect=is_symlink_true_below_dvc_root
        ):

            ret = main(["add", self.DATA])
            self.assertEqual(0, ret)

            stage_file_path_on_data_below_symlink = (
                os.path.basename(self.DATA) + DVC_FILE_SUFFIX
            )
            self.assertFalse(
                os.path.exists(stage_file_path_on_data_below_symlink)
            )

            stage_file_path = self.DATA + DVC_FILE_SUFFIX
            self.assertTrue(os.path.exists(stage_file_path))


class TestShouldThrowProperExceptionOnCorruptedStageFile(TestDvc):
    def test(self):
        ret = main(["add", self.FOO])
        assert 0 == ret

        foo_stage = relpath(self.FOO + DVC_FILE_SUFFIX)

        # corrupt stage file
        with open(foo_stage, "a+") as file:
            file.write("this will break yaml file structure")

        self._caplog.clear()

        ret = main(["add", self.BAR])
        assert 1 == ret

        expected_error = (
            f"unable to read: '{foo_stage}', YAML file structure is corrupted"
        )

        assert expected_error in self._caplog.text


class TestAddFilename(TestDvc):
    def test(self):
        ret = main(["add", self.FOO, self.BAR, "--file", "error.dvc"])
        self.assertNotEqual(0, ret)

        ret = main(["add", "-R", self.DATA_DIR, "--file", "error.dvc"])
        self.assertNotEqual(0, ret)

        with self.assertRaises(RecursiveAddingWhileUsingFilename):
            self.dvc.add(self.DATA_DIR, recursive=True, fname="error.dvc")

        ret = main(["add", self.DATA_DIR, "--file", "data_directory.dvc"])
        self.assertEqual(0, ret)
        self.assertTrue(os.path.exists("data_directory.dvc"))

        ret = main(["add", self.FOO, "--file", "bar.dvc"])
        self.assertEqual(0, ret)
        self.assertTrue(os.path.exists("bar.dvc"))
        self.assertFalse(os.path.exists("foo.dvc"))

        os.remove("bar.dvc")

        ret = main(["add", self.FOO, "--file", "bar.dvc"])
        self.assertEqual(0, ret)
        self.assertTrue(os.path.exists("bar.dvc"))
        self.assertFalse(os.path.exists("foo.dvc"))


def test_failed_add_cleanup(tmp_dir, scm, dvc):
    tmp_dir.gen({"foo": "foo", "bar": "bar"})

    # Add and corrupt a stage file
    dvc.add("foo")
    tmp_dir.gen("foo.dvc", "- broken\nyaml")

    with pytest.raises(YAMLFileCorruptedError):
        dvc.add("bar")

    assert not os.path.exists("bar.dvc")

    gitignore_content = get_gitignore_content()
    assert "/bar" not in gitignore_content


def test_should_not_track_git_internal_files(mocker, dvc, tmp_dir):
    stage_creator_spy = mocker.spy(dvc_module.repo.add, "_create_stages")

    ret = main(["add", "-R", dvc.root_dir])
    assert ret == 0

    created_stages_filenames = stage_creator_spy.mock.call_args[0][1]
    for fname in created_stages_filenames:
        assert ".git" not in fname


class TestAddUnprotected(TestDvc):
    def test(self):
        ret = main(["config", "cache.type", "hardlink"])
        self.assertEqual(ret, 0)

        ret = main(["add", self.FOO])
        self.assertEqual(ret, 0)

        self.assertFalse(os.access(self.FOO, os.W_OK))
        self.assertTrue(System.is_hardlink(self.FOO))

        ret = main(["unprotect", self.FOO])
        self.assertEqual(ret, 0)

        ret = main(["add", self.FOO])
        self.assertEqual(ret, 0)

        self.assertFalse(os.access(self.FOO, os.W_OK))
        self.assertTrue(System.is_hardlink(self.FOO))


@pytest.fixture
def temporary_windows_drive(tmp_path_factory):
    import string
    from ctypes import windll

    import win32api  # pylint: disable=import-error
    from win32con import DDD_REMOVE_DEFINITION  # pylint: disable=import-error

    drives = [
        s[0].upper()
        for s in win32api.GetLogicalDriveStrings().split("\000")
        if len(s) > 0
    ]

    new_drive_name = [
        letter for letter in string.ascii_uppercase if letter not in drives
    ][0]
    new_drive = f"{new_drive_name}:"

    target_path = tmp_path_factory.mktemp("tmp_windows_drive")

    set_up_result = windll.kernel32.DefineDosDeviceW(
        0, new_drive, os.fspath(target_path)
    )
    if set_up_result == 0:
        raise RuntimeError("Failed to mount windows drive!")

    # NOTE: new_drive has form of `A:` and joining it with some relative
    # path might result in non-existing path (A:path\\to)
    yield os.path.join(new_drive, os.sep)

    tear_down_result = windll.kernel32.DefineDosDeviceW(
        DDD_REMOVE_DEFINITION, new_drive, os.fspath(target_path)
    )
    if tear_down_result == 0:
        raise RuntimeError("Could not unmount windows drive!")


@pytest.mark.skipif(os.name != "nt", reason="Windows specific")
def test_windows_should_add_when_cache_on_different_drive(
    tmp_dir, dvc, temporary_windows_drive
):
    dvc.config["cache"]["dir"] = temporary_windows_drive
    dvc.odb = ODBManager(dvc)

    (stage,) = tmp_dir.dvc_gen({"file": "file"})
    cache_path = stage.outs[0].cache_path

    assert path_isin(cache_path, temporary_windows_drive)
    assert os.path.isfile(cache_path)
    filecmp.cmp("file", cache_path)


def test_readding_dir_should_not_unprotect_all(tmp_dir, dvc, mocker):
    tmp_dir.gen("dir/data", "data")

    dvc.odb.local.cache_types = ["symlink"]

    dvc.add("dir")
    tmp_dir.gen("dir/new_file", "new_file_content")

    unprotect_spy = mocker.spy(dvc.odb.local, "unprotect")
    dvc.add("dir")

    assert not unprotect_spy.mock.called
    assert System.is_symlink(os.path.join("dir", "new_file"))


def test_should_not_checkout_when_adding_cached_copy(tmp_dir, dvc, mocker):
    dvc.odb.local.cache_types = ["copy"]

    tmp_dir.dvc_gen({"foo": "foo", "bar": "bar"})

    shutil.copy("bar", "foo")

    copy_spy = mocker.spy(dvc.odb.local.fs, "copy")

    dvc.add("foo")

    assert copy_spy.mock.call_count == 0


@pytest.mark.parametrize(
    "link,new_link,link_test_func",
    [
        ("hardlink", "copy", lambda path: not System.is_hardlink(path)),
        ("symlink", "copy", lambda path: not System.is_symlink(path)),
        ("copy", "hardlink", System.is_hardlink),
        ("copy", "symlink", System.is_symlink),
    ],
)
def test_should_relink_on_repeated_add(
    link, new_link, link_test_func, tmp_dir, dvc
):
    from dvc.path_info import PathInfo

    dvc.config["cache"]["type"] = link

    tmp_dir.dvc_gen({"foo": "foo", "bar": "bar"})

    os.remove("foo")
    getattr(dvc.odb.local.fs, link)(PathInfo("bar"), PathInfo("foo"))

    dvc.odb.local.cache_types = [new_link]

    dvc.add("foo")

    assert link_test_func("foo")


@pytest.mark.parametrize("link", ["hardlink", "symlink", "copy"])
def test_should_protect_on_repeated_add(link, tmp_dir, dvc):
    dvc.odb.local.cache_types = [link]

    tmp_dir.dvc_gen({"foo": "foo"})

    dvc.unprotect("foo")

    dvc.add("foo")

    assert not os.access(
        os.path.join(".dvc", "cache", "ac", "bd18db4cc2f85cedef654fccc4a4d8"),
        os.W_OK,
    )

    # NOTE: Windows symlink perms don't propagate to the target
    if link == "copy" or (link == "symlink" and os.name == "nt"):
        assert os.access("foo", os.W_OK)
    else:
        assert not os.access("foo", os.W_OK)


def test_escape_gitignore_entries(tmp_dir, scm, dvc):
    fname = "file!with*weird#naming_[1].t?t"
    ignored_fname = r"/file\!with\*weird\#naming_\[1\].t\?t"

    if os.name == "nt":
        # Some characters are not supported by Windows in the filename
        # https://docs.microsoft.com/en-us/windows/win32/fileio/naming-a-file
        fname = "file!with_weird#naming_[1].txt"
        ignored_fname = r"/file\!with_weird\#naming_\[1\].txt"

    tmp_dir.dvc_gen(fname, "...")
    assert ignored_fname in get_gitignore_content()


def test_add_from_data_dir(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen({"dir": {"file1": "file1 content"}})

    tmp_dir.gen({"dir": {"file2": "file2 content"}})

    with pytest.raises(OverlappingOutputPathsError) as e:
        dvc.add(os.path.join("dir", "file2"), fname="file2.dvc")
    assert str(e.value) == (
        "Cannot add '{out}', because it is overlapping with other DVC "
        "tracked output: 'dir'.\n"
        "To include '{out}' in 'dir', run 'dvc commit dir.dvc'"
    ).format(out=os.path.join("dir", "file2"))


def test_not_raises_on_re_add(tmp_dir, dvc):
    tmp_dir.dvc_gen("file", "file content")

    tmp_dir.gen({"file2": "file2 content", "file": "modified file"})
    dvc.add(["file2", "file"])


@pytest.mark.parametrize("link", ["hardlink", "symlink", "copy"])
def test_add_empty_files(tmp_dir, dvc, link):
    file = "foo"
    dvc.odb.local.cache_types = [link]
    stages = tmp_dir.dvc_gen(file, "")

    assert (tmp_dir / file).exists()
    assert (tmp_dir / (file + DVC_FILE_SUFFIX)).exists()
    assert os.path.exists(stages[0].outs[0].cache_path)


def test_add_optimization_for_hardlink_on_empty_files(tmp_dir, dvc, mocker):
    dvc.odb.local.cache_types = ["hardlink"]
    tmp_dir.gen({"foo": "", "bar": "", "lorem": "lorem", "ipsum": "ipsum"})
    m = mocker.spy(LocalFileSystem, "is_hardlink")
    stages = dvc.add(["foo", "bar", "lorem", "ipsum"])

    assert m.call_count == 1
    assert m.call_args != call(tmp_dir / "foo")
    assert m.call_args != call(tmp_dir / "bar")

    for stage in stages[:2]:
        # hardlinks are not created for empty files
        assert not System.is_hardlink(stage.outs[0].path_info)

    for stage in stages[2:]:
        assert System.is_hardlink(stage.outs[0].path_info)

    for stage in stages:
        assert os.path.exists(stage.path)
        assert os.path.exists(stage.outs[0].cache_path)


def test_output_duplication_for_pipeline_tracked(tmp_dir, dvc, run_copy):
    tmp_dir.dvc_gen("foo", "foo")
    run_copy("foo", "bar", name="copy-foo-bar")
    with pytest.raises(OutputDuplicationError):
        dvc.add("bar")


def test_add_pipeline_file(tmp_dir, dvc, run_copy):
    from dvc.dvcfile import PIPELINE_FILE

    tmp_dir.dvc_gen("foo", "foo")
    run_copy("foo", "bar", name="copy-foo-bar")

    with pytest.raises(OutputIsStageFileError):
        dvc.add(PIPELINE_FILE)


def test_add_symlink_file(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"bar": "bar"}})

    (tmp_dir / "dir" / "foo").symlink_to(os.path.join(".", "bar"))

    dvc.add(os.path.join("dir", "foo"))

    assert not (tmp_dir / "foo.dvc").exists()
    assert (tmp_dir / "dir" / "foo.dvc").exists()
    assert not (tmp_dir / "dir" / "foo").is_symlink()
    assert not (tmp_dir / "dir" / "bar").is_symlink()
    assert (tmp_dir / "dir" / "foo").read_text() == "bar"
    assert (tmp_dir / "dir" / "bar").read_text() == "bar"

    assert (tmp_dir / ".dvc" / "cache").read_text() == {
        "37": {"b51d194a7513e45b56f6524f2d51f2": "bar"}
    }
    assert not (
        tmp_dir / ".dvc" / "cache" / "37" / "b51d194a7513e45b56f6524f2d51f2"
    ).is_symlink()

    # Test that subsequent add succeeds
    # See https://github.com/iterative/dvc/issues/4654
    dvc.add(os.path.join("dir", "foo"))


@pytest.mark.parametrize("external", [True, False])
def test_add_symlink_dir(make_tmp_dir, tmp_dir, dvc, external):
    if external:
        data_dir = make_tmp_dir("data")
        data_dir.gen({"foo": "foo"})
        target = os.fspath(data_dir)
    else:
        tmp_dir.gen({"data": {"foo": "foo"}})
        target = os.path.join(".", "data")

    tmp_dir.gen({"data": {"foo": "foo"}})

    (tmp_dir / "dir").symlink_to(target)

    with pytest.raises(DvcException):
        dvc.add("dir")


@pytest.mark.parametrize("external", [True, False])
def test_add_file_in_symlink_dir(make_tmp_dir, tmp_dir, dvc, external):
    if external:
        data_dir = make_tmp_dir("data")
        data_dir.gen({"dir": {"foo": "foo"}})
        target = os.fspath(data_dir / "dir")
    else:
        tmp_dir.gen({"data": {"foo": "foo"}})
        target = os.path.join(".", "data")

    (tmp_dir / "dir").symlink_to(target)

    with pytest.raises(DvcException):
        dvc.add(os.path.join("dir", "foo"))


def test_add_with_cache_link_error(tmp_dir, dvc, mocker, caplog):
    tmp_dir.gen("foo", "foo")

    mocker.patch(
        "dvc.checkout._do_link", side_effect=DvcException("link failed")
    )
    with caplog.at_level(logging.WARNING, logger="dvc"):
        dvc.add("foo")
        assert "reconfigure cache types" in caplog.text

    assert not (tmp_dir / "foo").exists()
    assert (tmp_dir / "foo.dvc").exists()
    assert (tmp_dir / ".dvc" / "cache").read_text() == {
        "ac": {"bd18db4cc2f85cedef654fccc4a4d8": "foo"}
    }


def test_add_preserve_meta(tmp_dir, dvc):
    text = textwrap.dedent(
        """\
        # top comment
        desc: top desc
        outs:
        - path: foo # out comment
          desc: out desc
        meta: some metadata
    """
    )
    tmp_dir.gen("foo.dvc", text)

    tmp_dir.dvc_gen("foo", "foo")
    assert (tmp_dir / "foo.dvc").read_text() == textwrap.dedent(
        """\
        # top comment
        desc: top desc
        outs:
        - path: foo # out comment
          desc: out desc
          md5: acbd18db4cc2f85cedef654fccc4a4d8
          size: 3
        meta: some metadata
    """
    )


# NOTE: unless long paths are enabled on Windows, PATH_MAX and NAME_MAX
# are the same 260 chars, which makes the test unnecessarily complex
@pytest.mark.skipif(os.name == "nt", reason="unsupported on Windows")
def test_add_long_fname(tmp_dir, dvc):
    name_max = os.pathconf(tmp_dir, "PC_NAME_MAX")
    name = "a" * name_max
    tmp_dir.gen({"data": {name: "foo"}})

    # nothing we can do in this case, as the resulting dvcfile
    # will definitely exceed NAME_MAX
    with pytest.raises(OSError) as info:
        dvc.add(os.path.join("data", name))
    assert info.value.errno == errno.ENAMETOOLONG

    dvc.add("data")
    assert (tmp_dir / "data").read_text() == {name: "foo"}


def test_add_to_remote(tmp_dir, dvc, local_cloud, local_remote):
    local_cloud.gen("foo", "foo")

    url = "remote://upstream/foo"
    [stage] = dvc.add(url, to_remote=True)

    assert not (tmp_dir / "foo").exists()
    assert (tmp_dir / "foo.dvc").exists()

    assert len(stage.deps) == 0
    assert len(stage.outs) == 1

    hash_info = stage.outs[0].hash_info
    assert local_remote.hash_to_path_info(hash_info.value).read_text() == "foo"


def test_add_to_remote_absolute(tmp_dir, make_tmp_dir, dvc, local_remote):
    tmp_abs_dir = make_tmp_dir("abs")
    tmp_foo = tmp_abs_dir / "foo"
    tmp_foo.write_text("foo")

    dvc.add(str(tmp_foo), to_remote=True)
    tmp_foo.unlink()

    foo = tmp_dir / "foo"
    assert foo.with_suffix(".dvc").exists()
    assert not os.path.exists(tmp_foo)

    dvc.pull("foo")
    assert not os.path.exists(tmp_foo)
    assert foo.read_text() == "foo"

    with pytest.raises(StageExternalOutputsError):
        tmp_bar = tmp_abs_dir / "bar"
        dvc.add(str(tmp_foo), out=str(tmp_bar), to_remote=True)


@pytest.mark.parametrize(
    "invalid_opt, kwargs",
    [
        ("multiple targets", {"targets": ["foo", "bar", "baz"]}),
        ("--no-commit", {"targets": ["foo"], "no_commit": True}),
        ("--recursive", {"targets": ["foo"], "recursive": True}),
        ("--external", {"targets": ["foo"], "external": True}),
    ],
)
def test_add_to_remote_invalid_combinations(dvc, invalid_opt, kwargs):
    with pytest.raises(InvalidArgumentError, match=invalid_opt):
        dvc.add(to_remote=True, **kwargs)


def test_add_to_cache_dir(tmp_dir, dvc, local_cloud):
    local_cloud.gen({"data": {"foo": "foo", "bar": "bar"}})

    (stage,) = dvc.add(str(local_cloud / "data"), out="data")
    assert len(stage.deps) == 0
    assert len(stage.outs) == 1

    data = tmp_dir / "data"
    assert data.read_text() == {"foo": "foo", "bar": "bar"}
    assert (tmp_dir / "data.dvc").exists()

    shutil.rmtree(data)
    status = dvc.checkout(str(data))
    assert status["added"] == ["data" + os.sep]
    assert data.read_text() == {"foo": "foo", "bar": "bar"}


def test_add_to_cache_file(tmp_dir, dvc, local_cloud):
    local_cloud.gen("foo", "foo")

    (stage,) = dvc.add(str(local_cloud / "foo"), out="foo")
    assert len(stage.deps) == 0
    assert len(stage.outs) == 1

    foo = tmp_dir / "foo"
    assert foo.read_text() == "foo"
    assert (tmp_dir / "foo.dvc").exists()

    foo.unlink()
    status = dvc.checkout(str(foo))
    assert status["added"] == ["foo"]
    assert foo.read_text() == "foo"


def test_add_to_cache_different_name(tmp_dir, dvc, local_cloud):
    local_cloud.gen({"data": {"foo": "foo", "bar": "bar"}})

    dvc.add(str(local_cloud / "data"), out="not_data")

    not_data = tmp_dir / "not_data"
    assert not_data.read_text() == {"foo": "foo", "bar": "bar"}
    assert (tmp_dir / "not_data.dvc").exists()

    assert not (tmp_dir / "data").exists()
    assert not (tmp_dir / "data.dvc").exists()

    shutil.rmtree(not_data)
    dvc.checkout(str(not_data))
    assert not_data.read_text() == {"foo": "foo", "bar": "bar"}
    assert not (tmp_dir / "data").exists()


def test_add_to_cache_not_exists(tmp_dir, dvc, local_cloud):
    local_cloud.gen({"data": {"foo": "foo", "bar": "bar"}})

    dest_dir = tmp_dir / "dir" / "that" / "does" / "not" / "exist"
    with pytest.raises(StagePathNotFoundError):
        dvc.add(str(local_cloud / "data"), out=str(dest_dir))

    dest_dir.parent.mkdir(parents=True)
    dvc.add(str(local_cloud / "data"), out=str(dest_dir))

    assert dest_dir.read_text() == {"foo": "foo", "bar": "bar"}
    assert dest_dir.with_suffix(".dvc").exists()


@pytest.mark.parametrize(
    "invalid_opt, kwargs",
    [
        ("multiple targets", {"targets": ["foo", "bar", "baz"]}),
        ("--no-commit", {"targets": ["foo"], "no_commit": True}),
        ("--recursive", {"targets": ["foo"], "recursive": True}),
    ],
)
def test_add_to_cache_invalid_combinations(dvc, invalid_opt, kwargs):
    with pytest.raises(InvalidArgumentError, match=invalid_opt):
        dvc.add(out="bar", **kwargs)


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
def test_add_to_cache_from_remote(tmp_dir, dvc, workspace):
    workspace.gen("foo", "foo")

    url = "remote://workspace/foo"
    dvc.add(url, out="foo")

    foo = tmp_dir / "foo"
    assert foo.read_text() == "foo"
    assert (tmp_dir / "foo.dvc").exists()

    # Change the contents of the remote location, in order to
    # ensure it retrieves file from the cache and not re-fetches it
    (workspace / "foo").write_text("bar")

    foo.unlink()
    dvc.checkout(str(foo))
    assert foo.read_text() == "foo"


def test_add_ignored(tmp_dir, scm, dvc):
    from dvc.dvcfile import FileIsGitIgnored

    tmp_dir.gen({"dir": {"subdir": {"file": "content"}}, ".gitignore": "dir/"})
    with pytest.raises(FileIsGitIgnored) as exc:
        dvc.add(targets=[os.path.join("dir", "subdir")])
    assert str(exc.value) == ("bad DVC file name '{}' is git-ignored.").format(
        os.path.join("dir", "subdir.dvc")
    )
