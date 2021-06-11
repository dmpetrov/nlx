import filecmp
import os
from unittest import TestCase

import pytest
from mock import patch

import dvc
from dvc.fs.local import LocalFileSystem
from dvc.path_info import PathInfo
from dvc.system import System
from dvc.utils import relpath
from dvc.utils.fs import (
    BasePathNotInCheckedPathException,
    contains_symlink_up_to,
    copy_fobj_to_file,
    copyfile,
    get_inode,
    get_mtime_and_size,
    makedirs,
    move,
    path_isin,
    remove,
    walk_files,
)
from tests.basic_env import TestDir


class TestMtimeAndSize(TestDir):
    def test(self):
        fs = LocalFileSystem(url=self.root_dir)
        file_time, file_size = get_mtime_and_size(self.DATA, fs)
        dir_time, dir_size = get_mtime_and_size(self.DATA_DIR, fs)

        actual_file_size = os.path.getsize(self.DATA)
        actual_dir_size = os.path.getsize(self.DATA) + os.path.getsize(
            self.DATA_SUB
        )

        self.assertIs(type(file_time), str)
        self.assertIs(type(file_size), str)
        self.assertEqual(file_size, str(actual_file_size))
        self.assertIs(type(dir_time), str)
        self.assertIs(type(dir_size), str)
        self.assertEqual(dir_size, str(actual_dir_size))


class TestContainsLink(TestCase):
    def test_should_raise_exception_on_base_path_not_in_path(self):
        with self.assertRaises(BasePathNotInCheckedPathException):
            contains_symlink_up_to(os.path.join("foo", "path"), "bar")

    @patch.object(System, "is_symlink", return_value=True)
    def test_should_return_true_on_symlink_in_path(self, _):
        base_path = "foo"
        path = os.path.join(base_path, "bar")
        self.assertTrue(contains_symlink_up_to(path, base_path))

    @patch.object(System, "is_symlink", return_value=False)
    def test_should_return_false_on_path_eq_to_base_path(self, _):
        path = "path"
        self.assertFalse(contains_symlink_up_to(path, path))

    @patch.object(System, "is_symlink", return_value=False)
    @patch.object(os.path, "dirname", side_effect=lambda arg: arg)
    def test_should_return_false_on_no_more_dirs_below_path(
        self, dirname_patch, _
    ):
        self.assertFalse(
            contains_symlink_up_to(os.path.join("foo", "path"), "foo")
        )
        dirname_patch.assert_called_once()

    @patch.object(System, "is_symlink", return_value=True)
    def test_should_return_false_when_base_path_is_symlink(self, _):
        base_path = "foo"
        target_path = os.path.join(base_path, "bar")

        def base_path_is_symlink(path):
            if path == base_path:
                return True
            return False

        with patch.object(
            System, "is_symlink", side_effect=base_path_is_symlink
        ):
            self.assertFalse(contains_symlink_up_to(target_path, base_path))

    def test_path_object_and_str_are_valid_arg_types(self):
        base_path = "foo"
        target_path = os.path.join(base_path, "bar")
        self.assertFalse(contains_symlink_up_to(target_path, base_path))
        self.assertFalse(
            contains_symlink_up_to(PathInfo(target_path), PathInfo(base_path))
        )


def test_should_call_recursive_on_no_condition_matched(mocker):
    mocker.patch.object(System, "is_symlink", return_value=False)

    contains_symlink_spy = mocker.spy(dvc.utils.fs, "contains_symlink_up_to")

    # call from full path to match contains_symlink_spy patch path
    assert not dvc.utils.fs.contains_symlink_up_to(
        os.path.join("foo", "path"), "foo"
    )
    assert contains_symlink_spy.mock.call_count == 2


@pytest.mark.skipif(os.name != "nt", reason="Windows specific")
def test_relpath_windows_different_drives():
    path1 = os.path.join("A:", os.sep, "some", "path")
    path2 = os.path.join("B:", os.sep, "other", "path")
    assert relpath(path1, path2) == path1

    info1, info2 = PathInfo(path1), PathInfo(path2)
    rel_info = relpath(info1, info2)
    assert isinstance(rel_info, str)
    assert rel_info == path1


def test_get_inode(tmp_dir):
    tmp_dir.gen("foo", "foo content")

    assert get_inode("foo") == get_inode(PathInfo("foo"))


def test_path_object_and_str_are_valid_types_get_mtime_and_size(tmp_dir):
    tmp_dir.gen(
        {"dir": {"dir_file": "dir file content"}, "file": "file_content"}
    )
    fs = LocalFileSystem(url=os.fspath(tmp_dir))

    time, size = get_mtime_and_size("dir", fs)
    object_time, object_size = get_mtime_and_size(PathInfo("dir"), fs)
    assert time == object_time
    assert size == object_size

    time, size = get_mtime_and_size("file", fs)
    object_time, object_size = get_mtime_and_size(PathInfo("file"), fs)
    assert time == object_time
    assert size == object_size


def test_move(tmp_dir):
    tmp_dir.gen({"foo": "foo content", "bar": "bar content"})
    src = "foo"
    src_info = PathInfo("bar")
    dest = os.path.join("some", "directory")
    dest_info = PathInfo(os.path.join("some", "path-like", "directory"))

    os.makedirs(dest)
    assert len(os.listdir(dest)) == 0
    move(src, dest)
    assert not os.path.isfile(src)
    assert len(os.listdir(dest)) == 1

    os.makedirs(dest_info)
    assert len(os.listdir(dest_info)) == 0
    move(src_info, dest_info)
    assert not os.path.isfile(src_info)
    assert len(os.listdir(dest_info)) == 1


def test_remove(tmp_dir):
    tmp_dir.gen({"foo": "foo content", "bar": "bar content"})
    path = "foo"
    path_info = PathInfo("bar")

    remove(path)
    assert not os.path.isfile(path)

    remove(path_info)
    assert not os.path.isfile(path_info)


def test_path_isin_positive():
    child = os.path.join("path", "to", "folder")

    assert path_isin(child, os.path.join("path", "to", ""))
    assert path_isin(child, os.path.join("path", "to"))
    assert path_isin(child, os.path.join("path", ""))
    assert path_isin(child, os.path.join("path"))


def test_path_isin_on_same_path():
    path = os.path.join("path", "to", "folder")
    path_with_sep = os.path.join(path, "")

    assert not path_isin(path, path)
    assert not path_isin(path, path_with_sep)
    assert not path_isin(path_with_sep, path)
    assert not path_isin(path_with_sep, path_with_sep)


def test_path_isin_on_common_substring_path():
    path1 = os.path.join("path", "to", "folder1")
    path2 = os.path.join("path", "to", "folder")

    assert not path_isin(path1, path2)


def test_path_isin_accepts_pathinfo():
    child = os.path.join("path", "to", "folder")
    parent = PathInfo(child) / ".."

    assert path_isin(child, parent)
    # pylint: disable=arguments-out-of-order
    assert not path_isin(parent, child)


def test_path_isin_with_absolute_path():
    parent = os.path.abspath("path")
    child = os.path.join(parent, "to", "folder")

    assert path_isin(child, parent)


def test_makedirs(tmp_dir):
    path = os.path.join(tmp_dir, "directory")
    path_info = PathInfo(os.path.join(tmp_dir, "another", "directory"))

    makedirs(path)
    assert os.path.isdir(path)

    makedirs(path_info)
    assert os.path.isdir(path_info)


@pytest.mark.parametrize("path", ["file", "dir"])
def test_copyfile(path, tmp_dir):
    tmp_dir.gen(
        {
            "foo": "foo content",
            "bar": "bar content",
            "file": "file content",
            "dir": {},
        }
    )
    src = "foo"
    dest = path
    src_info = PathInfo("bar")
    dest_info = PathInfo(path)

    copyfile(src, dest)
    if os.path.isdir(dest):
        assert filecmp.cmp(
            src, os.path.join(dest, os.path.basename(src)), shallow=False
        )
    else:
        assert filecmp.cmp(src, dest, shallow=False)

    copyfile(src_info, dest_info)
    if os.path.isdir(dest_info):
        assert filecmp.cmp(
            src_info,
            os.path.join(dest_info, os.path.basename(src_info)),
            shallow=False,
        )
    else:
        assert filecmp.cmp(src_info, dest_info, shallow=False)


def test_copy_fobj_to_file(tmp_dir):
    tmp_dir.gen({"foo": "foo content"})
    src = tmp_dir / "foo"
    dest = "path"

    with open(src, "rb") as fobj:
        copy_fobj_to_file(fobj, dest)
    assert filecmp.cmp(src, dest)


def test_walk_files(tmp_dir):
    assert list(walk_files(".")) == list(walk_files(tmp_dir))
