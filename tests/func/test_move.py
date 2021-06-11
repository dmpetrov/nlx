import os

import pytest

from dvc.dvcfile import DVC_FILE_SUFFIX
from dvc.exceptions import DvcException, MoveNotDataSourceError
from dvc.main import main
from dvc.utils.serialize import load_yaml
from tests.basic_env import TestDvc, TestDvcGit
from tests.func.test_repro import TestRepro
from tests.utils import cd


class TestMove(TestDvc):
    def test(self):
        dst = self.FOO + "1"
        self.dvc.add(self.FOO)
        self.dvc.move(self.FOO, dst)

        self.assertFalse(os.path.isfile(self.FOO))
        self.assertTrue(os.path.isfile(dst))


class TestMoveNonExistentFile(TestDvc):
    def test(self):
        with self.assertRaises(DvcException):
            self.dvc.move("non_existent_file", "dst")


class TestMoveDirectory(TestDvc):
    def test(self):
        dst = "dst"
        stages = self.dvc.add(self.DATA_DIR)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)
        self.dvc.move(self.DATA_DIR, dst)
        self.assertFalse(os.path.exists(self.DATA_DIR))
        self.assertTrue(os.path.exists(dst))


class TestCmdMove(TestDvc):
    def test(self):
        stages = self.dvc.add(self.FOO)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)

        ret = main(["move", self.FOO, self.FOO + "1"])
        self.assertEqual(ret, 0)

        ret = main(["move", "non-existing-file", "dst"])
        self.assertNotEqual(ret, 0)


class TestMoveNotDataSource(TestRepro):
    def test(self):
        from dvc.repo import Repo as DvcRepo

        self.dvc = DvcRepo(self._root_dir)
        with self.assertRaises(MoveNotDataSourceError):
            self.dvc.move(self.file1, "dst")

        ret = main(["move", self.file1, "dst"])
        self.assertNotEqual(ret, 0)


class TestMoveFileWithExtension(TestDvc):
    def test(self):
        with open(os.path.join(self.dvc.root_dir, "file.csv"), "w") as fd:
            fd.write("1,2,3\n")

        self.dvc.add("file.csv")

        self.assertTrue(os.path.exists("file.csv"))
        self.assertTrue(os.path.exists("file.csv.dvc"))

        ret = main(["move", "file.csv", "other_name.csv"])
        self.assertEqual(ret, 0)

        self.assertFalse(os.path.exists("file.csv"))
        self.assertFalse(os.path.exists("file.csv.dvc"))
        self.assertTrue(os.path.exists("other_name.csv"))
        self.assertTrue(os.path.exists("other_name.csv.dvc"))


class TestMoveFileToDirectory(TestDvc):
    def test(self):
        foo_dvc_file = self.FOO + DVC_FILE_SUFFIX
        ret = main(["add", self.FOO])
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.exists(foo_dvc_file))

        new_foo_path = os.path.join(self.DATA_DIR, self.FOO)
        new_foo_dvc_path = new_foo_path + DVC_FILE_SUFFIX
        ret = main(["move", self.FOO, new_foo_path])
        self.assertEqual(ret, 0)

        self.assertFalse(os.path.exists(self.FOO))
        self.assertFalse(os.path.exists(foo_dvc_file))
        self.assertTrue(os.path.exists(new_foo_path))
        self.assertTrue(os.path.exists(new_foo_dvc_path))


class TestMoveFileToDirectoryWithoutSpecifiedTargetName(TestDvc):
    def test(self):
        foo_stage_file_path = self.FOO + DVC_FILE_SUFFIX
        ret = main(["add", self.FOO])
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.exists(foo_stage_file_path))

        target_foo_path = os.path.join(self.DATA_DIR, self.FOO)
        target_foo_stage_file_path = target_foo_path + DVC_FILE_SUFFIX

        ret = main(["move", self.FOO, self.DATA_DIR])
        self.assertEqual(ret, 0)

        self.assertFalse(os.path.exists(self.FOO))
        self.assertFalse(os.path.exists(foo_stage_file_path))

        self.assertTrue(os.path.exists(target_foo_path))
        self.assertTrue(os.path.exists(target_foo_stage_file_path))

        new_stage = load_yaml(target_foo_stage_file_path)
        self.assertEqual(self.FOO, new_stage["outs"][0]["path"])


class TestMoveDirectoryShouldNotOverwriteExisting(TestDvcGit):
    def test(self):
        dir_name = "dir"
        orig_listdir = set(os.listdir(self.DATA_DIR))

        self.dvc.add(self.DATA_DIR)

        os.mkdir(dir_name)
        new_dir_name = os.path.join(dir_name, self.DATA_DIR)

        self.dvc.move(self.DATA_DIR, dir_name)

        data_dir_stage = self.DATA_DIR + DVC_FILE_SUFFIX
        self.assertFalse(os.path.exists(self.DATA_DIR))
        self.assertFalse(os.path.exists(data_dir_stage))

        self.assertTrue(os.path.exists(dir_name))
        self.assertEqual(
            set(os.listdir(dir_name)),
            {".gitignore", data_dir_stage, self.DATA_DIR},
        )

        self.assertTrue(os.path.exists(new_dir_name))
        self.assertTrue(os.path.isfile(new_dir_name + DVC_FILE_SUFFIX))
        self.assertEqual(set(os.listdir(new_dir_name)), orig_listdir)


class TestMoveFileBetweenDirectories(TestDvc):
    def test(self):
        data_stage_file = self.DATA + DVC_FILE_SUFFIX
        ret = main(["add", self.DATA])
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.exists(data_stage_file))

        new_data_dir = "data_dir2"
        os.makedirs(new_data_dir)

        ret = main(["move", self.DATA, new_data_dir])
        self.assertEqual(ret, 0)

        new_data_path = os.path.join(new_data_dir, os.path.basename(self.DATA))
        new_data_stage_file = new_data_path + DVC_FILE_SUFFIX

        self.assertFalse(os.path.exists(self.DATA))
        self.assertFalse(os.path.exists(data_stage_file))

        self.assertTrue(os.path.exists(new_data_path))
        self.assertTrue(os.path.exists(new_data_stage_file))

        new_stage_file = load_yaml(new_data_stage_file)
        self.assertEqual(
            os.path.basename(self.DATA), new_stage_file["outs"][0]["path"]
        )


class TestMoveFileInsideDirectory(TestDvc):
    def test(self):
        ret = main(["add", self.DATA])
        self.assertEqual(ret, 0)

        with cd(self.DATA_DIR):
            ret = main(["move", os.path.basename(self.DATA), "data.txt"])
            self.assertEqual(ret, 0)

        self.assertFalse(os.path.exists(self.DATA))

        data_fullpath = os.path.join(self.DATA_DIR, "data.txt")
        dvc_fullpath = os.path.join(self.DATA_DIR, "data.txt.dvc")
        self.assertTrue(os.path.exists(data_fullpath))
        self.assertTrue(os.path.exists(dvc_fullpath))


def test_move_should_save_stage_info(tmp_dir, dvc):
    tmp_dir.dvc_gen({"old_name": {"file1": "file1"}})

    dvc.move("old_name", "new_name")

    assert dvc.status() == {}


def test_should_move_to_dir_on_non_default_stage_file(tmp_dir, dvc):
    stage_file_name = "stage.dvc"
    tmp_dir.gen({"file": "file_content"})

    dvc.add("file", fname=stage_file_name)
    os.mkdir("directory")

    dvc.move("file", "directory")

    assert os.path.exists(os.path.join("directory", "file"))


def test_move_gitignored(tmp_dir, scm, dvc):
    from dvc.dvcfile import FileIsGitIgnored

    tmp_dir.dvc_gen({"foo": "foo"})

    os.mkdir("dir")
    (tmp_dir / "dir").gen(".gitignore", "*")

    with pytest.raises(FileIsGitIgnored):
        dvc.move("foo", "dir")

    assert (tmp_dir / "foo").read_text() == "foo"
    assert (tmp_dir / "foo.dvc").exists()
    assert not (tmp_dir / "dir" / "foo").exists()
    assert not (tmp_dir / "dir" / "foo.dvc").exists()


def test_move_output_overlap(tmp_dir, dvc):
    from dvc.exceptions import OverlappingOutputPathsError

    tmp_dir.dvc_gen({"foo": "foo", "dir": {"bar": "bar"}})

    with pytest.raises(OverlappingOutputPathsError):
        dvc.move("foo", "dir")

    assert (tmp_dir / "foo").read_text() == "foo"
    assert (tmp_dir / "foo.dvc").exists()
    assert not (tmp_dir / "dir" / "foo").exists()
    assert not (tmp_dir / "dir" / "foo.dvc").exists()
