import filecmp
import logging
import os
import textwrap
import uuid
from pathlib import Path

import mock
import pytest

from dvc.dependency.base import DependencyIsStageFileError
from dvc.dvcfile import DVC_FILE_SUFFIX
from dvc.exceptions import (
    ArgumentDuplicationError,
    CircularDependencyError,
    CyclicGraphError,
    OutputDuplicationError,
    OverlappingOutputPathsError,
    StagePathAsOutputError,
)
from dvc.main import main
from dvc.output import Output, OutputIsStageFileError
from dvc.repo import Repo as DvcRepo
from dvc.stage import Stage
from dvc.stage.exceptions import (
    StageFileAlreadyExistsError,
    StageFileBadNameError,
    StagePathNotDirectoryError,
    StagePathNotFoundError,
    StagePathOutsideError,
)
from dvc.system import System
from dvc.utils import file_md5
from dvc.utils.serialize import load_yaml
from tests.basic_env import TestDvc, TestDvcGit


class TestRun(TestDvc):
    def test(self):
        cmd = "python {} {} {}".format(self.CODE, self.FOO, "out")
        deps = [self.FOO, self.CODE]
        outs = [os.path.join(self.dvc.root_dir, "out")]
        outs_no_cache = []
        fname = "out.dvc"

        self.dvc.add(self.FOO)
        stage = self.dvc.run(
            cmd=cmd,
            deps=deps,
            outs=outs,
            outs_no_cache=outs_no_cache,
            fname=fname,
            single_stage=True,
        )

        self.assertTrue(filecmp.cmp(self.FOO, "out", shallow=False))
        self.assertTrue(os.path.isfile(stage.path))
        self.assertEqual(stage.cmd, cmd)
        self.assertEqual(len(stage.deps), len(deps))
        self.assertEqual(len(stage.outs), len(outs + outs_no_cache))
        self.assertEqual(stage.outs[0].fspath, outs[0])
        self.assertEqual(
            stage.outs[0].hash_info.value, file_md5(self.FOO, self.dvc.fs)
        )
        self.assertTrue(stage.path, fname)

        with self.assertRaises(OutputDuplicationError):
            self.dvc.run(
                cmd=cmd,
                deps=deps,
                outs=outs,
                outs_no_cache=outs_no_cache,
                fname="duplicate" + fname,
                single_stage=True,
            )


class TestRunEmpty(TestDvc):
    def test(self):
        self.dvc.run(
            cmd="echo hello world",
            deps=[],
            outs=[],
            outs_no_cache=[],
            fname="empty.dvc",
            single_stage=True,
        )


class TestRunMissingDep(TestDvc):
    def test(self):
        from dvc.dependency.base import DependencyDoesNotExistError

        with self.assertRaises(DependencyDoesNotExistError):
            self.dvc.run(
                cmd="command",
                deps=["non-existing-dep"],
                outs=[],
                outs_no_cache=[],
                fname="empty.dvc",
                single_stage=True,
            )


class TestRunNoExec(TestDvcGit):
    def test(self):
        self.dvc.run(
            cmd="python {} {} {}".format(self.CODE, self.FOO, "out"),
            deps=[self.CODE, self.FOO],
            outs=["out"],
            no_exec=True,
            single_stage=True,
        )
        self.assertFalse(os.path.exists("out"))
        with open(".gitignore") as fobj:
            self.assertEqual(fobj.read(), "/out\n")


class TestRunCircularDependency(TestDvc):
    def test(self):
        with self.assertRaises(CircularDependencyError):
            self.dvc.run(
                cmd="command",
                deps=[self.FOO],
                outs=[self.FOO],
                fname="circular-dependency.dvc",
                single_stage=True,
            )

    def test_outs_no_cache(self):
        with self.assertRaises(CircularDependencyError):
            self.dvc.run(
                cmd="command",
                deps=[self.FOO],
                outs_no_cache=[self.FOO],
                fname="circular-dependency.dvc",
                single_stage=True,
            )

    def test_non_normalized_paths(self):
        with self.assertRaises(CircularDependencyError):
            self.dvc.run(
                cmd="command",
                deps=["./foo"],
                outs=["foo"],
                fname="circular-dependency.dvc",
                single_stage=True,
            )

    def test_graph(self):
        self.dvc.run(
            deps=[self.FOO],
            outs=["bar.txt"],
            cmd="echo bar > bar.txt",
            single_stage=True,
        )

        self.dvc.run(
            deps=["bar.txt"],
            outs=["baz.txt"],
            cmd="echo baz > baz.txt",
            single_stage=True,
        )

        with self.assertRaises(CyclicGraphError):
            self.dvc.run(
                deps=["baz.txt"],
                outs=[self.FOO],
                cmd="echo baz > foo",
                single_stage=True,
            )


class TestRunDuplicatedArguments(TestDvc):
    def test(self):
        with self.assertRaises(ArgumentDuplicationError):
            self.dvc.run(
                cmd="command",
                deps=[],
                outs=[self.FOO, self.FOO],
                fname="circular-dependency.dvc",
                single_stage=True,
            )

    def test_outs_no_cache(self):
        with self.assertRaises(ArgumentDuplicationError):
            self.dvc.run(
                cmd="command",
                outs=[self.FOO],
                outs_no_cache=[self.FOO],
                fname="circular-dependency.dvc",
                single_stage=True,
            )

    def test_non_normalized_paths(self):
        with self.assertRaises(ArgumentDuplicationError):
            self.dvc.run(
                cmd="command",
                deps=[],
                outs=["foo", "./foo"],
                fname="circular-dependency.dvc",
                single_stage=True,
            )


class TestRunStageInsideOutput(TestDvc):
    def test_cwd(self):
        self.dvc.run(
            cmd=f"mkdir {self.DATA_DIR}",
            deps=[],
            outs=[self.DATA_DIR],
            single_stage=True,
        )

        with self.assertRaises(StagePathAsOutputError):
            self.dvc.run(
                cmd="command",
                fname=os.path.join(self.DATA_DIR, "inside-cwd.dvc"),
                single_stage=True,
            )

    def test_file_name(self):
        self.dvc.run(
            cmd=f"mkdir {self.DATA_DIR}",
            deps=[],
            outs=[self.DATA_DIR],
            single_stage=True,
        )

        with self.assertRaises(StagePathAsOutputError):
            self.dvc.run(
                cmd="command",
                outs=[self.FOO],
                fname=os.path.join(self.DATA_DIR, "inside-cwd.dvc"),
                single_stage=True,
            )


class TestRunBadCwd(TestDvc):
    def test(self):
        with self.assertRaises(StagePathOutsideError):
            self.dvc.run(cmd="command", wdir=self.mkdtemp(), single_stage=True)

    def test_same_prefix(self):
        with self.assertRaises(StagePathOutsideError):
            path = f"{self._root_dir}-{uuid.uuid4()}"
            os.mkdir(path)
            self.dvc.run(cmd="command", wdir=path, single_stage=True)


class TestRunBadWdir(TestDvc):
    def test(self):
        with self.assertRaises(StagePathOutsideError):
            self.dvc.run(cmd="command", wdir=self.mkdtemp(), single_stage=True)

    def test_same_prefix(self):
        with self.assertRaises(StagePathOutsideError):
            path = f"{self._root_dir}-{uuid.uuid4()}"
            os.mkdir(path)
            self.dvc.run(cmd="command", wdir=path, single_stage=True)

    def test_not_found(self):
        with self.assertRaises(StagePathNotFoundError):
            path = os.path.join(self._root_dir, str(uuid.uuid4()))
            self.dvc.run(cmd="command", wdir=path, single_stage=True)

    def test_not_dir(self):
        with self.assertRaises(StagePathNotDirectoryError):
            path = os.path.join(self._root_dir, str(uuid.uuid4()))
            os.mkdir(path)
            path = os.path.join(path, str(uuid.uuid4()))
            open(path, "a").close()
            self.dvc.run(cmd="command", wdir=path, single_stage=True)


class TestRunBadName(TestDvc):
    def test(self):
        with self.assertRaises(StagePathOutsideError):
            self.dvc.run(
                cmd="command",
                fname=os.path.join(self.mkdtemp(), self.FOO + DVC_FILE_SUFFIX),
                single_stage=True,
            )

    def test_same_prefix(self):
        with self.assertRaises(StagePathOutsideError):
            path = f"{self._root_dir}-{uuid.uuid4()}"
            os.mkdir(path)
            self.dvc.run(
                cmd="command",
                fname=os.path.join(path, self.FOO + DVC_FILE_SUFFIX),
                single_stage=True,
            )

    def test_not_found(self):
        with self.assertRaises(StagePathNotFoundError):
            path = os.path.join(self._root_dir, str(uuid.uuid4()))
            self.dvc.run(
                cmd="command",
                fname=os.path.join(path, self.FOO + DVC_FILE_SUFFIX),
                single_stage=True,
            )


class TestRunRemoveOuts(TestDvc):
    def test(self):
        with open(self.CODE, "w+") as fobj:
            fobj.write("import sys\n")
            fobj.write("import os\n")
            fobj.write("if os.path.exists(sys.argv[1]):\n")
            fobj.write("    sys.exit(1)\n")
            fobj.write("open(sys.argv[1], 'w+').close()\n")

        self.dvc.run(
            deps=[self.CODE],
            outs=[self.FOO],
            cmd=f"python {self.CODE} {self.FOO}",
            single_stage=True,
        )


class TestRunUnprotectOutsCopy(TestDvc):
    def test(self):
        with open(self.CODE, "w+") as fobj:
            fobj.write("import sys\n")
            fobj.write("with open(sys.argv[1], 'a+') as fobj:\n")
            fobj.write("    fobj.write('foo')\n")

        ret = main(["config", "cache.type", "copy"])
        self.assertEqual(ret, 0)

        ret = main(
            [
                "run",
                "-d",
                self.CODE,
                "-o",
                self.FOO,
                "--single-stage",
                "python",
                self.CODE,
                self.FOO,
            ]
        )
        self.assertEqual(ret, 0)
        self.assertTrue(os.access(self.FOO, os.W_OK))
        with open(self.FOO) as fd:
            self.assertEqual(fd.read(), "foo")

        ret = main(
            [
                "run",
                "--force",
                "--no-run-cache",
                "--single-stage",
                "-d",
                self.CODE,
                "-o",
                self.FOO,
                "python",
                self.CODE,
                self.FOO,
            ]
        )
        self.assertEqual(ret, 0)
        self.assertTrue(os.access(self.FOO, os.W_OK))
        with open(self.FOO) as fd:
            self.assertEqual(fd.read(), "foo")


class TestRunUnprotectOutsSymlink(TestDvc):
    def test(self):
        with open(self.CODE, "w+") as fobj:
            fobj.write("import sys\n")
            fobj.write("import os\n")
            fobj.write("with open(sys.argv[1], 'a+') as fobj:\n")
            fobj.write("    fobj.write('foo')\n")

        ret = main(["config", "cache.type", "symlink"])
        self.assertEqual(ret, 0)

        self.assertEqual(ret, 0)
        ret = main(
            [
                "run",
                "-d",
                self.CODE,
                "-o",
                self.FOO,
                "--single-stage",
                "python",
                self.CODE,
                self.FOO,
            ]
        )
        self.assertEqual(ret, 0)

        if os.name == "nt":
            # NOTE: Windows symlink perms don't propagate to the target
            self.assertTrue(os.access(self.FOO, os.W_OK))
        else:
            self.assertFalse(os.access(self.FOO, os.W_OK))

        self.assertTrue(System.is_symlink(self.FOO))
        with open(self.FOO) as fd:
            self.assertEqual(fd.read(), "foo")

        ret = main(
            [
                "run",
                "--force",
                "--no-run-cache",
                "--single-stage",
                "-d",
                self.CODE,
                "-o",
                self.FOO,
                "python",
                self.CODE,
                self.FOO,
            ]
        )
        self.assertEqual(ret, 0)

        if os.name == "nt":
            # NOTE: Windows symlink perms don't propagate to the target
            self.assertTrue(os.access(self.FOO, os.W_OK))
        else:
            self.assertFalse(os.access(self.FOO, os.W_OK))

        self.assertTrue(System.is_symlink(self.FOO))
        with open(self.FOO) as fd:
            self.assertEqual(fd.read(), "foo")


class TestRunUnprotectOutsHardlink(TestDvc):
    def test(self):
        with open(self.CODE, "w+") as fobj:
            fobj.write("import sys\n")
            fobj.write("import os\n")
            fobj.write("with open(sys.argv[1], 'a+') as fobj:\n")
            fobj.write("    fobj.write('foo')\n")

        ret = main(["config", "cache.type", "hardlink"])
        self.assertEqual(ret, 0)

        self.assertEqual(ret, 0)
        ret = main(
            [
                "run",
                "-d",
                self.CODE,
                "-o",
                self.FOO,
                "--single-stage",
                "python",
                self.CODE,
                self.FOO,
            ]
        )
        self.assertEqual(ret, 0)
        self.assertFalse(os.access(self.FOO, os.W_OK))
        self.assertTrue(System.is_hardlink(self.FOO))
        with open(self.FOO) as fd:
            self.assertEqual(fd.read(), "foo")

        ret = main(
            [
                "run",
                "--force",
                "--no-run-cache",
                "--single-stage",
                "-d",
                self.CODE,
                "-o",
                self.FOO,
                "python",
                self.CODE,
                self.FOO,
            ]
        )
        self.assertEqual(ret, 0)
        self.assertFalse(os.access(self.FOO, os.W_OK))
        self.assertTrue(System.is_hardlink(self.FOO))
        with open(self.FOO) as fd:
            self.assertEqual(fd.read(), "foo")


class TestCmdRunOverwrite(TestDvc):
    def test(self):
        # NOTE: using sleep() is a workaround  for filesystems
        # with low mtime resolution. We have to use mtime since
        # comparing mtime's is the only way to check that the stage
        # file didn't change(size and inode in the first test down
        # below don't change).
        import time

        ret = main(
            [
                "run",
                "-d",
                self.FOO,
                "-d",
                self.CODE,
                "-o",
                "out",
                "--file",
                "out.dvc",
                "--single-stage",
                "python",
                self.CODE,
                self.FOO,
                "out",
            ]
        )
        self.assertEqual(ret, 0)

        stage_mtime = os.path.getmtime("out.dvc")

        time.sleep(1)

        ret = main(
            [
                "run",
                "-d",
                self.FOO,
                "-d",
                self.CODE,
                "--force",
                "--no-run-cache",
                "--single-stage",
                "-o",
                "out",
                "--file",
                "out.dvc",
                "python",
                self.CODE,
                self.FOO,
                "out",
            ]
        )
        self.assertEqual(ret, 0)

        # NOTE: check that dvcfile was overwritten
        self.assertNotEqual(stage_mtime, os.path.getmtime("out.dvc"))
        stage_mtime = os.path.getmtime("out.dvc")

        time.sleep(1)

        ret = main(
            [
                "run",
                "--force",
                "--single-stage",
                "--file",
                "out.dvc",
                "-d",
                self.BAR,
                f"cat {self.BAR}",
            ]
        )
        self.assertEqual(ret, 0)

        # NOTE: check that dvcfile was overwritten
        self.assertNotEqual(stage_mtime, os.path.getmtime("out.dvc"))


class TestCmdRunCliMetrics(TestDvc):
    def test_cached(self):
        ret = main(
            [
                "run",
                "-m",
                "metrics.txt",
                "--single-stage",
                "echo test > metrics.txt",
            ]
        )
        self.assertEqual(ret, 0)
        with open("metrics.txt") as fd:
            self.assertEqual(fd.read().rstrip(), "test")

    def test_not_cached(self):
        ret = main(
            [
                "run",
                "-M",
                "metrics.txt",
                "--single-stage",
                "echo test > metrics.txt",
            ]
        )
        self.assertEqual(ret, 0)
        with open("metrics.txt") as fd:
            self.assertEqual(fd.read().rstrip(), "test")


class TestCmdRunWorkingDirectory(TestDvc):
    def test_default_wdir_is_not_written(self):
        stage = self.dvc.run(
            cmd=f"echo test > {self.FOO}",
            outs=[self.FOO],
            wdir=".",
            single_stage=True,
        )
        d = load_yaml(stage.relpath)
        self.assertNotIn(Stage.PARAM_WDIR, d.keys())

        stage = self.dvc.run(
            cmd=f"echo test > {self.BAR}", outs=[self.BAR], single_stage=True
        )
        d = load_yaml(stage.relpath)
        self.assertNotIn(Stage.PARAM_WDIR, d.keys())

    def test_fname_changes_path_and_wdir(self):
        dname = "dir"
        os.mkdir(os.path.join(self._root_dir, dname))
        foo = os.path.join(dname, self.FOO)
        fname = os.path.join(dname, "stage" + DVC_FILE_SUFFIX)
        stage = self.dvc.run(
            cmd=f"echo test > {foo}",
            outs=[foo],
            fname=fname,
            single_stage=True,
        )
        self.assertEqual(stage.wdir, os.path.realpath(self._root_dir))
        self.assertEqual(
            stage.path, os.path.join(os.path.realpath(self._root_dir), fname)
        )

        # Check that it is dumped properly (relative to fname)
        d = load_yaml(stage.relpath)
        self.assertEqual(d[Stage.PARAM_WDIR], "..")


def test_rerun_deterministic(tmp_dir, run_copy, mocker):
    from dvc.stage.run import subprocess

    tmp_dir.gen("foo", "foo content")

    spy = mocker.spy(subprocess, "Popen")

    run_copy("foo", "out", single_stage=True)
    assert spy.called

    spy.reset_mock()
    run_copy("foo", "out", single_stage=True)
    assert not spy.called


def test_rerun_deterministic_ignore_cache(tmp_dir, run_copy, mocker):
    from dvc.stage.run import subprocess

    tmp_dir.gen("foo", "foo content")

    spy = mocker.spy(subprocess, "Popen")

    run_copy("foo", "out", single_stage=True)
    assert spy.called

    spy.reset_mock()
    run_copy("foo", "out", run_cache=False, single_stage=True)
    assert spy.called


def test_rerun_callback(dvc):
    def run_callback(force=False):
        return dvc.run(
            cmd="echo content > out", force=force, single_stage=True
        )

    assert run_callback() is not None
    with pytest.raises(StageFileAlreadyExistsError):
        assert run_callback() is not None
    assert run_callback(force=True) is not None


def test_rerun_changed_dep(tmp_dir, run_copy):
    tmp_dir.gen("foo", "foo content")
    assert run_copy("foo", "out", single_stage=True) is not None

    tmp_dir.gen("foo", "changed content")
    with pytest.raises(StageFileAlreadyExistsError):
        run_copy("foo", "out", force=False, single_stage=True)
    assert run_copy("foo", "out", force=True, single_stage=True)


def test_rerun_changed_stage(tmp_dir, run_copy):
    tmp_dir.gen("foo", "foo content")
    assert run_copy("foo", "out", single_stage=True) is not None

    tmp_dir.gen("bar", "bar content")
    with pytest.raises(StageFileAlreadyExistsError):
        run_copy("bar", "out", force=False, single_stage=True)


def test_rerun_changed_out(tmp_dir, run_copy):
    tmp_dir.gen("foo", "foo content")
    assert run_copy("foo", "out", single_stage=True) is not None

    Path("out").write_text("modification")
    with pytest.raises(StageFileAlreadyExistsError):
        run_copy("foo", "out", force=False, single_stage=True)


class TestRunCommit(TestDvc):
    def test(self):
        fname = "test"
        ret = main(
            [
                "run",
                "-o",
                fname,
                "--no-commit",
                "--single-stage",
                "echo",
                "test",
                ">",
                fname,
            ]
        )
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile(fname))
        self.assertFalse(os.path.exists(self.dvc.odb.local.cache_dir))

        ret = main(["commit", fname + ".dvc"])
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile(fname))
        self.assertEqual(len(os.listdir(self.dvc.odb.local.cache_dir)), 1)


class TestRunPersist(TestDvc):
    @property
    def outs_command(self):
        raise NotImplementedError

    def _test(self):
        file = "file.txt"
        file_content = "content"
        stage_file = file + DVC_FILE_SUFFIX

        self.run_command(file, file_content)
        self.stage_should_contain_persist_flag(stage_file)

        self.should_append_upon_repro(file, stage_file)

        self.should_remove_persistent_outs(file, stage_file)

    def run_command(self, file, file_content):
        ret = main(
            [
                "run",
                "--single-stage",
                "--always-changed",
                self.outs_command,
                file,
                f"echo {file_content} >> {file}",
            ]
        )
        self.assertEqual(0, ret)

    def stage_should_contain_persist_flag(self, stage_file):
        stage_file_content = load_yaml(stage_file)
        self.assertEqual(
            True, stage_file_content["outs"][0][Output.PARAM_PERSIST]
        )

    def should_append_upon_repro(self, file, stage_file):
        ret = main(["repro", stage_file])
        self.assertEqual(0, ret)

        with open(file) as fobj:
            lines = fobj.readlines()
        self.assertEqual(2, len(lines))

    def should_remove_persistent_outs(self, file, stage_file):
        ret = main(["remove", stage_file, "--outs"])
        self.assertEqual(0, ret)

        self.assertFalse(os.path.exists(file))


class TestRunPersistOuts(TestRunPersist):
    @property
    def outs_command(self):
        return "--outs-persist"

    def test(self):
        self._test()


class TestRunPersistOutsNoCache(TestRunPersist):
    @property
    def outs_command(self):
        return "--outs-persist-no-cache"

    def test(self):
        self._test()


class TestShouldRaiseOnOverlappingOutputPaths(TestDvc):
    def test(self):
        ret = main(["add", self.DATA_DIR])
        self.assertEqual(0, ret)

        with self.assertRaises(OverlappingOutputPathsError) as err:
            self.dvc.run(
                outs=[self.DATA],
                cmd=f"echo data >> {self.DATA}",
                single_stage=True,
            )
        error_output = str(err.exception)

        data_dir_stage = self.DATA_DIR + DVC_FILE_SUFFIX
        data_stage = os.path.basename(self.DATA) + DVC_FILE_SUFFIX

        self.assertIn("The output paths:\n", error_output)
        self.assertIn(
            f"\n'{self.DATA_DIR}'('{data_dir_stage}')\n", error_output
        )
        self.assertIn(f"\n'{self.DATA}'('{data_stage}')\n", error_output)
        self.assertIn(
            "overlap and are thus in the same tracked directory.\n"
            "To keep reproducibility, outputs should be in separate "
            "tracked directories or tracked individually.",
            error_output,
        )


class TestRerunWithSameOutputs(TestDvc):
    def _read_content_only(self, path):
        with open(path) as fobj:
            return [line.rstrip() for line in fobj]

    @property
    def _outs_command(self):
        raise NotImplementedError

    def _run_twice_with_same_outputs(self):
        ret = main(
            [
                "run",
                "--single-stage",
                "--outs",
                self.FOO,
                f"echo {self.FOO_CONTENTS} > {self.FOO}",
            ]
        )
        self.assertEqual(0, ret)

        output_file_content = self._read_content_only(self.FOO)
        self.assertEqual([self.FOO_CONTENTS], output_file_content)

        ret = main(
            [
                "run",
                self._outs_command,
                self.FOO,
                "--force",
                "--single-stage",
                f"echo {self.BAR_CONTENTS} >> {self.FOO}",
            ]
        )
        self.assertEqual(0, ret)


class TestNewRunShouldRemoveOutsOnNoPersist(TestRerunWithSameOutputs):
    def test(self):
        self._run_twice_with_same_outputs()

        output_file_content = self._read_content_only(self.FOO)
        self.assertEqual([self.BAR_CONTENTS], output_file_content)

    @property
    def _outs_command(self):
        return "--outs"


class TestNewRunShouldNotRemoveOutsOnPersist(TestRerunWithSameOutputs):
    def test(self):
        self._run_twice_with_same_outputs()

        output_file_content = self._read_content_only(self.FOO)
        self.assertEqual(
            [self.FOO_CONTENTS, self.BAR_CONTENTS], output_file_content
        )

    @property
    def _outs_command(self):
        return "--outs-persist"


class TestShouldNotCheckoutUponCorruptedLocalHardlinkCache(TestDvc):
    def setUp(self):
        super().setUp()
        ret = main(["config", "cache.type", "hardlink"])
        self.assertEqual(ret, 0)
        self.dvc.close()

        self.dvc = DvcRepo(".")

    def test(self):
        cmd = f"python {self.CODE} {self.FOO} {self.BAR}"
        stage = self.dvc.run(
            deps=[self.FOO], outs=[self.BAR], cmd=cmd, single_stage=True
        )

        os.chmod(self.BAR, 0o644)
        with open(self.BAR, "w") as fd:
            fd.write("corrupting the output cache")

        patch_checkout = mock.patch.object(
            stage.outs[0], "checkout", wraps=stage.outs[0].checkout
        )
        from dvc.stage.run import cmd_run

        patch_run = mock.patch("dvc.stage.run.cmd_run", wraps=cmd_run)

        with self.dvc.lock:
            with patch_checkout as mock_checkout:
                with patch_run as mock_run:
                    stage.run()

                    mock_run.assert_called_once()
                    mock_checkout.assert_not_called()


def test_bad_stage_fname(tmp_dir, dvc, run_copy):
    tmp_dir.dvc_gen("foo", "foo content")

    with pytest.raises(StageFileBadNameError):
        # fname should end with .dvc
        run_copy("foo", "foo_copy", fname="out_stage", single_stage=True)

    # Check that command hasn't been run
    assert not (tmp_dir / "foo_copy").exists()


def test_should_raise_on_stage_dependency(run_copy):
    with pytest.raises(DependencyIsStageFileError):
        run_copy("name.dvc", "stage_copy", single_stage=True)


def test_should_raise_on_stage_output(tmp_dir, dvc, run_copy):
    tmp_dir.dvc_gen("foo", "foo content")

    with pytest.raises(OutputIsStageFileError):
        run_copy("foo", "name.dvc", single_stage=True)


@pytest.mark.parametrize("metrics_type", ["metrics", "metrics_no_cache"])
def test_metrics_dir(tmp_dir, dvc, caplog, run_copy_metrics, metrics_type):
    copyargs = {metrics_type: ["dir_metric"]}
    tmp_dir.gen({"dir": {"file": "content"}})
    with caplog.at_level(logging.DEBUG, "dvc"):
        run_copy_metrics("dir", "dir_metric", **copyargs)
    assert (
        "directory 'dir_metric' cannot be used as metrics." in caplog.messages
    )


def test_run_force_preserves_comments_and_meta(tmp_dir, dvc, run_copy):
    tmp_dir.gen({"foo": "foo", "foo1": "foo1"})
    text = textwrap.dedent(
        """\
      desc: top desc
      cmd: python copy.py foo bar
      deps:
      - path: copy.py
      - path: foo
      outs:
      # comment preserved
      - path: bar
        desc: out desc
      meta:
        name: copy-foo-bar
    """
    )
    (tmp_dir / "bar.dvc").write_text(text)
    dvc.reproduce("bar.dvc")

    # CRLF on windows makes the generated file bigger in size
    code_size = 143 if os.name == "nt" else 142
    assert (tmp_dir / "bar.dvc").read_text() == textwrap.dedent(
        f"""\
        desc: top desc
        cmd: python copy.py foo bar
        deps:
        - path: copy.py
          md5: 90c27dd80b698fe766f0c3ee0b6b9729
          size: {code_size}
        - path: foo
          md5: acbd18db4cc2f85cedef654fccc4a4d8
          size: 3
        outs:
        # comment preserved
        - path: bar
          desc: out desc
          md5: acbd18db4cc2f85cedef654fccc4a4d8
          size: 3
        meta:
          name: copy-foo-bar
        md5: be659ce4a33cebb85d4e8e1335d394ad
    """
    )

    run_copy("foo1", "bar1", single_stage=True, force=True, fname="bar.dvc")
    assert (tmp_dir / "bar.dvc").read_text() == textwrap.dedent(
        f"""\
        desc: top desc
        cmd: python copy.py foo1 bar1
        deps:
        - path: foo1
          md5: 299a0be4a5a79e6a59fdd251b19d78bb
          size: 4
        - path: copy.py
          md5: 90c27dd80b698fe766f0c3ee0b6b9729
          size: {code_size}
        outs:
        # comment preserved
        - path: bar1
          md5: 299a0be4a5a79e6a59fdd251b19d78bb
          size: 4
        meta:
          name: copy-foo-bar
        md5: 9e725b11cb393e6a7468369fa50328b7
    """
    )
