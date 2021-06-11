import filecmp
import os
import re
import shutil
from pathlib import Path

import pytest
from mock import patch

from dvc.dvcfile import DVC_FILE, Dvcfile
from dvc.exceptions import (
    CyclicGraphError,
    ReproductionError,
    StagePathAsOutputError,
)
from dvc.fs.local import LocalFileSystem
from dvc.main import main
from dvc.output import Output
from dvc.stage import Stage
from dvc.stage.exceptions import StageFileDoesNotExistError
from dvc.system import System
from dvc.utils import file_md5, relpath
from dvc.utils.fs import remove
from dvc.utils.serialize import dump_yaml, load_yaml
from tests.basic_env import TestDvc


class SingleStageRun:
    def _run(self, **kwargs):
        kwargs["single_stage"] = True
        kwargs.pop("name", None)
        return self.dvc.run(**kwargs)  # noqa, pylint: disable=no-member

    @staticmethod
    def _get_stage_target(stage):
        return stage.addressing


class TestRepro(SingleStageRun, TestDvc):
    def setUp(self):
        super().setUp()

        stages = self.dvc.add(self.FOO)
        self.assertEqual(len(stages), 1)
        self.foo_stage = stages[0]
        self.assertTrue(self.foo_stage is not None)

        self.file1 = "file1"
        self.file1_stage = self.file1 + ".dvc"
        self.stage = self._run(
            fname=self.file1_stage,
            outs=[self.file1],
            deps=[self.FOO, self.CODE],
            cmd=f"python {self.CODE} {self.FOO} {self.file1}",
            name="run1",
        )


class TestReproFail(TestRepro):
    def test(self):
        os.unlink(self.CODE)

        ret = main(["repro", self._get_stage_target(self.stage)])
        self.assertNotEqual(ret, 0)


class TestReproCyclicGraph(SingleStageRun, TestDvc):
    def test(self):
        self._run(
            deps=[self.FOO],
            outs=["bar.txt"],
            cmd="echo bar > bar.txt",
            name="copybarbar-txt",
        )

        self._run(
            deps=["bar.txt"],
            outs=["baz.txt"],
            cmd="echo baz > baz.txt",
            name="copybazbaz-txt",
        )

        stage_dump = {
            "cmd": "echo baz > foo",
            "deps": [{"path": "baz.txt"}],
            "outs": [{"path": self.FOO}],
        }
        dump_yaml("cycle.dvc", stage_dump)

        with self.assertRaises(CyclicGraphError):
            self.dvc.reproduce("cycle.dvc")


class TestReproWorkingDirectoryAsOutput(TestDvc):
    """
    |  stage.cwd  |  out.path | cwd as output |
    |:-----------:|:---------:|:-------------:|
    |     dir     |    dir    |      True     |
    | dir/subdir/ |    dir    |      True     |
    |     dir     |   dir-1   |     False     |
    |      .      | something |     False     |
    """

    def test(self):
        # File structure:
        #       .
        #       |-- dir1
        #       |  |__ dir2.dvc         (out.path == ../dir2)
        #       |__ dir2
        #           |__ something.dvc    (stage.cwd == ./dir2)

        os.mkdir(os.path.join(self.dvc.root_dir, "dir1"))

        self.dvc.run(
            fname=os.path.join("dir1", "dir2.dvc"),
            wdir="dir1",
            outs=[os.path.join("..", "dir2")],
            cmd="mkdir {path}".format(path=os.path.join("..", "dir2")),
            single_stage=True,
        )

        faulty_stage_path = os.path.join("dir2", "something.dvc")

        output = os.path.join("..", "something")
        stage_dump = {
            "cmd": f"echo something > {output}",
            "outs": [{"path": output}],
        }
        dump_yaml(faulty_stage_path, stage_dump)

        with self.assertRaises(StagePathAsOutputError):
            self.dvc.reproduce(faulty_stage_path)

    def test_nested(self):
        #       .
        #       |-- a
        #       |  |__ nested
        #       |     |__ dir
        #       |       |__ error.dvc     (stage.cwd == 'a/nested/dir')
        #       |__ b
        #          |__ nested.dvc         (stage.out == 'a/nested')
        dir1 = "b"
        dir2 = "a"

        os.mkdir(dir1)
        os.mkdir(dir2)

        nested_dir = os.path.join(dir2, "nested")
        out_dir = relpath(nested_dir, dir1)

        nested_stage = self.dvc.run(
            fname=os.path.join(dir1, "b.dvc"),
            wdir=dir1,
            outs=[out_dir],  # ../a/nested
            cmd=f"mkdir {out_dir}",
            single_stage=True,
        )

        os.mkdir(os.path.join(nested_dir, "dir"))

        error_stage_path = os.path.join(nested_dir, "dir", "error.dvc")

        output = os.path.join("..", "..", "something")
        stage_dump = {
            "cmd": f"echo something > {output}",
            "outs": [{"path": output}],
        }
        dump_yaml(error_stage_path, stage_dump)

        # NOTE: os.walk() walks in a sorted order and we need dir2 subdirs to
        # be processed before dir1 to load error.dvc first.
        self.dvc.stages = [
            nested_stage,
            Dvcfile(self.dvc, error_stage_path).stage,
        ]

        with patch.object(self.dvc, "_reset"):  # to prevent `stages` resetting
            with self.assertRaises(StagePathAsOutputError):
                self.dvc.reproduce(error_stage_path)

    def test_similar_paths(self):
        # File structure:
        #
        #       .
        #       |-- something.dvc   (out.path == something)
        #       |-- something
        #       |__ something-1
        #          |-- a
        #          |__ a.dvc        (stage.cwd == something-1)

        self.dvc.run(
            outs=["something"], cmd="mkdir something", single_stage=True
        )

        os.mkdir("something-1")

        stage = os.path.join("something-1", "a.dvc")

        stage_dump = {"cmd": "echo a > a", "outs": [{"path": "a"}]}
        dump_yaml(stage, stage_dump)

        try:
            self.dvc.reproduce(stage)
        except StagePathAsOutputError:
            self.fail("should not raise StagePathAsOutputError")


class TestReproDepUnderDir(SingleStageRun, TestDvc):
    def test(self):
        stages = self.dvc.add(self.DATA_DIR)
        self.assertTrue(stages and stages[0] is not None)

        file1 = "file1"
        stage = self._run(
            fname=file1 + ".dvc",
            outs=[file1],
            deps=[self.DATA, self.CODE],
            cmd=f"python {self.CODE} {self.DATA} {file1}",
            name="copy-data-file1",
        )

        self.assertTrue(filecmp.cmp(file1, self.DATA, shallow=False))

        os.unlink(self.DATA)
        shutil.copyfile(self.FOO, self.DATA)

        stages = self.dvc.reproduce(self._get_stage_target(stage))
        self.assertEqual(len(stages), 2)
        self.assertTrue(filecmp.cmp(file1, self.FOO, shallow=False))


class TestReproDepDirWithOutputsUnderIt(SingleStageRun, TestDvc):
    def test(self):
        stages = self.dvc.add(self.DATA)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)

        stages = self.dvc.add(self.DATA_SUB)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)

        deps = [self.DATA, self.DATA_SUB]
        stage = self.dvc.run(
            cmd="ls {}".format(" ".join(deps)),
            fname="dvcfile2.dvc",
            deps=deps,
            single_stage=True,
        )
        self.assertTrue(stage is not None)

        file1 = "file1"
        file1_stage = file1 + ".dvc"
        stage = self._run(
            fname=file1_stage,
            deps=[self.DATA_DIR],
            outs=[file1],
            cmd=f"python {self.CODE} {self.DATA} {file1}",
            name="copy-data-file1",
        )
        self.assertTrue(stage is not None)

        os.unlink(self.DATA)
        shutil.copyfile(self.FOO, self.DATA)

        stages = self.dvc.reproduce(self._get_stage_target(stage))
        self.assertEqual(len(stages), 2)


class TestReproForce(TestRepro):
    def test(self):
        stages = self.dvc.reproduce(
            self._get_stage_target(self.stage), force=True
        )
        self.assertEqual(len(stages), 2)


class TestReproChangedCode(TestRepro):
    def test(self):
        self.swap_code()

        stages = self.dvc.reproduce(self._get_stage_target(self.stage))

        self.assertTrue(filecmp.cmp(self.file1, self.BAR, shallow=False))
        self.assertEqual(len(stages), 1)

    def swap_code(self):
        os.unlink(self.CODE)
        new_contents = self.CODE_CONTENTS
        new_contents += "\nshutil.copyfile('{}', " "sys.argv[2])\n".format(
            self.BAR
        )
        self.create(self.CODE, new_contents)


class TestReproChangedData(TestRepro):
    def test(self):
        self.swap_foo_with_bar()

        stages = self.dvc.reproduce(self._get_stage_target(self.stage))

        self.assertTrue(filecmp.cmp(self.file1, self.BAR, shallow=False))
        self.assertEqual(len(stages), 2)

    def swap_foo_with_bar(self):
        os.unlink(self.FOO)
        shutil.copyfile(self.BAR, self.FOO)


class TestReproDry(TestReproChangedData):
    def test(self):
        self.swap_foo_with_bar()

        stages = self.dvc.reproduce(
            self._get_stage_target(self.stage), dry=True
        )

        self.assertTrue(len(stages), 2)
        self.assertFalse(filecmp.cmp(self.file1, self.BAR, shallow=False))

        ret = main(["repro", "--dry", self._get_stage_target(self.stage)])
        self.assertEqual(ret, 0)
        self.assertFalse(filecmp.cmp(self.file1, self.BAR, shallow=False))


class TestReproUpToDate(TestRepro):
    def test(self):
        ret = main(["repro", self._get_stage_target(self.stage)])
        self.assertEqual(ret, 0)


class TestReproDryNoExec(TestDvc):
    def test(self):
        deps = []
        for d in range(3):
            idir = f"idir{d}"
            odir = f"odir{d}"

            deps.append("-d")
            deps.append(odir)

            os.mkdir(idir)

            f = os.path.join(idir, "file")
            with open(f, "w+") as fobj:
                fobj.write(str(d))

            ret = main(
                [
                    "run",
                    "--no-exec",
                    "--single-stage",
                    "-d",
                    idir,
                    "-o",
                    odir,
                    "python -c 'import shutil; "
                    'shutil.copytree("{}", "{}")\''.format(idir, odir),
                ]
            )
            self.assertEqual(ret, 0)

        ret = main(
            [
                "run",
                "--no-exec",
                "--single-stage",
                "--file",
                DVC_FILE,
                *deps,
                "ls {}".format(
                    " ".join(dep for i, dep in enumerate(deps) if i % 2)
                ),
            ]
        )
        self.assertEqual(ret, 0)

        ret = main(["repro", "--dry", DVC_FILE])
        self.assertEqual(ret, 0)


class TestReproChangedDeepData(TestReproChangedData):
    def setUp(self):
        super().setUp()

        self.file2 = "file2"
        self.stage = self._run(
            fname=self.file2 + ".dvc",
            outs=[self.file2],
            deps=[self.file1, self.CODE],
            cmd=f"python {self.CODE} {self.file1} {self.file2}",
            name="copy-file-file2",
        )

    def test(self):
        self.swap_foo_with_bar()

        stages = self.dvc.reproduce(self._get_stage_target(self.stage))

        self.assertTrue(filecmp.cmp(self.file1, self.BAR, shallow=False))
        self.assertTrue(filecmp.cmp(self.file2, self.BAR, shallow=False))
        self.assertEqual(len(stages), 3)


class TestReproForceDownstream(TestDvc):
    def test(self):
        stages = self.dvc.add(self.FOO)
        self.assertEqual(len(stages), 1)
        foo_stage = stages[0]
        self.assertTrue(foo_stage is not None)

        code1 = "code1.py"
        shutil.copyfile(self.CODE, code1)
        file1 = "file1"
        file1_stage = self.dvc.run(
            outs=[file1],
            deps=[self.FOO, code1],
            cmd=f"python {code1} {self.FOO} {file1}",
            single_stage=True,
        )
        self.assertTrue(file1_stage is not None)

        code2 = "code2.py"
        shutil.copyfile(self.CODE, code2)
        file2 = "file2"
        file2_stage = self.dvc.run(
            outs=[file2],
            deps=[file1, code2],
            cmd=f"python {code2} {file1} {file2}",
            single_stage=True,
        )
        self.assertTrue(file2_stage is not None)

        code3 = "code3.py"
        shutil.copyfile(self.CODE, code3)
        file3 = "file3"
        file3_stage = self.dvc.run(
            outs=[file3],
            deps=[file2, code3],
            cmd=f"python {code3} {file2} {file3}",
            single_stage=True,
        )
        self.assertTrue(file3_stage is not None)

        with open(code2, "a") as fobj:
            fobj.write("\n\n")

        stages = self.dvc.reproduce(file3_stage.path, force_downstream=True)
        self.assertEqual(len(stages), 2)
        self.assertEqual(stages[0].path, file2_stage.path)
        self.assertEqual(stages[1].path, file3_stage.path)


class TestReproPipeline(TestReproChangedDeepData):
    def test(self):
        stages = self.dvc.reproduce(
            self._get_stage_target(self.stage), force=True, pipeline=True
        )
        self.assertEqual(len(stages), 3)

    def test_cli(self):
        ret = main(
            ["repro", "--pipeline", "-f", self._get_stage_target(self.stage)]
        )
        self.assertEqual(ret, 0)


class TestReproPipelines(SingleStageRun, TestDvc):
    def setUp(self):
        super().setUp()

        stages = self.dvc.add(self.FOO)
        self.assertEqual(len(stages), 1)
        self.foo_stage = stages[0]
        self.assertTrue(self.foo_stage is not None)

        stages = self.dvc.add(self.BAR)
        self.assertEqual(len(stages), 1)
        self.bar_stage = stages[0]
        self.assertTrue(self.bar_stage is not None)

        self.file1 = "file1"
        self.file1_stage = self.dvc.run(
            fname=self.file1 + ".dvc",
            outs=[self.file1],
            deps=[self.FOO, self.CODE],
            cmd=f"python {self.CODE} {self.FOO} {self.file1}",
            single_stage=True,
        )

        self.file2 = "file2"
        self.file2_stage = self._run(
            fname=self.file2 + ".dvc",
            outs=[self.file2],
            deps=[self.BAR, self.CODE],
            cmd=f"python {self.CODE} {self.BAR} {self.file2}",
            name="copy-BAR-file2",
        )

    def test(self):
        stages = self.dvc.reproduce(all_pipelines=True, force=True)
        self.assertEqual(len(stages), 4)
        self.assertTrue(self.file1_stage in stages)
        self.assertTrue(self.file2_stage in stages)

    def test_cli(self):
        ret = main(["repro", "-f", "-P"])
        self.assertEqual(ret, 0)


class TestReproFrozen(TestReproChangedData):
    def test(self):
        file2 = "file2"
        file2_stage = self._run(
            fname=file2 + ".dvc",
            outs=[file2],
            deps=[self.file1, self.CODE],
            cmd=f"python {self.CODE} {self.file1} {file2}",
            name="copy-file1-file2",
        )

        self.swap_foo_with_bar()

        ret = main(["freeze", self._get_stage_target(file2_stage)])
        self.assertEqual(ret, 0)
        stages = self.dvc.reproduce(self._get_stage_target(file2_stage))
        self.assertEqual(len(stages), 0)

        ret = main(["unfreeze", self._get_stage_target(file2_stage)])
        self.assertEqual(ret, 0)
        stages = self.dvc.reproduce(self._get_stage_target(file2_stage))
        self.assertTrue(filecmp.cmp(self.file1, self.BAR, shallow=False))
        self.assertTrue(filecmp.cmp(file2, self.BAR, shallow=False))
        self.assertEqual(len(stages), 3)

    def test_non_existing(self):
        with self.assertRaises(StageFileDoesNotExistError):
            self.dvc.freeze("Dvcfile")
            self.dvc.freeze("pipelines.yaml")
            self.dvc.freeze("pipelines.yaml:name")
            self.dvc.freeze("Dvcfile:name")
            self.dvc.freeze("stage.dvc")
            self.dvc.freeze("stage.dvc:name")
            self.dvc.freeze("not-existing-stage.json")

        ret = main(["freeze", "non-existing-stage"])
        self.assertNotEqual(ret, 0)


class TestReproFrozenCallback(SingleStageRun, TestDvc):
    def test(self):
        file1 = "file1"
        file1_stage = file1 + ".dvc"
        # NOTE: purposefully not specifying deps or outs
        # to create a callback stage.
        stage = self._run(
            fname=file1_stage,
            cmd=f"python {self.CODE} {self.FOO} {file1}",
            name="copy-FOO-file1",
        )
        self.assertTrue(stage is not None)

        stages = self.dvc.reproduce(self._get_stage_target(stage))
        self.assertEqual(len(stages), 1)

        self.dvc.freeze(self._get_stage_target(stage))
        stages = self.dvc.reproduce(self._get_stage_target(stage))
        self.assertEqual(len(stages), 0)

        self.dvc.unfreeze(self._get_stage_target(stage))
        stages = self.dvc.reproduce(self._get_stage_target(stage))
        self.assertEqual(len(stages), 1)


class TestReproFrozenUnchanged(TestRepro):
    def test(self):
        """
        Check that freezing/unfreezing doesn't affect stage state
        """
        target = self._get_stage_target(self.stage)
        self.dvc.freeze(target)
        stages = self.dvc.reproduce(target)
        self.assertEqual(len(stages), 0)

        self.dvc.unfreeze(target)
        stages = self.dvc.reproduce(target)
        self.assertEqual(len(stages), 0)


class TestReproMetricsAddUnchanged(TestDvc):
    def test(self):
        """
        Check that adding/removing metrics doesn't affect stage state
        """
        stages = self.dvc.add(self.FOO)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)

        file1 = "file1"
        file1_stage = file1 + ".dvc"
        self.dvc.run(
            fname=file1_stage,
            outs_no_cache=[file1],
            deps=[self.FOO, self.CODE],
            cmd=f"python {self.CODE} {self.FOO} {file1}",
            single_stage=True,
        )

        stages = self.dvc.reproduce(file1_stage)
        self.assertEqual(len(stages), 0)

        d = load_yaml(file1_stage)
        d["outs"][0]["metric"] = True
        dump_yaml(file1_stage, d)

        stages = self.dvc.reproduce(file1_stage)
        self.assertEqual(len(stages), 0)

        d = load_yaml(file1_stage)
        d["outs"][0]["metric"] = False
        dump_yaml(file1_stage, d)

        stages = self.dvc.reproduce(file1_stage)
        self.assertEqual(len(stages), 0)


class TestReproPhony(TestReproChangedData):
    def test(self):
        stage = self._run(
            cmd="cat " + self.file1, deps=[self.file1], name="no_cmd"
        )

        self.swap_foo_with_bar()

        self.dvc.reproduce(self._get_stage_target(stage))

        self.assertTrue(filecmp.cmp(self.file1, self.BAR, shallow=False))


class TestNonExistingOutput(TestRepro):
    def test(self):
        os.unlink(self.FOO)

        with self.assertRaises(ReproductionError):
            self.dvc.reproduce(self._get_stage_target(self.stage))


class TestReproDataSource(TestReproChangedData):
    def test(self):
        self.swap_foo_with_bar()

        stages = self.dvc.reproduce(self.foo_stage.path)

        self.assertTrue(filecmp.cmp(self.FOO, self.BAR, shallow=False))
        self.assertEqual(
            stages[0].outs[0].hash_info.value, file_md5(self.BAR, self.dvc.fs)
        )


class TestReproChangedDir(SingleStageRun, TestDvc):
    def test(self):
        file_name = "file"
        shutil.copyfile(self.FOO, file_name)

        dir_name = "dir"
        dir_code = "dir.py"
        code = (
            'import os; import shutil; os.mkdir("{}"); '
            'shutil.copyfile("{}", os.path.join("{}", "{}"))'
        )

        with open(dir_code, "w+") as fd:
            fd.write(code.format(dir_name, file_name, dir_name, file_name))

        stage = self._run(
            outs=[dir_name],
            deps=[file_name, dir_code],
            cmd=f"python {dir_code}",
            name="copy-in-dir",
        )
        target = self._get_stage_target(stage)

        stages = self.dvc.reproduce(target)
        self.assertEqual(len(stages), 0)

        os.unlink(file_name)
        shutil.copyfile(self.BAR, file_name)

        stages = self.dvc.reproduce(target)
        self.assertEqual(len(stages), 1)


class TestReproChangedDirData(SingleStageRun, TestDvc):
    def test(self):
        dir_name = "dir"
        dir_code = "dir_code.py"

        with open(dir_code, "w+") as fd:
            fd.write(
                "import os; import sys; import shutil; "
                "shutil.copytree(sys.argv[1], sys.argv[2])"
            )

        stage = self._run(
            outs=[dir_name],
            deps=[self.DATA_DIR, dir_code],
            cmd=f"python {dir_code} {self.DATA_DIR} {dir_name}",
            name="copy-dir",
        )
        target = self._get_stage_target(stage)

        self.assertTrue(stage is not None)

        stages = self.dvc.reproduce(target)
        self.assertEqual(len(stages), 0)

        with open(self.DATA_SUB, "a") as fd:
            fd.write("add")

        stages = self.dvc.reproduce(target)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)

        # Check that dvc indeed registers changed output dir
        shutil.move(self.BAR, dir_name)
        stages = self.dvc.reproduce(target)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)

        # Check that dvc registers mtime change for the directory.
        System.hardlink(self.DATA_SUB, self.DATA_SUB + ".lnk")
        stages = self.dvc.reproduce(target)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)


class TestReproMissingMd5InStageFile(TestRepro):
    def test(self):
        d = load_yaml(self.file1_stage)
        del d[Stage.PARAM_OUTS][0][LocalFileSystem.PARAM_CHECKSUM]
        del d[Stage.PARAM_DEPS][0][LocalFileSystem.PARAM_CHECKSUM]
        dump_yaml(self.file1_stage, d)

        stages = self.dvc.reproduce(self.file1_stage)
        self.assertEqual(len(stages), 1)


class TestCmdRepro(TestReproChangedData):
    def test(self):
        self.swap_foo_with_bar()

        ret = main(["status"])
        self.assertEqual(ret, 0)

        ret = main(["repro", self._get_stage_target(self.stage)])
        self.assertEqual(ret, 0)

        ret = main(["repro", "non-existing-file"])
        self.assertNotEqual(ret, 0)


class TestReproShell(TestDvc):
    def test(self):
        if os.name == "nt":
            return

        fname = "shell.txt"
        stage = fname + ".dvc"

        self.dvc.run(
            fname=stage,
            outs=[fname],
            cmd=f"echo $SHELL > {fname}",
            single_stage=True,
        )

        with open(fname) as fd:
            self.assertEqual(os.getenv("SHELL"), fd.read().strip())

        os.unlink(fname)

        self.dvc.reproduce(stage)

        with open(fname) as fd:
            self.assertEqual(os.getenv("SHELL"), fd.read().strip())


class TestReproAllPipelines(SingleStageRun, TestDvc):
    def test(self):
        stages = [
            self._run(
                fname="start.dvc",
                outs=["start.txt"],
                cmd="echo start > start.txt",
                name="start",
            ),
            self._run(
                fname="middle.dvc",
                deps=["start.txt"],
                outs=["middle.txt"],
                cmd="echo middle > middle.txt",
                name="middle",
            ),
            self._run(
                fname="final.dvc",
                deps=["middle.txt"],
                outs=["final.txt"],
                cmd="echo final > final.txt",
                name="final",
            ),
            self._run(
                fname="disconnected.dvc",
                outs=["disconnected.txt"],
                cmd="echo other > disconnected.txt",
                name="disconnected",
            ),
        ]

        from dvc.state import StateNoop

        self.dvc.state = StateNoop()

        with patch.object(
            Stage, "reproduce", side_effect=stages
        ) as mock_reproduce:
            ret = main(["repro", "--all-pipelines"])
            self.assertEqual(ret, 0)
            self.assertEqual(mock_reproduce.call_count, 4)


class TestReproNoCommit(TestRepro):
    def test(self):
        remove(self.dvc.odb.local.cache_dir)
        ret = main(
            ["repro", self._get_stage_target(self.stage), "--no-commit"]
        )
        self.assertEqual(ret, 0)
        self.assertEqual(os.listdir(self.dvc.odb.local.cache_dir), ["runs"])


class TestReproAlreadyCached(TestRepro):
    def test(self):
        stage = self._run(
            fname="datetime.dvc",
            always_changed=True,
            deps=[],
            outs=["datetime.txt"],
            cmd='python -c "import time; print(time.time())" > datetime.txt',
            name="datetime",
        )
        run_out = stage.outs[0]
        repro_out = self.dvc.reproduce(self._get_stage_target(stage))[0].outs[
            0
        ]

        self.assertNotEqual(run_out.hash_info, repro_out.hash_info)

    def test_force_with_dependencies(self):
        run_out = self.dvc.run(
            fname="datetime.dvc",
            deps=[self.FOO],
            outs=["datetime.txt"],
            cmd='python -c "import time; print(time.time())" > datetime.txt',
            single_stage=True,
        ).outs[0]

        ret = main(["repro", "--force", "datetime.dvc"])
        self.assertEqual(ret, 0)

        repro_out = Dvcfile(self.dvc, "datetime.dvc").stage.outs[0]

        self.assertNotEqual(run_out.hash_info, repro_out.hash_info)

    def test_force_import(self):
        remove(self.BAR)
        ret = main(["import-url", self.FOO, self.BAR])
        self.assertEqual(ret, 0)

        patch_download = patch.object(
            LocalFileSystem,
            "download",
            side_effect=LocalFileSystem.download,
            autospec=True,
        )

        patch_checkout = patch.object(
            Output,
            "checkout",
            side_effect=Output.checkout,
            autospec=True,
        )

        with patch_download as mock_download:
            with patch_checkout as mock_checkout:
                assert main(["unfreeze", "bar.dvc"]) == 0
                ret = main(["repro", "--force", "bar.dvc"])
                self.assertEqual(ret, 0)
                self.assertEqual(mock_download.call_count, 1)
                self.assertEqual(mock_checkout.call_count, 0)


class TestShouldDisplayMetricsOnReproWithMetricsOption(TestDvc):
    def test(self):
        metrics_file = "metrics_file"
        metrics_value = 0.123489015
        ret = main(
            [
                "run",
                "--single-stage",
                "-m",
                metrics_file,
                f"echo {metrics_value} >> {metrics_file}",
            ]
        )
        self.assertEqual(0, ret)

        self._caplog.clear()
        self._capsys.readouterr()  # clearing the buffer

        from dvc.dvcfile import DVC_FILE_SUFFIX

        ret = main(
            ["repro", "--force", "--metrics", metrics_file + DVC_FILE_SUFFIX]
        )
        self.assertEqual(0, ret)

        expected_metrics_display = f"Path\n{metrics_file}  {metrics_value}\n"
        actual, _ = self._capsys.readouterr()
        self.assertIn(expected_metrics_display, actual)


@pytest.fixture
def repro_dir(tmp_dir, dvc, run_copy):
    # Creates repo with following structure:
    #    data_dir/dir_file              origin_data
    #         |       |                   |
    #         |       |              origin_copy.dvc
    # unrelated2.dvc  |               |       |
    #                 |               |    unrelated1.dvc
    #    dir/subdir/dir_file_copy.dvc |
    #                  |              |
    #                  |        dir/origin_copy_2.dvc
    #                  |            |
    #                   \          /
    #                    \        /
    #                   dir/Dvcfile
    tmp_dir.gen(
        {
            "origin_data": "origin data content",
            "data_dir": {"dir_file": "dir file content"},
            "dir": {"subdir": {}},
        }
    )

    stages = {}

    origin_copy = tmp_dir / "origin_copy"
    stage = run_copy("origin_data", os.fspath(origin_copy), single_stage=True)
    assert stage is not None
    assert origin_copy.read_text() == "origin data content"
    stages["origin_copy"] = stage

    origin_copy_2 = tmp_dir / "dir" / "origin_copy_2"
    stage = run_copy(
        os.fspath(origin_copy),
        os.fspath(origin_copy_2),
        fname=os.fspath(origin_copy_2) + ".dvc",
        single_stage=True,
    )
    assert stage is not None
    assert origin_copy_2.read_text() == "origin data content"
    stages["origin_copy_2"] = stage

    dir_file_path = tmp_dir / "data_dir" / "dir_file"
    dir_file_copy = tmp_dir / "dir" / "subdir" / "dir_file_copy"
    stage = run_copy(
        os.fspath(dir_file_path),
        os.fspath(dir_file_copy),
        fname=os.fspath(dir_file_copy) + ".dvc",
        single_stage=True,
    )
    assert stage is not None
    assert dir_file_copy.read_text() == "dir file content"
    stages["dir_file_copy"] = stage

    last_stage = tmp_dir / "dir" / DVC_FILE
    deps = [os.fspath(origin_copy_2), os.fspath(dir_file_copy)]
    stage = dvc.run(
        cmd="echo {}".format(" ".join(deps)),
        fname=os.fspath(last_stage),
        deps=deps,
        single_stage=True,
    )
    assert stage is not None
    stages["last_stage"] = stage

    # Unrelated are to verify that reproducing `dir` will not trigger them too
    assert (
        run_copy(os.fspath(origin_copy), "unrelated1", single_stage=True)
        is not None
    )
    assert (
        run_copy(os.fspath(dir_file_path), "unrelated2", single_stage=True)
        is not None
    )

    yield stages


def _rewrite_file(path_elements, new_content):
    if isinstance(path_elements, str):
        path_elements = [path_elements]
    file = Path(os.sep.join(path_elements))
    file.unlink()
    file.write_text(new_content)


def _read_out(stage):
    return Path(stage.outs[0].fspath).read_text()


def test_recursive_repro_default(dvc, repro_dir):
    """
    Test recursive repro on dir after a dep outside this dir has changed.
    """
    _rewrite_file("origin_data", "new origin data content")

    stages = dvc.reproduce("dir", recursive=True)

    # Check that the dependency ("origin_copy") and the dependent stages
    # inside the folder have been reproduced ("origin_copy_2", "last_stage")
    assert stages == [
        repro_dir["origin_copy"],
        repro_dir["origin_copy_2"],
        repro_dir["last_stage"],
    ]
    assert _read_out(repro_dir["origin_copy"]) == "new origin data content"
    assert _read_out(repro_dir["origin_copy_2"]) == "new origin data content"


def test_recursive_repro_single(dvc, repro_dir):
    """
    Test recursive single-item repro on dir
    after a dep outside this dir has changed.
    """
    _rewrite_file("origin_data", "new origin content")
    _rewrite_file(["data_dir", "dir_file"], "new dir file content")

    stages = dvc.reproduce("dir", recursive=True, single_item=True)
    # Check that just stages inside given dir
    # with changed direct deps have been reproduced.
    # This means that "origin_copy_2" stage should not be reproduced
    # since it depends on "origin_copy".
    # Also check that "dir_file_copy" stage was reproduced before "last_stage"
    assert stages == [repro_dir["dir_file_copy"], repro_dir["last_stage"]]
    assert _read_out(repro_dir["dir_file_copy"]) == "new dir file content"


def test_recursive_repro_single_force(dvc, repro_dir):
    """
    Test recursive single-item force repro on dir
    without any dependencies changing.
    """
    stages = dvc.reproduce("dir", recursive=True, single_item=True, force=True)
    # Check that all stages inside given dir have been reproduced
    # Also check that "dir_file_copy" stage was reproduced before "last_stage"
    # and that "origin_copy" stage was reproduced before "last_stage" stage
    assert len(stages) == 3
    assert set(stages) == {
        repro_dir["origin_copy_2"],
        repro_dir["dir_file_copy"],
        repro_dir["last_stage"],
    }
    assert stages.index(repro_dir["origin_copy_2"]) < stages.index(
        repro_dir["last_stage"]
    )
    assert stages.index(repro_dir["dir_file_copy"]) < stages.index(
        repro_dir["last_stage"]
    )


def test_recursive_repro_empty_dir(tmp_dir, dvc):
    """
    Test recursive repro on an empty directory
    """
    (tmp_dir / "emptydir").mkdir()

    stages = dvc.reproduce("emptydir", recursive=True, force=True)
    assert stages == []


def test_recursive_repro_recursive_missing_file(dvc):
    """
    Test recursive repro on a missing file
    """
    with pytest.raises(StageFileDoesNotExistError):
        dvc.reproduce("notExistingStage.dvc", recursive=True)
    with pytest.raises(StageFileDoesNotExistError):
        dvc.reproduce("notExistingDir/", recursive=True)


def test_recursive_repro_on_stage_file(dvc, repro_dir):
    """
    Test recursive repro on a stage file instead of directory
    """
    stages = dvc.reproduce(
        repro_dir["origin_copy_2"].relpath, recursive=True, force=True
    )
    assert stages == [repro_dir["origin_copy"], repro_dir["origin_copy_2"]]


def test_dvc_formatting_retained(tmp_dir, dvc, run_copy):
    tmp_dir.dvc_gen("foo", "foo content")
    stage = run_copy(
        "foo", "foo_copy", fname="foo_copy.dvc", single_stage=True
    )
    stage_path = tmp_dir / stage.relpath

    # Add comments and custom formatting to DVC-file
    lines = list(map(_format_dvc_line, stage_path.read_text().splitlines()))
    lines.insert(0, "# Starting comment")
    stage_text = "".join(line + "\n" for line in lines)
    stage_path.write_text(stage_text)

    # Rewrite data source and repro
    (tmp_dir / "foo").write_text("new foo")
    dvc.reproduce("foo_copy.dvc", force=True)

    def _hide_md5(text):
        return re.sub(r"\b[a-f0-9]{32}\b", "<md5>", text)

    def _hide_size(text):
        return re.sub(r"size: [0-9]*\b", "size: <size>", text)

    def _mask(text):
        return _hide_size(_hide_md5(text))

    assert _mask(stage_text) == _mask(stage_path.read_text())


def _format_dvc_line(line):
    # Add line comment for all cache and md5 keys
    if "cache:" in line or "md5:" in line:
        return line + " # line comment"
    # Format command as one word per line
    if line.startswith("cmd: "):
        pre, command = line.split(None, 1)
        return pre + " >\n" + "\n".join("  " + s for s in command.split())
    return line


def test_downstream(dvc):
    # The dependency graph should look like this:
    #
    #       E
    #      / \
    #     D   F
    #    / \   \
    #   B   C   G
    #    \ /
    #     A
    #
    assert main(["run", "--single-stage", "-o", "A", "echo A>A"]) == 0
    assert (
        main(["run", "--single-stage", "-d", "A", "-o", "B", "echo B>B"]) == 0
    )
    assert (
        main(["run", "--single-stage", "-d", "A", "-o", "C", "echo C>C"]) == 0
    )
    assert (
        main(
            [
                "run",
                "--single-stage",
                "-d",
                "B",
                "-d",
                "C",
                "-o",
                "D",
                "echo D>D",
            ]
        )
        == 0
    )
    assert main(["run", "--single-stage", "-o", "G", "echo G>G"]) == 0
    assert (
        main(["run", "--single-stage", "-d", "G", "-o", "F", "echo F>F"]) == 0
    )
    assert (
        main(
            [
                "run",
                "--single-stage",
                "-d",
                "D",
                "-d",
                "F",
                "-o",
                "E",
                "echo E>E",
            ]
        )
        == 0
    )

    # We want the evaluation to move from B to E
    #
    #       E
    #      /
    #     D
    #    /
    #   B
    #
    evaluation = dvc.reproduce("B.dvc", downstream=True, force=True)

    assert len(evaluation) == 3
    assert evaluation[0].relpath == "B.dvc"
    assert evaluation[1].relpath == "D.dvc"
    assert evaluation[2].relpath == "E.dvc"

    # B, C should be run (in any order) before D
    # See https://github.com/iterative/dvc/issues/3602
    evaluation = dvc.reproduce("A.dvc", downstream=True, force=True)

    assert len(evaluation) == 5
    assert evaluation[0].relpath == "A.dvc"
    assert {evaluation[1].relpath, evaluation[2].relpath} == {"B.dvc", "C.dvc"}
    assert evaluation[3].relpath == "D.dvc"
    assert evaluation[4].relpath == "E.dvc"


def test_repro_when_cmd_changes(tmp_dir, dvc, run_copy, mocker):
    from dvc.dvcfile import SingleStageFile

    tmp_dir.gen("foo", "foo")
    stage = run_copy("foo", "bar", single_stage=True)
    assert not dvc.reproduce(stage.addressing)

    from dvc.stage.run import cmd_run

    m = mocker.patch("dvc.stage.run.cmd_run", wraps=cmd_run)

    data = SingleStageFile(dvc, stage.path)._load()[0]
    data["cmd"] = "  ".join(stage.cmd.split())  # change cmd spacing by two
    dump_yaml(stage.path, data)

    assert dvc.status([stage.addressing]) == {
        stage.addressing: ["changed checksum"]
    }
    assert dvc.reproduce(stage.addressing)[0] == stage
    m.assert_called_once_with(
        stage, checkpoint_func=None, dry=False, run_env=None
    )
