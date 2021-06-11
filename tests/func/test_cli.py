import os

from dvc.cli import parse_args
from dvc.command.add import CmdAdd
from dvc.command.base import CmdBase
from dvc.command.checkout import CmdCheckout
from dvc.command.config import CmdConfig
from dvc.command.data_sync import CmdDataPull, CmdDataPush
from dvc.command.gc import CmdGC
from dvc.command.init import CmdInit
from dvc.command.remove import CmdRemove
from dvc.command.repro import CmdRepro
from dvc.command.run import CmdRun
from dvc.command.status import CmdDataStatus
from dvc.exceptions import DvcException, DvcParserError
from tests.basic_env import TestDvc


class TestArgParse(TestDvc):
    def test(self):
        args = parse_args(["init"])
        self.assertIsInstance(args.func(args), CmdInit)


class TestRun(TestDvc):
    def test(self):
        dep1 = "dep1"
        dep2 = "dep2"

        out1 = "out1"
        out2 = "out2"

        out_no_cache1 = "out_no_cache1"
        out_no_cache2 = "out_no_cache2"

        fname = "dvc.dvc"
        cmd = "cmd"
        arg1 = "arg1"
        arg2 = "arg2"

        args = parse_args(
            [
                "run",
                "-d",
                dep1,
                "--deps",
                dep2,
                "-o",
                out1,
                "--outs",
                out2,
                "-O",
                out_no_cache1,
                "--outs-no-cache",
                out_no_cache2,
                "--file",
                fname,
                cmd,
                arg1,
                arg2,
            ]
        )
        cmd_cls = args.func(args)
        self.assertIsInstance(cmd_cls, CmdRun)
        self.assertEqual(args.deps, [dep1, dep2])
        self.assertEqual(args.outs, [out1, out2])
        self.assertEqual(args.outs_no_cache, [out_no_cache1, out_no_cache2])
        self.assertEqual(args.file, fname)
        self.assertEqual(args.cmd, [cmd, arg1, arg2])

        cmd_cls.repo.close()


class TestPull(TestDvc):
    def test(self):
        args = parse_args(["pull"])
        cmd = args.func(args)
        self.assertIsInstance(cmd, CmdDataPull)

        cmd.repo.close()


class TestPush(TestDvc):
    def test(self):
        args = parse_args(["push"])
        cmd = args.func(args)
        self.assertIsInstance(cmd, CmdDataPush)

        cmd.repo.close()


class TestStatus(TestDvc):
    def test(self):
        args = parse_args(["status"])
        cmd = args.func(args)
        self.assertIsInstance(cmd, CmdDataStatus)

        cmd.repo.close()


class TestRepro(TestDvc):
    def test(self):
        target1 = "1"
        target2 = "2"

        args = parse_args(
            ["repro", target1, target2, "-f", "--force", "-s", "--single-item"]
        )

        cmd = args.func(args)
        self.assertIsInstance(cmd, CmdRepro)
        self.assertEqual(args.targets, [target1, target2])
        self.assertEqual(args.force, True)
        self.assertEqual(args.single_item, True)

        cmd.repo.close()


class TestRemove(TestDvc):
    def test(self):
        target1 = "1"
        target2 = "2"

        args = parse_args(["remove", target1, target2])

        cmd = args.func(args)
        self.assertIsInstance(cmd, CmdRemove)
        self.assertEqual(args.targets, [target1, target2])

        cmd.repo.close()


class TestAdd(TestDvc):
    def test(self):
        target1 = "1"
        target2 = "2"

        args = parse_args(["add", target1, target2])

        cmd = args.func(args)
        self.assertIsInstance(cmd, CmdAdd)
        self.assertEqual(args.targets, [target1, target2])

        cmd.repo.close()


class TestGC(TestDvc):
    def test(self):
        args = parse_args(["gc"])
        cmd = args.func(args)
        self.assertIsInstance(cmd, CmdGC)

        cmd.repo.close()


class TestGCMultipleDvcRepos(TestDvc):
    def test(self):
        args = parse_args(["gc", "-p", "/tmp/asdf", "/tmp/xyz"])

        cmd = args.func(args)
        self.assertIsInstance(cmd, CmdGC)

        self.assertEqual(args.repos, ["/tmp/asdf", "/tmp/xyz"])

        cmd.repo.close()


class TestConfig(TestDvc):
    def test(self):
        name = "section.option"
        value = "1"

        args = parse_args(["config", "-u", "--unset", name, value])

        cmd = args.func(args)
        self.assertIsInstance(cmd, CmdConfig)
        self.assertEqual(args.unset, True)
        self.assertEqual(args.name, (False, "section", "option"))
        self.assertEqual(args.value, value)

    def test_config_list(self):
        args = parse_args(["config", "--list"])

        self.assertTrue(args.list)
        self.assertIsNone(args.name)
        self.assertIsNone(args.value)


class TestCheckout(TestDvc):
    def test(self):
        args = parse_args(["checkout"])
        cmd = args.func(args)
        self.assertIsInstance(cmd, CmdCheckout)

        cmd.repo.close()


class TestFindRoot(TestDvc):
    def test(self):
        class Cmd(CmdBase):
            def run(self):
                pass

        class A:
            quiet = False
            verbose = True
            cd = os.path.pardir

        args = A()
        with self.assertRaises(DvcException):
            Cmd(args)


class TestCd(TestDvc):
    def test(self):
        class Cmd(CmdBase):
            def run(self):
                pass

        class A:
            quiet = False
            verbose = True
            cd = os.path.pardir

        parent_dir = os.path.realpath(os.path.pardir)
        args = A()
        with self.assertRaises(DvcException):
            Cmd(args)
        current_dir = os.path.realpath(os.path.curdir)
        self.assertEqual(parent_dir, current_dir)


def test_unknown_command_help(capsys):
    try:
        _ = parse_args(["unknown"])
    except DvcParserError:
        pass
    captured = capsys.readouterr()
    output = captured.out
    try:
        _ = parse_args(["--help"])
    except SystemExit:
        pass
    captured = capsys.readouterr()
    help_output = captured.out
    assert output == help_output


def test_unknown_subcommand_help(capsys):
    sample_subcommand = "push"
    try:
        _ = parse_args([sample_subcommand, "--unknown"])
    except DvcParserError:
        pass
    captured = capsys.readouterr()
    output = captured.out
    try:
        _ = parse_args([sample_subcommand, "--help"])
    except SystemExit:
        pass
    captured = capsys.readouterr()
    help_output = captured.out
    assert output == help_output
