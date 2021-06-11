import logging

from dvc.cli import parse_args
from dvc.command.add import CmdAdd


def test_add(mocker, dvc):
    cli_args = parse_args(
        [
            "add",
            "--recursive",
            "--no-commit",
            "--external",
            "--glob",
            "--file",
            "file",
            "--desc",
            "stage description",
            "data",
        ]
    )
    assert cli_args.func == CmdAdd

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "add", autospec=True)

    assert cmd.run() == 0

    m.assert_called_once_with(
        ["data"],
        recursive=True,
        no_commit=True,
        glob=True,
        fname="file",
        external=True,
        out=None,
        remote=None,
        to_remote=False,
        desc="stage description",
        jobs=None,
    )


def test_add_to_remote(mocker):
    cli_args = parse_args(
        [
            "add",
            "s3://bucket/foo",
            "--to-remote",
            "--out",
            "bar",
            "--remote",
            "remote",
        ]
    )
    assert cli_args.func == CmdAdd

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "add", autospec=True)

    assert cmd.run() == 0

    m.assert_called_once_with(
        ["s3://bucket/foo"],
        recursive=False,
        no_commit=False,
        glob=False,
        fname=None,
        external=False,
        out="bar",
        remote="remote",
        to_remote=True,
        desc=None,
        jobs=None,
    )


def test_add_to_remote_invalid_combinations(mocker, caplog):
    cli_args = parse_args(
        ["add", "s3://bucket/foo", "s3://bucket/bar", "--to-remote"]
    )
    assert cli_args.func == CmdAdd

    cmd = cli_args.func(cli_args)
    with caplog.at_level(logging.ERROR, logger="dvc"):
        assert cmd.run() == 1
        expected_msg = "multiple targets can't be used with --to-remote"
        assert expected_msg in caplog.text

    for option, value in (("--remote", "remote"), ("--jobs", "4")):
        cli_args = parse_args(["add", "foo", option, value])

        cmd = cli_args.func(cli_args)
        with caplog.at_level(logging.ERROR, logger="dvc"):
            assert cmd.run() == 1
            expected_msg = f"{option} can't be used without --to-remote"
            assert expected_msg in caplog.text


def test_add_to_cache_invalid_combinations(mocker, caplog):
    cli_args = parse_args(
        ["add", "s3://bucket/foo", "s3://bucket/bar", "-o", "foo"]
    )
    assert cli_args.func == CmdAdd

    cmd = cli_args.func(cli_args)
    with caplog.at_level(logging.ERROR, logger="dvc"):
        assert cmd.run() == 1
        expected_msg = "multiple targets can't be used with -o"
        assert expected_msg in caplog.text
