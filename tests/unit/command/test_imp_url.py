import logging

from dvc.cli import parse_args
from dvc.command.imp_url import CmdImportUrl
from dvc.exceptions import DvcException


def test_import_url(mocker):
    cli_args = parse_args(
        [
            "import-url",
            "src",
            "out",
            "--file",
            "file",
            "--jobs",
            "4",
            "--desc",
            "description",
        ]
    )
    assert cli_args.func == CmdImportUrl

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "imp_url", autospec=True)

    assert cmd.run() == 0

    m.assert_called_once_with(
        "src",
        out="out",
        fname="file",
        no_exec=False,
        remote=None,
        to_remote=False,
        desc="description",
        jobs=4,
    )


def test_failed_import_url(mocker, caplog):
    cli_args = parse_args(["import-url", "http://somesite.com/file_name"])
    assert cli_args.func == CmdImportUrl

    cmd = cli_args.func(cli_args)
    mocker.patch.object(cmd.repo, "imp_url", side_effect=DvcException("error"))
    with caplog.at_level(logging.ERROR, logger="dvc"):
        assert cmd.run() == 1
        expected_error = (
            "failed to import http://somesite.com/file_name. "
            "You could also try downloading it manually, and "
            "adding it with `dvc add`."
        )
        assert expected_error in caplog.text


def test_import_url_no_exec(mocker):
    cli_args = parse_args(
        [
            "import-url",
            "--no-exec",
            "src",
            "out",
            "--file",
            "file",
            "--desc",
            "description",
        ]
    )

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "imp_url", autospec=True)

    assert cmd.run() == 0

    m.assert_called_once_with(
        "src",
        out="out",
        fname="file",
        no_exec=True,
        remote=None,
        to_remote=False,
        desc="description",
        jobs=None,
    )


def test_import_url_to_remote(mocker):
    cli_args = parse_args(
        [
            "import-url",
            "s3://bucket/foo",
            "bar",
            "--to-remote",
            "--remote",
            "remote",
            "--desc",
            "description",
        ]
    )
    assert cli_args.func == CmdImportUrl

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "imp_url", autospec=True)

    assert cmd.run() == 0

    m.assert_called_once_with(
        "s3://bucket/foo",
        out="bar",
        fname=None,
        no_exec=False,
        remote="remote",
        to_remote=True,
        desc="description",
        jobs=None,
    )


def test_import_url_to_remote_invalid_combination(mocker, caplog):
    cli_args = parse_args(
        [
            "import-url",
            "s3://bucket/foo",
            "bar",
            "--to-remote",
            "--remote",
            "remote",
            "--no-exec",
        ]
    )
    assert cli_args.func == CmdImportUrl

    cmd = cli_args.func(cli_args)
    with caplog.at_level(logging.ERROR, logger="dvc"):
        assert cmd.run() == 1
        expected_msg = "--no-exec can't be combined with --to-remote"
        assert expected_msg in caplog.text

    cli_args = parse_args(
        ["import-url", "s3://bucket/foo", "bar", "--remote", "remote"]
    )

    cmd = cli_args.func(cli_args)
    with caplog.at_level(logging.ERROR, logger="dvc"):
        assert cmd.run() == 1
        expected_msg = "--remote can't be used without --to-remote"
        assert expected_msg in caplog.text
