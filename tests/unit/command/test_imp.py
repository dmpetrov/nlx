from dvc.cli import parse_args
from dvc.command.imp import CmdImport


def test_import(mocker):
    cli_args = parse_args(
        [
            "import",
            "repo_url",
            "src",
            "--out",
            "out",
            "--file",
            "file",
            "--rev",
            "version",
            "--desc",
            "description",
            "--jobs",
            "3",
        ]
    )
    assert cli_args.func == CmdImport

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "imp", autospec=True)

    assert cmd.run() == 0

    m.assert_called_once_with(
        "repo_url",
        path="src",
        out="out",
        fname="file",
        rev="version",
        no_exec=False,
        desc="description",
        jobs=3,
    )


def test_import_no_exec(mocker):
    cli_args = parse_args(
        [
            "import",
            "repo_url",
            "src",
            "--out",
            "out",
            "--file",
            "file",
            "--rev",
            "version",
            "--no-exec",
            "--desc",
            "description",
        ]
    )

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "imp", autospec=True)

    assert cmd.run() == 0

    m.assert_called_once_with(
        "repo_url",
        path="src",
        out="out",
        fname="file",
        rev="version",
        no_exec=True,
        desc="description",
        jobs=None,
    )
