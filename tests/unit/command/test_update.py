from dvc.cli import parse_args
from dvc.command.update import CmdUpdate


def test_update(dvc, mocker):
    cli_args = parse_args(
        [
            "update",
            "target1",
            "target2",
            "--rev",
            "REV",
            "--recursive",
            "-j",
            "8",
        ]
    )
    assert cli_args.func == CmdUpdate
    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.Repo.update")

    assert cmd.run() == 0

    m.assert_called_once_with(
        targets=["target1", "target2"],
        rev="REV",
        recursive=True,
        to_remote=False,
        remote=None,
        jobs=8,
    )


def test_update_to_remote(dvc, mocker):
    cli_args = parse_args(
        [
            "update",
            "target1",
            "target2",
            "--to-remote",
            "-j",
            "5",
            "-r",
            "remote",
            "--recursive",
        ]
    )
    assert cli_args.func == CmdUpdate
    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.Repo.update")

    assert cmd.run() == 0

    m.assert_called_once_with(
        targets=["target1", "target2"],
        rev=None,
        recursive=True,
        to_remote=True,
        remote="remote",
        jobs=5,
    )
