from dvc.cli import parse_args
from dvc.command.live import CmdLiveDiff, CmdLiveShow


def test_live_diff(dvc, mocker):
    cli_args = parse_args(
        [
            "live",
            "diff",
            "--out",
            "result.extension",
            "target",
            "--revs",
            "HEAD",
            "rev1",
        ]
    )
    assert cli_args.func == CmdLiveDiff

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.live.Live.show", return_value=({}, {}))

    assert cmd.run() == 1

    m.assert_called_once_with(target="target", revs=["HEAD", "rev1"])


def test_live_show(dvc, mocker):
    cli_args = parse_args(
        ["live", "show", "-o", "result.extension", "datafile"]
    )
    assert cli_args.func == CmdLiveShow

    cmd = cli_args.func(cli_args)

    m = mocker.patch("dvc.repo.live.Live.show", return_value=({}, {}))

    assert cmd.run() == 1

    m.assert_called_once_with(target="datafile", revs=None)
