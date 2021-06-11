from dvc.cli import parse_args
from dvc.command.get_url import CmdGetUrl


def test_get_url(mocker):
    cli_args = parse_args(["get-url", "src", "out", "-j", "5"])
    assert cli_args.func == CmdGetUrl

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.Repo.get_url")

    assert cmd.run() == 0

    m.assert_called_once_with("src", out="out", jobs=5)
