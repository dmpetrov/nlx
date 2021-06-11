from dvc.cli import parse_args
from dvc.command.get import CmdGet


def test_get(mocker):
    cli_args = parse_args(
        [
            "get",
            "repo_url",
            "src",
            "--out",
            "out",
            "--rev",
            "version",
            "--jobs",
            "4",
        ]
    )
    assert cli_args.func == CmdGet

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.Repo.get")

    assert cmd.run() == 0

    m.assert_called_once_with(
        "repo_url", path="src", out="out", rev="version", jobs=4
    )


def test_get_url(mocker, caplog):
    cli_args = parse_args(
        ["get", "repo_url", "src", "--rev", "version", "--show-url"]
    )
    assert cli_args.func == CmdGet

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.api.get_url", return_value="resource_url")

    assert cmd.run() == 0
    assert "resource_url" in caplog.text

    m.assert_called_once_with("src", repo="repo_url", rev="version")
