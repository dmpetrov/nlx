import json

from dvc.cli import parse_args
from dvc.command.ls import CmdList


def _test_cli(mocker, *args):
    cli_args = parse_args(["list", *args])
    assert cli_args.func == CmdList

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.Repo.ls")

    assert cmd.run() == 0
    return m


def test_list(mocker):
    url = "local_dir"
    m = _test_cli(mocker, url)
    m.assert_called_once_with(
        url, None, recursive=False, rev=None, dvc_only=False
    )


def test_list_recursive(mocker):
    url = "local_dir"
    m = _test_cli(mocker, url, "-R")
    m.assert_called_once_with(
        url, None, recursive=True, rev=None, dvc_only=False
    )


def test_list_git_ssh_rev(mocker):
    url = "git@github.com:repo"
    m = _test_cli(mocker, url, "--rev", "123")
    m.assert_called_once_with(
        url, None, recursive=False, rev="123", dvc_only=False
    )


def test_list_targets(mocker):
    url = "local_dir"
    target = "subdir"
    m = _test_cli(mocker, url, target)
    m.assert_called_once_with(
        url, target, recursive=False, rev=None, dvc_only=False
    )


def test_list_outputs_only(mocker):
    url = "local_dir"
    m = _test_cli(mocker, url, None, "--dvc-only")
    m.assert_called_once_with(
        url, None, recursive=False, rev=None, dvc_only=True
    )


def test_show_json(mocker, capsys):
    cli_args = parse_args(["list", "local_dir", "--show-json"])
    assert cli_args.func == CmdList

    cmd = cli_args.func(cli_args)

    result = [{"key": "val"}]
    mocker.patch("dvc.repo.Repo.ls", return_value=result)

    assert cmd.run() == 0
    out, _ = capsys.readouterr()
    assert json.dumps(result) in out


def test_show_colors(mocker, capsys, monkeypatch):
    cli_args = parse_args(["list", "local_dir"])
    assert cli_args.func == CmdList
    cmd = cli_args.func(cli_args)

    monkeypatch.setenv(
        "LS_COLORS", "ex=01;32:rs=0:di=01;34:*.xml=01;31:*.dvc=01;33:"
    )
    result = [
        {"isdir": False, "isexec": 0, "isout": False, "path": ".dvcignore"},
        {"isdir": False, "isexec": 0, "isout": False, "path": ".gitignore"},
        {"isdir": False, "isexec": 0, "isout": False, "path": "README.md"},
        {"isdir": True, "isexec": 0, "isout": True, "path": "data"},
        {"isdir": False, "isexec": 0, "isout": True, "path": "structure.xml"},
        {
            "isdir": False,
            "isexec": 0,
            "isout": False,
            "path": "structure.xml.dvc",
        },
        {"isdir": True, "isexec": 0, "isout": False, "path": "src"},
        {"isdir": False, "isexec": 1, "isout": False, "path": "run.sh"},
    ]
    mocker.patch("dvc.repo.Repo.ls", return_value=result)

    assert cmd.run() == 0
    out, _ = capsys.readouterr()
    entries = out.splitlines()

    assert entries == [
        ".dvcignore",
        ".gitignore",
        "README.md",
        "\x1b[01;34mdata\x1b[0m",
        "\x1b[01;31mstructure.xml\x1b[0m",
        "\x1b[01;33mstructure.xml.dvc\x1b[0m",
        "\x1b[01;34msrc\x1b[0m",
        "\x1b[01;32mrun.sh\x1b[0m",
    ]
