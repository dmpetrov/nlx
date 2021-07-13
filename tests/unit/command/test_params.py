from dvc.cli import parse_args
from dvc.command.params import CmdParamsDiff


def test_params_diff(dvc, mocker):
    cli_args = parse_args(
        [
            "params",
            "diff",
            "HEAD~10",
            "HEAD~1",
            "--targets",
            "target",
            "--all",
            "--show-json",
            "--show-md",
            "--no-path",
            "--deps",
        ]
    )
    assert cli_args.func == CmdParamsDiff

    cmd = cli_args.func(cli_args)
    params_diff = mocker.patch("dvc.repo.params.diff.diff", return_value={})
    show_diff_mock = mocker.patch("dvc.compare.show_diff")

    assert cmd.run() == 0

    params_diff.assert_called_once_with(
        cmd.repo,
        a_rev="HEAD~10",
        b_rev="HEAD~1",
        targets=["target"],
        all=True,
        deps=True,
    )
    show_diff_mock.assert_not_called()


def test_params_diff_from_cli(dvc, mocker):
    cli_args = parse_args(["params", "diff"])
    assert cli_args.func == CmdParamsDiff

    cmd = cli_args.func(cli_args)
    params_diff = mocker.patch("dvc.repo.params.diff.diff", return_value={})
    show_diff_mock = mocker.patch("dvc.compare.show_diff")

    assert cmd.run() == 0

    params_diff.assert_called_once_with(
        cmd.repo,
        a_rev=None,
        b_rev=None,
        all=False,
        targets=None,
        deps=False,
    )
    show_diff_mock.assert_called_once_with(
        {}, title="Param", markdown=False, no_path=False, show_changes=False
    )


def test_params_diff_show_json(dvc, mocker, capsys):
    cli_args = parse_args(
        ["params", "diff", "HEAD~10", "HEAD~1", "--show-json"]
    )
    cmd = cli_args.func(cli_args)
    mocker.patch(
        "dvc.repo.params.diff.diff", return_value={"params.yaml": {"a": "b"}}
    )
    show_diff_mock = mocker.patch("dvc.compare.show_diff")

    assert cmd.run() == 0
    out, _ = capsys.readouterr()
    assert '{"params.yaml": {"a": "b"}}\n' in out
    show_diff_mock.assert_not_called()
