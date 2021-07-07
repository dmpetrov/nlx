import json

from dvc.cli import parse_args
from dvc.command.metrics import CmdMetricsDiff, CmdMetricsShow


def test_metrics_diff(dvc, mocker, capsys):
    cli_args = parse_args(
        [
            "metrics",
            "diff",
            "HEAD~10",
            "HEAD~1",
            "-R",
            "--all",
            "--show-md",
            "--targets",
            "target1",
            "target2",
            "--no-path",
        ]
    )

    assert cli_args.func == CmdMetricsDiff

    cmd = cli_args.func(cli_args)
    diff = {"metrics.yaml": {"": {"old": 1, "new": 3}}}
    metrics_diff = mocker.patch(
        "dvc.repo.metrics.diff.diff", return_value=diff
    )
    show_diff_mock = mocker.patch("dvc.compare.show_diff")

    assert cmd.run() == 0

    metrics_diff.assert_called_once_with(
        cmd.repo,
        targets=["target1", "target2"],
        a_rev="HEAD~10",
        b_rev="HEAD~1",
        recursive=True,
        all=True,
    )
    show_diff_mock.assert_called_once_with(
        diff,
        title="Metric",
        no_path=True,
        precision=5,
        markdown=True,
        round_digits=True,
    )


def test_metrics_diff_json(dvc, mocker, capsys):
    cli_args = parse_args(
        [
            "metrics",
            "diff",
            "HEAD~10",
            "HEAD~1",
            "-R",
            "--all",
            "--show-json",
            "--targets",
            "target1",
            "target2",
            "--no-path",
            "--precision",
            "10",
        ]
    )

    assert cli_args.func == CmdMetricsDiff
    cmd = cli_args.func(cli_args)

    diff = {"metrics.yaml": {"": {"old": 1, "new": 3}}}
    metrics_diff = mocker.patch(
        "dvc.repo.metrics.diff.diff", return_value=diff
    )
    show_diff_mock = mocker.patch("dvc.compare.show_diff")

    assert cmd.run() == 0
    out, _ = capsys.readouterr()
    metrics_diff.assert_called_once_with(
        cmd.repo,
        targets=["target1", "target2"],
        a_rev="HEAD~10",
        b_rev="HEAD~1",
        recursive=True,
        all=True,
    )
    show_diff_mock.assert_not_called()
    assert json.dumps(diff) in out


def test_metrics_show(dvc, mocker):
    cli_args = parse_args(
        [
            "metrics",
            "show",
            "-R",
            "--all-tags",
            "--all-branches",
            "--all-commits",
            "target1",
            "target2",
            "--precision",
            "8",
        ]
    )
    assert cli_args.func == CmdMetricsShow

    cmd = cli_args.func(cli_args)
    m1 = mocker.patch("dvc.repo.metrics.show.show", return_value={})
    m2 = mocker.patch("dvc.compare.show_metrics", return_value="")

    assert cmd.run() == 0

    m1.assert_called_once_with(
        cmd.repo,
        ["target1", "target2"],
        recursive=True,
        all_tags=True,
        all_branches=True,
        all_commits=True,
    )
    m2.assert_called_once_with(
        {},
        markdown=False,
        all_tags=True,
        all_branches=True,
        all_commits=True,
        precision=8,
        round_digits=True,
    )


def test_metrics_show_json(dvc, mocker, capsys):
    cli_args = parse_args(
        [
            "metrics",
            "show",
            "--show-json",
            "-R",
            "--all-tags",
            "--all-branches",
            "--all-commits",
            "target1",
            "target2",
            "--precision",
            "8",
        ]
    )

    assert cli_args.func == CmdMetricsShow
    cmd = cli_args.func(cli_args)
    d = {
        "branch_1": {"metrics.json": {"b": {"ad": 1, "bc": 2}, "c": 4}},
        "branch_2": {"metrics.json": {"a": 1, "b": {"ad": 3, "bc": 4}}},
    }
    metrics_show = mocker.patch("dvc.repo.metrics.show.show", return_value=d)
    show_metrics_mock = mocker.patch("dvc.compare.show_metrics")

    assert cmd.run() == 0
    out, _ = capsys.readouterr()
    metrics_show.assert_called_once_with(
        cmd.repo,
        ["target1", "target2"],
        recursive=True,
        all_tags=True,
        all_branches=True,
        all_commits=True,
    )
    show_metrics_mock.assert_not_called()
    assert json.dumps(d) in out
