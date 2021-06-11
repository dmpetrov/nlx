import json

import pytest

from dvc.cli import parse_args
from dvc.command.status import CmdDataStatus


def test_cloud_status(tmp_dir, dvc, mocker):
    cli_args = parse_args(
        [
            "status",
            "--cloud",
            "target1",
            "target2",
            "--jobs",
            "2",
            "--remote",
            "remote",
            "--all-branches",
            "--all-tags",
            "--all-commits",
            "--with-deps",
            "--recursive",
        ]
    )
    assert cli_args.func == CmdDataStatus

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "status", autospec=True, return_value={})

    assert cmd.run() == 0

    m.assert_called_once_with(
        cloud=True,
        targets=["target1", "target2"],
        jobs=2,
        remote="remote",
        all_branches=True,
        all_tags=True,
        all_commits=True,
        with_deps=True,
        recursive=True,
    )


@pytest.mark.parametrize("status", [{}, {"a": "b", "c": [1, 2, 3]}, [1, 2, 3]])
def test_status_show_json(dvc, mocker, caplog, status):
    cli_args = parse_args(["status", "--show-json"])
    assert cli_args.func == CmdDataStatus

    cmd = cli_args.func(cli_args)

    mocker.patch.object(cmd.repo, "status", autospec=True, return_value=status)
    caplog.clear()
    assert cmd.run() == 0
    assert caplog.messages == [json.dumps(status)]


@pytest.mark.parametrize(
    "status, ret", [({}, 0), ({"a": "b", "c": [1, 2, 3]}, 1), ([1, 2, 3], 1)]
)
def test_status_quiet(dvc, mocker, caplog, capsys, status, ret):
    cli_args = parse_args(["status", "-q"])
    assert cli_args.func == CmdDataStatus

    cmd = cli_args.func(cli_args)

    mocker.patch.object(cmd.repo, "status", autospec=True, return_value=status)
    caplog.clear()
    assert cmd.run() == ret
    assert not caplog.messages
    captured = capsys.readouterr()
    assert not captured.out


def test_status_empty(dvc, mocker, capsys):
    cli_args = parse_args(["status"])
    assert cli_args.func == CmdDataStatus

    cmd = cli_args.func(cli_args)

    spy = mocker.spy(cmd.repo.stage, "collect_repo")

    assert cmd.run() == 0

    captured = capsys.readouterr()
    assert "no data or pipelines tracked" in captured.out
    # stages should only be collected once
    assert spy.call_count == 1


@pytest.mark.parametrize(
    "cloud_opts, expected_message",
    [
        (["--cloud"], "Cache and remote 'default' are in sync"),
        (["--remote", "remote1"], "Cache and remote 'remote1' are in sync"),
        ([], "Data and pipelines are up to date"),
    ],
)
def test_status_up_to_date(dvc, mocker, capsys, cloud_opts, expected_message):
    cli_args = parse_args(["status", *cloud_opts])
    assert cli_args.func == CmdDataStatus

    cmd = cli_args.func(cli_args)

    mocker.patch.dict(cmd.repo.config, {"core": {"remote": "default"}})
    mocker.patch.object(cmd.repo, "status", autospec=True, return_value={})
    mocker.patch.object(
        cmd.repo.stage, "collect_repo", return_value=[object()], autospec=True
    )

    assert cmd.run() == 0
    captured = capsys.readouterr()
    assert expected_message in captured.out
