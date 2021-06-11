from dvc.cli import parse_args
from dvc.command.run import CmdRun
from tests.utils.asserts import called_once_with_subset


def test_run(mocker, dvc):
    cli_args = parse_args(
        [
            "run",
            "--name",
            "nam",
            "--deps",
            "deps",
            "--outs",
            "outs",
            "--outs-no-cache",
            "outs-no-cache",
            "--metrics",
            "metrics",
            "--metrics-no-cache",
            "metrics-no-cache",
            "--plots",
            "plots",
            "--plots-no-cache",
            "plots-no-cache",
            "--live",
            "live",
            "--live-no-cache",
            "live-no-cache",
            "--live-no-summary",
            "--live-no-html",
            "--file",
            "file",
            "--wdir",
            "wdir",
            "--no-exec",
            "--force",
            "--no-run-cache",
            "--no-commit",
            "--outs-persist",
            "outs-persist",
            "--outs-persist-no-cache",
            "outs-persist-no-cache",
            "--checkpoints",
            "checkpoints",
            "--always-changed",
            "--params",
            "file:param1,param2",
            "--params",
            "param3",
            "--external",
            "--desc",
            "description",
            "command",
        ]
    )
    assert cli_args.func == CmdRun

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "run", autospec=True)

    assert cmd.run() == 0

    assert called_once_with_subset(
        m,
        deps=["deps"],
        outs=["outs"],
        outs_no_cache=["outs-no-cache"],
        metrics=["metrics"],
        metrics_no_cache=["metrics-no-cache"],
        plots=["plots"],
        plots_no_cache=["plots-no-cache"],
        outs_persist=["outs-persist"],
        outs_persist_no_cache=["outs-persist-no-cache"],
        checkpoints=["checkpoints"],
        params=["file:param1,param2", "param3"],
        live="live",
        live_no_cache="live-no-cache",
        live_no_summary=True,
        live_no_html=True,
        fname="file",
        wdir="wdir",
        no_exec=True,
        force=True,
        run_cache=False,
        no_commit=True,
        always_changed=True,
        cmd="command",
        name="nam",
        single_stage=False,
        external=True,
        desc="description",
    )


def test_run_args_from_cli(mocker, dvc):
    args = parse_args(["run", "echo", "foo"])
    cmd = args.func(args)
    m = mocker.patch.object(cmd.repo, "run", autospec=True)
    assert cmd.run() == 0
    assert called_once_with_subset(
        m,
        deps=[],
        outs=[],
        outs_no_cache=[],
        metrics=[],
        metrics_no_cache=[],
        plots=[],
        plots_no_cache=[],
        live=None,
        live_no_cache=None,
        live_no_summary=False,
        live_no_html=False,
        outs_persist=[],
        outs_persist_no_cache=[],
        checkpoints=[],
        params=[],
        fname=None,
        wdir=None,
        no_exec=False,
        force=False,
        run_cache=True,
        no_commit=False,
        always_changed=False,
        cmd="echo foo",
        name=None,
        single_stage=False,
        external=False,
        desc=None,
    )


def test_run_args_with_spaces(mocker, dvc):
    args = parse_args(["run", "echo", "foo bar"])
    cmd = args.func(args)
    m = mocker.patch.object(cmd.repo, "run", autospec=True)
    assert cmd.run() == 0
    assert called_once_with_subset(
        m,
        deps=[],
        outs=[],
        outs_no_cache=[],
        metrics=[],
        metrics_no_cache=[],
        plots=[],
        plots_no_cache=[],
        live=None,
        live_no_cache=None,
        live_no_summary=False,
        live_no_html=False,
        outs_persist=[],
        outs_persist_no_cache=[],
        checkpoints=[],
        params=[],
        fname=None,
        wdir=None,
        no_exec=False,
        force=False,
        run_cache=True,
        no_commit=False,
        always_changed=False,
        cmd='echo "foo bar"',
        name=None,
        single_stage=False,
        external=False,
        desc=None,
    )
