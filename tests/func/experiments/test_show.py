import logging
import os
from datetime import datetime

import pytest
from funcy import first

from dvc.exceptions import InvalidArgumentError
from dvc.main import main
from dvc.repo.experiments.base import EXPS_STASH, ExpRefInfo
from dvc.repo.experiments.executor.base import BaseExecutor, ExecutorInfo
from dvc.utils.fs import makedirs
from dvc.utils.serialize import YAMLFileCorruptedError, dump_yaml
from tests.func.test_repro_multistage import COPY_SCRIPT


def test_show_simple(tmp_dir, scm, dvc, exp_stage):
    assert dvc.experiments.show()["workspace"] == {
        "baseline": {
            "data": {
                "metrics": {"metrics.yaml": {"data": {"foo": 1}}},
                "params": {"params.yaml": {"data": {"foo": 1}}},
                "queued": False,
                "running": False,
                "executor": None,
                "timestamp": None,
            }
        }
    }


@pytest.mark.parametrize("workspace", [True, False])
def test_show_experiment(tmp_dir, scm, dvc, exp_stage, workspace):
    baseline_rev = scm.get_rev()
    timestamp = datetime.fromtimestamp(
        scm.gitpython.repo.rev_parse(baseline_rev).committed_date
    )

    dvc.experiments.run(
        exp_stage.addressing, params=["foo=2"], tmp_dir=not workspace
    )
    results = dvc.experiments.show()

    expected_baseline = {
        "data": {
            "metrics": {"metrics.yaml": {"data": {"foo": 1}}},
            "params": {"params.yaml": {"data": {"foo": 1}}},
            "queued": False,
            "running": False,
            "executor": None,
            "timestamp": timestamp,
            "name": "master",
        }
    }
    expected_params = {"foo": 2}

    assert set(results.keys()) == {"workspace", baseline_rev}
    experiments = results[baseline_rev]
    assert len(experiments) == 2
    for rev, exp in experiments.items():
        if rev == "baseline":
            assert exp == expected_baseline
        else:
            assert (
                exp["data"]["metrics"]["metrics.yaml"]["data"]
                == expected_params
            )
            assert (
                exp["data"]["params"]["params.yaml"]["data"] == expected_params
            )


def test_show_queued(tmp_dir, scm, dvc, exp_stage):
    baseline_rev = scm.get_rev()

    dvc.experiments.run(exp_stage.addressing, params=["foo=2"], queue=True)
    exp_rev = dvc.experiments.scm.resolve_rev(f"{EXPS_STASH}@{{0}}")

    results = dvc.experiments.show()[baseline_rev]
    assert len(results) == 2
    exp = results[exp_rev]
    assert exp["data"]["queued"]
    assert exp["data"]["params"]["params.yaml"]["data"] == {"foo": 2}

    # test that only queued experiments for the current baseline are returned
    tmp_dir.gen("foo", "foo")
    scm.add(["foo"])
    scm.commit("new commit")
    new_rev = scm.get_rev()

    dvc.experiments.run(exp_stage.addressing, params=["foo=3"], queue=True)
    exp_rev = dvc.experiments.scm.resolve_rev(f"{EXPS_STASH}@{{0}}")

    results = dvc.experiments.show()[new_rev]
    assert len(results) == 2
    exp = results[exp_rev]
    assert exp["data"]["queued"]
    assert exp["data"]["params"]["params.yaml"]["data"] == {"foo": 3}


@pytest.mark.parametrize("workspace", [True, False])
def test_show_checkpoint(
    tmp_dir, scm, dvc, checkpoint_stage, capsys, workspace
):
    baseline_rev = scm.get_rev()
    results = dvc.experiments.run(
        checkpoint_stage.addressing, params=["foo=2"], tmp_dir=not workspace
    )
    exp_rev = first(results)

    results = dvc.experiments.show()[baseline_rev]
    assert len(results) == checkpoint_stage.iterations + 1

    checkpoints = []
    for rev, exp in results.items():
        if rev != "baseline":
            checkpoints.append(rev)
            assert exp["data"]["checkpoint_tip"] == exp_rev

    capsys.readouterr()
    assert main(["exp", "show", "--no-pager"]) == 0
    cap = capsys.readouterr()

    for i, rev in enumerate(checkpoints):
        if i == 0:
            name = dvc.experiments.get_exact_name(rev)
            name = f"{rev[:7]} [{name}]"
            fs = "╓"
        elif i == len(checkpoints) - 1:
            name = rev[:7]
            fs = "╨"
        else:
            name = rev[:7]
            fs = "╟"
        assert f"{fs} {name}" in cap.out


@pytest.mark.parametrize("workspace", [True, False])
def test_show_checkpoint_branch(
    tmp_dir, scm, dvc, checkpoint_stage, capsys, workspace
):
    results = dvc.experiments.run(
        checkpoint_stage.addressing, params=["foo=2"], tmp_dir=not workspace
    )
    branch_rev = first(results)
    if not workspace:
        dvc.experiments.apply(branch_rev)

    results = dvc.experiments.run(
        checkpoint_stage.addressing,
        checkpoint_resume=branch_rev,
        tmp_dir=not workspace,
    )
    checkpoint_a = first(results)

    dvc.experiments.apply(branch_rev)
    results = dvc.experiments.run(
        checkpoint_stage.addressing,
        checkpoint_resume=branch_rev,
        params=["foo=100"],
        tmp_dir=not workspace,
    )
    checkpoint_b = first(results)

    capsys.readouterr()
    assert main(["exp", "show", "--no-pager"]) == 0
    cap = capsys.readouterr()

    for rev in (checkpoint_a, checkpoint_b):
        ref = dvc.experiments.get_branch_by_rev(rev)
        ref_info = ExpRefInfo.from_ref(ref)
        name = f"{rev[:7]} [{ref_info.name}]"
        assert f"╓ {name}" in cap.out
    assert f"({branch_rev[:7]})" in cap.out


@pytest.mark.parametrize(
    "i_metrics,i_params,e_metrics,e_params,included,excluded",
    [
        (
            "foo",
            "foo",
            None,
            None,
            ["foo"],
            ["bar", "train/foo", "nested.foo"],
        ),
        (
            None,
            None,
            "foo",
            "foo",
            ["bar", "train/foo", "nested.foo"],
            ["foo"],
        ),
        (
            "foo,bar",
            "foo,bar",
            None,
            None,
            ["foo", "bar"],
            ["train/foo", "train/bar", "nested.foo", "nested.bar"],
        ),
        (
            "metrics.yaml:foo,bar",
            "params.yaml:foo,bar",
            None,
            None,
            ["foo", "bar"],
            ["train/foo", "train/bar", "nested.foo", "nested.bar"],
        ),
        (
            "train/*",
            "train/*",
            None,
            None,
            ["train/foo", "train/bar"],
            ["foo", "bar", "nested.foo", "nested.bar"],
        ),
        (
            None,
            None,
            "train/*",
            "train/*",
            ["foo", "bar", "nested.foo", "nested.bar"],
            ["train/foo", "train/bar"],
        ),
        (
            "train/*",
            "train/*",
            "*foo",
            "*foo",
            ["train/bar"],
            ["train/foo", "foo", "bar", "nested.foo", "nested.bar"],
        ),
        (
            "nested.*",
            "nested.*",
            None,
            None,
            ["nested.foo", "nested.bar"],
            ["foo", "bar", "train/foo", "train/bar"],
        ),
        (
            None,
            None,
            "nested.*",
            "nested.*",
            ["foo", "bar", "train/foo", "train/bar"],
            ["nested.foo", "nested.bar"],
        ),
        (
            "*.*",
            "*.*",
            "*.bar",
            "*.bar",
            ["nested.foo"],
            ["foo", "bar", "nested.bar", "train/foo", "train/bar"],
        ),
    ],
)
def test_show_filter(
    tmp_dir,
    scm,
    dvc,
    capsys,
    i_metrics,
    i_params,
    e_metrics,
    e_params,
    included,
    excluded,
):
    capsys.readouterr()
    div = "│" if os.name == "nt" else "┃"

    tmp_dir.gen("copy.py", COPY_SCRIPT)
    params_file = tmp_dir / "params.yaml"
    params_data = {
        "foo": 1,
        "bar": 1,
        "train/foo": 1,
        "train/bar": 1,
        "nested": {"foo": 1, "bar": 1},
    }
    dump_yaml(params_file, params_data)

    dvc.run(
        cmd="python copy.py params.yaml metrics.yaml",
        metrics_no_cache=["metrics.yaml"],
        params=["foo"],
        name="copy-file",
        deps=["copy.py"],
    )
    scm.add(
        [
            "dvc.yaml",
            "dvc.lock",
            "copy.py",
            "params.yaml",
            "metrics.yaml",
            ".gitignore",
        ]
    )
    scm.commit("init")

    command = ["exp", "show", "--no-pager", "--no-timestamp"]
    if i_metrics is not None:
        command.append(f"--include-metrics={i_metrics}")
    if i_params is not None:
        command.append(f"--include-params={i_params}")
    if e_metrics is not None:
        command.append(f"--exclude-metrics={e_metrics}")
    if e_params is not None:
        command.append(f"--exclude-params={e_params}")

    assert main(command) == 0
    cap = capsys.readouterr()

    for i in included:
        assert f"{div} {i} {div}" in cap.out
    for e in excluded:
        assert f"{div} {e} {div}" not in cap.out


def test_show_multiple_commits(tmp_dir, scm, dvc, exp_stage):
    init_rev = scm.get_rev()
    tmp_dir.scm_gen("file", "file", "commit")
    next_rev = scm.get_rev()

    with pytest.raises(InvalidArgumentError):
        dvc.experiments.show(num=-1)

    expected = {"workspace", init_rev, next_rev}
    results = dvc.experiments.show(num=2)
    assert set(results.keys()) == expected

    expected = {"workspace"} | set(scm.branch_revs("master"))
    results = dvc.experiments.show(all_commits=True)
    assert set(results.keys()) == expected

    results = dvc.experiments.show(num=100)
    assert set(results.keys()) == expected


def test_show_sort(tmp_dir, scm, dvc, exp_stage, caplog):
    with caplog.at_level(logging.ERROR):
        assert main(["exp", "show", "--no-pager", "--sort-by=bar"]) != 0
        assert "Unknown sort column" in caplog.text

    with caplog.at_level(logging.ERROR):
        assert main(["exp", "show", "--no-pager", "--sort-by=foo"]) != 0
        assert "Ambiguous sort column" in caplog.text

    assert (
        main(["exp", "show", "--no-pager", "--sort-by=params.yaml:foo"]) == 0
    )

    assert (
        main(["exp", "show", "--no-pager", "--sort-by=metrics.yaml:foo"]) == 0
    )


def test_show_running_workspace(tmp_dir, scm, dvc, exp_stage, capsys):
    pid_dir = os.path.join(dvc.tmp_dir, dvc.experiments.EXEC_PID_DIR)
    makedirs(pid_dir, True)
    info = ExecutorInfo(None, None, None, BaseExecutor.DEFAULT_LOCATION)
    pidfile = os.path.join(pid_dir, f"workspace{BaseExecutor.PIDFILE_EXT}")
    dump_yaml(pidfile, info.to_dict())

    assert dvc.experiments.show()["workspace"] == {
        "baseline": {
            "data": {
                "metrics": {"metrics.yaml": {"data": {"foo": 1}}},
                "params": {"params.yaml": {"data": {"foo": 1}}},
                "queued": False,
                "running": True,
                "executor": info.location,
                "timestamp": None,
            }
        }
    }

    capsys.readouterr()
    assert main(["exp", "show", "--no-pager"]) == 0
    cap = capsys.readouterr()
    assert "Running" in cap.out
    assert info.location in cap.out


def test_show_running_executor(tmp_dir, scm, dvc, exp_stage):
    baseline_rev = scm.get_rev()
    dvc.experiments.run(exp_stage.addressing, params=["foo=2"], queue=True)
    exp_rev = dvc.experiments.scm.resolve_rev(f"{EXPS_STASH}@{{0}}")

    pid_dir = os.path.join(dvc.tmp_dir, dvc.experiments.EXEC_PID_DIR)
    makedirs(pid_dir, True)
    info = ExecutorInfo(None, None, None, BaseExecutor.DEFAULT_LOCATION)
    pidfile = os.path.join(pid_dir, f"{exp_rev}{BaseExecutor.PIDFILE_EXT}")
    dump_yaml(pidfile, info.to_dict())

    results = dvc.experiments.show()
    assert not results[baseline_rev][exp_rev]["data"]["queued"]
    assert results[baseline_rev][exp_rev]["data"]["running"]
    assert results[baseline_rev][exp_rev]["data"]["executor"] == info.location
    assert not results["workspace"]["baseline"]["data"]["running"]


@pytest.mark.parametrize("workspace", [True, False])
def test_show_running_checkpoint(
    tmp_dir, scm, dvc, checkpoint_stage, workspace, mocker
):
    from dvc.repo.experiments.base import EXEC_BRANCH
    from dvc.repo.experiments.executor.local import TempDirExecutor
    from dvc.repo.experiments.utils import exp_refs_by_rev

    baseline_rev = scm.get_rev()
    dvc.experiments.run(
        checkpoint_stage.addressing, params=["foo=2"], queue=True
    )
    stash_rev = dvc.experiments.scm.resolve_rev(f"{EXPS_STASH}@{{0}}")

    run_results = dvc.experiments.run(run_all=True)
    checkpoint_rev = first(run_results)
    exp_ref = first(exp_refs_by_rev(scm, checkpoint_rev))

    pid_dir = os.path.join(dvc.tmp_dir, dvc.experiments.EXEC_PID_DIR)
    makedirs(pid_dir, True)
    executor = (
        BaseExecutor.DEFAULT_LOCATION
        if workspace
        else TempDirExecutor.DEFAULT_LOCATION
    )
    info = ExecutorInfo(123, "foo.git", baseline_rev, executor)
    rev = "workspace" if workspace else stash_rev
    pidfile = os.path.join(pid_dir, f"{rev}{BaseExecutor.PIDFILE_EXT}")
    dump_yaml(pidfile, info.to_dict())

    mocker.patch.object(
        BaseExecutor, "fetch_exps", return_value=[str(exp_ref)]
    )
    if workspace:
        scm.set_ref(EXEC_BRANCH, str(exp_ref), symbolic=True)
    results = dvc.experiments.show()
    assert results[baseline_rev][checkpoint_rev]["data"]["running"]
    assert (
        results[baseline_rev][checkpoint_rev]["data"]["executor"]
        == info.location
    )
    assert not results["workspace"]["baseline"]["data"]["running"]


def test_show_with_broken_repo(tmp_dir, scm, dvc, exp_stage, caplog):
    baseline_rev = scm.get_rev()
    exp1 = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp2 = dvc.experiments.run(exp_stage.addressing, params=["foo=3"])

    with open("dvc.yaml", "a") as fd:
        fd.write("breaking the yaml!")

    result = dvc.experiments.show()
    rev1 = first(exp1)
    rev2 = first(exp2)

    assert result[baseline_rev][rev1]["data"]["params"]["params.yaml"][
        "data"
    ] == {"foo": 2}
    assert result[baseline_rev][rev2]["data"]["params"]["params.yaml"][
        "data"
    ] == {"foo": 3}

    assert isinstance(
        result["workspace"]["baseline"]["error"], YAMLFileCorruptedError
    )
