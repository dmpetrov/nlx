import pytest
from funcy import first

from tests.func.test_repro_multistage import COPY_SCRIPT


@pytest.mark.parametrize("queued, expected", [(True, 0), (False, 1)])
def test_workspace(tmp_dir, scm, dvc, queued, expected):
    tmp_dir.gen("copy.py", COPY_SCRIPT)
    tmp_dir.gen("params.yaml", "foo: 1")

    stage = dvc.run(
        cmd="python copy.py params.yaml metrics.yaml",
        metrics_no_cache=["metrics.yaml"],
        params=["foo"],
        name="foo",
    )
    scm.add(["dvc.yaml", "dvc.lock", "copy.py", "params.yaml", "metrics.yaml"])
    scm.commit("v1")

    dvc.experiments.run(stage.addressing, params=["foo=2"])
    dvc.experiments.run(stage.addressing, params=["foo=3"], queue=True)

    removed = dvc.experiments.gc(workspace=True, queued=queued)
    assert removed == expected


@pytest.mark.parametrize("queued, expected", [(True, 0), (False, 2)])
def test_all_commits(tmp_dir, scm, dvc, queued, expected):
    tmp_dir.gen("copy.py", COPY_SCRIPT)
    tmp_dir.gen("params.yaml", "foo: 1")

    stage = dvc.run(
        cmd="python copy.py params.yaml metrics.yaml",
        metrics_no_cache=["metrics.yaml"],
        params=["foo"],
        name="foo",
    )
    scm.add(["dvc.yaml", "dvc.lock", "copy.py", "params.yaml", "metrics.yaml"])
    scm.commit("v1")

    results = dvc.experiments.run(stage.addressing, params=["foo=2"])
    exp_rev = first(results)
    dvc.experiments.run(stage.addressing, params=["foo=3"], queue=True)

    dvc.experiments.apply(exp_rev)
    scm.add(["dvc.yaml", "dvc.lock", "copy.py", "params.yaml", "metrics.yaml"])
    scm.commit("v2")

    dvc.experiments.run(stage.addressing, params=["foo=4"])
    dvc.experiments.run(stage.addressing, params=["foo=5"], queue=True)

    removed = dvc.experiments.gc(all_commits=True, queued=queued)
    assert removed == expected


@pytest.mark.parametrize("queued, expected", [(True, 2), (False, 3)])
def test_all_branches(tmp_dir, scm, dvc, queued, expected):
    tmp_dir.gen("copy.py", COPY_SCRIPT)
    tmp_dir.gen("params.yaml", "foo: 1")

    stage = dvc.run(
        cmd="python copy.py params.yaml metrics.yaml",
        metrics_no_cache=["metrics.yaml"],
        params=["foo"],
        name="foo",
    )
    scm.add(["dvc.yaml", "dvc.lock", "copy.py", "params.yaml", "metrics.yaml"])
    scm.commit("v1")

    results = dvc.experiments.run(stage.addressing, params=["foo=2"])
    exp_rev = first(results)
    dvc.experiments.run(stage.addressing, params=["foo=3"], queue=True)

    dvc.experiments.apply(exp_rev)
    scm.add(["dvc.yaml", "dvc.lock", "copy.py", "params.yaml", "metrics.yaml"])
    scm.commit("v2")

    scm.branch("branch")

    results = dvc.experiments.run(stage.addressing, params=["foo=4"])
    dvc.experiments.run(stage.addressing, params=["foo=5"], queue=True)
    exp_rev = first(results)

    dvc.experiments.apply(exp_rev)
    scm.add(["dvc.yaml", "dvc.lock", "copy.py", "params.yaml", "metrics.yaml"])
    scm.commit("v3")

    removed = dvc.experiments.gc(all_branches=True, queued=queued)
    assert removed == expected


@pytest.mark.parametrize("queued, expected", [(True, 2), (False, 3)])
def test_all_tags(tmp_dir, scm, dvc, queued, expected):
    tmp_dir.gen("copy.py", COPY_SCRIPT)
    tmp_dir.gen("params.yaml", "foo: 1")

    stage = dvc.run(
        cmd="python copy.py params.yaml metrics.yaml",
        metrics_no_cache=["metrics.yaml"],
        params=["foo"],
        name="foo",
    )
    scm.add(["dvc.yaml", "dvc.lock", "copy.py", "params.yaml", "metrics.yaml"])
    scm.commit("v1")

    results = dvc.experiments.run(stage.addressing, params=["foo=2"])
    exp_rev = first(results)
    dvc.experiments.run(stage.addressing, params=["foo=3"], queue=True)

    dvc.experiments.apply(exp_rev)
    scm.add(["dvc.yaml", "dvc.lock", "copy.py", "params.yaml", "metrics.yaml"])
    scm.commit("v2")
    scm.tag("tag-v2")

    dvc.experiments.run(stage.addressing, params=["foo=4"])
    dvc.experiments.run(stage.addressing, params=["foo=5"], queue=True)

    removed = dvc.experiments.gc(all_tags=True, queued=queued)
    assert removed == expected
