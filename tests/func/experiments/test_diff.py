from funcy import first


def test_diff_empty(tmp_dir, scm, dvc, exp_stage):
    assert dvc.experiments.diff() == {"params": {}, "metrics": {}}


def test_diff_head(tmp_dir, scm, dvc, exp_stage):
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp = first(results)

    assert dvc.experiments.diff(a_rev="HEAD", b_rev=exp) == {
        "params": {"params.yaml": {"foo": {"diff": 1, "old": 1, "new": 2}}},
        "metrics": {"metrics.yaml": {"foo": {"diff": 1, "old": 1, "new": 2}}},
    }


def test_diff_exp(tmp_dir, scm, dvc, exp_stage):
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp_a = first(results)
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=3"])
    exp_b = first(results)

    assert dvc.experiments.diff(a_rev=exp_a, b_rev=exp_b) == {
        "params": {"params.yaml": {"foo": {"diff": 1, "old": 2, "new": 3}}},
        "metrics": {"metrics.yaml": {"foo": {"diff": 1, "old": 2, "new": 3}}},
    }
