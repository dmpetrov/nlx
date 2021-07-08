from dvc.repo import Repo


def test_show_empty(dvc):
    assert dvc.params.show() == {}


def test_show(tmp_dir, dvc):
    tmp_dir.gen("params.yaml", "foo: bar")
    dvc.run(cmd="echo params.yaml", params=["foo"], single_stage=True)
    assert dvc.params.show() == {
        "": {"data": {"params.yaml": {"data": {"foo": "bar"}}}}
    }


def test_show_toml(tmp_dir, dvc):
    tmp_dir.gen("params.toml", "[foo]\nbar = 42\nbaz = [1, 2]\n")
    dvc.run(
        cmd="echo params.toml", params=["params.toml:foo"], single_stage=True
    )
    assert dvc.params.show() == {
        "": {
            "data": {
                "params.toml": {"data": {"foo": {"bar": 42, "baz": [1, 2]}}}
            }
        }
    }


def test_show_py(tmp_dir, dvc):
    tmp_dir.gen(
        "params.py",
        "CONST = 1\nIS_DIR: bool = True\n\n\nclass Config:\n    foo = 42\n",
    )
    dvc.run(
        cmd="echo params.py",
        params=["params.py:CONST,IS_DIR,Config.foo"],
        single_stage=True,
    )
    assert dvc.params.show() == {
        "": {
            "data": {
                "params.py": {
                    "data": {"CONST": 1, "Config": {"foo": 42}, "IS_DIR": True}
                }
            }
        }
    }


def test_show_multiple(tmp_dir, dvc):
    tmp_dir.gen("params.yaml", "foo: bar\nbaz: qux\n")
    dvc.run(
        cmd="echo params.yaml",
        fname="foo.dvc",
        params=["foo"],
        single_stage=True,
    )
    dvc.run(
        cmd="echo params.yaml",
        fname="baz.dvc",
        params=["baz"],
        single_stage=True,
    )
    assert dvc.params.show() == {
        "": {"data": {"params.yaml": {"data": {"baz": "qux", "foo": "bar"}}}}
    }


def test_show_list(tmp_dir, dvc):
    tmp_dir.gen("params.yaml", "foo:\n- bar\n- baz\n")
    dvc.run(cmd="echo params.yaml", params=["foo"], single_stage=True)
    assert dvc.params.show() == {
        "": {"data": {"params.yaml": {"data": {"foo": ["bar", "baz"]}}}}
    }


def test_show_branch(tmp_dir, scm, dvc):
    tmp_dir.gen("params.yaml", "foo: bar")
    dvc.run(cmd="echo params.yaml", params=["foo"], single_stage=True)
    scm.add(["params.yaml", "Dvcfile"])
    scm.commit("init")

    with tmp_dir.branch("branch", new=True):
        tmp_dir.scm_gen("params.yaml", "foo: baz", commit="branch")

    assert dvc.params.show(revs=["branch"]) == {
        "branch": {"data": {"params.yaml": {"data": {"foo": "baz"}}}},
        "workspace": {"data": {"params.yaml": {"data": {"foo": "bar"}}}},
    }


def test_pipeline_params(tmp_dir, scm, dvc, run_copy):
    from dvc.dvcfile import PIPELINE_FILE

    tmp_dir.gen(
        {"foo": "foo", "params.yaml": "foo: bar\nxyz: val\nabc: ignore"}
    )
    run_copy("foo", "bar", name="copy-foo-bar", params=["foo,xyz"])
    scm.add(["params.yaml", PIPELINE_FILE])
    scm.commit("add stage")

    tmp_dir.scm_gen(
        "params.yaml", "foo: baz\nxyz: val\nabc: ignore", commit="baz"
    )
    tmp_dir.scm_gen(
        "params.yaml", "foo: qux\nxyz: val\nabc: ignore", commit="qux"
    )

    assert dvc.params.show(revs=["master"], deps=True) == {
        "master": {
            "data": {"params.yaml": {"data": {"foo": "qux", "xyz": "val"}}}
        }
    }
    assert dvc.params.show(revs=["master"]) == {
        "master": {
            "data": {
                "params.yaml": {
                    "data": {"abc": "ignore", "foo": "qux", "xyz": "val"}
                }
            }
        }
    }


def test_show_no_repo(tmp_dir):
    tmp_dir.gen({"foo": "foo", "params_file.yaml": "foo: bar\nxyz: val"})

    dvc = Repo(uninitialized=True)

    assert dvc.params.show(targets=["params_file.yaml"]) == {
        "": {
            "data": {
                "params_file.yaml": {"data": {"foo": "bar", "xyz": "val"}}
            }
        }
    }
