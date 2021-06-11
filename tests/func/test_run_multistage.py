import os
import textwrap

import pytest

from dvc.exceptions import InvalidArgumentError
from dvc.stage.exceptions import DuplicateStageName, InvalidStageName
from dvc.utils.serialize import dump_yaml, parse_yaml_for_update


def test_run_with_name(tmp_dir, dvc, run_copy):
    from dvc.dvcfile import PIPELINE_FILE, PIPELINE_LOCK
    from dvc.stage import PipelineStage

    tmp_dir.dvc_gen("foo", "foo")
    assert not os.path.exists(PIPELINE_FILE)
    stage = run_copy("foo", "bar", name="copy-foo-to-bar")
    assert isinstance(stage, PipelineStage)
    assert stage.name == "copy-foo-to-bar"
    assert os.path.exists(PIPELINE_FILE)
    assert os.path.exists(PIPELINE_LOCK)


def test_run_no_exec(tmp_dir, dvc, run_copy):
    from dvc.dvcfile import PIPELINE_FILE, PIPELINE_LOCK
    from dvc.stage import PipelineStage

    tmp_dir.dvc_gen("foo", "foo")
    assert not os.path.exists(PIPELINE_FILE)
    stage = run_copy("foo", "bar", name="copy-foo-to-bar", no_exec=True)
    assert isinstance(stage, PipelineStage)
    assert stage.name == "copy-foo-to-bar"
    assert os.path.exists(PIPELINE_FILE)
    assert not os.path.exists(PIPELINE_LOCK)

    data, _ = stage.dvcfile._load()
    assert data["stages"]["copy-foo-to-bar"] == {
        "cmd": "python copy.py foo bar",
        "deps": ["copy.py", "foo"],
        "outs": ["bar"],
    }


def test_run_with_multistage_and_single_stage(tmp_dir, dvc, run_copy):
    from dvc.stage import PipelineStage, Stage

    tmp_dir.dvc_gen("foo", "foo")
    stage1 = run_copy("foo", "foo1", single_stage=True)
    stage2 = run_copy("foo1", "foo2", name="copy-foo1-foo2")
    stage3 = run_copy("foo2", "foo3", single_stage=True)

    assert isinstance(stage2, PipelineStage)
    assert isinstance(stage1, Stage)
    assert isinstance(stage3, Stage)
    assert stage2.name == "copy-foo1-foo2"


def test_run_multi_stage_repeat(tmp_dir, dvc, run_copy):
    from dvc.dvcfile import PIPELINE_FILE, Dvcfile
    from dvc.stage import PipelineStage

    tmp_dir.dvc_gen("foo", "foo")
    run_copy("foo", "foo1", name="copy-foo-foo1")
    run_copy("foo1", "foo2", name="copy-foo1-foo2")
    run_copy("foo2", "foo3", single_stage=True)

    stages = list(Dvcfile(dvc, PIPELINE_FILE).stages.values())
    assert len(stages) == 2
    assert all(isinstance(stage, PipelineStage) for stage in stages)
    assert {stage.name for stage in stages} == {
        "copy-foo-foo1",
        "copy-foo1-foo2",
    }


def test_multi_stage_run_cached(tmp_dir, dvc, run_copy, mocker):
    from dvc.stage.run import subprocess

    tmp_dir.dvc_gen("foo", "foo")

    run_copy("foo", "foo2", name="copy-foo1-foo2")
    spy = mocker.spy(subprocess, "Popen")
    run_copy("foo", "foo2", name="copy-foo1-foo2")
    assert not spy.called


def test_multistage_dump_on_non_cached_outputs(tmp_dir, dvc):
    tmp_dir.dvc_gen("foo")
    dvc.run(
        cmd="cp foo foo1",
        deps=["foo"],
        name="copy-foo1-foo2",
        outs_no_cache=["foo1"],
    )


def test_multistage_with_wdir(tmp_dir, dvc):
    from dvc.dvcfile import Dvcfile

    tmp_dir.gen({"dir": {"foo": "foo", "bar": "bar"}})
    stage = dvc.run(
        cmd="cp foo foo1",
        deps=["foo"],
        name="copy-foo1-foo2",
        outs=["foo1"],
        wdir="dir",
    )

    data, _ = Dvcfile(dvc, stage.path)._load()
    assert "dir" == data["stages"]["copy-foo1-foo2"]["wdir"]


def test_multistage_always_changed(tmp_dir, dvc):
    from dvc.dvcfile import Dvcfile

    tmp_dir.gen({"foo": "foo", "bar": "bar"})
    stage = dvc.run(
        cmd="cp foo foo1",
        deps=["foo"],
        name="copy-foo1-foo2",
        outs=["foo1"],
        always_changed=True,
    )

    data, _ = Dvcfile(dvc, stage.path)._load()
    assert data["stages"]["copy-foo1-foo2"]["always_changed"]


def test_graph(tmp_dir, dvc):
    from dvc.exceptions import CyclicGraphError

    tmp_dir.gen({"foo": "foo", "bar": "bar"})

    dvc.run(deps=["foo"], outs=["bar"], cmd="echo foo > bar", name="1")

    dvc.run(deps=["bar"], outs=["baz"], cmd="echo bar > baz", name="2")

    with pytest.raises(CyclicGraphError):
        dvc.run(deps=["baz"], outs=["foo"], cmd="echo baz > foo", name="3")


def test_run_dump_on_multistage(tmp_dir, dvc, run_head):
    from dvc.dvcfile import PIPELINE_FILE, Dvcfile

    tmp_dir.gen(
        {
            "dir": {
                "foo": "foo\nfoo",
                "bar": "bar\nbar",
                "foobar": "foobar\foobar",
            }
        }
    )

    dvc.run(
        cmd="cp foo foo2",
        deps=["foo"],
        name="copy-foo-foo2",
        wdir="dir",
        outs_persist=["foo2"],
        always_changed=True,
    )
    data = Dvcfile(dvc, PIPELINE_FILE)._load()[0]
    assert data == {
        "stages": {
            "copy-foo-foo2": {
                "cmd": "cp foo foo2",
                "deps": ["foo"],
                "outs": [{"foo2": {"persist": True}}],
                "always_changed": True,
                "wdir": "dir",
            }
        }
    }

    run_head(
        "foo",
        "bar",
        "foobar",
        name="head-files",
        outs=["bar-1"],
        outs_persist=["foo-1"],
        metrics_no_cache=["foobar-1"],
        wdir="dir",
    )
    assert Dvcfile(dvc, PIPELINE_FILE)._load()[0] == {
        "stages": {
            "head-files": {
                "cmd": "python {} foo bar foobar".format(
                    (tmp_dir / "head.py").resolve()
                ),
                "wdir": "dir",
                "deps": ["bar", "foo", "foobar"],
                "outs": ["bar-1", {"foo-1": {"persist": True}}],
                "metrics": [{"foobar-1": {"cache": False}}],
            },
            **data["stages"],
        }
    }


@pytest.mark.parametrize(
    "char", ["@:", "#", "$", ":", "/", "\\", ".", ";", ","]
)
def test_run_with_invalid_stage_name(run_copy, char):
    with pytest.raises(InvalidStageName):
        run_copy("foo", "bar", name=f"copy_name-{char}")


def test_run_with_name_having_hyphen_underscore(tmp_dir, dvc, run_copy):
    tmp_dir.dvc_gen("foo", "foo")
    run_copy("foo", "bar", name="copy-foo_bar")


def test_run_already_exists(tmp_dir, dvc, run_copy):
    tmp_dir.dvc_gen("foo", "foo")
    run_copy("foo", "bar", name="copy")
    with pytest.raises(DuplicateStageName):
        run_copy("bar", "foobar", name="copy", force=False)
    run_copy("bar", "foobar", name="copy", force=True)


supported_params = {
    "name": "Answer",
    "answer": 42,
    "floats": 42.0,
    "lists": [42, 42.0, "42"],
    "nested": {"nested1": {"nested2": "42", "nested2-2": 41.99999}},
}


def test_run_params_default(tmp_dir, dvc):
    from dvc.dependency import ParamsDependency

    dump_yaml(tmp_dir / "params.yaml", supported_params)
    stage = dvc.run(
        name="read_params",
        params=["nested.nested1.nested2"],
        cmd="cat params.yaml",
    )
    assert isinstance(stage.deps[0], ParamsDependency)
    assert stage.deps[0].params == ["nested.nested1.nested2"]

    lockfile = stage.dvcfile._lockfile
    assert lockfile.load()["stages"]["read_params"]["params"] == {
        "params.yaml": {"nested.nested1.nested2": "42"}
    }

    data, _ = stage.dvcfile._load()
    assert data["stages"]["read_params"]["params"] == [
        "nested.nested1.nested2"
    ]


def test_run_params_custom_file(tmp_dir, dvc):
    from dvc.dependency import ParamsDependency

    dump_yaml(tmp_dir / "params2.yaml", supported_params)
    stage = dvc.run(
        name="read_params",
        params=["params2.yaml:lists"],
        cmd="cat params2.yaml",
    )

    isinstance(stage.deps[0], ParamsDependency)
    assert stage.deps[0].params == ["lists"]
    lockfile = stage.dvcfile._lockfile
    assert lockfile.load()["stages"]["read_params"]["params"] == {
        "params2.yaml": {"lists": [42, 42.0, "42"]}
    }

    data, _ = stage.dvcfile._load()
    assert data["stages"]["read_params"]["params"] == [
        {"params2.yaml": ["lists"]}
    ]


def test_run_params_no_exec(tmp_dir, dvc):
    from dvc.dependency import ParamsDependency

    dump_yaml(tmp_dir / "params2.yaml", supported_params)
    stage = dvc.run(
        name="read_params",
        params=["params2.yaml:lists"],
        cmd="cat params2.yaml",
        no_exec=True,
    )

    isinstance(stage.deps[0], ParamsDependency)
    assert stage.deps[0].params == ["lists"]
    assert not stage.dvcfile._lockfile.exists()

    data, _ = stage.dvcfile._load()
    assert data["stages"]["read_params"]["params"] == [
        {"params2.yaml": ["lists"]}
    ]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"outs": ["foo"], "deps": ["bar"]},
        {"outs": ["foo"], "deps": ["bar"], "name": "copy-foo-bar"},
    ],
)
def test_run_without_cmd(tmp_dir, dvc, kwargs):
    with pytest.raises(InvalidArgumentError) as exc:
        dvc.run(**kwargs)
    assert "command is not specified" == str(exc.value)


def test_run_overwrite_order(tmp_dir, dvc, run_copy):
    from dvc.dvcfile import PIPELINE_FILE

    tmp_dir.gen({"foo": "foo", "foo1": "foo1"})
    run_copy("foo", "bar", name="copy-foo-bar")
    run_copy("bar", "foobar", name="copy-bar-foobar")

    run_copy("foo1", "bar1", name="copy-foo-bar", force=True)

    data = parse_yaml_for_update(
        (tmp_dir / PIPELINE_FILE).read_text(), PIPELINE_FILE
    )
    assert list(data["stages"].keys()) == ["copy-foo-bar", "copy-bar-foobar"]


def test_run_overwrite_preserves_meta_and_comment(tmp_dir, dvc, run_copy):
    from dvc.dvcfile import PIPELINE_FILE

    tmp_dir.gen({"foo": "foo", "foo1": "foo1"})
    text = textwrap.dedent(
        """\
        stages:
          copy-foo-bar:
            cmd: python copy.py {src} {dest}
            deps:
            - copy.py
            - {src}
            outs:
            # comments are preserved
            - {dest}
            meta:
              name: meta is preserved too
    """
    )
    (tmp_dir / PIPELINE_FILE).write_text(text.format(src="foo", dest="bar"))
    assert dvc.reproduce(PIPELINE_FILE)

    assert run_copy("foo1", "bar1", name="copy-foo-bar", force=True)

    assert (tmp_dir / PIPELINE_FILE).read_text() == text.format(
        src="foo1", dest="bar1"
    )


@pytest.mark.parametrize(
    "workspace, hash_name, foo_hash, bar_hash",
    [
        (
            pytest.lazy_fixture("local_cloud"),
            "md5",
            "acbd18db4cc2f85cedef654fccc4a4d8",
            "37b51d194a7513e45b56f6524f2d51f2",
        ),
        pytest.param(
            pytest.lazy_fixture("ssh"),
            "md5",
            "acbd18db4cc2f85cedef654fccc4a4d8",
            "37b51d194a7513e45b56f6524f2d51f2",
            marks=pytest.mark.skipif(
                os.name == "nt", reason="disabled on windows"
            ),
        ),
        (
            pytest.lazy_fixture("s3"),
            "etag",
            "acbd18db4cc2f85cedef654fccc4a4d8",
            "37b51d194a7513e45b56f6524f2d51f2",
        ),
        (
            pytest.lazy_fixture("hdfs"),
            "checksum",
            "0000020000000000000000003dba826b9be9c6a8e2f8310a770555c4",
            "00000200000000000000000075433c81259d3c38e364b348af52e84d",
        ),
    ],
    indirect=["workspace"],
)
def test_run_external_outputs(
    tmp_dir, dvc, workspace, hash_name, foo_hash, bar_hash
):
    workspace.gen("foo", "foo")
    dvc.run(
        name="mystage",
        cmd="mycmd",
        deps=["remote://workspace/foo"],
        outs=["remote://workspace/bar"],
        no_exec=True,
    )

    dvc_yaml = (
        "stages:\n"
        "  mystage:\n"
        "    cmd: mycmd\n"
        "    deps:\n"
        "    - remote://workspace/foo\n"
        "    outs:\n"
        "    - remote://workspace/bar\n"
    )

    assert (tmp_dir / "dvc.yaml").read_text() == dvc_yaml
    assert not (tmp_dir / "dvc.lock").exists()

    workspace.gen("bar", "bar")
    dvc.commit("dvc.yaml", force=True)

    assert (tmp_dir / "dvc.yaml").read_text() == dvc_yaml
    assert (tmp_dir / "dvc.lock").read_text() == (
        "schema: '2.0'\n"
        "stages:\n"
        "  mystage:\n"
        "    cmd: mycmd\n"
        "    deps:\n"
        "    - path: remote://workspace/foo\n"
        f"      {hash_name}: {foo_hash}\n"
        "      size: 3\n"
        "    outs:\n"
        "    - path: remote://workspace/bar\n"
        f"      {hash_name}: {bar_hash}\n"
        "      size: 3\n"
    )

    assert (workspace / "foo").read_text() == "foo"
    assert (workspace / "bar").read_text() == "bar"
    assert (
        workspace / "cache" / bar_hash[:2] / bar_hash[2:]
    ).read_text() == "bar"
