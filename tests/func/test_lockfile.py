import logging
import os
from collections import OrderedDict
from operator import itemgetter

import pytest

from dvc.dvcfile import PIPELINE_LOCK, Lockfile, LockfileCorruptedError
from dvc.hash_info import HashInfo
from dvc.stage.utils import split_params_deps
from dvc.utils.fs import remove
from dvc.utils.serialize import (
    dump_yaml,
    dumps_yaml,
    load_yaml,
    parse_yaml_for_update,
)
from tests.func.test_run_multistage import supported_params

FS_STRUCTURE = {
    "foo": "bar\nfoobar",
    "bar": "foo\nfoobar",
    "foobar": "foobar\nbar",
    "params.yaml": dumps_yaml(supported_params),
    "params2.yaml": dumps_yaml(supported_params),
}


def read_lock_file(file=PIPELINE_LOCK):
    with open(file) as f:
        data = parse_yaml_for_update(f.read(), file)
    assert isinstance(data, OrderedDict)
    return data


def assert_eq_lockfile(previous, new):
    for content in (previous, new):
        assert isinstance(content, OrderedDict)

    # if they both are OrderedDict, then `==` will also check for order
    assert previous == new


def test_deps_outs_are_sorted_by_path(tmp_dir, dvc, run_head):
    tmp_dir.gen(FS_STRUCTURE)
    deps = ["foo", "bar", "foobar"]
    run_head(*deps, name="copy-first-line")

    initial_content = read_lock_file()
    lock = initial_content["stages"]["copy-first-line"]

    # lock stage key order:
    assert list(lock.keys()) == ["cmd", "deps", "outs"]

    # `path` key appear first and then the `md5`
    assert all(
        list(dep.keys()) == ["path", "md5", "size"] for dep in lock["deps"]
    )
    assert all(
        list(out.keys()) == ["path", "md5", "size"] for out in lock["outs"]
    )

    # deps are always sorted by the file path naming
    assert list(map(itemgetter("path"), lock["deps"])) == sorted(deps)

    # outs are too
    assert list(map(itemgetter("path"), lock["outs"])) == [
        d + "-1" for d in sorted(deps)
    ]


def test_order_is_preserved_when_pipeline_order_changes(
    tmp_dir, dvc, run_head
):
    tmp_dir.gen(FS_STRUCTURE)
    deps = ["foo", "bar", "foobar"]
    stage = run_head(*deps, name="copy-first-line")

    initial_content = read_lock_file()
    # reverse order of stage.outs and dump to the pipeline file
    # then, again change stage.deps and dump to the pipeline file
    reversal = stage.outs.reverse, stage.deps.reverse
    for reverse_items in reversal:
        reverse_items()
        stage.dvcfile._dump_pipeline_file(stage)

        # we only changed the order, should not reproduce
        assert not dvc.reproduce(stage.addressing)

        new_lock_content = read_lock_file()
        assert_eq_lockfile(new_lock_content, initial_content)

        (tmp_dir / PIPELINE_LOCK).unlink()
        assert dvc.reproduce(stage.addressing) == [stage]
        new_lock_content = read_lock_file()
        assert_eq_lockfile(new_lock_content, initial_content)


def test_cmd_changes_other_orders_are_preserved(tmp_dir, dvc, run_head):
    tmp_dir.gen(FS_STRUCTURE)
    deps = ["foo", "bar", "foobar"]
    stage = run_head(*deps, name="copy-first-line")

    initial_content = read_lock_file()
    # let's change cmd in pipeline file
    # it should only change "cmd", otherwise it should be
    # structurally same as cmd
    stage.cmd = "  ".join(stage.cmd.split())
    stage.dvcfile._dump_pipeline_file(stage)

    initial_content["stages"]["copy-first-line"]["cmd"] = stage.cmd

    assert dvc.reproduce(stage.addressing) == [stage]

    new_lock_content = read_lock_file()
    assert_eq_lockfile(new_lock_content, initial_content)


def test_params_dump(tmp_dir, dvc, run_head):
    tmp_dir.gen(FS_STRUCTURE)

    stage = run_head(
        "foo",
        "bar",
        "foobar",
        name="copy-first-line",
        params=[
            "params2.yaml:answer,lists,name",
            "params.yaml:lists,floats,nested.nested1,nested.nested1.nested2",
        ],
    )

    initial_content = read_lock_file()
    lock = initial_content["stages"]["copy-first-line"]

    # lock stage key order:
    assert list(lock.keys()) == ["cmd", "deps", "params", "outs"]
    assert list(lock["params"].keys()) == ["params.yaml", "params2.yaml"]

    # # params keys are always sorted by the name
    assert list(lock["params"]["params.yaml"].keys()) == [
        "floats",
        "lists",
        "nested.nested1",
        "nested.nested1.nested2",
    ]
    assert list(lock["params"]["params2.yaml"]) == ["answer", "lists", "name"]

    assert not dvc.reproduce(stage.addressing)

    # let's change the order of params and dump them in pipeline file
    params, _ = split_params_deps(stage)
    for param in params:
        param.params.reverse()

    stage.dvcfile._dump_pipeline_file(stage)
    assert not dvc.reproduce(stage.addressing)

    (tmp_dir / PIPELINE_LOCK).unlink()
    assert dvc.reproduce(stage.addressing) == [stage]
    assert_eq_lockfile(initial_content, read_lock_file())

    # remove build-cache and check if the same structure is built
    for item in [dvc.stage_cache.cache_dir, PIPELINE_LOCK]:
        remove(item)
    assert dvc.reproduce(stage.addressing) == [stage]
    assert_eq_lockfile(initial_content, read_lock_file())


@pytest.fixture
def v1_repo_lock(tmp_dir, dvc):
    """Generates a repo having v1 format lockfile"""
    size = 5 if os.name == "nt" else 4
    hi = HashInfo(
        name="md5", size=size, value="c157a79031e1c40f85931829bc5fc552"
    )
    v1_lockdata = {
        "foo": {"cmd": "echo foo"},
        "bar": {
            "cmd": "echo bar>bar.txt",
            "outs": [{"path": "bar.txt", **hi.to_dict()}],
        },
    }
    dvc.run(cmd="echo foo", name="foo", no_exec=True)
    dvc.run(cmd="echo bar>bar.txt", outs=["bar.txt"], name="bar", no_exec=True)
    dump_yaml(tmp_dir / "dvc.lock", v1_lockdata)
    yield v1_lockdata


def test_can_read_v1_lockfile(tmp_dir, dvc, v1_repo_lock):
    assert dvc.status() == {
        "bar": [{"changed outs": {"bar.txt": "not in cache"}}],
        "foo": ["always changed"],
    }


def test_migrates_v1_lockfile_to_v2_during_dump(
    tmp_dir, dvc, v1_repo_lock, caplog
):
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="dvc.dvcfile"):
        assert dvc.reproduce()

    assert "Migrating lock file 'dvc.lock' from v1 to v2" in caplog.messages
    d = load_yaml(tmp_dir / "dvc.lock")
    assert d == {"stages": v1_repo_lock, "schema": "2.0"}


@pytest.mark.parametrize(
    "version_info", [{"schema": "1.1"}, {"schema": "2.1"}, {"schema": "3.0"}]
)
def test_lockfile_invalid_versions(tmp_dir, dvc, version_info):
    lockdata = {**version_info, "stages": {"foo": {"cmd": "echo foo"}}}
    dump_yaml("dvc.lock", lockdata)
    with pytest.raises(LockfileCorruptedError) as exc_info:
        Lockfile(dvc, tmp_dir / "dvc.lock").load()

    assert str(exc_info.value) == "Lockfile 'dvc.lock' is corrupted."
    assert (
        str(exc_info.value.__cause__) == "'dvc.lock' format error: "
        f"invalid schema version {version_info['schema']}, "
        "expected one of ['2.0'] for dictionary value @ "
        "data['schema']"
    )
