from collections import OrderedDict

import pytest
from voluptuous import Schema as _Schema

from dvc.dvcfile import PIPELINE_FILE
from dvc.hash_info import HashInfo
from dvc.schema import LOCK_FILE_STAGE_SCHEMA, LOCKFILE_STAGES_SCHEMA
from dvc.stage import PipelineStage, create_stage
from dvc.stage.serialize import DEFAULT_PARAMS_FILE, to_lockfile
from dvc.stage.serialize import (
    to_single_stage_lockfile as _to_single_stage_lockfile,
)
from dvc.stage.utils import split_params_deps

kwargs = {"name": "something", "cmd": "command", "path": PIPELINE_FILE}
Schema = _Schema(LOCK_FILE_STAGE_SCHEMA)


def to_single_stage_lockfile(stage):
    """Validate schema on each serialization."""
    e = _to_single_stage_lockfile(stage)
    assert Schema(e)
    return e


def test_lock(dvc):
    stage = create_stage(PipelineStage, dvc, **kwargs)
    assert to_single_stage_lockfile(stage) == {"cmd": "command"}


def test_lock_deps(dvc):
    stage = create_stage(PipelineStage, dvc, deps=["input"], **kwargs)
    stage.deps[0].hash_info = HashInfo("md5", "md-five")
    assert to_single_stage_lockfile(stage) == OrderedDict(
        [
            ("cmd", "command"),
            ("deps", [OrderedDict([("path", "input"), ("md5", "md-five")])]),
        ]
    )


def test_lock_deps_order(dvc):
    stage = create_stage(
        PipelineStage, dvc, deps=["input1", "input0"], **kwargs
    )
    stage.deps[0].hash_info = HashInfo("md5", "md-one1")
    stage.deps[1].hash_info = HashInfo("md5", "md-zer0")
    assert to_single_stage_lockfile(stage) == OrderedDict(
        [
            ("cmd", "command"),
            (
                "deps",
                [
                    OrderedDict([("path", "input0"), ("md5", "md-zer0")]),
                    OrderedDict([("path", "input1"), ("md5", "md-one1")]),
                ],
            ),
        ]
    )


def test_lock_params(dvc):
    stage = create_stage(
        PipelineStage, dvc, params=["lorem.ipsum", "abc"], **kwargs
    )
    stage.deps[0].hash_info = HashInfo(
        "params", {"lorem.ipsum": {"lorem1": 1, "lorem2": 2}, "abc": 3}
    )
    assert to_single_stage_lockfile(stage)["params"][
        DEFAULT_PARAMS_FILE
    ] == OrderedDict([("abc", 3), ("lorem.ipsum", {"lorem1": 1, "lorem2": 2})])


def test_lock_params_file_sorted(dvc):
    stage = create_stage(
        PipelineStage,
        dvc,
        params=[
            "lorem.ipsum",
            "abc",
            {"myparams.yaml": ["foo", "foobar"]},
            {"a-params-file.yaml": ["bar", "barr"]},
        ],
        **kwargs
    )
    stage.deps[0].hash_info = HashInfo(
        "params", {"lorem.ipsum": {"lorem1": 1, "lorem2": 2}, "abc": 3}
    )
    stage.deps[1].hash_info = HashInfo(
        "params", {"foo": ["f", "o", "o"], "foobar": "foobar"}
    )
    stage.deps[2].hash_info = HashInfo(
        "params", {"bar": ["b", "a", "r"], "barr": "barr"}
    )
    assert to_single_stage_lockfile(stage)["params"] == OrderedDict(
        [
            (
                DEFAULT_PARAMS_FILE,
                OrderedDict(
                    [("abc", 3), ("lorem.ipsum", {"lorem1": 1, "lorem2": 2})]
                ),
            ),
            (
                "a-params-file.yaml",
                OrderedDict([("bar", ["b", "a", "r"]), ("barr", "barr")]),
            ),
            (
                "myparams.yaml",
                OrderedDict([("foo", ["f", "o", "o"]), ("foobar", "foobar")]),
            ),
        ]
    )


def test_lock_params_no_values_filled(dvc):
    stage = create_stage(
        PipelineStage, dvc, params=["lorem.ipsum", "abc"], **kwargs
    )
    assert to_single_stage_lockfile(stage) == {"cmd": "command"}


@pytest.mark.parametrize("typ", ["plots", "metrics", "outs"])
def test_lock_outs(dvc, typ):
    stage = create_stage(PipelineStage, dvc, **{typ: ["input"]}, **kwargs)
    stage.outs[0].hash_info = HashInfo("md5", "md-five")
    assert to_single_stage_lockfile(stage) == OrderedDict(
        [
            ("cmd", "command"),
            ("outs", [OrderedDict([("path", "input"), ("md5", "md-five")])]),
        ]
    )


@pytest.mark.parametrize("typ", ["plots", "metrics", "outs"])
def test_lock_outs_isexec(dvc, typ):
    stage = create_stage(PipelineStage, dvc, **{typ: ["input"]}, **kwargs)
    stage.outs[0].hash_info = HashInfo("md5", "md-five")
    stage.outs[0].isexec = True
    assert to_single_stage_lockfile(stage) == OrderedDict(
        [
            ("cmd", "command"),
            (
                "outs",
                [
                    OrderedDict(
                        [
                            ("path", "input"),
                            ("md5", "md-five"),
                            ("isexec", True),
                        ]
                    )
                ],
            ),
        ]
    )


@pytest.mark.parametrize("typ", ["plots", "metrics", "outs"])
def test_lock_outs_order(dvc, typ):
    stage = create_stage(
        PipelineStage, dvc, **{typ: ["input1", "input0"]}, **kwargs
    )
    stage.outs[0].hash_info = HashInfo("md5", "md-one1")
    stage.outs[1].hash_info = HashInfo("md5", "md-zer0")
    assert to_single_stage_lockfile(stage) == OrderedDict(
        [
            ("cmd", "command"),
            (
                "outs",
                [
                    OrderedDict([("path", "input0"), ("md5", "md-zer0")]),
                    OrderedDict([("path", "input1"), ("md5", "md-one1")]),
                ],
            ),
        ]
    )


def test_dump_nondefault_hash(dvc):
    stage = create_stage(
        PipelineStage, dvc, deps=["s3://dvc-temp/file"], **kwargs
    )
    stage.deps[0].hash_info = HashInfo("md5", "value")
    assert to_single_stage_lockfile(stage) == OrderedDict(
        [
            ("cmd", "command"),
            (
                "deps",
                [
                    OrderedDict(
                        [("path", "s3://dvc-temp/file"), ("md5", "value")]
                    )
                ],
            ),
        ]
    )


def test_order(dvc):
    stage = create_stage(
        PipelineStage,
        dvc,
        deps=["input"],
        outs=["output"],
        params=["foo-param"],
        **kwargs
    )
    params, deps = split_params_deps(stage)

    deps[0].hash_info = HashInfo("md5", "md-five")
    params[0].hash_info = HashInfo("params", {"foo-param": "value"})
    stage.outs[0].hash_info = HashInfo("md5", "md5-output")

    assert to_single_stage_lockfile(stage) == OrderedDict(
        [
            ("cmd", "command"),
            ("deps", [{"path": "input", "md5": "md-five"}]),
            ("params", {"params.yaml": {"foo-param": "value"}}),
            ("outs", [{"path": "output", "md5": "md5-output"}]),
        ]
    )


def test_to_lockfile(dvc):
    stage = create_stage(PipelineStage, dvc, deps=["input"], **kwargs)
    stage.deps[0].hash_info = HashInfo("md5", "md-five")
    entry = to_lockfile(stage)
    assert len(entry) == 1
    _Schema(LOCKFILE_STAGES_SCHEMA)(entry)
    assert entry == {
        "something": OrderedDict(
            [
                ("cmd", "command"),
                ("deps", [{"path": "input", "md5": "md-five"}]),
            ]
        )
    }
