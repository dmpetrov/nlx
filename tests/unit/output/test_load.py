# pylint: disable=unsubscriptable-object

import pytest

from dvc import output
from dvc.fs.local import LocalFileSystem
from dvc.fs.s3 import S3FileSystem
from dvc.output import Output
from dvc.stage import Stage


@pytest.mark.parametrize(
    "out_type,type_test_func",
    [
        ("outs", lambda o: not (o.metric or o.plot)),
        ("metrics", lambda o: o.metric and not o.plot),
        ("plots", lambda o: o.plot and not o.metric),
    ],
    ids=("outs", "metrics", "plots"),
)
def test_load_from_pipeline(dvc, out_type, type_test_func):
    outs = output.load_from_pipeline(
        Stage(dvc),
        [
            "file1",
            "file2",
            {"file3": {"cache": True}},
            {},
            {"file4": {"cache": False}},
            {"file5": {"persist": False}},
            {"file6": {"persist": True, "cache": False}},
        ],
        out_type,
    )
    cached_outs = {"file1", "file2", "file3", "file5"}
    persisted_outs = {"file6"}
    assert len(outs) == 6

    for i, out in enumerate(outs, start=1):
        assert isinstance(out, Output)
        assert isinstance(out.fs, LocalFileSystem)
        assert out.def_path == f"file{i}"
        assert out.use_cache == (out.def_path in cached_outs)
        assert out.persist == (out.def_path in persisted_outs)
        assert not out.hash_info
        assert type_test_func(out)


def test_load_from_pipeline_accumulates_flag(dvc):
    outs = output.load_from_pipeline(
        Stage(dvc),
        [
            "file1",
            {"file2": {"cache": False}},
            {"file1": {"persist": False}},
            {"file2": {"persist": True}},
        ],
        "outs",
    )
    for out in outs:
        assert isinstance(out, Output)
        assert isinstance(out.fs, LocalFileSystem)
        assert not out.plot and not out.metric
        assert not out.hash_info

    assert outs[0].use_cache and not outs[0].persist
    assert not outs[1].use_cache and outs[1].persist


def test_load_remote_files_from_pipeline(dvc):
    stage = Stage(dvc)
    (out,) = output.load_from_pipeline(
        stage, [{"s3://dvc-test/file.txt": {"cache": False}}], typ="metrics"
    )
    assert isinstance(out, Output)
    assert isinstance(out.fs, S3FileSystem)
    assert not out.plot and out.metric
    assert not out.persist
    assert not out.hash_info


@pytest.mark.parametrize("typ", [None, "", "illegal"])
def test_load_from_pipeline_error_on_typ(dvc, typ):
    with pytest.raises(ValueError):
        output.load_from_pipeline(Stage(dvc), ["file1"], typ)


@pytest.mark.parametrize("key", [3, "list".split()])
def test_load_from_pipeline_illegal_type(dvc, key):
    stage = Stage(dvc)
    with pytest.raises(ValueError):
        output.load_from_pipeline(stage, [key], "outs")
    with pytest.raises(ValueError):
        output.load_from_pipeline(stage, [{"key": key}], "outs")


def test_plots_load_from_pipeline(dvc):
    outs = output.load_from_pipeline(
        Stage(dvc),
        [
            "file1",
            {
                "file2": {
                    "persist": True,
                    "cache": False,
                    "x": 3,
                    "random": "val",
                }
            },
        ],
        "plots",
    )
    assert isinstance(outs[0], Output)
    assert isinstance(outs[0].fs, LocalFileSystem)
    assert outs[0].use_cache
    assert outs[0].plot is True and not outs[0].metric
    assert not outs[0].persist

    assert isinstance(outs[1], Output)
    assert isinstance(outs[1].fs, LocalFileSystem)
    assert not outs[1].use_cache
    assert outs[1].plot == {"x": 3} and not outs[1].metric
    assert outs[1].persist
