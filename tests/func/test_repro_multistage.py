import os
from copy import deepcopy
from textwrap import dedent

import pytest
from funcy import lsplit

from dvc.dvcfile import PIPELINE_FILE, PIPELINE_LOCK
from dvc.exceptions import CyclicGraphError, ReproductionError
from dvc.main import main
from dvc.stage import PipelineStage
from dvc.utils.serialize import dump_yaml, load_yaml
from tests.func import test_repro

COPY_SCRIPT_FORMAT = dedent(
    """\
    import sys
    import shutil
    shutil.copyfile({}, {})
"""
)
COPY_SCRIPT = COPY_SCRIPT_FORMAT.format("sys.argv[1]", "sys.argv[2]")


class MultiStageRun:
    def _run(self, **kwargs):
        assert kwargs.get("name")
        kwargs.pop("fname", None)
        # ignore fname for now
        return self.dvc.run(**kwargs)  # noqa, pylint: disable=no-member

    @staticmethod
    def _get_stage_target(stage):
        return stage.addressing


class TestReproFailMultiStage(MultiStageRun, test_repro.TestReproFail):
    pass


class TestReproCyclicGraphMultiStage(
    MultiStageRun, test_repro.TestReproCyclicGraph
):
    pass


class TestReproUnderDirMultiStage(
    MultiStageRun, test_repro.TestReproDepUnderDir
):
    pass


class TestReproDepDirWithOutputsUnderItMultiStage(
    MultiStageRun, test_repro.TestReproDepDirWithOutputsUnderIt
):
    pass


class TestReproForceMultiStage(MultiStageRun, test_repro.TestReproForce):
    pass


class TestReproChangedCodeMultiStage(
    MultiStageRun, test_repro.TestReproChangedCode
):
    pass


class TestReproChangedDataMultiStage(
    MultiStageRun, test_repro.TestReproChangedData
):
    pass


class TestReproDry(MultiStageRun, test_repro.TestReproDry):
    pass


class TestReproUpToDateMultiStage(MultiStageRun, test_repro.TestReproUpToDate):
    pass


class TestReproChangedDeepDataMultiStage(
    MultiStageRun, test_repro.TestReproChangedDeepData
):
    pass


class TestReproPipelineMultiStage(MultiStageRun, test_repro.TestReproPipeline):
    pass


class TestReproPipelinesMultiStage(
    MultiStageRun, test_repro.TestReproPipelines
):
    pass


class TestReproFrozenMultiStage(MultiStageRun, test_repro.TestReproFrozen):
    pass


class TestReproFrozenCallbackMultiStage(
    MultiStageRun, test_repro.TestReproFrozenCallback
):
    pass


class TestReproFrozenUnchangedMultiStage(
    MultiStageRun, test_repro.TestReproFrozenUnchanged
):
    pass


class TestReproPhonyMultiStage(MultiStageRun, test_repro.TestReproPhony):
    pass


class TestCmdReproMultiStage(MultiStageRun, test_repro.TestCmdRepro):
    pass


class TestReproAllPipelinesMultiStage(
    MultiStageRun, test_repro.TestReproAllPipelines
):
    pass


class TestReproNoCommit(MultiStageRun, test_repro.TestReproNoCommit):
    pass


class TestNonExistingOutputMultiStage(
    MultiStageRun, test_repro.TestNonExistingOutput
):
    pass


class TestReproAlreadyCachedMultiStage(
    MultiStageRun, test_repro.TestReproAlreadyCached
):
    pass


class TestReproChangedDirMultiStage(
    MultiStageRun, test_repro.TestReproChangedDir
):
    pass


class TestReproChangedDirDataMultiStage(
    MultiStageRun, test_repro.TestReproChangedDirData
):
    pass


def test_non_existing_stage_name(tmp_dir, dvc, run_copy):
    from dvc.exceptions import DvcException

    tmp_dir.gen("file1", "file1")
    run_copy("file1", "file2", name="copy-file1-file2")

    with pytest.raises(DvcException):
        dvc.freeze(":copy-file1-file3")

    assert main(["freeze", ":copy-file1-file3"]) != 0


def test_repro_frozen(tmp_dir, dvc, run_copy):
    (data_stage,) = tmp_dir.dvc_gen("data", "foo")
    stage0 = run_copy("data", "stage0", name="copy-data-stage0")
    run_copy("stage0", "stage1", name="copy-data-stage1")
    run_copy("stage1", "stage2", name="copy-data-stage2")

    dvc.freeze("copy-data-stage1")

    tmp_dir.gen("data", "bar")
    stages = dvc.reproduce()
    assert stages == [data_stage, stage0]


def test_downstream(tmp_dir, dvc):
    # The dependency graph should look like this:
    #
    #       E
    #      / \
    #     D   F
    #    / \   \
    #   B   C   G
    #    \ /
    #     A
    #
    assert main(["run", "-n", "A-gen", "-o", "A", "echo A>A"]) == 0
    assert main(["run", "-n", "B-gen", "-d", "A", "-o", "B", "echo B>B"]) == 0
    assert (
        main(["run", "--single-stage", "-d", "A", "-o", "C", "echo C>C"]) == 0
    )
    assert (
        main(
            ["run", "-n", "D-gen", "-d", "B", "-d", "C", "-o", "D", "echo D>D"]
        )
        == 0
    )
    assert main(["run", "--single-stage", "-o", "G", "echo G>G"]) == 0
    assert main(["run", "-n", "F-gen", "-d", "G", "-o", "F", "echo F>F"]) == 0
    assert (
        main(
            [
                "run",
                "--single-stage",
                "-d",
                "D",
                "-d",
                "F",
                "-o",
                "E",
                "echo E>E",
            ]
        )
        == 0
    )

    # We want the evaluation to move from B to E
    #
    #       E
    #      /
    #     D
    #    /
    #   B
    #
    evaluation = dvc.reproduce(
        PIPELINE_FILE + ":B-gen", downstream=True, force=True
    )

    assert len(evaluation) == 3
    assert (
        isinstance(evaluation[0], PipelineStage)
        and evaluation[0].relpath == PIPELINE_FILE
        and evaluation[0].name == "B-gen"
    )
    assert (
        isinstance(evaluation[1], PipelineStage)
        and evaluation[1].relpath == PIPELINE_FILE
        and evaluation[1].name == "D-gen"
    )
    assert (
        not isinstance(evaluation[2], PipelineStage)
        and evaluation[2].relpath == "E.dvc"
    )

    # B, C should be run (in any order) before D
    # See https://github.com/iterative/dvc/issues/3602
    evaluation = dvc.reproduce(
        PIPELINE_FILE + ":A-gen", downstream=True, force=True
    )

    assert len(evaluation) == 5
    assert (
        isinstance(evaluation[0], PipelineStage)
        and evaluation[0].relpath == PIPELINE_FILE
        and evaluation[0].name == "A-gen"
    )
    names = set()
    for stage in evaluation[1:3]:
        if isinstance(stage, PipelineStage):
            assert stage.relpath == PIPELINE_FILE
            names.add(stage.name)
        else:
            names.add(stage.relpath)
    assert names == {"B-gen", "C.dvc"}
    assert (
        isinstance(evaluation[3], PipelineStage)
        and evaluation[3].relpath == PIPELINE_FILE
        and evaluation[3].name == "D-gen"
    )
    assert (
        not isinstance(evaluation[4], PipelineStage)
        and evaluation[4].relpath == "E.dvc"
    )


def test_repro_when_cmd_changes(tmp_dir, dvc, run_copy, mocker):
    from dvc.dvcfile import PipelineFile

    tmp_dir.gen("foo", "foo")
    stage = run_copy("foo", "bar", name="copy-file")
    target = "copy-file"
    assert not dvc.reproduce(target)

    from dvc.stage.run import cmd_run

    m = mocker.patch("dvc.stage.run.cmd_run", wraps=cmd_run)
    stage.cmd = "  ".join(stage.cmd.split())  # change cmd spacing by two
    PipelineFile(dvc, PIPELINE_FILE)._dump_pipeline_file(stage)

    assert dvc.status([target]) == {target: ["changed command"]}
    assert dvc.reproduce(target)[0] == stage
    m.assert_called_once_with(
        stage, checkpoint_func=None, dry=False, run_env=None
    )


def test_repro_when_new_deps_is_added_in_dvcfile(tmp_dir, dvc, run_copy):
    from dvc.dvcfile import Dvcfile

    tmp_dir.gen("copy.py", COPY_SCRIPT)
    tmp_dir.gen({"foo": "foo", "bar": "bar"})
    stage = dvc.run(
        cmd="python copy.py {} {}".format("foo", "foobar"),
        outs=["foobar"],
        deps=["foo"],
        name="copy-file",
    )
    target = PIPELINE_FILE + ":copy-file"
    assert not dvc.reproduce(target)

    dvcfile = Dvcfile(dvc, stage.path)
    data, _ = dvcfile._load()
    data["stages"]["copy-file"]["deps"] += ["copy.py"]
    dump_yaml(stage.path, data)

    assert dvc.reproduce(target)[0] == stage


def test_repro_when_new_outs_is_added_in_dvcfile(tmp_dir, dvc):
    from dvc.dvcfile import Dvcfile

    tmp_dir.gen("copy.py", COPY_SCRIPT)
    tmp_dir.gen({"foo": "foo", "bar": "bar"})
    stage = dvc.run(
        cmd="python copy.py {} {}".format("foo", "foobar"),
        outs=[],  # scenario where user forgot to add
        deps=["foo"],
        name="copy-file",
    )
    target = ":copy-file"
    assert not dvc.reproduce(target)

    dvcfile = Dvcfile(dvc, stage.path)
    data, _ = dvcfile._load()
    data["stages"]["copy-file"]["outs"] = ["foobar"]
    dump_yaml(stage.path, data)

    assert dvc.reproduce(target)[0] == stage


def test_repro_when_new_deps_is_moved(tmp_dir, dvc):
    from dvc.dvcfile import Dvcfile

    tmp_dir.gen("copy.py", COPY_SCRIPT)
    tmp_dir.gen({"foo": "foo", "bar": "foo"})
    stage = dvc.run(
        cmd="python copy.py {} {}".format("foo", "foobar"),
        outs=["foobar"],
        deps=["foo"],
        name="copy-file",
    )
    target = ":copy-file"
    assert not dvc.reproduce(target)

    tmp_dir.gen("copy.py", COPY_SCRIPT_FORMAT.format("'bar'", "'foobar'"))
    from shutil import move

    move("foo", "bar")

    dvcfile = Dvcfile(dvc, stage.path)
    data, _ = dvcfile._load()
    data["stages"]["copy-file"]["deps"] = ["bar"]
    dump_yaml(stage.path, data)

    assert dvc.reproduce(target)[0] == stage


def test_repro_when_new_out_overlaps_others_stage_outs(tmp_dir, dvc):
    from dvc.exceptions import OverlappingOutputPathsError

    tmp_dir.gen({"dir": {"file1": "file1"}, "foo": "foo"})
    dvc.add("dir")
    dump_yaml(
        PIPELINE_FILE,
        {
            "stages": {
                "run-copy": {
                    "cmd": "python copy {} {}".format("foo", "dir/foo"),
                    "deps": ["foo"],
                    "outs": ["dir/foo"],
                }
            }
        },
    )
    with pytest.raises(OverlappingOutputPathsError):
        dvc.reproduce(":run-copy")


def test_repro_when_new_deps_added_does_not_exist(tmp_dir, dvc):
    tmp_dir.gen("copy.py", COPY_SCRIPT)
    tmp_dir.gen("foo", "foo")
    dump_yaml(
        PIPELINE_FILE,
        {
            "stages": {
                "run-copy": {
                    "cmd": "python copy.py {} {}".format("foo", "foobar"),
                    "deps": ["foo", "bar"],
                    "outs": ["foobar"],
                }
            }
        },
    )
    with pytest.raises(ReproductionError):
        dvc.reproduce(":run-copy")


def test_repro_when_new_outs_added_does_not_exist(tmp_dir, dvc):
    tmp_dir.gen("copy.py", COPY_SCRIPT)
    tmp_dir.gen("foo", "foo")
    dump_yaml(
        PIPELINE_FILE,
        {
            "stages": {
                "run-copy": {
                    "cmd": "python copy.py {} {}".format("foo", "foobar"),
                    "deps": ["foo"],
                    "outs": ["foobar", "bar"],
                }
            }
        },
    )
    with pytest.raises(ReproductionError):
        dvc.reproduce(":run-copy")


def test_repro_when_lockfile_gets_deleted(tmp_dir, dvc):
    tmp_dir.gen("copy.py", COPY_SCRIPT)
    tmp_dir.gen("foo", "foo")
    dump_yaml(
        PIPELINE_FILE,
        {
            "stages": {
                "run-copy": {
                    "cmd": "python copy.py {} {}".format("foo", "foobar"),
                    "deps": ["foo"],
                    "outs": ["foobar"],
                }
            }
        },
    )
    assert dvc.reproduce(":run-copy")
    assert os.path.exists(PIPELINE_LOCK)

    assert not dvc.reproduce(":run-copy")
    os.unlink(PIPELINE_LOCK)
    stages = dvc.reproduce(":run-copy")
    assert (
        stages
        and stages[0].relpath == PIPELINE_FILE
        and stages[0].name == "run-copy"
    )


def test_cyclic_graph_error(tmp_dir, dvc, run_copy):
    tmp_dir.gen("foo", "foo")
    run_copy("foo", "bar", name="copy-foo-bar")
    run_copy("bar", "baz", name="copy-bar-baz")
    run_copy("baz", "foobar", name="copy-baz-foobar")

    data = load_yaml(PIPELINE_FILE)
    data["stages"]["copy-baz-foo"] = {
        "cmd": "echo baz > foo",
        "deps": ["baz"],
        "outs": ["foo"],
    }
    dump_yaml(PIPELINE_FILE, data)
    with pytest.raises(CyclicGraphError):
        dvc.reproduce(":copy-baz-foo")


def test_repro_multiple_params(tmp_dir, dvc):
    from dvc.stage.utils import split_params_deps
    from tests.func.test_run_multistage import supported_params

    dump_yaml(tmp_dir / "params2.yaml", supported_params)
    dump_yaml(tmp_dir / "params.yaml", supported_params)

    (tmp_dir / "foo").write_text("foo")
    stage = dvc.run(
        name="read_params",
        deps=["foo"],
        outs=["bar"],
        params=[
            "params2.yaml:lists,floats,name",
            "answer,floats,nested.nested1",
        ],
        cmd="cat params2.yaml params.yaml > bar",
    )

    params, deps = split_params_deps(stage)
    assert len(params) == 2
    assert len(deps) == 1
    assert len(stage.outs) == 1

    lockfile = stage.dvcfile._lockfile
    assert lockfile.load()["stages"]["read_params"]["params"] == {
        "params2.yaml": {
            "lists": [42, 42.0, "42"],
            "floats": 42.0,
            "name": "Answer",
        },
        "params.yaml": {
            "answer": 42,
            "floats": 42.0,
            "nested.nested1": {"nested2": "42", "nested2-2": 41.99999},
        },
    }
    data, _ = stage.dvcfile._load()
    params = data["stages"]["read_params"]["params"]

    custom, defaults = lsplit(lambda v: isinstance(v, dict), params)
    assert set(custom[0]["params2.yaml"]) == {"name", "lists", "floats"}
    assert set(defaults) == {"answer", "floats", "nested.nested1"}

    assert not dvc.reproduce(stage.addressing)
    params = deepcopy(supported_params)
    params["answer"] = 43
    dump_yaml(tmp_dir / "params.yaml", params)

    assert dvc.reproduce(stage.addressing) == [stage]


@pytest.mark.parametrize("multiline", [True, False])
def test_repro_list_of_commands_in_order(tmp_dir, dvc, multiline):
    cmd = ["echo foo>foo", "echo bar>bar"]
    if multiline:
        cmd = "\n".join(cmd)

    dump_yaml("dvc.yaml", {"stages": {"multi": {"cmd": cmd}}})

    (tmp_dir / "dvc.yaml").write_text(
        dedent(
            """\
            stages:
              multi:
                cmd:
                - echo foo>foo
                - echo bar>bar
        """
        )
    )
    dvc.reproduce(targets=["multi"])
    assert (tmp_dir / "foo").read_text() == "foo\n"
    assert (tmp_dir / "bar").read_text() == "bar\n"


@pytest.mark.parametrize("multiline", [True, False])
def test_repro_list_of_commands_raise_and_stops_after_failure(
    tmp_dir, dvc, multiline
):
    cmd = ["echo foo>foo", "failed_command", "echo baz>bar"]
    if multiline:
        cmd = "\n".join(cmd)

    dump_yaml("dvc.yaml", {"stages": {"multi": {"cmd": cmd}}})

    with pytest.raises(ReproductionError):
        dvc.reproduce(targets=["multi"])
    assert (tmp_dir / "foo").read_text() == "foo\n"
    assert not (tmp_dir / "bar").exists()
