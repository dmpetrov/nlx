import os
from operator import itemgetter

import pytest
from funcy import raiser

from dvc.dvcfile import PIPELINE_FILE, FileIsGitIgnored
from dvc.exceptions import NoOutputOrStageError
from dvc.path_info import PathInfo
from dvc.repo import Repo
from dvc.stage.exceptions import (
    StageFileDoesNotExistError,
    StageFileFormatError,
    StageNameUnspecified,
    StageNotFound,
)
from dvc.utils import relpath
from dvc.utils.fs import remove
from dvc.utils.serialize import dump_yaml


def test_collect(tmp_dir, scm, dvc, run_copy):
    def collect_outs(*args, **kwargs):
        return {
            str(out)
            for stage in dvc.stage.collect(*args, **kwargs)
            for out in stage.outs
        }

    tmp_dir.dvc_gen("foo", "foo")
    run_copy("foo", "bar", single_stage=True)
    scm.add([".gitignore", "foo.dvc", "bar.dvc"])
    scm.commit("Add foo and bar")

    scm.checkout("new-branch", create_new=True)

    run_copy("bar", "buzz", single_stage=True)
    scm.add([".gitignore", "buzz.dvc"])
    scm.commit("Add buzz")

    assert collect_outs("bar.dvc", with_deps=True) == {"foo", "bar"}
    assert collect_outs("buzz.dvc", with_deps=True) == {"foo", "bar", "buzz"}
    assert collect_outs("buzz.dvc", with_deps=False) == {"buzz"}

    run_copy("foo", "foobar", name="copy-foo-foobar")
    assert collect_outs(":copy-foo-foobar") == {"foobar"}
    assert collect_outs(":copy-foo-foobar", with_deps=True) == {
        "foobar",
        "foo",
    }
    assert collect_outs("dvc.yaml:copy-foo-foobar", recursive=True) == {
        "foobar"
    }
    assert collect_outs("copy-foo-foobar") == {"foobar"}
    assert collect_outs("copy-foo-foobar", with_deps=True) == {"foobar", "foo"}
    assert collect_outs("copy-foo-foobar", recursive=True) == {"foobar"}

    run_copy("foobar", "baz", name="copy-foobar-baz")
    assert collect_outs("dvc.yaml") == {"foobar", "baz"}
    assert collect_outs("dvc.yaml", with_deps=True) == {"foobar", "baz", "foo"}


def test_collect_dir_recursive(tmp_dir, dvc, run_head):
    tmp_dir.gen({"dir": {"foo": "foo"}})
    (stage1,) = dvc.add("dir", recursive=True)
    with (tmp_dir / "dir").chdir():
        stage2 = run_head("foo", name="copy-foo-bar")
        stage3 = run_head("foo-1", single_stage=True)
    assert set(dvc.stage.collect("dir", recursive=True)) == {
        stage1,
        stage2,
        stage3,
    }


def test_collect_with_not_existing_output_or_stage_name(
    tmp_dir, dvc, run_copy
):
    with pytest.raises(StageFileDoesNotExistError):
        dvc.stage.collect("some_file")
    tmp_dir.dvc_gen("foo", "foo")
    run_copy("foo", "bar", name="copy-foo-bar")
    with pytest.raises(StageNotFound):
        dvc.stage.collect("some_file")


def test_stages(tmp_dir, dvc):
    def collect_stages():
        return {stage.relpath for stage in Repo(os.fspath(tmp_dir)).stages}

    tmp_dir.dvc_gen({"file": "a", "dir/file": "b", "dir/subdir/file": "c"})

    assert collect_stages() == {
        "file.dvc",
        os.path.join("dir", "file.dvc"),
        os.path.join("dir", "subdir", "file.dvc"),
    }

    tmp_dir.gen(".dvcignore", "dir")

    assert collect_stages() == {"file.dvc"}


@pytest.fixture
def stages(tmp_dir, run_copy):
    stage1, stage2 = tmp_dir.dvc_gen({"foo": "foo", "lorem": "lorem"})
    return {
        "foo-generate": stage1,
        "lorem-generate": stage2,
        "copy-foo-bar": run_copy("foo", "bar", single_stage=True),
        "copy-bar-foobar": run_copy("bar", "foobar", name="copy-bar-foobar"),
        "copy-lorem-ipsum": run_copy(
            "lorem", "ipsum", name="copy-lorem-ipsum"
        ),
    }


def test_collect_not_a_group_stage_with_group_flag(tmp_dir, dvc, stages):
    assert set(dvc.stage.collect("copy-bar-foobar", accept_group=True)) == {
        stages["copy-bar-foobar"]
    }
    assert set(
        dvc.stage.collect("copy-bar-foobar", accept_group=True, with_deps=True)
    ) == {
        stages["copy-bar-foobar"],
        stages["copy-foo-bar"],
        stages["foo-generate"],
    }
    assert set(
        dvc.stage.collect_granular("copy-bar-foobar", accept_group=True)
    ) == {(stages["copy-bar-foobar"], None)}
    assert set(
        dvc.stage.collect_granular(
            "copy-bar-foobar", accept_group=True, with_deps=True
        )
    ) == {
        (stages["copy-bar-foobar"], None),
        (stages["copy-foo-bar"], None),
        (stages["foo-generate"], None),
    }


def test_collect_generated(tmp_dir, dvc):
    d = {
        "vars": [{"vars": [1, 2, 3, 4, 5]}],
        "stages": {
            "build": {"foreach": "${vars}", "do": {"cmd": "echo ${item}"}}
        },
    }
    dump_yaml("dvc.yaml", d)

    all_stages = set(dvc.stages)
    assert len(all_stages) == 5

    assert set(dvc.stage.collect()) == all_stages
    assert set(dvc.stage.collect("build", accept_group=True)) == all_stages
    assert (
        set(dvc.stage.collect("build", accept_group=True, with_deps=True))
        == all_stages
    )
    assert set(dvc.stage.collect("build*", glob=True)) == all_stages
    assert (
        set(dvc.stage.collect("build*", glob=True, with_deps=True))
        == all_stages
    )

    stages_info = {(stage, None) for stage in all_stages}
    assert (
        set(dvc.stage.collect_granular("build", accept_group=True))
        == stages_info
    )
    assert (
        set(
            dvc.stage.collect_granular(
                "build", accept_group=True, with_deps=True
            )
        )
        == stages_info
    )


def test_collect_glob(tmp_dir, dvc, stages):
    assert set(dvc.stage.collect("copy*", glob=True)) == {
        stages[key] for key in ["copy-bar-foobar", "copy-lorem-ipsum"]
    }
    assert set(
        dvc.stage.collect("copy-lorem*", glob=True, with_deps=True)
    ) == {stages[key] for key in ["copy-lorem-ipsum", "lorem-generate"]}


def test_collect_granular_with_no_target(tmp_dir, dvc, stages):
    assert set(map(itemgetter(0), dvc.stage.collect_granular())) == set(
        stages.values()
    )
    assert list(map(itemgetter(1), dvc.stage.collect_granular())) == [
        None
    ] * len(stages)


def test_collect_granular_with_target(tmp_dir, dvc, stages):
    assert dvc.stage.collect_granular("bar.dvc") == [
        (stages["copy-foo-bar"], None)
    ]
    assert dvc.stage.collect_granular(PIPELINE_FILE) == [
        (stages["copy-bar-foobar"], None),
        (stages["copy-lorem-ipsum"], None),
    ]
    assert dvc.stage.collect_granular(":") == [
        (stages["copy-bar-foobar"], None),
        (stages["copy-lorem-ipsum"], None),
    ]
    assert dvc.stage.collect_granular("copy-bar-foobar") == [
        (stages["copy-bar-foobar"], None)
    ]
    assert dvc.stage.collect_granular(":copy-bar-foobar") == [
        (stages["copy-bar-foobar"], None)
    ]
    assert dvc.stage.collect_granular("dvc.yaml:copy-bar-foobar") == [
        (stages["copy-bar-foobar"], None)
    ]

    with (tmp_dir / dvc.DVC_DIR).chdir():
        assert dvc.stage.collect_granular(
            relpath(tmp_dir / PIPELINE_FILE) + ":copy-bar-foobar"
        ) == [(stages["copy-bar-foobar"], None)]

    assert dvc.stage.collect_granular("foobar") == [
        (stages["copy-bar-foobar"], PathInfo(tmp_dir / "foobar"))
    ]


@pytest.mark.parametrize(
    "target",
    [
        "not_existing.dvc",
        "not_existing.dvc:stage_name",
        "not_existing/dvc.yaml",
        "not_existing/dvc.yaml:stage_name",
    ],
)
def test_collect_with_not_existing_dvcfile(tmp_dir, dvc, target):
    with pytest.raises(StageFileDoesNotExistError):
        dvc.stage.collect_granular(target)
    with pytest.raises(StageFileDoesNotExistError):
        dvc.stage.collect(target)


def test_collect_granular_with_not_existing_output_or_stage_name(tmp_dir, dvc):
    with pytest.raises(NoOutputOrStageError):
        dvc.stage.collect_granular("some_file")
    with pytest.raises(NoOutputOrStageError):
        dvc.stage.collect_granular("some_file", recursive=True)


def test_collect_granular_with_deps(tmp_dir, dvc, stages):
    assert (
        set(
            map(
                itemgetter(0),
                dvc.stage.collect_granular("bar.dvc", with_deps=True),
            )
        )
        == {stages["copy-foo-bar"], stages["foo-generate"]}
    )
    assert set(
        map(
            itemgetter(0),
            dvc.stage.collect_granular("copy-bar-foobar", with_deps=True),
        )
    ) == {
        stages["copy-bar-foobar"],
        stages["copy-foo-bar"],
        stages["foo-generate"],
    }
    assert (
        set(
            map(
                itemgetter(0),
                dvc.stage.collect_granular(PIPELINE_FILE, with_deps=True),
            )
        )
        == set(stages.values())
    )


def test_collect_granular_same_output_name_stage_name(tmp_dir, dvc, run_copy):
    (stage1,) = tmp_dir.dvc_gen("foo", "foo")
    (stage2,) = tmp_dir.dvc_gen("copy-foo-bar", "copy-foo-bar")
    stage3 = run_copy("foo", "bar", name="copy-foo-bar")

    assert dvc.stage.collect_granular("copy-foo-bar") == [(stage3, None)]

    coll = dvc.stage.collect_granular("copy-foo-bar", with_deps=True)
    assert set(map(itemgetter(0), coll)) == {stage3, stage1}
    assert list(map(itemgetter(1), coll)) == [None] * 2

    assert dvc.stage.collect_granular("./copy-foo-bar") == [
        (stage2, PathInfo(tmp_dir / "copy-foo-bar"))
    ]
    assert dvc.stage.collect_granular("./copy-foo-bar", with_deps=True) == [
        (stage2, PathInfo(tmp_dir / "copy-foo-bar"))
    ]


def test_collect_granular_priority_on_collision(tmp_dir, dvc, run_copy):
    tmp_dir.gen({"dir": {"foo": "foo"}, "foo": "foo"})
    (stage1,) = dvc.add("dir", recursive=True)
    stage2 = run_copy("foo", "bar", name="dir")

    assert dvc.stage.collect_granular("dir") == [(stage2, None)]
    assert dvc.stage.collect_granular("dir", recursive=True) == [
        (stage1, None)
    ]

    remove(tmp_dir / "dir")

    assert dvc.stage.collect_granular("dir") == [(stage2, None)]
    assert dvc.stage.collect_granular("dir", recursive=True) == [
        (stage2, None)
    ]


def test_collect_granular_collision_output_dir_stage_name(
    tmp_dir, dvc, run_copy
):
    stage1, *_ = tmp_dir.dvc_gen({"dir": {"foo": "foo"}, "foo": "foo"})
    stage3 = run_copy("foo", "bar", name="dir")

    assert dvc.stage.collect_granular("dir") == [(stage3, None)]
    assert not dvc.stage.collect_granular("dir", recursive=True)
    assert dvc.stage.collect_granular("./dir") == [
        (stage1, PathInfo(tmp_dir / "dir"))
    ]


def test_collect_granular_not_existing_stage_name(tmp_dir, dvc, run_copy):
    tmp_dir.dvc_gen("foo", "foo")
    (stage,) = tmp_dir.dvc_gen("copy-foo-bar", "copy-foo-bar")
    run_copy("foo", "bar", name="copy-foo-bar")

    assert dvc.stage.collect_granular(
        "copy-foo-bar.dvc:stage_name_not_needed"
    ) == [(stage, None)]
    with pytest.raises(StageNotFound):
        dvc.stage.collect_granular("dvc.yaml:does-not-exist")


def test_get_stages(tmp_dir, dvc, run_copy):
    with pytest.raises(StageFileDoesNotExistError):
        dvc.stage.load_all()

    tmp_dir.gen("foo", "foo")
    stage1 = run_copy("foo", "bar", name="copy-foo-bar")
    stage2 = run_copy("bar", "foobar", name="copy-bar-foobar")

    assert set(dvc.stage.load_all()) == {stage1, stage2}
    assert set(dvc.stage.load_all(path=PIPELINE_FILE)) == {stage1, stage2}
    assert set(dvc.stage.load_all(name="copy-bar-foobar")) == {stage2}
    assert set(
        dvc.stage.load_all(path=PIPELINE_FILE, name="copy-bar-foobar")
    ) == {stage2}

    with pytest.raises(StageFileDoesNotExistError):
        dvc.stage.load_all(path=relpath(tmp_dir / ".." / PIPELINE_FILE))

    with pytest.raises(StageNotFound):
        dvc.stage.load_all(path=PIPELINE_FILE, name="copy")


def test_get_stages_old_dvcfile(tmp_dir, dvc):
    (stage1,) = tmp_dir.dvc_gen("foo", "foo")
    assert set(dvc.stage.load_all("foo.dvc")) == {stage1}
    assert set(dvc.stage.load_all("foo.dvc", name="foo-generate")) == {stage1}

    with pytest.raises(StageFileDoesNotExistError):
        dvc.stage.load_all(path=relpath(tmp_dir / ".." / "foo.dvc"))


def test_get_stage(tmp_dir, dvc, run_copy):
    tmp_dir.gen("foo", "foo")
    stage1 = run_copy("foo", "bar", name="copy-foo-bar")

    with pytest.raises(StageNameUnspecified):
        dvc.stage.load_one()

    with pytest.raises(StageNameUnspecified):
        dvc.stage.load_one(path=PIPELINE_FILE)

    assert (
        dvc.stage.load_one(path=PIPELINE_FILE, name="copy-foo-bar") == stage1
    )
    assert dvc.stage.load_one(name="copy-foo-bar") == stage1

    with pytest.raises(StageFileDoesNotExistError):
        dvc.stage.load_one(path="something.yaml", name="name")

    with pytest.raises(StageNotFound):
        dvc.stage.load_one(name="random_name")


def test_get_stage_single_stage_dvcfile(tmp_dir, dvc):
    (stage1,) = tmp_dir.dvc_gen("foo", "foo")
    assert dvc.stage.load_one("foo.dvc") == stage1
    assert dvc.stage.load_one("foo.dvc", name="jpt") == stage1
    with pytest.raises(StageFileDoesNotExistError):
        dvc.stage.load_one(path="bar.dvc", name="name")


def test_collect_optimization(tmp_dir, dvc, mocker):
    (stage,) = tmp_dir.dvc_gen("foo", "foo text")

    # Forget cached stages and graph and error out on collection
    dvc._reset()
    mocker.patch(
        "dvc.repo.Repo.stages",
        property(raiser(Exception("Should not collect"))),
    )

    # Should read stage directly instead of collecting the whole graph
    dvc.stage.collect(stage.path)
    dvc.stage.collect_granular(stage.path)


def test_collect_optimization_on_stage_name(tmp_dir, dvc, mocker, run_copy):
    tmp_dir.dvc_gen("foo", "foo")
    stage = run_copy("foo", "bar", name="copy-foo-bar")
    # Forget cached stages and graph and error out on collection
    dvc._reset()
    mocker.patch(
        "dvc.repo.Repo.stages",
        property(raiser(Exception("Should not collect"))),
    )

    # Should read stage directly instead of collecting the whole graph
    assert dvc.stage.collect("copy-foo-bar") == [stage]
    assert dvc.stage.collect_granular("copy-foo-bar") == [(stage, None)]


def test_collect_repo_callback(tmp_dir, dvc, mocker):
    mock = mocker.Mock()
    dvc.stage_collection_error_handler = mock

    (stage,) = tmp_dir.dvc_gen("foo", "foo")
    dump_yaml(tmp_dir / PIPELINE_FILE, {"stages": {"cmd": "echo hello world"}})

    dvc._reset()
    assert dvc.stages == [stage]
    mock.assert_called_once()

    file_path, exc = mock.call_args[0]
    assert file_path == PIPELINE_FILE
    assert isinstance(exc, StageFileFormatError)


def test_gitignored_collect_repo(tmp_dir, dvc, scm):
    (stage,) = tmp_dir.dvc_gen({"data": {"foo": "foo", "bar": "bar"}})

    assert dvc.stage.collect_repo() == [stage]

    scm.ignore(stage.path)
    scm._reset()

    assert not dvc.stage.collect_repo()


def test_gitignored_file_try_collect_granular_for_data_files(
    tmp_dir, dvc, scm
):
    (stage,) = tmp_dir.dvc_gen({"data": {"foo": "foo", "bar": "bar"}})
    path = PathInfo("data") / "foo"

    assert dvc.stage.collect_granular(str(path)) == [
        (stage, PathInfo(tmp_dir / path))
    ]

    scm.ignore(stage.path)
    dvc._reset()

    with pytest.raises(NoOutputOrStageError):
        dvc.stage.collect_granular(str(path))


def test_gitignored_file_try_collect_granular_for_dvc_yaml_files(
    tmp_dir, dvc, scm, stages
):
    assert dvc.stage.collect_granular("bar") == [
        (stages["copy-foo-bar"], PathInfo(tmp_dir / "bar"))
    ]

    scm.ignore(tmp_dir / "dvc.yaml")
    scm._reset()

    with pytest.raises(FileIsGitIgnored):
        dvc.stage.collect_granular("bar")
