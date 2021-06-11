import os
import shutil

import pytest

from dvc.exceptions import OutputDuplicationError
from dvc.hash_info import HashInfo
from dvc.repo import NotDvcRepoError, Repo, locked


def test_is_dvc_internal(dvc):
    assert dvc.is_dvc_internal(os.path.join("path", "to", ".dvc", "file"))
    assert not dvc.is_dvc_internal(os.path.join("path", "to-non-.dvc", "file"))


@pytest.mark.parametrize(
    "path",
    [
        os.path.join("dir", "subdir", "file"),
        os.path.join("dir", "subdir"),
        "dir",
    ],
)
def test_find_outs_by_path(tmp_dir, dvc, path):
    (stage,) = tmp_dir.dvc_gen(
        {"dir": {"subdir": {"file": "file"}, "other": "other"}}
    )

    outs = dvc.find_outs_by_path(path, strict=False)
    assert len(outs) == 1
    assert outs[0].path_info == stage.outs[0].path_info


def test_find_outs_by_path_does_graph_checks(tmp_dir, dvc):
    tmp_dir.dvc_gen("foo", "foo")
    shutil.copyfile("foo.dvc", "foo-2.dvc")

    dvc._reset()
    with pytest.raises(OutputDuplicationError):
        dvc.find_outs_by_path("foo")


@pytest.mark.parametrize(
    "path",
    [os.path.join("dir", "subdir", "file"), os.path.join("dir", "subdir")],
)
def test_used_cache(tmp_dir, dvc, path):
    from dvc.objects.tree import Tree

    tmp_dir.dvc_gen({"dir": {"subdir": {"file": "file"}, "other": "other"}})

    expected_objs = {
        HashInfo("md5", "70922d6bf66eb073053a82f77d58c536.dir"),
        HashInfo("md5", "8c7dd922ad47494fc02c388e12c00eac"),
    }
    expected_external = {}

    objs, external = dvc.used_cache([path])
    used = set()
    for obj in objs:
        used.add(obj.hash_info)
        if isinstance(obj, Tree):
            used.update(entry_obj.hash_info for _, entry_obj in obj)

    assert used == expected_objs
    assert external == expected_external


def test_locked(mocker):
    repo = mocker.MagicMock()
    repo._lock_depth = 0
    repo.method = locked(repo.method)

    args = ()
    kwargs = {}
    repo.method(repo, args, kwargs)

    assert repo.method_calls == [
        mocker.call._reset(),
        mocker.call.method(repo, args, kwargs),
        mocker.call._reset(),
    ]


def test_skip_graph_checks(tmp_dir, dvc, mocker, run_copy):
    # See https://github.com/iterative/dvc/issues/2671 for more info
    mock_build_graph = mocker.patch("dvc.repo.build_graph")

    # sanity check
    tmp_dir.gen("foo", "foo text")
    dvc.add("foo")
    run_copy("foo", "bar", single_stage=True)
    assert mock_build_graph.called

    # check that our hack can be enabled
    mock_build_graph.reset_mock()
    dvc._skip_graph_checks = True
    tmp_dir.gen("baz", "baz text")
    dvc.add("baz")
    run_copy("baz", "qux", single_stage=True)
    assert not mock_build_graph.called

    # check that our hack can be disabled
    mock_build_graph.reset_mock()
    dvc._skip_graph_checks = False
    tmp_dir.gen("quux", "quux text")
    dvc.add("quux")
    run_copy("quux", "quuz", single_stage=True)
    assert mock_build_graph.called


def test_branch_config(tmp_dir, scm):
    tmp_dir.scm_gen("foo", "foo", commit="init")

    # sanity check
    with pytest.raises(NotDvcRepoError):
        Repo().close()

    scm.checkout("branch", create_new=True)
    dvc = Repo.init()
    with dvc.config.edit() as conf:
        conf["remote"]["branch"] = {"url": "/some/path"}
    dvc.close()

    scm.add([os.path.join(".dvc", "config")])
    scm.commit("init dvc")
    scm.checkout("master")

    with pytest.raises(NotDvcRepoError):
        Repo(scm=scm, rev="master").close()

    dvc = Repo(scm=scm, rev="branch")
    try:
        assert dvc.config["remote"]["branch"]["url"] == "/some/path"
    finally:
        dvc.close()


def test_dynamic_cache_initalization(tmp_dir, scm):
    dvc = Repo.init()
    with dvc.config.edit() as conf:
        conf["cache"]["ssh"] = "foo"
        conf["remote"]["foo"] = {"url": "remote://bar/baz"}
    dvc.close()

    Repo(str(tmp_dir)).close()
