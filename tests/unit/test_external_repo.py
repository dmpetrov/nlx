import os
from unittest.mock import call

import pytest

from dvc.external_repo import external_repo
from tests.unit.fs.test_repo import make_subrepo


def test_hook_is_called(tmp_dir, erepo_dir, mocker):
    subrepo_paths = [
        "subrepo1",
        "subrepo2",
        os.path.join("dir", "subrepo3"),
        os.path.join("dir", "subrepo4"),
        "subrepo5",
        os.path.join("subrepo5", "subrepo6"),
    ]
    subrepos = [erepo_dir / path for path in subrepo_paths]
    for repo in subrepos:
        make_subrepo(repo, erepo_dir.scm)

    for repo in subrepos + [erepo_dir]:
        with repo.chdir():
            repo.scm_gen("foo", "foo", commit=f"git add {repo}/foo")
            repo.dvc_gen("bar", "bar", commit=f"dvc add {repo}/bar")

    with external_repo(str(erepo_dir)) as repo:
        spy = mocker.spy(repo.repo_fs, "repo_factory")

        list(repo.repo_fs.walk(repo.root_dir, ignore_subrepos=False))  # drain
        assert spy.call_count == len(subrepos)

        paths = [os.path.join(repo.root_dir, path) for path in subrepo_paths]
        spy.assert_has_calls(
            [
                call(
                    path,
                    scm=repo.scm,
                    rev=repo.get_rev(),
                    repo_factory=repo.repo_fs.repo_factory,
                )
                for path in paths
            ],
            any_order=True,
        )


@pytest.mark.parametrize("root_is_dvc", [False, True])
def test_subrepo_is_constructed_properly(
    tmp_dir, scm, mocker, make_tmp_dir, root_is_dvc
):
    if root_is_dvc:
        make_subrepo(tmp_dir, scm)

    subrepo = tmp_dir / "subrepo"
    make_subrepo(subrepo, scm)
    local_cache = subrepo.dvc.odb.local.cache_dir

    tmp_dir.scm_gen("bar", "bar", commit="add bar")
    subrepo.dvc_gen("foo", "foo", commit="add foo")

    cache_dir = make_tmp_dir("temp-cache")
    with external_repo(
        str(tmp_dir), cache_dir=str(cache_dir), cache_types=["symlink"]
    ) as repo:
        spy = mocker.spy(repo.repo_fs, "repo_factory")

        list(repo.repo_fs.walk(repo.root_dir, ignore_subrepos=False))  # drain
        assert spy.call_count == 1
        subrepo = spy.spy_return

        assert repo.url == str(tmp_dir)
        assert repo.config["cache"]["dir"] == str(cache_dir)
        assert repo.odb.local.cache_dir == str(cache_dir)
        assert subrepo.odb.local.cache_dir == str(cache_dir)

        assert repo.config["cache"]["type"] == ["symlink"]
        assert repo.odb.local.cache_types == ["symlink"]
        assert subrepo.odb.local.cache_types == ["symlink"]

        assert (
            subrepo.config["remote"]["auto-generated-upstream"]["url"]
            == local_cache
        )
        if root_is_dvc:
            main_cache = tmp_dir.dvc.odb.local.cache_dir
            assert repo.config["remote"]["auto-generated-upstream"][
                "url"
            ] == str(main_cache)
