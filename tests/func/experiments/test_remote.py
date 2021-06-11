import os

import pytest
from funcy import first

from dvc.exceptions import DvcException
from dvc.repo.experiments.utils import exp_refs_by_rev


@pytest.fixture
def git_upstream(tmp_dir, erepo_dir):
    url = "file://{}".format(erepo_dir.resolve().as_posix())
    tmp_dir.scm.gitpython.repo.create_remote("upstream", url)
    erepo_dir.remote = "upstream"
    erepo_dir.url = url
    return erepo_dir


@pytest.fixture
def git_downstream(tmp_dir, erepo_dir):
    url = "file://{}".format(tmp_dir.resolve().as_posix())
    erepo_dir.scm.gitpython.repo.create_remote("upstream", url)
    erepo_dir.remote = "upstream"
    erepo_dir.url = url
    return erepo_dir


@pytest.mark.parametrize("use_url", [True, False])
def test_push(tmp_dir, scm, dvc, git_upstream, exp_stage, use_url):
    from dvc.exceptions import InvalidArgumentError

    remote = git_upstream.url if use_url else git_upstream.remote
    with pytest.raises(InvalidArgumentError):
        dvc.experiments.push(remote, "foo")

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp = first(results)
    ref_info = first(exp_refs_by_rev(scm, exp))

    dvc.experiments.push(remote, ref_info.name)
    assert git_upstream.scm.get_ref(str(ref_info)) == exp

    git_upstream.scm.remove_ref(str(ref_info))

    dvc.experiments.push(remote, str(ref_info))
    assert git_upstream.scm.get_ref(str(ref_info)) == exp


def test_push_diverged(tmp_dir, scm, dvc, git_upstream, exp_stage):
    git_upstream.scm_gen("foo", "foo", commit="init")
    remote_rev = git_upstream.scm.get_rev()

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp = first(results)
    ref_info = first(exp_refs_by_rev(scm, exp))

    git_upstream.scm.set_ref(str(ref_info), remote_rev)

    with pytest.raises(DvcException):
        dvc.experiments.push(git_upstream.remote, ref_info.name)
    assert git_upstream.scm.get_ref(str(ref_info)) == remote_rev

    dvc.experiments.push(git_upstream.remote, ref_info.name, force=True)
    assert git_upstream.scm.get_ref(str(ref_info)) == exp


def test_push_checkpoint(tmp_dir, scm, dvc, git_upstream, checkpoint_stage):
    results = dvc.experiments.run(
        checkpoint_stage.addressing, params=["foo=2"]
    )
    exp_a = first(results)
    ref_info_a = first(exp_refs_by_rev(scm, exp_a))

    dvc.experiments.push(git_upstream.remote, ref_info_a.name, force=True)
    assert git_upstream.scm.get_ref(str(ref_info_a)) == exp_a

    results = dvc.experiments.run(
        checkpoint_stage.addressing, checkpoint_resume=exp_a
    )
    exp_b = first(results)
    ref_info_b = first(exp_refs_by_rev(scm, exp_b))

    tmp_dir.scm_gen("new", "new", commit="new")

    dvc.experiments.push(git_upstream.remote, ref_info_b.name, force=True)
    assert git_upstream.scm.get_ref(str(ref_info_b)) == exp_b


def test_push_ambiguous_name(tmp_dir, scm, dvc, git_upstream, exp_stage):
    from dvc.exceptions import InvalidArgumentError

    remote = git_upstream.remote

    results = dvc.experiments.run(
        exp_stage.addressing, params=["foo=2"], name="foo"
    )
    exp_a = first(results)
    ref_info_a = first(exp_refs_by_rev(scm, exp_a))

    tmp_dir.scm_gen("new", "new", commit="new")
    results = dvc.experiments.run(
        exp_stage.addressing, params=["foo=3"], name="foo"
    )
    exp_b = first(results)
    ref_info_b = first(exp_refs_by_rev(scm, exp_b))

    dvc.experiments.push(remote, "foo")
    assert git_upstream.scm.get_ref(str(ref_info_b)) == exp_b

    tmp_dir.scm_gen("new", "new 2", commit="new 2")

    with pytest.raises(InvalidArgumentError):
        dvc.experiments.push(remote, "foo")

    dvc.experiments.push(remote, str(ref_info_a))
    assert git_upstream.scm.get_ref(str(ref_info_a)) == exp_a


@pytest.mark.parametrize("use_url", [True, False])
def test_list_remote(tmp_dir, scm, dvc, git_downstream, exp_stage, use_url):
    baseline_a = scm.get_rev()
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp_a = first(results)
    ref_info_a = first(exp_refs_by_rev(scm, exp_a))

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=3"])
    exp_b = first(results)
    ref_info_b = first(exp_refs_by_rev(scm, exp_b))

    tmp_dir.scm_gen("new", "new", commit="new")
    baseline_c = scm.get_rev()
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=4"])
    exp_c = first(results)
    ref_info_c = first(exp_refs_by_rev(scm, exp_c))

    remote = git_downstream.url if use_url else git_downstream.remote

    assert git_downstream.scm.get_ref("HEAD") != scm.get_ref("HEAD")
    downstream_exp = git_downstream.dvc.experiments
    assert downstream_exp.ls(git_remote=remote) == {}

    exp_list = downstream_exp.ls(rev=baseline_a, git_remote=remote)
    assert {key: set(val) for key, val in exp_list.items()} == {
        baseline_a: {ref_info_a.name, ref_info_b.name}
    }

    exp_list = downstream_exp.ls(all_=True, git_remote=remote)
    assert {key: set(val) for key, val in exp_list.items()} == {
        baseline_a: {ref_info_a.name, ref_info_b.name},
        baseline_c: {ref_info_c.name},
    }


@pytest.mark.parametrize("use_url", [True, False])
def test_pull(tmp_dir, scm, dvc, git_downstream, exp_stage, use_url):
    from dvc.exceptions import InvalidArgumentError

    remote = git_downstream.url if use_url else git_downstream.remote
    downstream_exp = git_downstream.dvc.experiments
    with pytest.raises(InvalidArgumentError):
        downstream_exp.pull(remote, "foo")

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp = first(results)
    ref_info = first(exp_refs_by_rev(scm, exp))

    downstream_exp.pull(remote, ref_info.name)
    assert git_downstream.scm.get_ref(str(ref_info)) == exp

    git_downstream.scm.remove_ref(str(ref_info))

    downstream_exp.pull(remote, str(ref_info))
    assert git_downstream.scm.get_ref(str(ref_info)) == exp


def test_pull_diverged(tmp_dir, scm, dvc, git_downstream, exp_stage):
    git_downstream.scm_gen("foo", "foo", commit="init")
    remote_rev = git_downstream.scm.get_rev()

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp = first(results)
    ref_info = first(exp_refs_by_rev(scm, exp))

    git_downstream.scm.set_ref(str(ref_info), remote_rev)

    downstream_exp = git_downstream.dvc.experiments
    with pytest.raises(DvcException):
        downstream_exp.pull(git_downstream.remote, ref_info.name)
    assert git_downstream.scm.get_ref(str(ref_info)) == remote_rev

    downstream_exp.pull(git_downstream.remote, ref_info.name, force=True)
    assert git_downstream.scm.get_ref(str(ref_info)) == exp


def test_pull_checkpoint(tmp_dir, scm, dvc, git_downstream, checkpoint_stage):
    results = dvc.experiments.run(
        checkpoint_stage.addressing, params=["foo=2"]
    )
    exp_a = first(results)
    ref_info_a = first(exp_refs_by_rev(scm, exp_a))

    downstream_exp = git_downstream.dvc.experiments
    downstream_exp.pull(git_downstream.remote, ref_info_a.name, force=True)
    assert git_downstream.scm.get_ref(str(ref_info_a)) == exp_a

    results = dvc.experiments.run(
        checkpoint_stage.addressing, checkpoint_resume=exp_a
    )
    exp_b = first(results)
    ref_info_b = first(exp_refs_by_rev(scm, exp_b))

    downstream_exp.pull(git_downstream.remote, ref_info_b.name, force=True)
    assert git_downstream.scm.get_ref(str(ref_info_b)) == exp_b


def test_pull_ambiguous_name(tmp_dir, scm, dvc, git_downstream, exp_stage):
    from dvc.exceptions import InvalidArgumentError

    results = dvc.experiments.run(
        exp_stage.addressing, params=["foo=2"], name="foo"
    )
    exp_a = first(results)
    ref_info_a = first(exp_refs_by_rev(scm, exp_a))

    tmp_dir.scm_gen("new", "new", commit="new")
    results = dvc.experiments.run(
        exp_stage.addressing, params=["foo=3"], name="foo"
    )
    exp_b = first(results)
    ref_info_b = first(exp_refs_by_rev(scm, exp_b))

    remote = git_downstream.remote
    downstream_exp = git_downstream.dvc.experiments
    with pytest.raises(InvalidArgumentError):
        downstream_exp.pull(remote, "foo")

    downstream_exp.pull(remote, str(ref_info_b))
    assert git_downstream.scm.get_ref(str(ref_info_b)) == exp_b

    with git_downstream.scm.detach_head(ref_info_a.baseline_sha):
        downstream_exp.pull(remote, "foo")
    assert git_downstream.scm.get_ref(str(ref_info_a)) == exp_a


def test_push_pull_cache(
    tmp_dir, scm, dvc, git_upstream, checkpoint_stage, local_remote
):
    from dvc.utils.fs import remove
    from tests.func.test_diff import digest

    remote = git_upstream.remote
    results = dvc.experiments.run(
        checkpoint_stage.addressing, params=["foo=2"]
    )
    exp = first(results)
    ref_info = first(exp_refs_by_rev(scm, exp))

    dvc.experiments.push(remote, ref_info.name, push_cache=True)
    for x in range(2, checkpoint_stage.iterations + 1):
        hash_ = digest(str(x))
        path = os.path.join(local_remote.url, hash_[:2], hash_[2:])
        assert os.path.exists(path)
        assert open(path).read() == str(x)

    remove(dvc.odb.local.cache_dir)

    dvc.experiments.pull(remote, ref_info.name, pull_cache=True)
    for x in range(2, checkpoint_stage.iterations + 1):
        hash_ = digest(str(x))
        path = os.path.join(dvc.odb.local.cache_dir, hash_[:2], hash_[2:])
        assert os.path.exists(path)
        assert open(path).read() == str(x)
