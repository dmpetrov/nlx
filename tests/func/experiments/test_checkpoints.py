import pytest
from funcy import first

from dvc.exceptions import DvcException
from dvc.repo.experiments import MultipleBranchError
from dvc.repo.experiments.base import EXEC_APPLY, EXEC_CHECKPOINT


@pytest.mark.parametrize("workspace", [True, False])
@pytest.mark.parametrize("links", ["reflink,copy", "hardlink,symlink"])
def test_new_checkpoint(
    tmp_dir, scm, dvc, checkpoint_stage, mocker, workspace, links
):
    with dvc.config.edit() as conf:
        conf["cache"]["type"] = links

    new_mock = mocker.spy(dvc.experiments, "new")
    results = dvc.experiments.run(
        checkpoint_stage.addressing, params=["foo=2"], tmp_dir=not workspace
    )
    exp = first(results)

    new_mock.assert_called_once()
    for rev in dvc.brancher([exp]):
        if rev == "workspace":
            continue
        fs = dvc.repo_fs
        with fs.open(tmp_dir / "foo") as fobj:
            assert fobj.read().strip() == str(checkpoint_stage.iterations)
        with fs.open(tmp_dir / "metrics.yaml") as fobj:
            assert fobj.read().strip() == "foo: 2"

    if workspace:
        assert scm.get_ref(EXEC_APPLY) == exp
    assert scm.get_ref(EXEC_CHECKPOINT) == exp
    if workspace:
        assert (tmp_dir / "foo").read_text().strip() == str(
            checkpoint_stage.iterations
        )
        assert (tmp_dir / "metrics.yaml").read_text().strip() == "foo: 2"


@pytest.mark.parametrize(
    "checkpoint_resume, workspace",
    [(None, True), (None, False), ("foo", True), ("foo", False)],
)
def test_resume_checkpoint(
    tmp_dir, scm, dvc, checkpoint_stage, checkpoint_resume, workspace
):
    results = dvc.experiments.run(
        checkpoint_stage.addressing, params=["foo=2"], tmp_dir=not workspace
    )

    with pytest.raises(DvcException):
        dvc.experiments.run(
            checkpoint_stage.addressing,
            checkpoint_resume="abc1234",
            tmp_dir=not workspace,
        )

    if checkpoint_resume:
        checkpoint_resume = first(results)

    if not workspace:
        dvc.experiments.apply(first(results))
    results = dvc.experiments.run(
        checkpoint_stage.addressing,
        checkpoint_resume=checkpoint_resume,
        tmp_dir=not workspace,
    )
    exp = first(results)

    for rev in dvc.brancher([exp]):
        if rev == "workspace":
            continue
        fs = dvc.repo_fs
        with fs.open(tmp_dir / "foo") as fobj:
            assert fobj.read().strip() == str(2 * checkpoint_stage.iterations)
        with fs.open(tmp_dir / "metrics.yaml") as fobj:
            assert fobj.read().strip() == "foo: 2"

    if workspace:
        assert scm.get_ref(EXEC_APPLY) == exp
    assert scm.get_ref(EXEC_CHECKPOINT) == exp


@pytest.mark.parametrize("workspace", [True, False])
def test_reset_checkpoint(
    tmp_dir, scm, dvc, checkpoint_stage, caplog, workspace
):
    dvc.experiments.run(
        checkpoint_stage.addressing, name="foo", tmp_dir=not workspace
    )

    results = dvc.experiments.run(
        checkpoint_stage.addressing,
        params=["foo=2"],
        name="foo",
        tmp_dir=not workspace,
        reset=True,
    )
    exp = first(results)

    for rev in dvc.brancher([exp]):
        if rev == "workspace":
            continue
        fs = dvc.repo_fs
        with fs.open(tmp_dir / "foo") as fobj:
            assert fobj.read().strip() == str(checkpoint_stage.iterations)
        with fs.open(tmp_dir / "metrics.yaml") as fobj:
            assert fobj.read().strip() == "foo: 2"

    if workspace:
        assert scm.get_ref(EXEC_APPLY) == exp
    assert scm.get_ref(EXEC_CHECKPOINT) == exp


@pytest.mark.parametrize("workspace", [True, False])
def test_resume_branch(tmp_dir, scm, dvc, checkpoint_stage, workspace):
    results = dvc.experiments.run(
        checkpoint_stage.addressing, params=["foo=2"], tmp_dir=not workspace
    )
    branch_rev = first(results)
    if not workspace:
        dvc.experiments.apply(branch_rev)

    results = dvc.experiments.run(
        checkpoint_stage.addressing,
        checkpoint_resume=branch_rev,
        tmp_dir=not workspace,
    )
    checkpoint_a = first(results)

    dvc.experiments.apply(branch_rev)
    results = dvc.experiments.run(
        checkpoint_stage.addressing,
        checkpoint_resume=branch_rev,
        params=["foo=100"],
        tmp_dir=not workspace,
    )
    checkpoint_b = first(results)

    for rev in dvc.brancher([checkpoint_a]):
        if rev == "workspace":
            continue
        fs = dvc.repo_fs
        with fs.open(tmp_dir / "foo") as fobj:
            assert fobj.read().strip() == str(2 * checkpoint_stage.iterations)
        with fs.open(tmp_dir / "metrics.yaml") as fobj:
            assert fobj.read().strip() == "foo: 2"

    for rev in dvc.brancher([checkpoint_b]):
        if rev == "workspace":
            continue
        fs = dvc.repo_fs
        with fs.open(tmp_dir / "foo") as fobj:
            assert fobj.read().strip() == str(2 * checkpoint_stage.iterations)
        with fs.open(tmp_dir / "metrics.yaml") as fobj:
            assert fobj.read().strip() == "foo: 100"

    with pytest.raises(MultipleBranchError):
        dvc.experiments.get_branch_by_rev(branch_rev)

    assert branch_rev == dvc.experiments.scm.gitpython.repo.git.merge_base(
        checkpoint_a, checkpoint_b
    )


@pytest.mark.parametrize("workspace", [True, False])
def test_resume_non_head_checkpoint(
    tmp_dir, scm, dvc, checkpoint_stage, workspace
):
    orig_head = scm.get_rev()
    results = dvc.experiments.run(
        checkpoint_stage.addressing, params=["foo=2"], tmp_dir=not workspace
    )
    checkpoint_head = first(results)
    orig_branch = dvc.experiments.get_branch_by_rev(checkpoint_head)

    rev = list(scm.branch_revs(checkpoint_head, orig_head))[-1]
    dvc.experiments.apply(rev)

    with pytest.raises(DvcException):
        dvc.experiments.run(checkpoint_stage.addressing, tmp_dir=not workspace)

    results = dvc.experiments.run(
        checkpoint_stage.addressing, params=["foo=100"], tmp_dir=not workspace
    )
    new_head = first(results)
    assert orig_branch != dvc.experiments.get_branch_by_rev(new_head)
