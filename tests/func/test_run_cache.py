import os

from dvc.dvcfile import PIPELINE_LOCK
from dvc.utils import relpath
from dvc.utils.fs import remove


def _recurse_count_files(path):
    return len([os.path.join(r, f) for r, _, fs in os.walk(path) for f in fs])


def test_push_pull(tmp_dir, dvc, erepo_dir, run_copy, local_remote):
    tmp_dir.gen("foo", "foo")
    run_copy("foo", "bar", name="copy-foo-bar")
    assert dvc.push(run_cache=True) == 2
    erepo_dir.add_remote(config=local_remote.config)
    with erepo_dir.chdir():
        assert not os.path.exists(erepo_dir.dvc.stage_cache.cache_dir)
        assert erepo_dir.dvc.pull(run_cache=True)["fetched"] == 0
        assert os.listdir(erepo_dir.dvc.stage_cache.cache_dir)


def test_restore(tmp_dir, dvc, run_copy, mocker):
    tmp_dir.gen("foo", "foo")
    run_copy("foo", "bar", name="copy-foo-bar")

    mock_restore = mocker.spy(dvc.stage_cache, "restore")
    mock_run = mocker.patch("dvc.stage.run.cmd_run")

    # removing any information that `dvc` could use to re-generate from
    (tmp_dir / "bar").unlink()
    (tmp_dir / PIPELINE_LOCK).unlink()

    (stage,) = dvc.reproduce("copy-foo-bar")

    mock_restore.assert_called_once_with(stage)
    mock_run.assert_not_called()
    assert (tmp_dir / "bar").exists() and not (tmp_dir / "foo").unlink()
    assert (tmp_dir / PIPELINE_LOCK).exists()


def test_save(tmp_dir, dvc, run_copy):
    run_cache_dir = dvc.stage_cache.cache_dir
    assert not os.path.exists(run_cache_dir)

    tmp_dir.gen("foo", "foo")
    stage = run_copy("foo", "bar", name="copy-foo-bar")
    assert _recurse_count_files(run_cache_dir) == 1
    assert dvc.stage_cache._load(stage)


def test_do_not_save_on_no_exec_and_dry(tmp_dir, dvc, run_copy):
    run_cache_dir = dvc.stage_cache.cache_dir
    assert not os.path.exists(run_cache_dir)

    tmp_dir.gen("foo", "foo")
    stage = run_copy("foo", "bar", name="copy-foo-bar", no_exec=True)

    assert _recurse_count_files(run_cache_dir) == 0
    assert not dvc.stage_cache._load(stage)

    (stage,) = dvc.reproduce("copy-foo-bar", dry=True)

    assert _recurse_count_files(run_cache_dir) == 0
    assert not dvc.stage_cache._load(stage)


def test_uncached_outs_are_cached(tmp_dir, dvc, run_copy):
    tmp_dir.gen("foo", "foo")
    stage = dvc.run(
        deps=["foo"],
        cmd="cp foo bar",
        outs_no_cache=["bar"],
        name="copy-foo-bar",
    )
    stage.outs[0].hash_info = stage.outs[0].get_hash()
    assert os.path.exists(relpath(stage.outs[0].cache_path))


def test_memory_for_multiple_runs_of_same_stage(
    tmp_dir, dvc, run_copy, mocker
):
    tmp_dir.gen("foo", "foo")
    assert not os.path.exists(dvc.stage_cache.cache_dir)
    run_copy("foo", "bar", name="copy-foo-bar")
    assert _recurse_count_files(dvc.stage_cache.cache_dir) == 1
    tmp_dir.gen("foo", "foobar")
    run_copy("foo", "bar", name="copy-foo-bar")
    assert _recurse_count_files(dvc.stage_cache.cache_dir) == 2

    from dvc.stage import run as _run

    mock_restore = mocker.spy(dvc.stage_cache, "restore")
    mock_run = mocker.spy(_run, "cmd_run")

    (tmp_dir / "bar").unlink()
    (tmp_dir / PIPELINE_LOCK).unlink()
    (stage,) = dvc.reproduce("copy-foo-bar")

    assert (tmp_dir / PIPELINE_LOCK).exists()
    assert (tmp_dir / "bar").read_text() == "foobar"
    mock_run.assert_not_called()
    mock_restore.assert_called_once_with(stage)
    mock_restore.reset_mock()

    (tmp_dir / PIPELINE_LOCK).unlink()
    tmp_dir.gen("foo", "foo")
    dvc.reproduce("copy-foo-bar")

    assert (tmp_dir / "bar").read_text() == "foo"
    mock_run.assert_not_called()
    mock_restore.assert_called_once_with(stage)
    assert (tmp_dir / "bar").exists() and not (tmp_dir / "foo").unlink()
    assert (tmp_dir / PIPELINE_LOCK).exists()


def test_memory_runs_of_multiple_stages(tmp_dir, dvc, run_copy, mocker):
    tmp_dir.gen("foo", "foo")
    assert not os.path.exists(dvc.stage_cache.cache_dir)

    run_copy("foo", "foo.bak", name="backup-foo")
    assert _recurse_count_files(dvc.stage_cache.cache_dir) == 1

    tmp_dir.gen("bar", "bar")
    run_copy("bar", "bar.bak", name="backup-bar")
    assert _recurse_count_files(dvc.stage_cache.cache_dir) == 2

    from dvc.stage import run as _run

    mock_restore = mocker.spy(dvc.stage_cache, "restore")
    mock_run = mocker.spy(_run, "cmd_run")

    (tmp_dir / "foo.bak").unlink()
    (tmp_dir / "bar.bak").unlink()
    (tmp_dir / PIPELINE_LOCK).unlink()
    (stage,) = dvc.reproduce("backup-foo")

    assert (tmp_dir / "foo.bak").read_text() == "foo"
    assert (tmp_dir / PIPELINE_LOCK).exists()
    mock_run.assert_not_called()
    mock_restore.assert_called_once_with(stage)
    mock_restore.reset_mock()

    (stage,) = dvc.reproduce("backup-bar")

    assert (tmp_dir / "bar.bak").read_text() == "bar"
    assert (tmp_dir / PIPELINE_LOCK).exists()
    mock_run.assert_not_called()
    mock_restore.assert_called_once_with(stage)


def test_restore_pull(tmp_dir, dvc, run_copy, mocker, local_remote):
    import dvc.output as dvc_output

    tmp_dir.gen("foo", "foo")
    stage = run_copy("foo", "bar", name="copy-foo-bar")

    dvc.push()

    mock_restore = mocker.spy(dvc.stage_cache, "restore")
    mock_run = mocker.patch("dvc.stage.run.cmd_run")
    mock_checkout = mocker.spy(dvc_output, "checkout")

    # removing any information that `dvc` could use to re-generate from
    (tmp_dir / "bar").unlink()
    (tmp_dir / PIPELINE_LOCK).unlink()
    remove(stage.outs[0].cache_path)

    (stage,) = dvc.reproduce("copy-foo-bar", pull=True)

    mock_restore.assert_called_once_with(stage, pull=True)
    mock_run.assert_not_called()
    assert mock_checkout.call_count == 2
    assert (tmp_dir / "bar").exists() and not (tmp_dir / "foo").unlink()
    assert (tmp_dir / PIPELINE_LOCK).exists()
