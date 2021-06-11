import os

import pytest

import dvc.output as dvc_output


def test_stage_cache(tmp_dir, dvc, mocker):
    tmp_dir.gen("dep", "dep")
    tmp_dir.gen(
        "script.py",
        (
            'open("out", "w+").write("out"); '
            'open("out_no_cache", "w+").write("out_no_cache")'
        ),
    )
    stage = dvc.run(
        cmd="python script.py",
        deps=["script.py", "dep"],
        outs=["out"],
        outs_no_cache=["out_no_cache"],
        single_stage=True,
    )

    with dvc.lock:
        stage.remove(remove_outs=True, force=True)

    assert not (tmp_dir / "out").exists()
    assert not (tmp_dir / "out_no_cache").exists()
    assert not (tmp_dir / "out.dvc").exists()

    cache_dir = os.path.join(
        dvc.stage_cache.cache_dir,
        "10",
        "10b45372fdf4ec14d3f779c5b256378d7a12780e4c7f549a44138e492f098bfe",
    )
    cache_file = os.path.join(
        cache_dir,
        "bb32e04c6da96a7192513390acedbe4cd6123f8fe5b0ba5fffe39716fe87f6f4",
    )

    assert os.path.isdir(cache_dir)
    assert os.listdir(cache_dir) == [os.path.basename(cache_file)]
    assert os.path.isfile(cache_file)

    run_spy = mocker.patch("dvc.stage.run.cmd_run")
    checkout_spy = mocker.spy(dvc_output, "checkout")
    with dvc.lock:
        stage.run()

    assert not run_spy.called
    assert checkout_spy.call_count == 4

    assert (tmp_dir / "out").exists()
    assert (tmp_dir / "out_no_cache").exists()
    assert (tmp_dir / "out").read_text() == "out"
    assert (tmp_dir / "out_no_cache").read_text() == "out_no_cache"


def test_stage_cache_params(tmp_dir, dvc, mocker):
    tmp_dir.gen("params.yaml", "foo: 1\nbar: 2")
    tmp_dir.gen("myparams.yaml", "baz: 3\nqux: 4")
    tmp_dir.gen(
        "script.py",
        (
            'open("out", "w+").write("out"); '
            'open("out_no_cache", "w+").write("out_no_cache")'
        ),
    )
    stage = dvc.run(
        cmd="python script.py",
        params=["foo,bar", "myparams.yaml:baz,qux"],
        outs=["out"],
        outs_no_cache=["out_no_cache"],
        single_stage=True,
    )

    with dvc.lock:
        stage.remove(remove_outs=True, force=True)

    assert not (tmp_dir / "out").exists()
    assert not (tmp_dir / "out_no_cache").exists()
    assert not (tmp_dir / "out.dvc").exists()

    cache_dir = os.path.join(
        dvc.stage_cache.cache_dir,
        "65",
        "651d0a5b82e05e48b03acf44954f6a8599760e652a143d517a17d1065eca61a1",
    )
    cache_file = os.path.join(
        cache_dir,
        "2196a5a4dd24c5759437511fcf9d6aa66b259e1dac58e3f212aefd1797a6f114",
    )

    assert os.path.isdir(cache_dir)
    assert os.listdir(cache_dir) == [os.path.basename(cache_file)]
    assert os.path.isfile(cache_file)

    run_spy = mocker.patch("dvc.stage.run.cmd_run")
    checkout_spy = mocker.spy(dvc_output, "checkout")
    with dvc.lock:
        stage.run()

    assert not run_spy.called
    assert checkout_spy.call_count == 4

    assert (tmp_dir / "out").exists()
    assert (tmp_dir / "out_no_cache").exists()
    assert (tmp_dir / "out").read_text() == "out"
    assert (tmp_dir / "out_no_cache").read_text() == "out_no_cache"


def test_stage_cache_wdir(tmp_dir, dvc, mocker):
    tmp_dir.gen("dep", "dep")
    tmp_dir.gen(
        "script.py",
        (
            'open("out", "w+").write("out"); '
            'open("out_no_cache", "w+").write("out_no_cache")'
        ),
    )
    tmp_dir.gen({"wdir": {}})
    stage = dvc.run(
        cmd="python ../script.py",
        deps=["../script.py", "../dep"],
        outs=["out"],
        outs_no_cache=["out_no_cache"],
        single_stage=True,
        wdir="wdir",
    )

    with dvc.lock:
        stage.remove(remove_outs=True, force=True)

    assert not (tmp_dir / "wdir" / "out").exists()
    assert not (tmp_dir / "wdir" / "out_no_cache").exists()
    assert not (tmp_dir / "wdir" / "out.dvc").exists()

    cache_dir = os.path.join(
        dvc.stage_cache.cache_dir,
        "d2",
        "d2b5da199f4da73a861027f5f76020a948794011db9704814fdb2a488ca93ec2",
    )
    cache_file = os.path.join(
        cache_dir,
        "65cc63ade5ab338541726b26185ebaf42331141ec3a670a7d6e8a227505afade",
    )

    assert os.path.isdir(cache_dir)
    assert os.listdir(cache_dir) == [os.path.basename(cache_file)]
    assert os.path.isfile(cache_file)

    run_spy = mocker.patch("dvc.stage.run.cmd_run")
    checkout_spy = mocker.spy(dvc_output, "checkout")
    with dvc.lock:
        stage.run()

    assert not run_spy.called
    assert checkout_spy.call_count == 4

    assert (tmp_dir / "wdir" / "out").exists()
    assert (tmp_dir / "wdir" / "out_no_cache").exists()
    assert (tmp_dir / "wdir" / "out").read_text() == "out"
    assert (tmp_dir / "wdir" / "out_no_cache").read_text() == "out_no_cache"


def test_shared_stage_cache(tmp_dir, dvc, run_copy):
    import stat

    from dvc.objects.db import ODBManager

    tmp_dir.gen("foo", "foo")

    with dvc.config.edit() as config:
        config["cache"]["shared"] = "group"

    dvc.odb = ODBManager(dvc)

    assert not os.path.exists(dvc.odb.local.cache_dir)

    run_copy("foo", "bar", name="copy-foo-bar")

    parent_cache_dir = os.path.join(dvc.stage_cache.cache_dir, "88")
    cache_dir = os.path.join(
        parent_cache_dir,
        "883395068439203a9de3d1e1649a16e9027bfd1ab5dab4f438d321c4a928b328",
    )
    cache_file = os.path.join(
        cache_dir,
        "e42b7ebb9bc5ac4bccab769c8d1338914dad25d7ffecc8671dbd4581bad4aa15",
    )

    # sanity check
    assert os.path.isdir(cache_dir)
    assert os.listdir(cache_dir) == [os.path.basename(cache_file)]
    assert os.path.isfile(cache_file)

    def _mode(path):
        return stat.S_IMODE(os.stat(path).st_mode)

    if os.name == "nt":
        dir_mode = 0o777
        file_mode = 0o666
    else:
        dir_mode = 0o2775
        file_mode = 0o664

    assert _mode(dvc.odb.local.cache_dir) == dir_mode
    assert _mode(dvc.stage_cache.cache_dir) == dir_mode
    assert _mode(parent_cache_dir) == dir_mode
    assert _mode(cache_dir) == dir_mode
    assert _mode(cache_file) == file_mode


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"cmd": "cmd"},
        {"cmd": "cmd", "deps": ["path"]},
        {"cmd": "cmd", "outs": ["path"]},
        {"always_changed": True},
    ],
)
def test_unhashable(tmp_dir, dvc, mocker, kwargs):
    from dvc.stage import Stage, create_stage
    from dvc.stage.cache import RunCacheNotFoundError, StageCache

    cache = StageCache(dvc)
    stage = create_stage(Stage, path="stage.dvc", repo=dvc, **kwargs)
    get_stage_hash = mocker.patch("dvc.stage.cache._get_stage_hash")
    assert cache.save(stage) is None
    assert get_stage_hash.not_called
    with pytest.raises(RunCacheNotFoundError):
        cache.restore(stage)
    assert get_stage_hash.not_called
