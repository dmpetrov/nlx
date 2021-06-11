from dvc.main import main


def test_root(tmp_dir, dvc, caplog):
    assert main(["root"]) == 0
    assert ".\n" in caplog.text


def test_root_locked(tmp_dir, dvc, caplog):
    # NOTE: check that `dvc root` is not blocked with dvc lock
    with dvc.lock:
        assert main(["root"]) == 0
    assert ".\n" in caplog.text
