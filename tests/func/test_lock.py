import pytest

from dvc.lock import Lock, LockError
from dvc.main import main


def test_with(tmp_dir, dvc, mocker):
    # patching to speedup tests
    mocker.patch("dvc.lock.DEFAULT_TIMEOUT", 0.01)

    lockfile = tmp_dir / dvc.tmp_dir / "lock"
    with Lock(lockfile):
        with pytest.raises(LockError), Lock(lockfile):
            pass


def test_cli(tmp_dir, dvc, mocker, caplog):
    # patching to speedup tests
    mocker.patch("dvc.lock.DEFAULT_TIMEOUT", 0.01)

    expected_error_msg = (
        "Unable to acquire lock. Most likely another DVC process is "
        "running or was terminated abruptly. Check the page "
        "<https://dvc.org/doc/user-guide/troubleshooting#lock-issue> "
        "for other possible reasons and to learn how to resolve this."
    )
    with Lock(tmp_dir / dvc.tmp_dir / "lock"):
        assert main(["add", "foo"]) == 1
    assert expected_error_msg in caplog.text
