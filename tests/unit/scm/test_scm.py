import pytest

from dvc.repo.scm_context import scm_context
from dvc.scm import NoSCM


def test_scm_context(dvc, mocker):
    scm = dvc.scm
    m = mocker.patch.object(
        scm, "track_file_changes", wraps=scm.track_file_changes
    )

    ret_val = 0
    wrapped = scm_context(mocker.Mock(return_value=ret_val))

    assert wrapped(dvc) == ret_val
    m.assert_called_once_with(config=dvc.config)


def test_track_file_changes(mocker):
    scm = mocker.Mock(NoSCM)

    with NoSCM.track_file_changes(scm):
        pass

    assert scm.reset_ignores.call_count == 1
    assert scm.remind_to_track.call_count == 1
    assert scm.track_changed_files.call_count == 0
    assert scm.cleanup_ignores.call_count == 0
    assert scm.reset_tracked_files.call_count == 1


def test_track_file_changes_autostage(mocker):
    scm = mocker.Mock(NoSCM)

    config = {"core": {"autostage": True}}
    with NoSCM.track_file_changes(scm, config=config):
        pass

    assert scm.track_changed_files.call_count == 1
    assert scm.reset_ignores.call_count == 1
    assert scm.remind_to_track.call_count == 0
    assert scm.cleanup_ignores.call_count == 0
    assert scm.reset_tracked_files.call_count == 1


def test_track_file_changes_throw_and_cleanup(mocker):
    scm = mocker.Mock(NoSCM)

    class CustomException(Exception):
        pass

    with pytest.raises(CustomException, match="oops"):
        with NoSCM.track_file_changes(scm):
            raise CustomException("oops")

    assert scm.cleanup_ignores.call_count == 1
    assert scm.reset_ignores.call_count == 0
    assert scm.track_changed_files.call_count == 0
    assert scm.remind_to_track.call_count == 0
    assert scm.reset_tracked_files.call_count == 0
