import os

import mock

from dvc.analytics import _scm_in_use
from dvc.main import main
from dvc.repo import Repo


@mock.patch("dvc.analytics.send")
def test_daemon_analytics(mock_send, tmp_path):
    report = os.fspath(tmp_path)
    assert 0 == main(["daemon", "analytics", report])

    mock_send.assert_called_with(report)


@mock.patch("dvc.analytics.collect_and_send_report")
@mock.patch("dvc.analytics.is_enabled", return_value=True)
def test_main_analytics(mock_is_enabled, mock_report, tmp_dir, dvc):
    tmp_dir.gen("foo", "text")
    assert 0 == main(["add", "foo"])
    assert mock_is_enabled.called
    assert mock_report.called


def test_scm_dvc_only(tmp_dir, dvc):
    scm = _scm_in_use()
    assert scm == "NoSCM"


def test_scm_git(tmp_dir, scm, dvc):
    scm = _scm_in_use()
    assert scm == "Git"


def test_scm_subrepo(tmp_dir, scm):
    subdir = tmp_dir / "subdir"
    subdir.mkdir()

    with subdir.chdir():
        Repo.init(subdir=True)
        scm = _scm_in_use()

    assert scm == "Git"
