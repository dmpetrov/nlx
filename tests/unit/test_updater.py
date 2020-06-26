import json
import logging
import os
import time

import mock
import pytest

from dvc import __version__
from dvc.updater import Updater


@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    monkeypatch.delenv("CI", None)
    monkeypatch.setenv("DVC_TEST", "False")


@pytest.fixture
def updater(tmp_path):
    return Updater(tmp_path)


@pytest.fixture
def mock_tty(mocker):
    return mocker.patch("sys.stdout.isatty", return_value=True)


@mock.patch("requests.get")
def test_fetch(mock_get, updater):
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {"version": __version__}

    assert not os.path.exists(updater.updater_file)

    updater.fetch(detach=False)

    mock_get.assert_called_once_with(Updater.URL, timeout=Updater.TIMEOUT_GET)
    assert os.path.isfile(updater.updater_file)

    with open(updater.updater_file) as fobj:
        info = json.load(fobj)

    assert info["version"] == __version__


@pytest.mark.parametrize(
    "current,latest,notify",
    [
        ("1.0.1", "1.0.1", False),
        ("1.0.1", "1.0.2", True),
        ("1.0.1", "1.0.0", False),
    ],
    ids=["uptodate", "behind", "ahead"],
)
def test_check_updates(mock_tty, updater, caplog, current, latest, notify):
    updater.current = current
    with open(updater.updater_file, "w+") as f:
        json.dump({"version": latest}, f)

    caplog.clear()
    with caplog.at_level(logging.INFO, logger="dvc.updater"):
        updater.check()

    if notify:
        assert f"Update available {current} -> {latest}" in caplog.text
    else:
        assert not caplog.text


@mock.patch("time.time", return_value=time.time() + 24 * 60 * 60 + 1)
def test_check_refetches_each_day(
    mock_time, mock_tty, updater, caplog, mocker
):
    updater.current = "1.0.1"
    with open(updater.updater_file, "w+") as f:
        json.dump({"version": "1.0.2"}, f)
    fetch = mocker.patch.object(updater, "fetch")
    with caplog.at_level(logging.INFO, logger="dvc.updater"):
        updater.check()
    assert not caplog.text
    fetch.assert_called_once()


def test_check_fetches_on_invalid_data_format(
    mock_tty, updater, caplog, mocker
):
    updater.current = "1.0.1"
    with open(updater.updater_file, "w+") as f:
        f.write('"{"version: "1.0.2"')
    fetch = mocker.patch.object(updater, "fetch")
    with caplog.at_level(logging.INFO, logger="dvc.updater"):
        updater.check()
    assert not caplog.text
    fetch.assert_called_once()


@mock.patch("dvc.updater.Updater._check")
def test_check(mock_check, updater):
    updater.check()
    updater.check()
    updater.check()

    assert mock_check.call_count == 3
