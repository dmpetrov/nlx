import mock
import pytest

import dvc.remote.slow_link_detection
from dvc.remote.slow_link_detection import slow_link_guard


@pytest.fixture(autouse=True)
def timeout_immediately(monkeypatch):
    monkeypatch.setattr(dvc.remote.slow_link_detection, "timeout_seconds", 0.0)


@pytest.fixture
def make_remote():
    def _make_remote(cache_type=None, should_warn=True):
        remote = mock.Mock()
        remote.cache_types = cache_type
        remote.slow_link_warning = should_warn
        return remote

    return _make_remote


def test_show_warning_once(caplog, make_remote):
    remote = make_remote()
    slow_link_guard(lambda x: None)(remote)
    slow_link_guard(lambda x: None)(remote)

    slow_link_detection = dvc.remote.slow_link_detection
    message = slow_link_detection.message  # noqa, pylint: disable=no-member
    assert len(caplog.records) == 1
    assert caplog.records[0].message == message


def test_dont_warn_when_cache_type_is_set(caplog, make_remote):
    remote = make_remote(cache_type="copy")
    slow_link_guard(lambda x: None)(remote)

    assert len(caplog.records) == 0


def test_dont_warn_when_warning_is_disabled(caplog, make_remote):
    remote = make_remote(should_warn=False)
    slow_link_guard(lambda x: None)(remote)

    assert len(caplog.records) == 0
