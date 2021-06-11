import pytest

from dvc.env import DVC_PAGER
from dvc.utils.pager import (
    DEFAULT_PAGER,
    LESS,
    PAGER_ENV,
    find_pager,
    make_pager,
    pager,
)


@pytest.fixture(autouse=True)
def clear_envs(monkeypatch):
    monkeypatch.delenv(DVC_PAGER, raising=False)
    monkeypatch.delenv(PAGER_ENV, raising=False)
    monkeypatch.delenv(LESS, raising=False)


def test_find_pager_when_not_isatty(mocker):
    mocker.patch("sys.stdout.isatty", return_value=False)
    assert find_pager() is None


def test_find_pager_uses_custom_pager_when_dvc_pager_env_var_is_defined(
    mocker, monkeypatch
):
    monkeypatch.setenv(DVC_PAGER, "my-pager")
    mocker.patch("sys.stdout.isatty", return_value=True)

    assert find_pager() == "my-pager"


def test_find_pager_uses_custom_pager_when_pager_env_is_defined(
    mocker, monkeypatch
):
    monkeypatch.setenv(PAGER_ENV, "my-pager")
    mocker.patch("sys.stdout.isatty", return_value=True)

    assert find_pager() == "my-pager"


def test_find_pager_uses_default_pager_when_found(mocker):
    mocker.patch("sys.stdout.isatty", return_value=True)
    mocker.patch("os.system", return_value=0)

    assert DEFAULT_PAGER in find_pager()


def test_find_pager_fails_to_find_any_pager(mocker):
    mocker.patch("os.system", return_value=1)
    mocker.patch("sys.stdout.isatty", return_value=True)

    assert find_pager() is None


@pytest.mark.parametrize("env", [DVC_PAGER, PAGER_ENV, None])
def test_dvc_sets_default_options_on_less_without_less_env(
    mocker, monkeypatch, env
):
    if env:
        monkeypatch.setenv(env, "less")
    mocker.patch("sys.stdout.isatty", return_value=True)
    mocker.patch("os.system", return_value=0)

    assert find_pager() == (
        "less --quit-if-one-screen --RAW-CONTROL-CHARS"
        " --chop-long-lines --no-init"
    )


@pytest.mark.parametrize("env", [DVC_PAGER, PAGER_ENV, None])
def test_dvc_sets_some_options_on_less_if_less_env_defined(
    mocker, monkeypatch, env
):
    if env:
        monkeypatch.setenv(env, "less")
    mocker.patch("sys.stdout.isatty", return_value=True)
    mocker.patch("os.system", return_value=0)
    monkeypatch.setenv(LESS, "-R")

    assert find_pager() == "less --RAW-CONTROL-CHARS --chop-long-lines"


def test_make_pager_when_no_pager_found(mocker, monkeypatch):
    assert make_pager(None).__name__ == "plainpager"


def test_pager(mocker, monkeypatch):
    monkeypatch.setenv(DVC_PAGER, "my-pager")
    mocker.patch("sys.stdout.isatty", return_value=True)

    m_make_pager = mocker.patch("dvc.utils.pager.make_pager")
    _pager = m_make_pager.return_value = mocker.MagicMock()

    pager("hello world")
    m_make_pager.assert_called_once_with("my-pager")
    _pager.assert_called_once_with("hello world")
