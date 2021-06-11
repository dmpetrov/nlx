import pytest

from dvc.state import _build_sqlite_uri


@pytest.mark.parametrize(
    "path, osname, result",
    [
        ("/abs/path", "posix", "file:///abs/path"),
        ("C:\\abs\\path", "nt", "file:///C:/abs/path"),
        ("/abs/p?ath", "posix", "file:///abs/p%3fath"),
        ("C:\\abs\\p?ath", "nt", "file:///C:/abs/p%3fath"),
        ("/abs/p#ath", "posix", "file:///abs/p%23ath"),
        ("C:\\abs\\p#ath", "nt", "file:///C:/abs/p%23ath"),
        ("/abs/path space", "posix", "file:///abs/path space"),
        ("C:\\abs\\path space", "nt", "file:///C:/abs/path space"),
        ("/abs/path%20encoded", "posix", "file:///abs/path%2520encoded"),
        ("C:\\abs\\path%20encoded", "nt", "file:///C:/abs/path%2520encoded"),
    ],
)
def test_build_uri(path, osname, result, mocker):
    mocker.patch("os.name", osname)
    assert _build_sqlite_uri(path, {}) == result
