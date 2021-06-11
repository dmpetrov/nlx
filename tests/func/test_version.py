import re

from dvc.main import main
from tests.unit.test_info import PYTHON_VERSION_REGEX


def test_(tmp_dir, dvc, scm, caplog):
    assert main(["version"]) == 0

    assert re.search(r"DVC version: \d+\.\d+\.\d+.*", caplog.text)
    assert re.search(f"Platform: {PYTHON_VERSION_REGEX} on .*", caplog.text)
    assert re.search(r"Supports: .*", caplog.text)
    assert re.search(r"Cache types: .*", caplog.text)
    assert re.search(r"Caches: local", caplog.text)
    assert re.search(r"Remotes: None", caplog.text)
    assert "Repo: dvc, git" in caplog.text
