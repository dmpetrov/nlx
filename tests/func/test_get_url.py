import os

import pytest

from dvc.dependency.base import DependencyDoesNotExistError
from dvc.repo import Repo


def test_get_file(tmp_dir):
    tmp_dir.gen({"foo": "foo contents"})

    Repo.get_url("foo", "foo_imported")

    assert (tmp_dir / "foo_imported").is_file()
    assert (tmp_dir / "foo_imported").read_text() == "foo contents"


def test_get_dir(tmp_dir):
    tmp_dir.gen({"foo": {"foo": "foo contents"}})

    Repo.get_url("foo", "foo_imported")

    assert (tmp_dir / "foo_imported").is_dir()
    assert (tmp_dir / "foo_imported" / "foo").is_file()
    assert (tmp_dir / "foo_imported" / "foo").read_text() == "foo contents"


@pytest.mark.parametrize("dname", [".", "dir", "dir/subdir"])
def test_get_url_to_dir(tmp_dir, tmp_path_factory, dname):
    tmp_dir.gen({"src": {"foo": "foo contents"}, "dir": {"subdir": {}}})

    Repo.get_url(os.path.join("src", "foo"), dname)

    assert (tmp_dir / dname).is_dir()
    assert (tmp_dir / dname / "foo").read_text() == "foo contents"


def test_get_url_nonexistent(tmp_dir):
    with pytest.raises(DependencyDoesNotExistError):
        Repo.get_url("nonexistent")
