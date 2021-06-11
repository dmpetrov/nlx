import os

from dvc.stage.utils import resolve_paths


def test_resolve_paths():
    p = os.path.join("dir", "subdir")
    file_path = os.path.join(p, "dvc.yaml")

    path, wdir = resolve_paths(path=file_path, wdir="dir")
    assert path == os.path.abspath(file_path)
    assert wdir == os.path.abspath(os.path.join(p, "dir"))

    path, wdir = resolve_paths(path=file_path)
    assert path == os.path.abspath(file_path)
    assert wdir == os.path.abspath(p)

    path, wdir = resolve_paths(path=file_path, wdir="../../some-dir")
    assert path == os.path.abspath(file_path)
    assert wdir == os.path.abspath("some-dir")
