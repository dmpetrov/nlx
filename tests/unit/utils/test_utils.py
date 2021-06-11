import os
import re

import pytest

from dvc.fs.local import LocalFileSystem
from dvc.path_info import PathInfo
from dvc.utils import (
    dict_sha256,
    file_md5,
    fix_env,
    parse_target,
    relpath,
    resolve_output,
    tmp_fname,
    to_chunks,
)


@pytest.mark.parametrize(
    "chunk_size, expected_chunks",
    [(1, [[1], [2], [3], [4]]), (2, [[1, 2], [3, 4]]), (3, [[1, 2, 3], [4]])],
)
def test_to_chunks_chunk_size(chunk_size, expected_chunks):
    list_to_chunk = [1, 2, 3, 4]
    result = list(to_chunks(list_to_chunk, chunk_size=chunk_size))
    assert result == expected_chunks


@pytest.mark.parametrize("num_chunks, chunk_size", [(1, 2), (None, None)])
def test_to_chunks_should_raise(num_chunks, chunk_size):
    list_to_chunk = [1, 2, 3]
    with pytest.raises(ValueError):
        to_chunks(list_to_chunk, num_chunks, chunk_size)


@pytest.mark.parametrize(
    "num_chunks, expected_chunks",
    [(4, [[1], [2], [3], [4]]), (3, [[1, 2], [3, 4]]), (2, [[1, 2], [3, 4]])],
)
def test_to_chunks_num_chunks(num_chunks, expected_chunks):
    list_to_chunk = [1, 2, 3, 4]
    result = to_chunks(list_to_chunk, num_chunks=num_chunks)
    assert result == expected_chunks


@pytest.mark.skipif(os.name == "nt", reason="pyenv-win is not supported")
@pytest.mark.parametrize(
    "path, orig",
    [
        (
            (
                "/pyenv/bin:/pyenv/libexec:/pyenv/plugins/plugin:"
                "/orig/path1:/orig/path2"
            ),
            "/orig/path1:/orig/path2",
        ),
        (
            "/pyenv/bin:/pyenv/libexec:/orig/path1:/orig/path2",
            "/orig/path1:/orig/path2",
        ),
        (
            (
                "/pyenv/bin:/some/libexec:/pyenv/plugins/plugin:"
                "/orig/path1:/orig/path2"
            ),
            "/orig/path1:/orig/path2",
        ),
        ("/orig/path1:/orig/path2", "/orig/path1:/orig/path2"),
        (
            "/orig/path1:/orig/path2:/pyenv/bin:/pyenv/libexec",
            "/orig/path1:/orig/path2:/pyenv/bin:/pyenv/libexec",
        ),
    ],
)
def test_fix_env_pyenv(path, orig):
    env = {
        "PATH": path,
        "PYENV_ROOT": "/pyenv",
        "PYENV_VERSION": "3.7.2",
        "PYENV_DIR": "/some/dir",
        "PYENV_HOOK_PATH": "/some/hook/path",
    }
    assert fix_env(env)["PATH"] == orig


def test_file_md5(tmp_dir):
    tmp_dir.gen("foo", "foo content")

    fs = LocalFileSystem()
    assert file_md5("foo", fs) == file_md5(PathInfo("foo"), fs)


def test_tmp_fname():
    file_path = os.path.join("path", "to", "file")
    file_path_info = PathInfo(file_path)

    def pattern(path):
        return r"^" + re.escape(path) + r"\.[a-z0-9]{22}\.tmp$"

    assert re.search(pattern(file_path), tmp_fname(file_path), re.IGNORECASE)
    assert re.search(
        pattern(file_path_info.fspath),
        tmp_fname(file_path_info),
        re.IGNORECASE,
    )


def test_relpath():
    path = "path"
    path_info = PathInfo(path)

    assert relpath(path) == relpath(path_info)


@pytest.mark.skipif(os.name != "nt", reason="Windows specific")
def test_relpath_windows(monkeypatch):
    """test that relpath correctly generated when run on a
    windows network share. The drive mapped path is mapped
    to a UNC path by os.path.realpath"""

    def dummy_realpath(path):
        return path.replace("x:", "\\\\server\\share")

    monkeypatch.setattr(os.path, "realpath", dummy_realpath)
    assert (
        relpath("x:\\dir1\\dir2\\file.txt", "\\\\server\\share\\dir1")
        == "dir2\\file.txt"
    )

    assert (
        relpath("y:\\dir1\\dir2\\file.txt", "\\\\server\\share\\dir1")
        == "y:\\dir1\\dir2\\file.txt"
    )


@pytest.mark.parametrize(
    "inp,out,is_dir,expected",
    [
        ["target", None, False, "target"],
        ["target", "dir", True, os.path.join("dir", "target")],
        ["target", "file_target", False, "file_target"],
        [
            "target",
            os.path.join("dir", "subdir"),
            True,
            os.path.join("dir", "subdir", "target"),
        ],
        ["dir/", None, False, "dir"],
        ["dir", None, False, "dir"],
        ["dir", "other_dir", False, "other_dir"],
        ["dir", "other_dir", True, os.path.join("other_dir", "dir")],
    ],
)
def test_resolve_output(inp, out, is_dir, expected, mocker):
    mocker.patch("os.path.isdir", return_value=is_dir)
    result = resolve_output(inp, out)
    assert result == expected


@pytest.mark.parametrize(
    "inp,out, default",
    [
        ["dvc.yaml", ("dvc.yaml", None), None],
        ["dvc.yaml:name", ("dvc.yaml", "name"), None],
        [":name", ("dvc.yaml", "name"), None],
        ["stage.dvc", ("stage.dvc", None), None],
        ["dvc.yaml:name", ("dvc.yaml", "name"), None],
        ["../models/stage.dvc", ("../models/stage.dvc", None), "def"],
        [":name", ("default", "name"), "default"],
        [":name", ("default", "name"), "default"],
        ["something.dvc:name", ("something.dvc", "name"), None],
        ["../something.dvc:name", ("../something.dvc", "name"), None],
        ["file", (None, "file"), None],
        ["build@15", (None, "build@15"), None],
        ["build@{'level': 35}", (None, "build@{'level': 35}"), None],
        [":build@15", ("dvc.yaml", "build@15"), None],
        [":build@{'level': 35}", ("dvc.yaml", "build@{'level': 35}"), None],
        ["dvc.yaml:build@15", ("dvc.yaml", "build@15"), None],
        [
            "dvc.yaml:build@{'level': 35}",
            ("dvc.yaml", "build@{'level': 35}"),
            None,
        ],
        [
            "build2@{'level': [1, 2, 3]}",
            (None, "build2@{'level': [1, 2, 3]}"),
            None,
        ],
        [
            ":build2@{'level': [1, 2, 3]}",
            ("dvc.yaml", "build2@{'level': [1, 2, 3]}"),
            None,
        ],
        [
            "dvc.yaml:build2@{'level': [1, 2, 3]}",
            ("dvc.yaml", "build2@{'level': [1, 2, 3]}"),
            None,
        ],
    ],
)
def test_parse_target(inp, out, default):
    assert parse_target(inp, default) == out


def test_hint_on_lockfile():
    with pytest.raises(Exception) as exc:
        assert parse_target("dvc.lock:name")
    assert "dvc.yaml:name" in str(exc.value)


@pytest.mark.parametrize(
    "d,sha",
    [
        (
            {
                "cmd": "echo content > out",
                "deps": {"dep": "2254342becceafbd04538e0a38696791"},
                "outs": {"out": "f75b8179e4bbe7e2b4a074dcef62de95"},
            },
            "f472eda60f09660a4750e8b3208cf90b3a3b24e5f42e0371d829710e9464d74a",
        ),
        (
            {
                "cmd": "echo content > out",
                "deps": {"dep": "2254342becceafbd04538e0a38696791"},
                "outs": ["out"],
            },
            "a239b67073bd58affcdb81fff3305d1726c6e7f9c86f3d4fca0e92e8147dc7b0",
        ),
    ],
)
def test_dict_sha256(d, sha):
    assert dict_sha256(d) == sha
