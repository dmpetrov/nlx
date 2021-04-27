import os
import shutil
from pathlib import Path

import pytest

from dvc.exceptions import DvcIgnoreInCollectedDirError, PathMissingError
from dvc.ignore import DvcIgnore, DvcIgnorePatterns
from dvc.output.base import OutputIsIgnoredError
from dvc.path_info import PathInfo
from dvc.pathspec_math import PatternInfo, merge_patterns
from dvc.repo import Repo
from dvc.utils.fs import get_mtime_and_size
from tests.dir_helpers import TmpDir


def _to_pattern_info_list(str_list: list):
    return [PatternInfo(a, "") for a in str_list]


def _get_pathinfo_set(dvc: Repo, path: PathInfo) -> list:
    root = PathInfo(dvc.root_dir)
    ls_list = dvc.ls(root, path.relpath(root), recursive=True)
    pathinfo_list = [path / PathInfo(entry["path"]) for entry in ls_list]
    return set(pathinfo_list)


@pytest.mark.parametrize("filename", ["ignored", "тест"])
def test_ignore(tmp_dir, dvc, filename):
    tmp_dir.gen({"dir": {filename: filename, "other": "text2"}})
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "dir/{}".format(filename))

    dvc._reset()

    path = PathInfo(tmp_dir)
    assert _get_pathinfo_set(dvc, path) == {
        path / DvcIgnore.DVCIGNORE_FILE,
        path / "dir" / "other",
    }


def test_rename_ignored_file(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"ignored": "...", "other": "text"}})

    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "ignored*")
    dvc._reset()

    mtime, size = get_mtime_and_size("dir", dvc.fs)

    shutil.move("dir/ignored", "dir/ignored_new")
    new_mtime, new_size = get_mtime_and_size("dir", dvc.fs)

    assert new_mtime == mtime and new_size == size


def test_rename_file(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"foo": "foo", "bar": "bar"}})
    mtime, size = get_mtime_and_size("dir", dvc.fs)

    shutil.move("dir/foo", "dir/foo_new")
    new_mtime, new_size = get_mtime_and_size("dir", dvc.fs)

    assert new_mtime != mtime and new_size == size


def test_remove_ignored_file(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"ignored": "...", "other": "text"}})
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "dir/ignored")
    dvc._reset()

    mtime, size = get_mtime_and_size("dir", dvc.fs)

    os.remove("dir/ignored")
    new_mtime, new_size = get_mtime_and_size("dir", dvc.fs)

    assert new_mtime == mtime and new_size == size


def test_remove_file(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"foo": "foo", "bar": "bar"}})
    mtime, size = get_mtime_and_size("dir", dvc.fs)

    os.remove("dir/foo")
    new_mtime, new_size = get_mtime_and_size("dir", dvc.fs)

    assert new_mtime != mtime and new_size != size


def test_dvcignore_in_out_dir(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"foo": "foo", DvcIgnore.DVCIGNORE_FILE: ""}})

    with pytest.raises(DvcIgnoreInCollectedDirError):
        dvc.add("dir")


@pytest.mark.parametrize("dname", ["dir", "dir/subdir"])
def test_ignore_collecting_dvcignores(tmp_dir, dvc, dname):
    tmp_dir.gen({"dir": {"subdir": {}}})

    top_ignore_file = (tmp_dir / dname).with_name(DvcIgnore.DVCIGNORE_FILE)
    top_ignore_file.write_text(os.path.basename(dname))
    dvc._reset()

    ignore_file = tmp_dir / dname / DvcIgnore.DVCIGNORE_FILE
    ignore_file.write_text("foo")

    dvcignore = dvc.dvcignore

    top_ignore_path = os.path.dirname(os.fspath(top_ignore_file))

    sub_dir_path = os.path.dirname(os.fspath(ignore_file))

    assert (
        DvcIgnorePatterns(
            *merge_patterns(
                _to_pattern_info_list([".hg/", ".git/", ".git", ".dvc/"]),
                os.fspath(tmp_dir),
                _to_pattern_info_list([os.path.basename(dname)]),
                top_ignore_path,
            )
        )
        == dvcignore._get_trie_pattern(top_ignore_path)
        == dvcignore._get_trie_pattern(sub_dir_path)
    )


def test_ignore_on_branch(tmp_dir, scm, dvc):
    tmp_dir.scm_gen({"foo": "foo", "bar": "bar"}, commit="add files")

    with tmp_dir.branch("branch", new=True):
        tmp_dir.scm_gen(DvcIgnore.DVCIGNORE_FILE, "foo", commit="add ignore")

    dvc._reset()
    path = PathInfo(tmp_dir)

    assert set(_get_pathinfo_set(dvc, path)) == {
        path / "foo",
        path / "bar",
        path / DvcIgnore.DVCIGNORE_FILE,
    }

    dvc.fs = scm.get_fs("branch")
    dvc.dvcignore.is_ignored(path / "foo")


def test_match_nested(tmp_dir, dvc):
    tmp_dir.gen(
        {
            ".dvcignore": "*.backup\ntmp",
            "foo": "foo",
            "tmp": "...",
            "dir": {"x.backup": "x backup", "tmp": "content"},
        }
    )
    dvc._reset()
    path = PathInfo(tmp_dir)
    result = _get_pathinfo_set(dvc, path)
    assert result == {path / DvcIgnore.DVCIGNORE_FILE, path / "foo"}


def test_ignore_external(tmp_dir, scm, dvc, tmp_path_factory):
    tmp_dir.gen(".dvcignore", "*.backup\ntmp")
    ext_dir = TmpDir(os.fspath(tmp_path_factory.mktemp("external_dir")))
    ext_dir.gen({"y.backup": "y", "tmp": {"file": "ext tmp"}})

    path = PathInfo(ext_dir)
    result = _get_pathinfo_set(dvc, PathInfo(ext_dir))
    assert result == {path / "y.backup", path / "tmp" / "file"}
    assert dvc.dvcignore.is_ignored_dir(os.fspath(ext_dir / "tmp")) is False
    assert (
        dvc.dvcignore.is_ignored_file(os.fspath(ext_dir / "y.backup")) is False
    )


def test_ignore_subrepo(tmp_dir, scm, dvc):
    tmp_dir.gen({".dvcignore": "foo", "subdir": {"foo": "foo"}})
    scm.add([".dvcignore"])
    scm.commit("init parent dvcignore")
    dvc._reset()

    subrepo_dir = tmp_dir / "subdir"

    with pytest.raises(PathMissingError):
        _get_pathinfo_set(dvc, PathInfo(subrepo_dir))

    with subrepo_dir.chdir():
        subrepo = Repo.init(subdir=True)
        scm.add(str(subrepo_dir / "foo"))
        scm.commit("subrepo init")

    for _ in subrepo.brancher(all_commits=True):
        assert subrepo.fs.exists(PathInfo(subrepo_dir / "foo"))


def test_ignore_resurface_subrepo(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen({"foo": "foo"}, commit="add foo")
    subrepo_dir = tmp_dir / "subdir"
    subrepo_dir.mkdir()
    with subrepo_dir.chdir():
        Repo.init(subdir=True)
        subrepo_dir.gen({"bar": {"bar": "bar"}})

    dvc._reset()

    files = ["foo"]
    dirs = ["bar"]
    root = os.fspath(subrepo_dir)
    assert list(dvc.dvcignore([(root, dirs, files)])) == [(root, dirs, files)]
    assert list(
        dvc.dvcignore([(root, dirs, files)], ignore_subrepos=False)
    ) == [(root, [], [])]

    assert dvc.dvcignore.is_ignored_dir(os.fspath(subrepo_dir / "bar"))
    assert not dvc.dvcignore.is_ignored_dir(
        os.fspath(subrepo_dir / "bar"), ignore_subrepos=False
    )


def test_ignore_blank_line(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"ignored": "text", "other": "text2"}})
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "foo\n\ndir/ignored")
    dvc._reset()
    path = PathInfo(tmp_dir)
    assert _get_pathinfo_set(dvc, path / "dir") == {path / "dir" / "other"}


# It is not possible to re-include a file if a parent directory of
# that file is excluded.
# Git doesn’t list excluded directories for performance reasons,
# so any patterns on contained files have no effect,
# no matter where they are defined.
@pytest.mark.parametrize(
    "data_struct, pattern_list, result_set",
    [
        (
            {"dir": {"subdir": {"not_ignore": "121"}}},
            ["subdir/*", "!not_ignore"],
            {os.path.join("dir", "subdir", "not_ignore")},
        ),
        (
            {"dir": {"subdir": {"should_ignore": "121"}}},
            ["subdir", "!should_ignore"],
            set(),
        ),
        (
            {"dir": {"subdir": {"should_ignore": "121"}}},
            ["subdir/", "!should_ignore"],
            set(),
        ),
    ],
)
def test_ignore_file_in_parent_path(
    tmp_dir, dvc, data_struct, pattern_list, result_set
):
    tmp_dir.gen(data_struct)
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "\n".join(pattern_list))
    dvc._reset()
    path = PathInfo(tmp_dir)
    #  temporary solution before #5841 fixed
    if not result_set:
        with pytest.raises(PathMissingError):
            _get_pathinfo_set(dvc, path / "dir")
    else:
        assert _get_pathinfo_set(dvc, path / "dir") == {
            path / relpath for relpath in result_set
        }


# If there is a separator at the end of the pattern then the pattern
# will only match directories,
# otherwise the pattern can match both files and directories.
# For example, a pattern doc/frotz/ matches doc/frotz directory,
# but not a/doc/frotz directory;
def test_ignore_sub_directory(tmp_dir, dvc):
    tmp_dir.gen(
        {
            "dir": {
                "doc": {"fortz": {"b": "b"}},
                "a": {"doc": {"fortz": {"a": "a"}}},
            }
        }
    )
    tmp_dir.gen({"dir": {DvcIgnore.DVCIGNORE_FILE: "doc/fortz"}})

    dvc._reset()
    path = PathInfo(tmp_dir)
    assert _get_pathinfo_set(dvc, path / "dir") == {
        path / "dir" / "a" / "doc" / "fortz" / "a",
        path / "dir" / DvcIgnore.DVCIGNORE_FILE,
    }


# however frotz/ matches frotz and a/frotz that is a directory
def test_ignore_directory(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"fortz": {}, "a": {"fortz": {}}}})
    tmp_dir.gen({"dir": {DvcIgnore.DVCIGNORE_FILE: "fortz"}})
    dvc._reset()
    path = PathInfo(tmp_dir)
    assert _get_pathinfo_set(dvc, path / "dir") == {
        path / "dir" / DvcIgnore.DVCIGNORE_FILE,
    }


def test_multi_ignore_file(tmp_dir, dvc, monkeypatch):
    tmp_dir.gen({"dir": {"subdir": {"should_ignore": "1", "not_ignore": "1"}}})
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "dir/subdir/*_ignore")
    tmp_dir.gen({"dir": {DvcIgnore.DVCIGNORE_FILE: "!subdir/not_ignore"}})
    dvc._reset()
    path = PathInfo(tmp_dir)
    assert _get_pathinfo_set(dvc, path / "dir") == {
        path / "dir" / "subdir" / "not_ignore",
        path / "dir" / DvcIgnore.DVCIGNORE_FILE,
    }


def test_pattern_trie_fs(tmp_dir, dvc):
    tmp_dir.gen(
        {
            "top": {
                "first": {
                    DvcIgnore.DVCIGNORE_FILE: "a\nb\nc",
                    "middle": {
                        "second": {
                            DvcIgnore.DVCIGNORE_FILE: "d\ne\nf",
                            "bottom": {},
                        }
                    },
                },
            },
            "other": {DvcIgnore.DVCIGNORE_FILE: "1\n2\n3"},
        }
    )
    dvc._reset()
    dvcignore = dvc.dvcignore

    ignore_pattern_top = dvcignore._get_trie_pattern(
        os.fspath(tmp_dir / "top")
    )
    ignore_pattern_other = dvcignore._get_trie_pattern(
        os.fspath(tmp_dir / "other")
    )
    ignore_pattern_first = dvcignore._get_trie_pattern(
        os.fspath(tmp_dir / "top" / "first")
    )
    ignore_pattern_middle = dvcignore._get_trie_pattern(
        os.fspath(tmp_dir / "top" / "first" / "middle")
    )
    ignore_pattern_second = dvcignore._get_trie_pattern(
        os.fspath(tmp_dir / "top" / "first" / "middle" / "second")
    )
    ignore_pattern_bottom = dvcignore._get_trie_pattern(
        os.fspath(tmp_dir / "top" / "first" / "middle" / "second" / "bottom")
    )

    base_pattern = (
        _to_pattern_info_list([".hg/", ".git/", ".git", ".dvc/"]),
        os.fspath(tmp_dir),
    )
    first_pattern = merge_patterns(
        *base_pattern,
        _to_pattern_info_list(["a", "b", "c"]),
        os.fspath(tmp_dir / "top" / "first")
    )
    second_pattern = merge_patterns(
        *first_pattern,
        _to_pattern_info_list(["d", "e", "f"]),
        os.fspath(tmp_dir / "top" / "first" / "middle" / "second")
    )
    other_pattern = merge_patterns(
        *base_pattern,
        _to_pattern_info_list(["1", "2", "3"]),
        os.fspath(tmp_dir / "other")
    )

    assert DvcIgnorePatterns(*base_pattern) == ignore_pattern_top
    assert DvcIgnorePatterns(*other_pattern) == ignore_pattern_other
    assert (
        DvcIgnorePatterns(*first_pattern)
        == ignore_pattern_first
        == ignore_pattern_middle
    )
    assert (
        DvcIgnorePatterns(*second_pattern)
        == ignore_pattern_second
        == ignore_pattern_bottom
    )


def test_ignore_in_added_dir(tmp_dir, dvc):
    tmp_dir.gen(
        {
            "dir": {
                "sub": {
                    "ignored": {"content": "ignored content"},
                    "not_ignored": "not ignored content",
                }
            },
            ".dvcignore": "**/ignored",
        }
    )
    dvc._reset()

    ignored_path = tmp_dir / "dir" / "sub" / "ignored"
    with pytest.raises(PathMissingError):
        _get_pathinfo_set(dvc, PathInfo(ignored_path))
    assert ignored_path.exists()

    dvc.add("dir")
    shutil.rmtree(ignored_path)
    dvc.checkout()

    assert not ignored_path.exists()


def test_ignored_output(tmp_dir, scm, dvc, run_copy):
    tmp_dir.gen({".dvcignore": "*.log\n!foo.log", "foo": "foo content"})

    with pytest.raises(OutputIsIgnoredError):
        run_copy("foo", "abc.log", name="copy")

    run_copy("foo", "foo.log", name="copy")


def test_ignored_output_nested(tmp_dir, scm, dvc, run_copy):
    tmp_dir.gen({".dvcignore": "/*.log", "copy": {"foo": "foo content"}})

    run_copy("foo", "foo.log", name="copy", wdir="copy")

    assert Path("copy/foo.log").exists()
