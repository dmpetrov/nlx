import hashlib
import os

import pytest
from funcy import first

from dvc.exceptions import DvcException
from dvc.utils.fs import remove


def digest(text):
    return hashlib.md5(bytes(text, "utf-8")).hexdigest()


def test_no_scm(tmp_dir, dvc):
    from dvc.scm.base import NoSCMError

    tmp_dir.dvc_gen("file", "text")

    with pytest.raises(NoSCMError):
        dvc.diff()


def test_added(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen("file", "text")

    assert dvc.diff() == {
        "added": [{"path": "file", "hash": digest("text")}],
        "deleted": [],
        "modified": [],
        "not in cache": [],
        "renamed": [],
    }


def test_no_cache_entry(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen("file", "first", commit="add a file")

    tmp_dir.dvc_gen({"dir": {"1": "1", "2": "2"}})
    tmp_dir.dvc_gen("file", "second")

    remove(tmp_dir / ".dvc" / "cache")

    dir_checksum = "5fb6b29836c388e093ca0715c872fe2a.dir"

    assert dvc.diff() == {
        "added": [
            {"path": os.path.join("dir", ""), "hash": dir_checksum},
            {"path": os.path.join("dir", "1"), "hash": digest("1")},
            {"path": os.path.join("dir", "2"), "hash": digest("2")},
        ],
        "deleted": [],
        "modified": [
            {
                "path": "file",
                "hash": {"old": digest("first"), "new": digest("second")},
            }
        ],
        "not in cache": [],
        "renamed": [],
    }


@pytest.mark.parametrize("delete_data", [True, False])
def test_deleted(tmp_dir, scm, dvc, delete_data):
    tmp_dir.dvc_gen("file", "text", commit="add file")
    (tmp_dir / "file.dvc").unlink()
    if delete_data:
        (tmp_dir / "file").unlink()

    assert dvc.diff() == {
        "added": [],
        "deleted": [{"path": "file", "hash": digest("text")}],
        "modified": [],
        "not in cache": [],
        "renamed": [],
    }


def test_modified(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen("file", "first", commit="first version")
    tmp_dir.dvc_gen("file", "second")

    assert dvc.diff() == {
        "added": [],
        "deleted": [],
        "modified": [
            {
                "path": "file",
                "hash": {"old": digest("first"), "new": digest("second")},
            }
        ],
        "not in cache": [],
        "renamed": [],
    }


def test_refs(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen("file", "first", commit="first version")
    tmp_dir.dvc_gen("file", "second", commit="second version")
    tmp_dir.dvc_gen("file", "third", commit="third version")

    HEAD_2 = digest("first")
    HEAD_1 = digest("second")
    HEAD = digest("third")

    assert dvc.diff("HEAD~1") == {
        "added": [],
        "deleted": [],
        "modified": [{"path": "file", "hash": {"old": HEAD_1, "new": HEAD}}],
        "not in cache": [],
        "renamed": [],
    }

    assert dvc.diff("HEAD~2", "HEAD~1") == {
        "added": [],
        "deleted": [],
        "modified": [{"path": "file", "hash": {"old": HEAD_2, "new": HEAD_1}}],
        "not in cache": [],
        "renamed": [],
    }

    with pytest.raises(DvcException, match=r"unknown Git revision 'missing'"):
        dvc.diff("missing")


def test_directories(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen({"dir": {"1": "1", "2": "2"}}, commit="add a directory")
    tmp_dir.dvc_gen({"dir": {"3": "3"}}, commit="add a file")
    tmp_dir.dvc_gen({"dir": {"2": "two"}}, commit="modify a file")

    (tmp_dir / "dir" / "2").unlink()
    assert dvc.status() != {}  # sanity check
    dvc.add("dir")
    scm.add(["dir.dvc"])
    scm.commit("delete a file")

    # The ":/<text>" format is a way to specify revisions by commit message:
    #       https://git-scm.com/docs/revisions
    #
    assert dvc.diff(":/init", ":/directory") == {
        "added": [
            {
                "path": os.path.join("dir", ""),
                "hash": "5fb6b29836c388e093ca0715c872fe2a.dir",
            },
            {"path": os.path.join("dir", "1"), "hash": digest("1")},
            {"path": os.path.join("dir", "2"), "hash": digest("2")},
        ],
        "deleted": [],
        "modified": [],
        "not in cache": [],
        "renamed": [],
    }

    assert dvc.diff(":/directory", ":/modify") == {
        "added": [{"path": os.path.join("dir", "3"), "hash": digest("3")}],
        "deleted": [],
        "modified": [
            {
                "path": os.path.join("dir", ""),
                "hash": {
                    "old": "5fb6b29836c388e093ca0715c872fe2a.dir",
                    "new": "9b5faf37366b3370fd98e3e60ca439c1.dir",
                },
            },
            {
                "path": os.path.join("dir", "2"),
                "hash": {"old": digest("2"), "new": digest("two")},
            },
        ],
        "not in cache": [],
        "renamed": [],
    }

    assert dvc.diff(":/modify", ":/delete") == {
        "added": [],
        "deleted": [{"path": os.path.join("dir", "2"), "hash": digest("two")}],
        "modified": [
            {
                "path": os.path.join("dir", ""),
                "hash": {
                    "old": "9b5faf37366b3370fd98e3e60ca439c1.dir",
                    "new": "83ae82fb367ac9926455870773ff09e6.dir",
                },
            }
        ],
        "not in cache": [],
        "renamed": [],
    }


def test_diff_no_cache(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen({"dir": {"file": "file content"}}, commit="first")
    scm.tag("v1")

    tmp_dir.dvc_gen(
        {"dir": {"file": "modified file content"}}, commit="second"
    )
    scm.tag("v2")

    remove(dvc.odb.local.cache_dir)

    # invalidate_dir_info to force cache loading
    dvc.odb.local._dir_info = {}

    diff = dvc.diff("v1", "v2")
    assert diff["added"] == []
    assert diff["deleted"] == []
    assert first(diff["modified"])["path"] == os.path.join("dir", "")
    assert diff["not in cache"] == []

    (tmp_dir / "dir" / "file").unlink()
    remove(str(tmp_dir / "dir"))
    diff = dvc.diff()
    assert diff["added"] == []
    assert diff["deleted"] == []
    assert diff["renamed"] == []
    assert diff["modified"] == []
    assert diff["not in cache"] == [
        {
            "path": os.path.join("dir", ""),
            "hash": "f0f7a307d223921557c929f944bf5303.dir",
        }
    ]


def test_diff_dirty(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen(
        {"file": "file_content", "dir": {"dir_file1": "dir file content"}},
        commit="initial",
    )

    (tmp_dir / "file").unlink()
    tmp_dir.gen({"dir": {"dir_file2": "dir file 2 content"}})
    tmp_dir.dvc_gen("new_file", "new_file_content")

    result = dvc.diff()

    assert result == {
        "added": [
            {
                "hash": digest("dir file 2 content"),
                "path": os.path.join("dir", "dir_file2"),
            },
            {"hash": "86d049de17c76ac44cdcac146042ec9b", "path": "new_file"},
        ],
        "deleted": [
            {"hash": "7f0b6bb0b7e951b7fd2b2a4a326297e1", "path": "file"}
        ],
        "modified": [
            {
                "hash": {
                    "new": "38175ad60f0e58ac94e0e2b7688afd81.dir",
                    "old": "92daf39af116ca2fb245acaeb2ae65f7.dir",
                },
                "path": os.path.join("dir", ""),
            }
        ],
        "not in cache": [],
        "renamed": [],
    }


def test_no_changes(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen("file", "first", commit="add a file")
    assert dvc.diff() == {}


def test_no_commits(tmp_dir):
    from dvc.repo import Repo
    from dvc.scm.git import Git
    from tests.dir_helpers import git_init

    git_init(".")
    assert Git().no_commits

    assert Repo.init().diff() == {}


def setup_targets_test(tmp_dir):
    tmp_dir.dvc_gen("file", "first", commit="add a file")

    tmp_dir.dvc_gen({"dir": {"1": "1", "2": "2"}})
    tmp_dir.dvc_gen("file", "second")

    tmp_dir.dvc_gen(os.path.join("dir_with", "file.txt"), "first")


def test_targets_missing_path(tmp_dir, scm, dvc):
    from dvc.exceptions import PathMissingError

    setup_targets_test(tmp_dir)

    with pytest.raises(PathMissingError):
        dvc.diff(targets=["missing"])


def test_targets_single_file(tmp_dir, scm, dvc):
    setup_targets_test(tmp_dir)

    assert dvc.diff(targets=["file"]) == {
        "added": [],
        "deleted": [],
        "modified": [
            {
                "path": "file",
                "hash": {"old": digest("first"), "new": digest("second")},
            }
        ],
        "not in cache": [],
        "renamed": [],
    }


def test_targets_single_dir(tmp_dir, scm, dvc):
    setup_targets_test(tmp_dir)

    dir_checksum = "5fb6b29836c388e093ca0715c872fe2a.dir"

    expected_result = {
        "added": [
            {"path": os.path.join("dir", ""), "hash": dir_checksum},
            {"path": os.path.join("dir", "1"), "hash": digest("1")},
            {"path": os.path.join("dir", "2"), "hash": digest("2")},
        ],
        "deleted": [],
        "modified": [],
        "not in cache": [],
        "renamed": [],
    }

    assert dvc.diff(targets=["dir"]) == expected_result
    assert dvc.diff(targets=["dir" + os.path.sep]) == expected_result


def test_targets_single_file_in_dir(tmp_dir, scm, dvc):
    setup_targets_test(tmp_dir)

    assert dvc.diff(targets=[os.path.join("dir", "1")]) == {
        "added": [{"path": os.path.join("dir", "1"), "hash": digest("1")}],
        "deleted": [],
        "modified": [],
        "not in cache": [],
        "renamed": [],
    }


def test_targets_two_files_in_dir(tmp_dir, scm, dvc):
    setup_targets_test(tmp_dir)

    assert dvc.diff(
        targets=[os.path.join("dir", "1"), os.path.join("dir", "2")]
    ) == {
        "added": [
            {"path": os.path.join("dir", "1"), "hash": digest("1")},
            {"path": os.path.join("dir", "2"), "hash": digest("2")},
        ],
        "deleted": [],
        "modified": [],
        "not in cache": [],
        "renamed": [],
    }


def test_targets_file_and_dir(tmp_dir, scm, dvc):
    setup_targets_test(tmp_dir)

    dir_checksum = "5fb6b29836c388e093ca0715c872fe2a.dir"

    assert dvc.diff(targets=["file", "dir"]) == {
        "added": [
            {"path": os.path.join("dir", ""), "hash": dir_checksum},
            {"path": os.path.join("dir", "1"), "hash": digest("1")},
            {"path": os.path.join("dir", "2"), "hash": digest("2")},
        ],
        "deleted": [],
        "modified": [
            {
                "path": "file",
                "hash": {"old": digest("first"), "new": digest("second")},
            }
        ],
        "not in cache": [],
        "renamed": [],
    }


def test_targets_single_dir_with_file(tmp_dir, scm, dvc):
    setup_targets_test(tmp_dir)

    expected_result = {
        "added": [
            {
                "path": os.path.join("dir_with", "file.txt"),
                "hash": digest("first"),
            }
        ],
        "deleted": [],
        "modified": [],
        "not in cache": [],
        "renamed": [],
    }

    assert dvc.diff(targets=["dir_with"]) == expected_result
    assert dvc.diff(targets=["dir_with" + os.path.sep]) == expected_result


def test_targets_single_file_in_dir_with_file(tmp_dir, scm, dvc):
    setup_targets_test(tmp_dir)

    assert dvc.diff(targets=[os.path.join("dir_with", "file.txt")]) == {
        "added": [
            {
                "path": os.path.join("dir_with", "file.txt"),
                "hash": digest("first"),
            }
        ],
        "deleted": [],
        "modified": [],
        "not in cache": [],
        "renamed": [],
    }


@pytest.mark.parametrize("commit_last", [True, False])
def test_diff_add_similar_files(tmp_dir, scm, dvc, commit_last):
    if commit_last:
        last_commit_msg = "commit #2"
        a_rev = "HEAD~1"
    else:
        last_commit_msg = None
        a_rev = "HEAD"

    tmp_dir.dvc_gen(
        {"dir": {"file": "text1", "subdir": {"file2": "text2"}}},
        commit="commit #1",
    )
    tmp_dir.dvc_gen(
        {"dir2": {"file": "text1", "subdir": {"file2": "text2"}}},
        commit=last_commit_msg,
    )
    assert dvc.diff(a_rev) == {
        "added": [
            {
                "path": os.path.join("dir2", ""),
                "hash": "cb58ee07cb01044db229e4d6121a0dfc.dir",
            },
            {
                "path": os.path.join("dir2", "file"),
                "hash": "cef7ccd89dacf1ced6f5ec91d759953f",
            },
            {
                "path": os.path.join("dir2", "subdir", "file2"),
                "hash": "fe6123a759017e4a2af4a2d19961ed71",
            },
        ],
        "deleted": [],
        "modified": [],
        "renamed": [],
        "not in cache": [],
    }


@pytest.mark.parametrize("commit_last", [True, False])
def test_diff_rename_folder(tmp_dir, scm, dvc, commit_last):
    if commit_last:
        last_commit_msg = "commit #2"
        a_rev = "HEAD~1"
    else:
        last_commit_msg = None
        a_rev = "HEAD"

    tmp_dir.dvc_gen(
        {"dir": {"file": "text1", "subdir": {"file2": "text2"}}},
        commit="commit #1",
    )
    (tmp_dir / "dir").replace(tmp_dir / "dir2")
    tmp_dir.dvc_add("dir2", commit=last_commit_msg)
    assert dvc.diff(a_rev) == {
        "added": [],
        "deleted": [],
        "modified": [],
        "renamed": [
            {
                "path": {
                    "old": os.path.join("dir", ""),
                    "new": os.path.join("dir2", ""),
                },
                "hash": "cb58ee07cb01044db229e4d6121a0dfc.dir",
            },
            {
                "path": {
                    "old": os.path.join("dir", "file"),
                    "new": os.path.join("dir2", "file"),
                },
                "hash": "cef7ccd89dacf1ced6f5ec91d759953f",
            },
            {
                "path": {
                    "old": os.path.join("dir", "subdir", "file2"),
                    "new": os.path.join("dir2", "subdir", "file2"),
                },
                "hash": "fe6123a759017e4a2af4a2d19961ed71",
            },
        ],
        "not in cache": [],
    }


@pytest.mark.parametrize("commit_last", [True, False])
def test_diff_rename_file(tmp_dir, scm, dvc, commit_last):
    if commit_last:
        last_commit_msg = "commit #2"
        a_rev = "HEAD~1"
    else:
        last_commit_msg = None
        a_rev = "HEAD"

    paths = tmp_dir.gen(
        {"dir": {"file": "text1", "subdir": {"file2": "text2"}}}
    )
    tmp_dir.dvc_add(paths, commit="commit #1")
    (tmp_dir / "dir" / "file").replace(tmp_dir / "dir" / "subdir" / "file3")

    tmp_dir.dvc_add(paths, commit=last_commit_msg)
    assert dvc.diff(a_rev) == {
        "added": [],
        "deleted": [],
        "modified": [
            {
                "path": os.path.join("dir", ""),
                "hash": {
                    "old": "cb58ee07cb01044db229e4d6121a0dfc.dir",
                    "new": "a4ac9c339aacc60b6a3152e362c319c8.dir",
                },
            }
        ],
        "renamed": [
            {
                "path": {
                    "old": os.path.join("dir", "file"),
                    "new": os.path.join("dir", "subdir", "file3"),
                },
                "hash": "cef7ccd89dacf1ced6f5ec91d759953f",
            }
        ],
        "not in cache": [],
    }


def test_rename_multiple_files_same_hashes(tmp_dir, scm, dvc):
    """Test diff by renaming >=2 instances of file with same hashes.

    DVC should be able to detect that they are renames, and should not include
    them in either of the `added` or the `deleted` section.
    """
    tmp_dir.dvc_gen(
        {"dir": {"foo": "foo", "subdir": {"foo": "foo"}}}, commit="commit #1"
    )
    remove(tmp_dir / "dir")
    # changing foo and subdir/foo to bar and subdir/bar respectively
    tmp_dir.dvc_gen(
        {"dir": {"bar": "foo", "subdir": {"bar": "foo"}}}, commit="commit #2"
    )
    assert dvc.diff("HEAD~") == {
        "added": [],
        "deleted": [],
        "modified": [
            {
                "hash": {
                    "new": "31b36b3ea5f4485e27f10578c47183b0.dir",
                    "old": "c7684c8b3b0d28cf80d5305e2d856bfc.dir",
                },
                "path": os.path.join("dir", ""),
            }
        ],
        "not in cache": [],
        "renamed": [
            {
                "hash": "acbd18db4cc2f85cedef654fccc4a4d8",
                "path": {
                    "new": os.path.join("dir", "bar"),
                    "old": os.path.join("dir", "foo"),
                },
            },
            {
                "hash": "acbd18db4cc2f85cedef654fccc4a4d8",
                "path": {
                    "new": os.path.join("dir", "subdir", "bar"),
                    "old": os.path.join("dir", "subdir", "foo"),
                },
            },
        ],
    }
