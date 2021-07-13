import os
from unittest.mock import MagicMock, mock_open, patch

import pytest

from dvc.ignore import DvcIgnorePatterns


def mock_dvcignore(dvcignore_path, patterns):
    fs = MagicMock()
    with patch.object(fs, "open", mock_open(read_data="\n".join(patterns))):
        ignore_patterns = DvcIgnorePatterns.from_file(
            dvcignore_path, fs, "mocked"
        )

    return ignore_patterns


@pytest.mark.parametrize(
    "file_to_ignore_relpath, patterns,  expected_match",
    [
        # all rules from https://git-scm.com/docs/gitignore
        ("to_ignore", ["to_ignore"], True),
        ("dont_ignore.txt", ["dont_ignore"], False),
        # A blank line matches no files, so it can serve as a separator for
        # readability.
        ("to_ignore", ["", "to_ignore"], True),
        # A line starting with # serves as a comment.
        # Put a backslash ("\") in front of the first hash for patterns
        # that begin with a hash.
        ("#to_ignore", ["\\#to_ignore"], True),
        ("#to_ignore", ["#to_ignore"], False),
        # Trailing spaces are ignored unless they are quoted with
        # backslash ("\").
        (" to_ignore", [" to_ignore"], False),
        (" to_ignore", ["\\ to_ignore"], True),
        # An optional prefix "!" which negates the pattern; any matching file
        # excluded by a previous pattern will become included again.
        ("to_ignore.txt", ["to_ignore*"], True),
        ("to_ignore.txt", ["to_ignore*", "!to_ignore.txt"], False),
        ("to_ignore.txt", ["!to_ignore.txt", "to_ignore*"], True),
        # It is not possible to re-include a file if a parent directory of
        # that file is excluded.
        # Git doesn’t list excluded directories for performance reasons,
        # so any patterns on contained files have no effect,
        # no matter where they are defined.
        # see (`tests/func/test_ignore.py::test_ignore_parent_path`)
        # Put a backslash ("\") in front of the first "!"
        # for patterns that begin with a literal "!",
        # for example, "\!important!.txt".
        ("!to_ignore.txt", ["\\!to_ignore.txt"], True),
        # The slash / is used as the directory separator.
        # Separators may occur at the beginning, middle or end of the
        # .gitignore search pattern.
        # If there is a separator at the beginning or middle (or both)
        # of the pattern, then the pattern is relative to the directory
        # level of the particular .gitignore file itself.
        # Otherwise the pattern may also match at any level below
        # the .gitignore level.
        ("file", ["/file"], True),
        (os.path.join("data", "file"), ["/file"], False),
        (os.path.join("data", "file"), ["data/file"], True),
        (os.path.join("other", "data", "file"), ["data/file"], False),
        (
            os.path.join(
                os.path.sep,
                "full",
                "path",
                "to",
                "ignore",
                "file",
                "to_ignore",
            ),
            ["to_ignore"],
            True,
        ),
        # If there is a separator at the end of the pattern then the pattern
        # will only match directories,
        # otherwise the pattern can match both files and directories.
        # For example, a pattern doc/frotz/ matches doc/frotz directory,
        # but not a/doc/frotz directory;
        # see (`tests/func/test_ignore.py::test_ignore_sub_directory`)
        # however frotz/ matches frotz and a/frotz that is a directory
        # (all paths are relative from the .gitignore file).
        # see (`tests/func/test_ignore.py::test_ignore_directory`)
        # An asterisk "*" matches anything except a slash.
        ("to_ignore.txt", ["/*.txt"], True),
        (os.path.join("path", "to_ignore.txt"), ["/*.txt"], False),
        (os.path.join("data", "file.txt"), ["data/*"], True),
        (os.path.join("data", "subdir", "file.txt"), ["data/*"], False),
        (os.path.join("data", "file.txt"), ["data/"], True),
        (os.path.join("data", "subdir", "file.txt"), ["data/"], True),
        (os.path.join("data", "subdir", "file.txt"), ["subdir/"], True),
        (os.path.join("data", "subdir", "file.txt"), ["/subdir/"], False),
        (os.path.join("data", "path"), ["path/"], False),
        (os.path.join(".git", "file.txt"), [".git/"], True),
        (os.path.join("data", ".dvc", "file.txt"), [".dvc/"], True),
        # wait for Git
        # (os.path.join("data", "sub", "file.txt"), ["data/*"], True),
        (
            os.path.join("rel", "path", "path2", "to_ignore"),
            ["rel/*/to_ignore"],
            False,
        ),
        ("file.txt", ["file.*"], True),
        # The character "?" matches any one character except "/".
        ("file.txt", ["fi?e.t?t"], True),
        ("fi/e.txt", ["fi?e.t?t"], False),
        # The range notation, e.g. [a-zA-Z], can be used
        # to match one of the characters in a range. See fnmatch(3) and
        # the FNM_PATHNAME flag for a more detailed description.
        ("file.txt", ["[a-zA-Z]ile.txt"], True),
        ("2ile.txt", ["[a-zA-Z]ile.txt"], False),
        # Two consecutive asterisks ("**") in patterns matched against
        # full pathname may have special meaning:
        # A leading "**" followed by a slash means match in all directories.
        # For example, "**/foo" matches file or directory "foo" anywhere, the
        # same as pattern "foo".
        # "**/foo/bar" matches file or directory "bar" anywhere that is
        # directly under directory "foo".
        (os.path.join("rel", "p", "p2", "to_ignore"), ["**/to_ignore"], True),
        (
            os.path.join("rel", "p", "p2", "to_ignore"),
            ["**/p2/to_ignore"],
            True,
        ),
        (
            os.path.join("rel", "path", "path2", "dont_ignore"),
            ["**/to_ignore"],
            False,
        ),
        # A trailing "/**" matches everything inside.
        # For example, "abc/**" matches all files inside directory "abc",
        # relative to the location of the .gitignore file, with infinite depth.
        (os.path.join("rel", "p", "p2", "to_ignore"), ["rel/**"], True),
        (os.path.join("rel", "p", "p2", "to_ignore"), ["p/**"], False),
        (
            os.path.join("rel", "path", "path2", "dont_ignore"),
            ["rel/**"],
            True,
        ),
        # A slash followed by two consecutive asterisks then a slash matches
        # zero or more directories.
        # For example, "a/**/b" matches "a/b", "a/x/b", "a/x/y/b" and so on.
        (os.path.join("rel", "p", "to_ignore"), ["rel/**/to_ignore"], True),
        (
            os.path.join("rel", "p", "p2", "to_ignore"),
            ["rel/**/to_ignore"],
            True,
        ),
        (
            os.path.join("rel", "path", "path2", "dont_ignore"),
            ["rel/**/to_ignore"],
            False,
        ),
        (
            os.path.join("rel", "path", "path2", "dont_ignore"),
            ["path/**/dont_ignore"],
            False,
        ),
        # Other consecutive asterisks are considered regular asterisks
        # and will match according to the previous rules.
        ("to_ignore.txt", ["/***.txt"], True),
        (os.path.join("path", "to_ignore.txt"), ["/****.txt"], False),
        (os.path.join("path", "to_ignore.txt"), ["****.txt"], True),
        (os.path.join("data", "file.txt"), ["data/***"], True),
        # bug from PathSpec
        # (os.path.join("data", "p", "file.txt"), ["data/***"], False),
        (os.path.join("data", "p", "file.txt"), ["***/file.txt"], False),
        (
            os.path.join("rel", "path", "path2", "to_ignore"),
            ["rel/***/to_ignore"],
            False,
        ),
    ],
)
def test_match_ignore_from_file(
    file_to_ignore_relpath, patterns, expected_match
):

    dvcignore_path = os.path.join(
        os.path.sep, "full", "path", "to", "ignore", "file", ".dvcignore"
    )
    dvcignore_dirname = os.path.dirname(dvcignore_path)

    ignore_file = mock_dvcignore(dvcignore_path, patterns)

    assert (
        ignore_file.matches(dvcignore_dirname, file_to_ignore_relpath)
        == expected_match
    )


@pytest.mark.parametrize("sub_dir", ["", "dir"])
@pytest.mark.parametrize("omit_dir", [".git", ".hg", ".dvc"])
def test_should_ignore_dir(omit_dir, sub_dir):
    root = os.path.join(os.path.sep, "walk", "dir", "root")
    ignore = DvcIgnorePatterns([".git/", ".hg/", ".dvc/"], root)

    dirs = [omit_dir, "dir1", "dir2"]
    files = [omit_dir, "file1", "file2"]

    if sub_dir:
        current = os.path.join(root, sub_dir)
    else:
        current = root

    new_dirs, new_files = ignore(current, dirs, files)

    assert set(new_dirs) == {"dir1", "dir2"}
    assert set(new_files) == {"file1", "file2", omit_dir}
