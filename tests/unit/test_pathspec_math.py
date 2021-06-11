import pytest

from dvc.pathspec_math import PatternInfo, _change_dirname


@pytest.mark.parametrize(
    "patterns, dirname, changed",
    [
        # A line starting with # serves as a comment.
        ("#comment", "/dir", "#comment"),
        # Put a backslash ("\") in front of the first hash for patterns that
        # begin with a hash.
        ("\\#hash", "/dir", "/dir/**/#hash"),
        ("\\#hash", "/#dir", "/#dir/**/#hash"),
        # Trailing spaces are ignored unless they are quoted with
        # backslash ("\").
        (" space", "/dir", "/dir/**/space"),
        ("\\ space", "/dir", "/dir/**/ space"),
        # An optional prefix "!" which negates the pattern;
        ("!include", "/dir", "!/dir/**/include"),
        # Put a backslash ("\") in front of the first "!" for patterns that
        # begin with a literal "!", for example, "\!important!.txt".
        ("\\!important!.txt", "/dir", "/dir/**/!important!.txt"),
        # If there is a separator at the beginning or middle (or both) of the
        # pattern, then the pattern is relative to the directory level of the
        # particular .gitignore file itself.
        ("/separator.txt", "/dir", "/dir/separator.txt"),
        ("subdir/separator.txt", "/dir", "/dir/subdir/separator.txt"),
        # Otherwise the pattern may also match at any level below
        # the .gitignore level.
        ("no_sep", "/dir", "/dir/**/no_sep"),
        # If there is a separator at the end of the pattern then the pattern
        # will only match directories, otherwise the pattern can match both
        # files and directories.
        ("doc/fortz/", "/dir", "/dir/doc/fortz/"),
        ("fortz/", "/dir", "/dir/**/fortz/"),
        # An asterisk "*" matches anything except a slash.
        ("*aste*risk*", "/dir", "/dir/**/*aste*risk*"),
        # The character "?" matches any one character except "/".
        ("?fi?le?", "/dir", "/dir/**/?fi?le?"),
        # The range notation, e.g. [a-zA-Z], can be used to match one of the
        # characters in a range. See fnmatch(3) and the FNM_PATHNAME flag
        # for a more detailed description.
        ("[a-zA-Z]file[a-zA-Z]", "/dir", "/dir/**/[a-zA-Z]file[a-zA-Z]"),
        # Two consecutive asterisks ("**") in patterns matched against full
        # pathname may have special meaning:
        # A leading "**" followed by a slash means match in all directories.
        # For example, "**/foo" matches file or directory "foo" anywhere,
        # the same as pattern "foo".
        ("**/foo", "/dir", "/dir/**/foo"),
        # "**/foo/bar" matches file or directory "bar" anywhere that is
        # directly under directory "foo".
        ("**/foo/bar", "/dir", "/dir/**/foo/bar"),
        # A trailing "/**" matches everything inside.
        # For example, "abc/**" matches all files inside directory "abc",
        # relative to the location of the .gitignore file, with infinite depth.
        ("abc/**", "/dir", "/dir/abc/**"),
        # A slash followed by two consecutive asterisks then a slash matches
        # zero or more directories. For example, "a/**/b"
        # matches "a/b", "a/x/b", "a/x/y/b" and so on.
        ("a/**/b", "/dir", "/dir/a/**/b"),
        # Other consecutive asterisks are considered regular asterisks and
        # will match according to the previous rules.
        ("/***.txt", "/dir", "/dir/***.txt"),
        ("data/***", "/dir", "/dir/data/***"),
        ("***/file.txt", "/dir", "/dir/***/file.txt"),
        ("***file", "/dir", "/dir/**/***file"),
        ("a/***/b", "/dir", "/dir/a/***/b"),
    ],
)
def test_dvcignore_pattern_change_dir(tmp_dir, patterns, dirname, changed):
    assert _change_dirname(dirname, [PatternInfo(patterns, "")], "/") == [
        PatternInfo(changed, "")
    ]
