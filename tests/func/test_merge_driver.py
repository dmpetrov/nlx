import os

import pytest

from dvc.main import main
from dvc.utils.fs import remove


def _gen(tmp_dir, struct, name):
    remove(tmp_dir / "data")
    if struct is None:
        (tmp_dir / name).touch()
    else:
        (stage,) = tmp_dir.dvc_gen({"data": struct})
        os.rename(stage.path, name)


@pytest.mark.parametrize(
    "ancestor, our, their, merged",
    [
        (
            {"foo": "foo"},
            {"foo": "foo", "bar": "bar"},
            {"foo": "foo", "baz": "baz"},
            {"foo": "foo", "bar": "bar", "baz": "baz"},
        ),
        (
            {"common": "common", "subdir": {"foo": "foo"}},
            {"common": "common", "subdir": {"foo": "foo", "bar": "bar"}},
            {"common": "common", "subdir": {"foo": "foo", "baz": "baz"}},
            {
                "common": "common",
                "subdir": {"foo": "foo", "bar": "bar", "baz": "baz"},
            },
        ),
        ({}, {"foo": "foo"}, {"bar": "bar"}, {"foo": "foo", "bar": "bar"}),
        ({}, {}, {"bar": "bar"}, {"bar": "bar"}),
        ({}, {"foo": "foo"}, {}, {"foo": "foo"}),
        (None, {"foo": "foo"}, {"bar": "bar"}, {"foo": "foo", "bar": "bar"}),
        (None, None, {"bar": "bar"}, {"bar": "bar"}),
        (None, {"foo": "foo"}, None, {"foo": "foo"}),
    ],
)
def test_merge(tmp_dir, dvc, ancestor, our, their, merged):
    _gen(tmp_dir, ancestor, "ancestor")
    _gen(tmp_dir, our, "our")
    _gen(tmp_dir, their, "their")

    assert (
        main(
            [
                "git-hook",
                "merge-driver",
                "--ancestor",
                "ancestor",
                "--our",
                "our",
                "--their",
                "their",
            ]
        )
        == 0
    )

    _gen(tmp_dir, merged, "merged")

    assert (tmp_dir / "our").read_text() == (tmp_dir / "merged").read_text()


@pytest.mark.parametrize(
    "ancestor, our, their, error",
    [
        (
            {"foo": "foo"},
            {"foo": "bar"},
            {"foo": "baz"},
            (
                "unable to auto-merge directories with "
                "diff that contains 'change'ed files"
            ),
        ),
        (
            {"common": "common", "foo": "foo"},
            {"common": "common", "bar": "bar"},
            {"baz": "baz"},
            (
                "unable to auto-merge directories with "
                "diff that contains 'remove'ed files"
            ),
        ),
    ],
)
def test_merge_conflict(tmp_dir, dvc, ancestor, our, their, error, caplog):
    _gen(tmp_dir, ancestor, "ancestor")
    _gen(tmp_dir, our, "our")
    _gen(tmp_dir, their, "their")

    assert (
        main(
            [
                "git-hook",
                "merge-driver",
                "--ancestor",
                "ancestor",
                "--our",
                "our",
                "--their",
                "their",
            ]
        )
        != 0
    )

    assert error in caplog.text


@pytest.mark.parametrize(
    "workspace", [pytest.lazy_fixture("ssh")], indirect=True
)
def test_merge_different_output_types(tmp_dir, dvc, caplog, workspace):
    (tmp_dir / "ancestor").touch()

    (tmp_dir / "our").write_text(
        "outs:\n- md5: f123456789.dir\n  path: ssh://example.com/path\n"
    )

    (tmp_dir / "their").write_text(
        "outs:\n- md5: f987654321.dir\n  path: path\n"
    )

    assert (
        main(
            [
                "git-hook",
                "merge-driver",
                "--ancestor",
                "ancestor",
                "--our",
                "our",
                "--their",
                "their",
            ]
        )
        != 0
    )

    error = "unable to auto-merge outputs of different types"
    assert error in caplog.text


def test_merge_different_output_options(tmp_dir, dvc, caplog):
    (tmp_dir / "ancestor").touch()

    (tmp_dir / "our").write_text(
        "outs:\n- md5: f123456789.dir\n  path: path\n"
    )

    (tmp_dir / "their").write_text(
        "outs:\n- md5: f987654321.dir\n  path: path\n  cache: false\n"
    )

    assert (
        main(
            [
                "git-hook",
                "merge-driver",
                "--ancestor",
                "ancestor",
                "--our",
                "our",
                "--their",
                "their",
            ]
        )
        != 0
    )

    error = "unable to auto-merge outputs with different options"
    assert error in caplog.text


def test_merge_file(tmp_dir, dvc, caplog):
    (tmp_dir / "ancestor").touch()

    (tmp_dir / "our").write_text(
        "outs:\n- md5: f123456789.dir\n  path: path\n"
    )

    (tmp_dir / "their").write_text("outs:\n- md5: f987654321\n  path: path\n")

    assert (
        main(
            [
                "git-hook",
                "merge-driver",
                "--ancestor",
                "ancestor",
                "--our",
                "our",
                "--their",
                "their",
            ]
        )
        != 0
    )

    err = "unable to auto-merge outputs that are not directories"
    assert err in caplog.text


def test_merge_non_dvc_add(tmp_dir, dvc, caplog):
    (tmp_dir / "ancestor").touch()

    (tmp_dir / "our").write_text(
        "outs:\n"
        "- md5: f123456789.dir\n"
        "  path: path\n"
        "- md5: ff123456789.dir\n"
        "  path: another\n"
    )

    (tmp_dir / "their").write_text("outs:\n- md5: f987654321\n  path: path\n")

    assert (
        main(
            [
                "git-hook",
                "merge-driver",
                "--ancestor",
                "ancestor",
                "--our",
                "our",
                "--their",
                "their",
            ]
        )
        != 0
    )

    error = "unable to auto-merge DVC files that weren't created by `dvc add`"
    assert error in caplog.text
