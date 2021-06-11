import collections
import logging
import os

import mock
import pytest

from dvc.cli import parse_args
from dvc.command.diff import _digest, _show_md


@pytest.mark.parametrize(
    "checksum, expected",
    [
        ("wxyz1234pq", "wxyz1234"),
        ({"old": "1234567890", "new": "0987654321"}, "12345678..09876543"),
    ],
    ids=["str", "dict"],
)
def test_digest(checksum, expected):
    assert expected == _digest(checksum)


def test_default(mocker, caplog):
    args = parse_args(["diff"])
    cmd = args.func(args)
    diff = {
        "added": [{"path": "file", "hash": "00000000"}],
        "deleted": [],
        "modified": [],
        "renamed": [
            {
                "path": {
                    "old": os.path.join("data", "file_old"),
                    "new": os.path.join("data", "file_new"),
                },
                "hash": "11111111",
            }
        ],
        "not in cache": [],
    }
    mocker.patch("dvc.repo.Repo.diff", return_value=diff)

    assert 0 == cmd.run()
    assert (
        "Added:\n"
        "    file\n"
        "\n"
        "Renamed:\n"
        "    data{sep}file_old -> data{sep}file_new\n"
        "\n"
        "files summary: 1 added, 0 deleted, "
        "1 renamed, 0 modified, 0 not in cache"
    ).format(sep=os.path.sep) in caplog.text


def test_show_hash(mocker, caplog):
    args = parse_args(["diff", "--show-hash"])
    cmd = args.func(args)
    diff = {
        "added": [],
        "deleted": [
            {"path": os.path.join("data", ""), "hash": "XXXXXXXX.dir"},
            {"path": os.path.join("data", "foo"), "hash": "11111111"},
            {"path": os.path.join("data", "bar"), "hash": "00000000"},
        ],
        "modified": [
            {"path": "file2", "hash": {"old": "AAAAAAAA", "new": "BBBBBBBB"}},
            {"path": "file1", "hash": {"old": "CCCCCCCC", "new": "DDDDDDDD"}},
        ],
        "renamed": [
            {
                "path": {
                    "old": os.path.join("data", "file_old"),
                    "new": os.path.join("data", "file_new"),
                },
                "hash": "11111111",
            }
        ],
        "not in cache": [],
    }
    mocker.patch("dvc.repo.Repo.diff", return_value=diff)
    assert 0 == cmd.run()
    assert (
        "Deleted:\n"
        "    XXXXXXXX  " + os.path.join("data", "") + "\n"
        "    00000000  " + os.path.join("data", "bar") + "\n"
        "    11111111  " + os.path.join("data", "foo") + "\n"
        "\n"
        "Renamed:\n"
        "    11111111  "
        + os.path.join("data", "file_old")
        + " -> "
        + os.path.join("data", "file_new")
        + "\n"
        "\n"
        "Modified:\n"
        "    CCCCCCCC..DDDDDDDD  file1\n"
        "    AAAAAAAA..BBBBBBBB  file2\n"
        "\n"
        "files summary: 0 added, 2 deleted, "
        "1 renamed, 2 modified, 0 not in cache"
    ) in caplog.text


def test_show_json(mocker, caplog):
    args = parse_args(["diff", "--show-json"])
    cmd = args.func(args)
    diff = {
        "added": [
            {"path": "file2", "hash": "22222222"},
            {"path": "file1", "hash": "11111111"},
        ],
        "deleted": [],
        "modified": [],
        "not in cache": [],
    }
    mocker.patch("dvc.repo.Repo.diff", return_value=diff)

    assert 0 == cmd.run()
    assert '"added": [{"path": "file1"}, {"path": "file2"}]' in caplog.text
    assert '"deleted": []' in caplog.text
    assert '"modified": []' in caplog.text
    assert '"not in cache": []' in caplog.text


def test_show_json_and_hash(mocker, caplog):
    args = parse_args(["diff", "--show-json", "--show-hash"])
    cmd = args.func(args)

    diff = {
        "added": [
            # py35: maintain a consistent key order for tests purposes
            collections.OrderedDict([("path", "file2"), ("hash", "22222222")]),
            collections.OrderedDict([("path", "file1"), ("hash", "11111111")]),
        ],
        "deleted": [],
        "modified": [],
        "renamed": [
            {
                "path": {"old": "file_old", "new": "file_new"},
                "hash": "11111111",
            }
        ],
        "not in cache": [],
    }
    mocker.patch("dvc.repo.Repo.diff", return_value=diff)

    assert 0 == cmd.run()
    assert (
        '"added": [{"path": "file1", "hash": "11111111"}, '
        '{"path": "file2", "hash": "22222222"}]' in caplog.text
    )
    assert '"deleted": []' in caplog.text
    assert '"modified": []' in caplog.text
    assert (
        '"renamed": [{"path": {"old": "file_old", '
        '"new": "file_new"}, "hash": "11111111"}]' in caplog.text
    )
    assert '"not in cache": []' in caplog.text


def test_show_json_hide_missing(mocker, caplog):
    args = parse_args(["diff", "--show-json", "--hide-missing"])
    cmd = args.func(args)
    diff = {
        "added": [
            {"path": "file2", "hash": "22222222"},
            {"path": "file1", "hash": "11111111"},
        ],
        "deleted": [],
        "modified": [],
        "renamed": [
            {
                "path": {"old": "file_old", "new": "file_new"},
                "hash": "11111111",
            }
        ],
        "not in cache": [],
    }
    mocker.patch("dvc.repo.Repo.diff", return_value=diff)

    assert 0 == cmd.run()
    assert '"added": [{"path": "file1"}, {"path": "file2"}]' in caplog.text
    assert '"deleted": []' in caplog.text
    assert (
        '"renamed": [{"path": {"old": "file_old", "new": "file_new"}'
        in caplog.text
    )
    assert '"modified": []' in caplog.text
    assert '"not in cache": []' not in caplog.text


@pytest.mark.parametrize("show_hash", [None, True, False])
@mock.patch("dvc.command.diff._show_md")
def test_diff_show_md_and_hash(mock_show_md, mocker, caplog, show_hash):
    options = ["diff", "--show-md"] + (["--show-hash"] if show_hash else [])
    args = parse_args(options)
    cmd = args.func(args)

    diff = {}
    show_hash = show_hash if show_hash else False
    mocker.patch("dvc.repo.Repo.diff", return_value=diff.copy())

    assert 0 == cmd.run()
    mock_show_md.assert_called_once_with(diff, show_hash, False)


def test_no_changes(mocker, caplog):
    args = parse_args(["diff", "--show-json"])
    cmd = args.func(args)
    mocker.patch("dvc.repo.Repo.diff", return_value={})

    def info():
        return [
            msg
            for name, level, msg in caplog.record_tuples
            if name.startswith("dvc") and level == logging.INFO
        ]

    assert 0 == cmd.run()
    assert ["{}"] == info()

    caplog.clear()

    args = parse_args(["diff"])
    cmd = args.func(args)
    assert 0 == cmd.run()
    assert not info()


def test_show_md(capsys):
    diff = {
        "deleted": [
            {"path": "zoo"},
            {"path": os.path.join("data", "")},
            {"path": os.path.join("data", "foo")},
            {"path": os.path.join("data", "bar")},
        ],
        "modified": [{"path": "file"}],
        "added": [{"path": "file"}],
        "renamed": [{"path": {"old": "file_old", "new": "file_new"}}],
        "not in cache": [{"path": "file2"}],
    }

    _show_md(diff)
    out, _ = capsys.readouterr()
    assert out == (
        "| Status       | Path                 |\n"
        "|--------------|----------------------|\n"
        "| added        | file                 |\n"
        "| deleted      | zoo                  |\n"
        "| deleted      | data{sep}                |\n"
        "| deleted      | data{sep}foo             |\n"
        "| deleted      | data{sep}bar             |\n"
        "| renamed      | file_old -> file_new |\n"
        "| modified     | file                 |\n"
        "| not in cache | file2                |\n"
        "\n"
    ).format(sep=os.path.sep)


def test_show_md_with_hash(capsys):
    diff = {
        "deleted": [
            {"path": "zoo", "hash": "22222"},
            {"path": os.path.join("data", ""), "hash": "XXXXXXXX.dir"},
            {"path": os.path.join("data", "foo"), "hash": "11111111"},
            {"path": os.path.join("data", "bar"), "hash": "00000000"},
        ],
        "modified": [
            {"path": "file", "hash": {"old": "AAAAAAAA", "new": "BBBBBBBB"}}
        ],
        "added": [{"path": "file", "hash": "00000000"}],
        "renamed": [
            {
                "path": {"old": "file_old", "new": "file_new"},
                "hash": "11111111",
            }
        ],
        "not in cache": [{"path": "file2", "hash": "12345678"}],
    }

    _show_md(diff, show_hash=True)

    out, _ = capsys.readouterr()
    assert out == (
        "| Status       | Hash               | Path                 |\n"
        "|--------------|--------------------|----------------------|\n"
        "| added        | 00000000           | file                 |\n"
        "| deleted      | 22222              | zoo                  |\n"
        "| deleted      | XXXXXXXX           | data{sep}                |\n"
        "| deleted      | 11111111           | data{sep}foo             |\n"
        "| deleted      | 00000000           | data{sep}bar             |\n"
        "| renamed      | 11111111           | file_old -> file_new |\n"
        "| modified     | AAAAAAAA..BBBBBBBB | file                 |\n"
        "| not in cache | 12345678           | file2                |\n"
        "\n"
    ).format(sep=os.path.sep)


def test_show_md_hide_missing(capsys):
    diff = {
        "deleted": [
            {"path": "zoo"},
            {"path": os.path.join("data", "")},
            {"path": os.path.join("data", "foo")},
            {"path": os.path.join("data", "bar")},
        ],
        "modified": [{"path": "file"}],
        "added": [{"path": "file"}],
        "renamed": [{"path": {"old": "file_old", "new": "file_new"}}],
        "not in cache": [{"path": "file2"}],
    }

    _show_md(diff, hide_missing=True)

    out, _ = capsys.readouterr()
    assert out == (
        "| Status   | Path                 |\n"
        "|----------|----------------------|\n"
        "| added    | file                 |\n"
        "| deleted  | zoo                  |\n"
        "| deleted  | data{sep}                |\n"
        "| deleted  | data{sep}foo             |\n"
        "| deleted  | data{sep}bar             |\n"
        "| renamed  | file_old -> file_new |\n"
        "| modified | file                 |\n"
        "\n"
    ).format(sep=os.path.sep)


def test_hide_missing(mocker, caplog):
    args = parse_args(["diff", "--hide-missing"])
    cmd = args.func(args)
    diff = {
        "added": [{"path": "file", "hash": "00000000"}],
        "deleted": [],
        "modified": [],
        "renamed": [
            {
                "path": {"old": "file_old", "new": "file_new"},
                "hash": "11111111",
            }
        ],
        "not in cache": [],
    }
    mocker.patch("dvc.repo.Repo.diff", return_value=diff)

    assert 0 == cmd.run()
    assert (
        "Added:\n"
        "    file\n"
        "\n"
        "Renamed:\n"
        "    file_old -> file_new\n"
        "\n"
        "files summary: 1 added, 0 deleted, 1 renamed, 0 modified"
    ) in caplog.text
    assert "not in cache" not in caplog.text
