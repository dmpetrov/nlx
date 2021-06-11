import textwrap

import pytest

from dvc.compare import diff_table, metrics_table, show_diff, show_metrics


@pytest.mark.parametrize("title", ["Metric", "Param"])
def test_diff_table(title):
    td = diff_table(
        {"metrics.json": {"a.b.c": {"old": 1, "new": 2, "diff": 3}}},
        title=title,
    )
    assert td.as_dict() == [
        {
            "Path": "metrics.json",
            title: "a.b.c",
            "Old": "1",
            "New": "2",
            "Change": "3",
        }
    ]


def test_diff_table_with_value_column():
    td = diff_table(
        {"metrics.json": {"a.b.c": {"old": 1, "new": 2, "diff": 3}}},
        title="Metric",
        old=False,
    )
    assert td.as_dict() == [
        {
            "Path": "metrics.json",
            "Metric": "a.b.c",
            "Value": "2",
            "Change": "3",
        }
    ]


def test_no_path():
    td = diff_table(
        {"metrics.json": {"a.b.c": {"old": 1, "new": 2, "diff": 3}}},
        title="Metric",
        no_path=True,
    )
    assert td.as_dict() == [
        {"Metric": "a.b.c", "Old": "1", "New": "2", "Change": "3"}
    ]


def test_do_not_show_changes():
    td = diff_table(
        {"metrics.json": {"a.b.c": {"old": 1, "new": 2, "diff": 3}}},
        title="Metric",
        show_changes=False,
    )
    assert td.as_dict() == [
        {"Path": "metrics.json", "Metric": "a.b.c", "Old": "1", "New": "2"}
    ]


def test_diff_table_precision():
    diff = {
        "metrics.json": {
            "a.b.c": {"old": 1.1234, "new": 2.2345, "diff": 3.3456}
        }
    }
    td = diff_table(diff, title="Metric", precision=3)
    assert td.as_dict() == [
        {
            "Path": "metrics.json",
            "Metric": "a.b.c",
            "Old": "1.12",
            "New": "2.23",
            "Change": "3.35",
        }
    ]


def test_diff_table_rounding():
    diff = {
        "metrics.json": {
            "a.b.c": {"old": 1.1234, "new": 2.2345, "diff": 3.3456}
        }
    }
    td = diff_table(diff, title="Metric", precision=3, round_digits=True)
    assert td.as_dict() == [
        {
            "Path": "metrics.json",
            "Metric": "a.b.c",
            "Old": "1.123",
            "New": "2.235",
            "Change": "3.346",
        }
    ]


@pytest.mark.parametrize(
    "extra, expected", [({"on_empty_diff": "no diff"}, "no diff"), ({}, "-")]
)
def test_diff_unsupported_diff_message(extra, expected):
    td = diff_table(
        {"metrics.json": {"": {"old": "1", "new": "2"}}},
        title="Metric",
        **extra
    )
    assert td.as_dict() == [
        {
            "Path": "metrics.json",
            "Metric": "",
            "Old": "1",
            "New": "2",
            "Change": expected,
        }
    ]


def test_diff_new():
    td = diff_table(
        {"param.json": {"a.b.d": {"old": None, "new": "new"}}}, title="Param"
    )
    assert td.as_dict() == [
        {
            "Path": "param.json",
            "Param": "a.b.d",
            "Old": "-",
            "New": "new",
            "Change": "-",
        }
    ]


def test_diff_old_deleted():
    td = diff_table(
        {"metric.json": {"a.b.d": {"old": "old", "new": None}}}, title="Metric"
    )
    assert td.as_dict() == [
        {
            "Path": "metric.json",
            "Metric": "a.b.d",
            "Old": "old",
            "New": "-",
            "Change": "-",
        }
    ]


def test_diff_sorted():
    td = diff_table(
        {
            "metrics.yaml": {
                "x.b": {"old": 5, "new": 6, "diff": 1},
                "a.d.e": {"old": 3, "new": 4, "diff": 1},
                "a.b.c": {"old": 1, "new": 2, "diff": 1},
            }
        },
        "Metric",
    )
    assert list(td) == [
        ["metrics.yaml", "a.b.c", "1", "2", "1"],
        ["metrics.yaml", "a.d.e", "3", "4", "1"],
        ["metrics.yaml", "x.b", "5", "6", "1"],
    ]


def test_diff_falsey_values():
    diff = {"metrics.yaml": {"x.b": {"old": 0, "new": 0.0, "diff": 0.0}}}
    td = diff_table(diff, "Metric")
    assert td.as_dict() == [
        {
            "Path": "metrics.yaml",
            "Metric": "x.b",
            "Old": "0",
            "New": "0.0",
            "Change": "0.0",
        }
    ]


@pytest.mark.parametrize(
    "composite, expected",
    [([2, 3], "[2, 3]"), ({"foo": 3, "bar": 3}, "{'foo': 3, 'bar': 3}")],
)
def test_diff_list(composite, expected):
    td = diff_table(
        {"params.yaml": {"a.b.c": {"old": 1, "new": composite}}}, "Param"
    )
    assert td.as_dict() == [
        {
            "Path": "params.yaml",
            "Param": "a.b.c",
            "Old": "1",
            "New": expected,
            "Change": "-",
        }
    ]


@pytest.mark.parametrize("markdown", [True, False])
def test_diff_mocked(mocker, markdown):
    ret = mocker.MagicMock()
    m = mocker.patch("dvc.compare.diff_table", return_value=ret)

    show_diff({}, "metrics", markdown=markdown)

    m.assert_called_once_with(
        {},
        title="metrics",
        old=True,
        no_path=False,
        precision=None,
        on_empty_diff=None,
        show_changes=True,
        round_digits=False,
    )
    ret.render.assert_called_once_with(markdown=markdown)


def test_diff_default(capsys):
    show_diff(
        {
            "metrics.yaml": {
                "x.b": {"old": 5, "new": 6},
                "a.d.e": {"old": 3, "new": 4, "diff": 1},
                "a.b.c": {"old": 1, "new": 2, "diff": 1},
            }
        },
        "Metric",
    )
    out, _ = capsys.readouterr()
    assert out == textwrap.dedent(
        """\
        Path          Metric    Old    New    Change
        metrics.yaml  a.b.c     1      2      1
        metrics.yaml  a.d.e     3      4      1
        metrics.yaml  x.b       5      6      -
        """
    )


def test_metrics_diff_md(capsys):
    show_diff(
        {
            "metrics.yaml": {
                "x.b": {"old": 5, "new": 6},
                "a.d.e": {"old": 3, "new": 4, "diff": 1},
                "a.b.c": {"old": 1, "new": 2, "diff": 1},
            }
        },
        "Metric",
        markdown=True,
    )
    out, _ = capsys.readouterr()
    assert out == textwrap.dedent(
        """\
        | Path         | Metric   | Old   | New   | Change   |
        |--------------|----------|-------|-------|----------|
        | metrics.yaml | a.b.c    | 1     | 2     | 1        |
        | metrics.yaml | a.d.e    | 3     | 4     | 1        |
        | metrics.yaml | x.b      | 5     | 6     | -        |

        """
    )


def test_metrics_show_with_valid_falsey_values():
    td = metrics_table(
        {"branch_1": {"metrics.json": {"a": 0, "b": {"ad": 0.0, "bc": 0.0}}}},
        all_branches=True,
    )
    assert td.as_dict() == [
        {
            "Revision": "branch_1",
            "Path": "metrics.json",
            "a": "0",
            "b.ad": "0.0",
            "b.bc": "0.0",
        }
    ]


def test_metrics_show_with_no_revision():
    td = metrics_table(
        {"branch_1": {"metrics.json": {"a": 0, "b": {"ad": 0.0, "bc": 0.0}}}},
        all_branches=False,
    )
    assert td.as_dict() == [
        {"Path": "metrics.json", "a": "0", "b.ad": "0.0", "b.bc": "0.0"}
    ]


def test_metrics_show_with_non_dict_values():
    td = metrics_table({"branch_1": {"metrics.json": 1}}, all_branches=True)
    assert td.as_dict() == [
        {"Revision": "branch_1", "Path": "metrics.json", "": "1"}
    ]


def test_metrics_show_with_multiple_revision():
    td = metrics_table(
        {
            "branch_1": {"metrics.json": {"a": 1, "b": {"ad": 1, "bc": 2}}},
            "branch_2": {"metrics.json": {"a": 1, "b": {"ad": 3, "bc": 4}}},
        },
        all_branches=True,
    )
    assert td.as_dict() == [
        {
            "Revision": "branch_1",
            "Path": "metrics.json",
            "a": "1",
            "b.ad": "1",
            "b.bc": "2",
        },
        {
            "Revision": "branch_2",
            "Path": "metrics.json",
            "a": "1",
            "b.ad": "3",
            "b.bc": "4",
        },
    ]


def test_metrics_show_with_one_revision_multiple_paths():
    td = metrics_table(
        {
            "branch_1": {
                "metrics.json": {"a": 1, "b": {"ad": 0.1, "bc": 1.03}},
                "metrics_1.json": {"a": 2.3, "b": {"ad": 6.5, "bc": 7.9}},
            }
        },
        all_branches=True,
    )
    assert td.as_dict() == [
        {
            "Revision": "branch_1",
            "Path": "metrics.json",
            "a": "1",
            "b.ad": "0.1",
            "b.bc": "1.03",
        },
        {
            "Revision": "branch_1",
            "Path": "metrics_1.json",
            "a": "2.3",
            "b.ad": "6.5",
            "b.bc": "7.9",
        },
    ]


def test_metrics_show_with_different_metrics_header():
    td = metrics_table(
        {
            "branch_1": {"metrics.json": {"b": {"ad": 1, "bc": 2}, "c": 4}},
            "branch_2": {"metrics.json": {"a": 1, "b": {"ad": 3, "bc": 4}}},
        },
        all_branches=True,
    )
    assert td.as_dict() == [
        {
            "Revision": "branch_1",
            "Path": "metrics.json",
            "a": "-",
            "b.ad": "1",
            "b.bc": "2",
            "c": "4",
        },
        {
            "Revision": "branch_2",
            "Path": "metrics.json",
            "a": "1",
            "b.ad": "3",
            "b.bc": "4",
            "c": "-",
        },
    ]


def test_metrics_show_precision():
    metrics = {
        "branch_1": {
            "metrics.json": {
                "a": 1.098765366365355,
                "b": {"ad": 1.5342673, "bc": 2.987725527},
            }
        }
    }

    td = metrics_table(metrics, all_branches=True, precision=4)
    assert td.as_dict() == [
        {
            "Revision": "branch_1",
            "Path": "metrics.json",
            "a": "1.099",
            "b.ad": "1.534",
            "b.bc": "2.988",
        }
    ]

    td = metrics_table(
        metrics, all_branches=True, precision=4, round_digits=True
    )
    assert td.as_dict() == [
        {
            "Revision": "branch_1",
            "Path": "metrics.json",
            "a": "1.0988",
            "b.ad": "1.5343",
            "b.bc": "2.9877",
        }
    ]

    td = metrics_table(metrics, all_branches=True, precision=7)
    assert td.as_dict() == [
        {
            "Revision": "branch_1",
            "Path": "metrics.json",
            "a": "1.098765",
            "b.ad": "1.534267",
            "b.bc": "2.987726",
        }
    ]


@pytest.mark.parametrize("markdown", [True, False])
def test_metrics_show_mocked(mocker, markdown):
    ret = mocker.MagicMock()
    m = mocker.patch("dvc.compare.metrics_table", return_value=ret)

    show_metrics({}, markdown=markdown)

    m.assert_called_once_with(
        {},
        all_branches=False,
        all_tags=False,
        all_commits=False,
        precision=None,
        round_digits=False,
    )
    ret.render.assert_called_once_with(markdown=markdown)


def test_metrics_show_default(capsys):
    show_metrics(
        {
            "metrics.yaml": {
                "x.b": {"old": 5, "new": 6},
                "a.d.e": {"old": 3, "new": 4, "diff": 1},
                "a.b.c": {"old": 1, "new": 2, "diff": 1},
            }
        }
    )
    out, _ = capsys.readouterr()
    assert out == textwrap.dedent(
        """\
        Path    diff    new    old
        x.b     -       6      5
        a.d.e   1       4      3
        a.b.c   1       2      1
        """
    )


def test_metrics_show_md(capsys):
    show_metrics(
        {
            "metrics.yaml": {
                "x.b": {"old": 5, "new": 6},
                "a.d.e": {"old": 3, "new": 4, "diff": 1},
                "a.b.c": {"old": 1, "new": 2, "diff": 1},
            }
        },
        markdown=True,
    )
    out, _ = capsys.readouterr()
    assert out == textwrap.dedent(
        """\
        | Path   | diff   | new   | old   |
        |--------|--------|-------|-------|
        | x.b    | -      | 6     | 5     |
        | a.d.e  | 1      | 4     | 3     |
        | a.b.c  | 1      | 2     | 1     |

        """
    )
