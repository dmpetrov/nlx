from collections import OrderedDict

import pytest

from dvc.repo.plots.data import _apply_path, _find_data, _lists


@pytest.mark.parametrize(
    "path,expected_result",
    [
        ("$.some.path[*].a", [{"a": 1}, {"a": 4}]),
        ("$.some.path", [{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}]),
    ],
)
def test_parse_json(path, expected_result):
    value = {
        "some": {"path": [{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}]}
    }

    result = _apply_path(value, path=path)

    assert result == expected_result


@pytest.mark.parametrize(
    "dictionary, expected_result",
    [
        ({}, []),
        ({"x": ["a", "b", "c"]}, [["a", "b", "c"]]),
        (
            OrderedDict([("x", {"y": ["a", "b"]}), ("z", {"w": ["c", "d"]})]),
            [["a", "b"], ["c", "d"]],
        ),
    ],
)
def test_finding_lists(dictionary, expected_result):
    result = _lists(dictionary)

    assert list(result) == expected_result


@pytest.mark.parametrize("fields", [{"x"}, set()])
def test_finding_data(fields):
    data = {"a": {"b": [{"x": 2, "y": 3}, {"x": 1, "y": 5}]}}

    result = _find_data(data, fields=fields)

    assert result == [{"x": 2, "y": 3}, {"x": 1, "y": 5}]
