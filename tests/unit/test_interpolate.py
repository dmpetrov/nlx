from math import inf, pi

import pytest

from dvc.parsing.context import Context, recurse_not_a_node


@pytest.mark.parametrize(
    "template, var", [("${value}", "value"), ("${ item }", "item")]
)
@pytest.mark.parametrize(
    "data", [True, 12, pi, None, False, 0, "0", "123", "Foobar", "", inf, 3e4]
)
def test_resolve_primitive_values(data, template, var):
    context = Context({var: data})
    assert context.resolve(template) == data


@pytest.mark.parametrize(
    "template, expected",
    [
        (r"\${value}", "${value}"),
        (r"\${ value }", "${ value }"),
        (r"\${ value } days", "${ value } days"),
        (r"Month of \${value}", "Month of ${value}"),
        (r"May the \${value} be with you", "May the ${value} be with you"),
        (
            r"Great shot kid, that was \${value} in a ${value}",
            "Great shot kid, that was ${value} in a value",
        ),
    ],
)
def test_escape(template, expected):
    context = Context({"value": "value"})
    assert context.resolve(template) == expected


def test_resolve_str():
    template = "My name is ${last}, ${first} ${last}"
    expected = "My name is Bond, James Bond"
    context = Context({"first": "James", "last": "Bond"})
    assert context.resolve(template) == expected


def test_resolve_primitives_dict_access():
    data = {
        "dict": {
            "num": 5,
            "string": "foo",
            "nested": {"float": pi, "string": "bar"},
        }
    }
    context = Context(data)

    assert context.resolve("${dict.num}") == 5
    assert context.resolve("${dict.string}") == "foo"
    assert context.resolve("${dict.nested.float}") == pi
    assert context.resolve("${dict.nested.string}") == "bar"

    assert context.resolve("Number ${dict.num}") == "Number 5"


def test_resolve_primitives_list_access():
    context = Context(
        {
            "dict": [
                {"f": "f"},
                {"fo": "fo"},
                {"foo": "foo"},
                {"foo": ["f", "o", "o"]},
            ]
        }
    )

    assert context.resolve("${dict[0].f}") == "f"
    assert context.resolve("${dict[1].fo}") == "fo"
    assert context.resolve("${dict[2].foo}") == "foo"
    assert context.resolve("${dict[3].foo[0]}") == "f"

    assert context.resolve("${ dict.1.fo}${dict.3.foo.1}bar") == "foobar"


def test_resolve_collection():
    from tests.func.parsing import (
        CONTEXT_DATA,
        RESOLVED_DVC_YAML_DATA,
        TEMPLATED_DVC_YAML_DATA,
    )

    context = Context(CONTEXT_DATA)
    resolved = context.resolve(TEMPLATED_DVC_YAML_DATA)
    assert resolved == RESOLVED_DVC_YAML_DATA
    assert recurse_not_a_node(resolved)


def test_resolve_unicode():
    context = Context({"नेपाली": {"चिया": ["चि", "या"]}})
    assert context.resolve_str("${नेपाली.चिया[0]}${नेपाली.चिया[1]}") == "चिया"
    assert (
        context.resolve_str("${नेपाली[चिया][0]}${नेपाली[चिया][1]}") == "चिया"
    )
