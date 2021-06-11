# pylint: disable=unidiomatic-typecheck
from dvc.utils.collections import apply_diff, chunk_dict


class MyDict(dict):
    pass


class MyInt(int):
    pass


def test_apply_diff_is_inplace():
    dest = MyDict()
    dest.attr = 42  # pylint: disable=attribute-defined-outside-init
    apply_diff({}, dest)

    assert type(dest) is MyDict, "Preserves class"
    assert dest.attr == 42, "Preserves custom attrs"


def test_apply_diff_mapping():
    src = {"a": 1}
    dest = {"b": 2}
    apply_diff(src, dest)
    assert dest == src, "Adds and removes keys"

    src = {"a": 1}
    dest = {"a": MyInt(1)}
    apply_diff(src, dest)
    assert type(dest["a"]) is MyInt, "Does not replace equals"

    src = {"d": {"a": 1}}
    inner = {}
    dest = {"d": inner}
    apply_diff(src, dest)
    assert dest["d"] is inner, "Updates inner dicts"


def test_apply_diff_seq():
    src = [1]
    dest = [MyInt(1)]
    apply_diff(src, dest)
    assert type(dest[0]) is MyInt, "Does not replace equals"

    src = {"l": [1]}
    inner = []
    dest = {"l": inner}
    apply_diff(src, dest)
    assert dest["l"] is inner, "Updates inner lists"


def test_chunk_dict():
    assert chunk_dict({}) == []

    d = {"a": 1, "b": 2, "c": 3}
    assert chunk_dict(d) == [{"a": 1}, {"b": 2}, {"c": 3}]
    assert chunk_dict(d, 2) == [{"a": 1, "b": 2}, {"c": 3}]
    assert chunk_dict(d, 3) == [d]
    assert chunk_dict(d, 4) == [d]
