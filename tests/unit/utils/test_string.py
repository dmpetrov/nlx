from dvc.utils import string


def test_fuzzy_match():

    assert string.fuzzy_match(
        "param", ["parameter", "argument", "sample", "file", "params"]
    ) == ["params", "parameter"]

    assert (
        string.fuzzy_match(
            "help", ["parameter", "argument", "sample", "file", "params"]
        )
        == []
    )

    assert (
        string.fuzzy_match(
            "", ["parameter", "argument", "sample", "file", "params"]
        )
        == []
    )

    assert string.fuzzy_match("help", []) == []
