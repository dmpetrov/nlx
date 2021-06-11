import pytest

from dvc.compare import TabularData


def test_table_empty(capsys):
    td = TabularData(["Col1", "Col2", "Col3"])
    assert dict(td.items()) == {"Col1": [], "Col2": [], "Col3": []}
    assert td.columns == [[], [], []]
    assert td.keys() == ["Col1", "Col2", "Col3"]
    assert list(td) == []
    assert td.Col1 == []
    assert td.Col2 == []
    assert td.Col3 == []

    assert td[1:] == []
    with pytest.raises(IndexError):
        _ = td[1]

    assert len(td) == 0
    assert td.shape == (3, 0)
    assert td.to_csv() == """Col1,Col2,Col3\r\n"""

    td.render()
    assert capsys.readouterr() == ("", "")

    td.render(rich_table=True)
    assert capsys.readouterr() == ("", "")

    td.render(markdown=True)
    assert capsys.readouterr() == (
        "| Col1   | Col2   | Col3   |\n|--------|--------|--------|\n\n",
        "",
    )

    td.rename("Col1", "Col11")
    assert td.keys() == ["Col11", "Col2", "Col3"]

    td.project("Col3", "Col11")
    assert td.keys() == ["Col3", "Col11"]


def test_list_operations():
    td = TabularData(["col1", "col2", "col3"])
    td.append(["1", "2", "3"])

    assert list(td) == [["1", "2", "3"]]
    td.extend((["11", "12", "13"], ["21", "22", "23"]))
    assert list(td) == [
        ["1", "2", "3"],
        ["11", "12", "13"],
        ["21", "22", "23"],
    ]
    td.insert(1, ["01", "02", "03"])
    assert list(td) == [
        ["1", "2", "3"],
        ["01", "02", "03"],
        ["11", "12", "13"],
        ["21", "22", "23"],
    ]
    assert td.shape == (3, 4)
    assert len(td) == 4
    assert td[1] == ["01", "02", "03"]
    assert td[1:] == [
        ["01", "02", "03"],
        ["11", "12", "13"],
        ["21", "22", "23"],
    ]
    assert td[::-1] == [
        ["21", "22", "23"],
        ["11", "12", "13"],
        ["01", "02", "03"],
        ["1", "2", "3"],
    ]
    del td[1]
    assert list(td) == [
        ["1", "2", "3"],
        ["11", "12", "13"],
        ["21", "22", "23"],
    ]
    assert td.shape == (3, 3)
    td[1:3] = [["51", "52", "53"], ["61", "62", "63"]]
    assert list(td) == [
        ["1", "2", "3"],
        ["51", "52", "53"],
        ["61", "62", "63"],
    ]
    td[1] = ["41", "42", "43"]
    assert td[1] == ["41", "42", "43"]

    del td[1:3]
    assert td.shape == (3, 1)

    assert td.to_csv() == "col1,col2,col3\r\n1,2,3\r\n"


def test_dict_like_interfaces():
    td = TabularData(["col-1", "col-2"])

    td.extend([["foo", "bar"], ["foobar", "foobar"]])
    assert td.keys() == ["col-1", "col-2"]
    assert dict(td.items()) == {
        "col-1": ["foo", "foobar"],
        "col-2": ["bar", "foobar"],
    }
    assert td.as_dict() == [
        {"col-1": "foo", "col-2": "bar"},
        {"col-1": "foobar", "col-2": "foobar"},
    ]
    assert td.as_dict(["col-1"]) == [{"col-1": "foo"}, {"col-1": "foobar"}]


def test_fill_value():
    td = TabularData(["col-1", "col-2", "col-3"], fill_value="?")
    td.append(["foo"])
    assert list(td) == [["foo", "?", "?"]]

    td.extend(
        [
            ["bar"],
            ["foobar", "foobar2"],
            ["f", "fo", "foo", "foob", "fooba", "foobar"],
        ]
    )
    assert list(td) == [
        ["foo", "?", "?"],
        ["bar", "?", "?"],
        ["foobar", "foobar2", "?"],
        ["f", "fo", "foo"],
    ]

    td.insert(1, ["lorem"])
    assert td[1] == ["lorem", "?", "?"]

    td[0] = ["lorem", "ipsum"]
    assert td[0] == ["lorem", "ipsum", "?"]

    td[1:2] = [["f", "fo"]]
    assert td[1:2] == [["f", "fo", "?"]]

    td.add_column("col-4")
    assert td.keys() == ["col-1", "col-2", "col-3", "col-4"]
    assert td[0][3] == "?"


def test_drop():
    td = TabularData(["col1", "col2", "col3"])
    td.append(["foo", "bar", "baz"])
    assert list(td) == [["foo", "bar", "baz"]]
    td.drop("col2")
    assert td.keys() == ["col1", "col3"]
    assert list(td) == [["foo", "baz"]]


def test_row_from_dict():
    td = TabularData(["col1", "col2"])
    td.row_from_dict({"col3": "value3", "col4": "value4"})
    assert td.keys() == ["col1", "col2", "col3", "col4"]
    assert dict(td.items()) == {
        "col1": [""],
        "col2": [""],
        "col3": ["value3"],
        "col4": ["value4"],
    }
    td.row_from_dict({"col3": "value3", "col5": "value5", "col6": "value6"})
    assert td.keys() == ["col1", "col2", "col3", "col4", "col5", "col6"]
    assert dict(td.items()) == {
        "col1": ["", ""],
        "col2": ["", ""],
        "col3": ["value3", "value3"],
        "col4": ["value4", ""],
        "col5": ["", "value5"],
        "col6": ["", "value6"],
    }
    assert td.shape == (6, 2)
    assert list(td) == [
        ["", "", "value3", "value4", "", ""],
        ["", "", "value3", "", "value5", "value6"],
    ]
