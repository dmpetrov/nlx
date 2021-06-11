import pytest

from dvc.cli import DvcParserError, parse_args
from dvc.command.config import CmdConfig


def test_config_formatter():
    example_config = {
        "section_foo": {"option_bar": True, "option_baz": False},
        "section_foo2": {
            "option_bar2": {"option_baz2": True},
            "option_baz3": {"option_baz4": False},
        },
        "section_foo3": {},
    }

    config_lines = tuple(CmdConfig._format_config(example_config))
    assert config_lines == (
        "section_foo.option_bar=True",
        "section_foo.option_baz=False",
        "section_foo2.option_bar2.option_baz2=True",
        "section_foo2.option_baz3.option_baz4=False",
    )


@pytest.mark.parametrize(
    "name", ["way.too.long", "no_option", "remote.way.too.long"]
)
def test_config_bad_name(name):
    with pytest.raises(DvcParserError):
        parse_args(["config", name])
