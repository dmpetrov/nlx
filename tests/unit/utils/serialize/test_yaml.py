import pytest

from dvc.utils.serialize._yaml import YAMLError, parse_yaml


def test_parse_yaml_duplicate_key_error():
    text = """\
    mykey:
    - foo
    mykey:
    - bar
    """
    with pytest.raises(YAMLError, match='found duplicate key "mykey"'):
        parse_yaml(text, "mypath")
