import pytest

from dvc.stage.utils import is_valid_name


@pytest.mark.parametrize("name", ["copy_name", "copy-name", "copyName", "12"])
def test_valid_stage_names(name):
    assert is_valid_name(name)


@pytest.mark.parametrize("name", ["copy$name", "copy-name?", "copy-name@v1"])
def test_invalid_stage_names(name):
    assert not is_valid_name(name)
