import pytest

from dvc.dependency import Dependency
from dvc.stage import Stage


def test_save_missing(dvc, mocker):
    stage = Stage(dvc)
    dep = Dependency(stage, "path")
    with mocker.patch.object(dep.fs, "exists", return_value=False):
        with pytest.raises(dep.DoesNotExistError):
            dep.save()
