import pytest

from dvc.fs.base import BaseFileSystem, RemoteMissingDepsError


@pytest.mark.parametrize(
    "pkg, msg",
    [
        (None, "Please report this bug to"),
        ("pip", "pip install"),
        ("conda", "conda install"),
    ],
)
def test_missing_deps(pkg, msg, mocker):
    requires = {"missing": "missing"}
    mocker.patch.object(BaseFileSystem, "REQUIRES", requires)
    mocker.patch("dvc.utils.pkg.PKG", pkg)
    with pytest.raises(RemoteMissingDepsError, match=msg):
        BaseFileSystem()
