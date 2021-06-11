import os
import textwrap

import pytest

from dvc.config import Config
from dvc.fs.local import LocalFileSystem


@pytest.mark.parametrize(
    "path, expected",
    [
        ("cache", "../cache"),
        (os.path.join("..", "cache"), "../../cache"),
        (os.getcwd(), os.getcwd().replace("\\", "/")),
        ("ssh://some/path", "ssh://some/path"),
    ],
)
def test_to_relpath(path, expected):
    assert Config._to_relpath(os.path.join(".", "config"), path) == expected


def test_get_fs(tmp_dir, scm):
    tmp_dir.scm_gen("foo", "foo", commit="add foo")

    fs = scm.get_fs("master")
    config = Config(fs=fs)

    assert config.fs == fs
    assert config.wfs != fs
    assert isinstance(config.wfs, LocalFileSystem)

    assert config._get_fs("repo") == fs
    assert config._get_fs("local") == config.wfs
    assert config._get_fs("global") == config.wfs
    assert config._get_fs("system") == config.wfs


def test_s3_ssl_verify(tmp_dir, dvc):
    config = Config(validate=False)
    with config.edit() as conf:
        conf["remote"]["remote-name"] = {"url": "s3://bucket/dvc"}

    assert "ssl_verify" not in config["remote"]["remote-name"]

    with config.edit() as conf:
        section = conf["remote"]["remote-name"]
        section["ssl_verify"] = False

    assert (tmp_dir / ".dvc" / "config").read_text() == textwrap.dedent(
        """\
        [core]
            no_scm = True
        ['remote "remote-name"']
            url = s3://bucket/dvc
            ssl_verify = False
        """
    )

    with config.edit() as conf:
        section = conf["remote"]["remote-name"]
        section["ssl_verify"] = "/path/to/custom/cabundle.pem"

    assert (tmp_dir / ".dvc" / "config").read_text() == textwrap.dedent(
        """\
        [core]
            no_scm = True
        ['remote "remote-name"']
            url = s3://bucket/dvc
            ssl_verify = /path/to/custom/cabundle.pem
        """
    )
