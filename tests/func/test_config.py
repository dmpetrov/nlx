import os
import textwrap

import pytest

from dvc.config import Config, ConfigError
from dvc.main import main


def test_config_set(tmp_dir, dvc):
    assert main(["config", "core.analytics", "false"]) == 0
    assert (tmp_dir / ".dvc" / "config").read_text() == textwrap.dedent(
        """\
        [core]
            no_scm = True
            analytics = false
        """
    )
    assert not (tmp_dir / ".dvc" / "config.local").exists()

    assert main(["config", "core.analytics", "true"]) == 0
    assert (tmp_dir / ".dvc" / "config").read_text() == textwrap.dedent(
        """\
        [core]
            no_scm = True
            analytics = true
        """
    )
    assert not (tmp_dir / ".dvc" / "config.local").exists()

    assert main(["config", "core.analytics", "--unset"]) == 0
    assert (tmp_dir / ".dvc" / "config").read_text() == textwrap.dedent(
        """\
        [core]
            no_scm = True
        """
    )
    assert not (tmp_dir / ".dvc" / "config.local").exists()


def test_config_set_local(tmp_dir, dvc):
    assert main(["config", "core.analytics", "false", "--local"]) == 0
    assert (tmp_dir / ".dvc" / "config").read_text() == textwrap.dedent(
        """\
        [core]
            no_scm = True
        """
    )
    assert (tmp_dir / ".dvc" / "config.local").read_text() == textwrap.dedent(
        """\
        [core]
            analytics = false
        """
    )

    assert main(["config", "core.analytics", "true", "--local"]) == 0
    assert (tmp_dir / ".dvc" / "config").read_text() == textwrap.dedent(
        """\
        [core]
            no_scm = True
        """
    )
    assert (tmp_dir / ".dvc" / "config.local").read_text() == textwrap.dedent(
        """\
        [core]
            analytics = true
        """
    )

    assert main(["config", "core.analytics", "--unset", "--local"]) == 0
    assert (tmp_dir / ".dvc" / "config").read_text() == textwrap.dedent(
        """\
        [core]
            no_scm = True
        """
    )
    assert (tmp_dir / ".dvc" / "config.local").read_text() == "\n"


@pytest.mark.parametrize(
    "args, ret, msg",
    [
        (["core.analytics"], 0, "False"),
        (["core.remote"], 0, "myremote"),
        (["remote.myremote.profile"], 0, "iterative"),
        (["remote.myremote.profile", "--local"], 0, "iterative"),
        (
            ["remote.myremote.profile", "--project"],
            251,
            "option 'profile' doesn't exist",
        ),
        (["remote.other.url"], 0, "gs://bucket/path"),
        (["remote.other.url", "--local"], 0, "gs://bucket/path"),
        (
            ["remote.other.url", "--project"],
            251,
            "remote 'other' doesn't exist",
        ),
    ],
)
def test_config_get(tmp_dir, dvc, caplog, args, ret, msg):
    (tmp_dir / ".dvc" / "config").write_text(
        textwrap.dedent(
            """\
        [core]
            no_scm = true
            analytics = False
            remote = myremote
        ['remote "myremote"']
            url = s3://bucket/path
            region = us-east-2
        """
        )
    )
    (tmp_dir / ".dvc" / "config.local").write_text(
        textwrap.dedent(
            """\
        ['remote "myremote"']
            profile = iterative
        ['remote "other"']
            url = gs://bucket/path
        """
        )
    )

    caplog.clear()
    assert main(["config"] + args) == ret
    assert msg in caplog.text


def test_config_list(tmp_dir, dvc, caplog):
    (tmp_dir / ".dvc" / "config").write_text(
        textwrap.dedent(
            """\
        [core]
            no_scm = true
            analytics = False
            remote = myremote
        ['remote "myremote"']
            url = s3://bucket/path
            region = us-east-2
        """
        )
    )
    (tmp_dir / ".dvc" / "config.local").write_text(
        textwrap.dedent(
            """\
        ['remote "myremote"']
            profile = iterative
            access_key_id = abcde
            secret_access_key = 123456
        ['remote "other"']
            url = gs://bucket/path
        """
        )
    )

    caplog.clear()
    assert main(["config", "--list"]) == 0
    assert "remote.myremote.url=s3://bucket/path" in caplog.text
    assert "remote.myremote.region=us-east-2" in caplog.text
    assert "remote.myremote.profile=iterative" in caplog.text
    assert "remote.myremote.access_key_id=abcde" in caplog.text
    assert "remote.myremote.secret_access_key=123456" in caplog.text
    assert "remote.other.url=gs://bucket/path" in caplog.text
    assert "core.analytics=False" in caplog.text
    assert "core.no_scm=true" in caplog.text
    assert "core.remote=myremote" in caplog.text


@pytest.mark.parametrize(
    "args", [["core.analytics"], ["core.analytics", "false"], ["--unset"]]
)
def test_list_bad_args(tmp_dir, dvc, caplog, args):
    caplog.clear()
    assert main(["config", "--list"] + args) == 1
    assert (
        "-l/--list can't be used together with any of these options: "
        "-u/--unset, name, value"
    ) in caplog.text


def test_set_invalid_key(dvc):
    with pytest.raises(ConfigError, match=r"extra keys not allowed"):
        with dvc.config.edit() as conf:
            conf["core"]["invalid_key"] = "value"


def test_merging_two_levels(dvc):
    with dvc.config.edit() as conf:
        conf["remote"]["test"] = {"url": "ssh://example.com"}

    with pytest.raises(
        ConfigError, match=r"expected 'url' for dictionary value"
    ):
        with dvc.config.edit("global") as conf:
            conf["remote"]["test"] = {"password": "1"}

    with dvc.config.edit("local") as conf:
        conf["remote"]["test"] = {"password": "1"}

    assert dvc.config["remote"]["test"] == {
        "url": "ssh://example.com",
        "password": "1",
    }


def test_config_loads_without_error_for_non_dvc_repo(tmp_dir):
    # regression testing for https://github.com/iterative/dvc/issues/3328
    Config(validate=True)


@pytest.mark.parametrize(
    "field, remote_url",
    [
        ("credentialpath", "s3://mybucket/my/path"),
        ("credentialpath", "gs://my-bucket/path"),
        ("keyfile", "ssh://user@example.com:1234/path/to/dir"),
        ("cert_path", "webdavs://example.com/files/USERNAME/"),
        ("key_path", "webdavs://example.com/files/USERNAME/"),
        ("gdrive_service_account_json_file_path", "gdrive://root/test"),
        ("gdrive_user_credentials_file", "gdrive://root/test"),
    ],
)
def test_load_relative_paths(dvc, field, remote_url):
    # set field to test
    with dvc.config.edit() as conf:
        conf["remote"]["test"] = {"url": remote_url, field: "file.txt"}

    # check if written paths are correct
    dvc_dir = dvc.config.dvc_dir
    assert dvc.config["remote"]["test"][field] == os.path.join(
        dvc_dir, "..", "file.txt"
    )

    # load config and check that it contains what we expect
    # (relative paths are evaluated correctly)
    cfg = Config(dvc_dir)
    assert cfg["remote"]["test"][field] == os.path.join(
        dvc_dir, "..", "file.txt"
    )


def test_config_remote(tmp_dir, dvc, caplog):
    (tmp_dir / ".dvc" / "config").write_text(
        "['remote \"myremote\"']\n"
        "  url = s3://bucket/path\n"
        "  region = myregion\n"
    )

    caplog.clear()
    assert main(["config", "remote.myremote.url"]) == 0
    assert "s3://bucket/path" in caplog.text

    caplog.clear()
    assert main(["config", "remote.myremote.region"]) == 0
    assert "myregion" in caplog.text


def test_config_show_origin_single(tmp_dir, dvc, caplog):
    (tmp_dir / ".dvc" / "config").write_text(
        "['remote \"myremote\"']\n"
        "  url = s3://bucket/path\n"
        "  region = myregion\n"
    )

    caplog.clear()
    assert (
        main(["config", "--show-origin", "--project", "remote.myremote.url"])
        == 0
    )
    assert (
        "{}\t{}\n".format(os.path.join(".dvc", "config"), "s3://bucket/path")
        in caplog.text
    )

    caplog.clear()
    assert (
        main(["config", "--show-origin", "--local", "remote.myremote.url"])
        == 251
    )

    caplog.clear()
    assert main(["config", "--list", "--project", "--show-origin"]) == 0
    assert (
        "{}\t{}\n".format(
            os.path.join(".dvc", "config"),
            "remote.myremote.url=s3://bucket/path",
        )
        in caplog.text
    )


def test_config_show_origin_merged(tmp_dir, dvc, caplog):
    (tmp_dir / ".dvc" / "config").write_text(
        "['remote \"myremote\"']\n"
        "  url = s3://bucket/path\n"
        "  region = myregion\n"
    )

    (tmp_dir / ".dvc" / "config.local").write_text(
        "['remote \"myremote\"']\n  timeout = 100\n"
    )

    caplog.clear()
    assert main(["config", "--list", "--show-origin"]) == 0
    assert (
        "{}\t{}\n".format(
            os.path.join(".dvc", "config"),
            "remote.myremote.url=s3://bucket/path",
        )
        in caplog.text
    )
    assert (
        "{}\t{}\n".format(
            os.path.join(".dvc", "config.local"), "remote.myremote.timeout=100"
        )
        in caplog.text
    )
