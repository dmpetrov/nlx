import importlib
import os
import sys
import textwrap

import pytest

from dvc.config import ConfigError
from dvc.fs.s3 import S3FileSystem

bucket_name = "bucket-name"
prefix = "some/prefix"
url = f"s3://{bucket_name}/{prefix}"
key_id = "key-id"
key_secret = "key-secret"
session_token = "session-token"


@pytest.fixture(autouse=True)
def grants():
    return {
        "grant_read": "id=read-permission-id,id=other-read-permission-id",
        "grant_read_acp": "id=read-acp-permission-id",
        "grant_write_acp": "id=write-acp-permission-id",
        "grant_full_control": "id=full-control-permission-id",
    }


def test_verify_ssl_default_param(dvc):
    config = {"url": url}
    fs = S3FileSystem(**config)

    assert "client_kwargs" not in fs.fs_args

    config = {
        "url": url,
        "endpointurl": "https://my.custom.s3:1234",
    }
    fs = S3FileSystem(**config)

    assert "verify" not in fs.fs_args["client_kwargs"]


def test_s3_config_credentialpath(dvc, monkeypatch):
    environment = {}
    monkeypatch.setattr(os, "environ", environment)

    config = {"url": url, "credentialpath": "somewhere"}
    S3FileSystem(**config)
    assert environment["AWS_SHARED_CREDENTIALS_FILE"] == "somewhere"
    environment.clear()

    config = {"url": url, "configpath": "somewhere"}
    S3FileSystem(**config)
    assert environment["AWS_CONFIG_FILE"] == "somewhere"
    environment.clear()

    config = {
        "url": url,
        "credentialpath": "somewhere",
        "configpath": "elsewhere",
    }
    S3FileSystem(**config)
    assert environment["AWS_SHARED_CREDENTIALS_FILE"] == "somewhere"
    assert environment["AWS_CONFIG_FILE"] == "elsewhere"
    environment.clear()


def test_ssl_verify_bool_param(dvc):
    config = {"url": url, "ssl_verify": False}
    fs = S3FileSystem(**config)

    assert fs.fs_args["client_kwargs"]["verify"] == config["ssl_verify"]


def test_ssl_verify_path_param(dvc):
    config = {"url": url, "ssl_verify": "/path/to/custom/cabundle.pem"}
    fs = S3FileSystem(**config)

    assert fs.fs_args["client_kwargs"]["verify"] == config["ssl_verify"]


def test_ssl_verify_none_param(dvc):
    config = {"url": url, "ssl_verify": None}
    fs = S3FileSystem(**config)

    assert "client_kwargs" not in fs.fs_args

    config = {
        "url": url,
        "endpointurl": "https://my.custom.s3:1234",
        "ssl_verify": None,
    }
    fs = S3FileSystem(**config)

    assert "verify" not in fs.fs_args["client_kwargs"]


def test_grants(dvc):
    config = {
        "url": url,
        "grant_read": "id=read-permission-id,id=other-read-permission-id",
        "grant_read_acp": "id=read-acp-permission-id",
        "grant_write_acp": "id=write-acp-permission-id",
        "grant_full_control": "id=full-control-permission-id",
    }
    fs = S3FileSystem(**config)

    extra_args = fs.fs_args["s3_additional_kwargs"]
    assert (
        extra_args["GrantRead"]
        == "id=read-permission-id,id=other-read-permission-id"
    )
    assert extra_args["GrantReadACP"] == "id=read-acp-permission-id"
    assert extra_args["GrantWriteACP"] == "id=write-acp-permission-id"
    assert extra_args["GrantFullControl"] == "id=full-control-permission-id"


def test_grants_mutually_exclusive_acl_error(dvc, grants):
    for grant_option, grant_value in grants.items():
        config = {"url": url, "acl": "public-read", grant_option: grant_value}

        with pytest.raises(ConfigError):
            S3FileSystem(**config)


def test_sse_kms_key_id(dvc):
    fs = S3FileSystem(url=url, sse_kms_key_id="key")
    assert fs.fs_args["s3_additional_kwargs"]["SSEKMSKeyId"] == "key"


def test_key_id_and_secret(dvc):
    fs = S3FileSystem(
        url=url,
        access_key_id=key_id,
        secret_access_key=key_secret,
        session_token=session_token,
    )
    assert fs.fs_args["key"] == key_id
    assert fs.fs_args["secret"] == key_secret
    assert fs.fs_args["token"] == session_token


KB = 1024
MB = KB ** 2
GB = KB ** 3


def test_s3_aws_config(tmp_dir, dvc, s3, monkeypatch):
    config_directory = tmp_dir / ".aws"
    config_directory.mkdir()
    (config_directory / "config").write_text(
        textwrap.dedent(
            """\
    [default]
    s3 =
      max_concurrent_requests = 20000
      max_queue_size = 1000
      multipart_threshold = 1000KiB
      multipart_chunksize = 64MB
      use_accelerate_endpoint = true
      addressing_style = path
    """
        )
    )

    if sys.platform == "win32":
        var = "USERPROFILE"
    else:
        var = "HOME"

    with monkeypatch.context() as m:
        m.setenv(var, str(tmp_dir))
        # Fresh import to see the effects of changing HOME variable
        s3_mod = importlib.reload(sys.modules[S3FileSystem.__module__])
        fs = s3_mod.S3FileSystem(**s3.config)

    importlib.reload(sys.modules[S3FileSystem.__module__])

    s3_config = fs.fs_args["config_kwargs"]["s3"]
    assert s3_config["use_accelerate_endpoint"]
    assert s3_config["addressing_style"] == "path"

    transfer_config = fs._transfer_config
    assert transfer_config.max_io_queue_size == 1000
    assert transfer_config.multipart_chunksize == 64 * MB
    assert transfer_config.multipart_threshold == 1000 * KB
    assert transfer_config.max_request_concurrency == 20000


def test_s3_aws_config_different_profile(tmp_dir, dvc, s3, monkeypatch):
    config_file = tmp_dir / "aws_config.ini"
    config_file.write_text(
        textwrap.dedent(
            """\
    [default]
    extra = keys
    s3 =
      addressing_style = auto
      use_accelerate_endpoint = true
      multipart_threshold = ThisIsNotGoingToBeCasted!
    [profile dev]
    some_extra = keys
    s3 =
      addressing_style = virtual
      multipart_threshold = 2GiB
    """
        )
    )
    monkeypatch.setenv("AWS_CONFIG_FILE", config_file)

    fs = S3FileSystem(profile="dev", **s3.config)

    s3_config = fs.fs_args["config_kwargs"]["s3"]
    assert s3_config["addressing_style"] == "virtual"

    transfer_config = fs._transfer_config
    assert transfer_config.multipart_threshold == 2 * GB
