import locale
import os
import uuid

import pytest
from funcy import cached_property

from dvc.path_info import CloudURLInfo
from dvc.utils import env2bool

from .base import Base

TEST_AWS_REPO_BUCKET = os.environ.get("DVC_TEST_AWS_REPO_BUCKET", "dvc-temp")
TEST_AWS_ENDPOINT_URL = "http://127.0.0.1:{port}/"


class S3(Base, CloudURLInfo):

    IS_OBJECT_STORAGE = True
    TEST_AWS_ENDPOINT_URL = None

    @cached_property
    def config(self):
        return {"url": self.url, "endpointurl": self.TEST_AWS_ENDPOINT_URL}

    @staticmethod
    def should_test():
        do_test = env2bool("DVC_TEST_AWS", undefined=None)
        if do_test is not None:
            return do_test

        if os.getenv("AWS_ACCESS_KEY_ID") and os.getenv(
            "AWS_SECRET_ACCESS_KEY"
        ):
            return True

        return False

    @staticmethod
    def _get_storagepath():
        return (
            TEST_AWS_REPO_BUCKET
            + "/"
            + "dvc_test_caches"
            + "/"
            + str(uuid.uuid4())
        )

    @staticmethod
    def get_url():
        return "s3://" + S3._get_storagepath()

    @cached_property
    def _s3(self):
        import boto3

        return boto3.client("s3", endpoint_url=self.config["endpointurl"])

    def is_file(self):
        from botocore.exceptions import ClientError

        if self.path.endswith("/"):
            return False

        try:
            self._s3.head_object(Bucket=self.bucket, Key=self.path)
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "404":
                raise
            return False

        return True

    def is_dir(self):
        path = (self / "").path
        resp = self._s3.list_objects(Bucket=self.bucket, Prefix=path)
        return bool(resp.get("Contents"))

    def exists(self):
        return self.is_file() or self.is_dir()

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        assert mode == 0o777
        assert parents

    def write_bytes(self, contents):
        self._s3.put_object(Bucket=self.bucket, Key=self.path, Body=contents)

    def read_bytes(self):
        data = self._s3.get_object(Bucket=self.bucket, Key=self.path)
        return data["Body"].read()

    def read_text(self, encoding=None, errors=None):
        if not encoding:
            encoding = locale.getpreferredencoding(False)
        assert errors is None
        return self.read_bytes().decode(encoding)


@pytest.fixture
def s3_fake_creds_file(monkeypatch):
    # https://github.com/spulec/moto#other-caveats
    import pathlib

    aws_dir = pathlib.Path("~").expanduser() / ".aws"
    aws_dir.mkdir(exist_ok=True)

    aws_creds = aws_dir / "credentials"
    initially_exists = aws_creds.exists()

    if not initially_exists:
        aws_creds.touch()

    try:
        with monkeypatch.context() as m:
            m.setenv("AWS_ACCESS_KEY_ID", "testing")
            m.setenv("AWS_SECRET_ACCESS_KEY", "testing")
            m.setenv("AWS_SECURITY_TOKEN", "testing")
            m.setenv("AWS_SESSION_TOKEN", "testing")
            yield
    finally:
        if aws_creds.exists() and not initially_exists:
            aws_creds.unlink()


@pytest.fixture(scope="session")
def s3_server(test_config, docker_compose, docker_services):
    import requests

    test_config.requires("s3")

    port = docker_services.port_for("motoserver", 5000)
    endpoint_url = TEST_AWS_ENDPOINT_URL.format(port=port)

    def _check():
        try:
            r = requests.get(endpoint_url)
            return r.ok
        except requests.RequestException:
            return False

    docker_services.wait_until_responsive(
        timeout=60.0, pause=0.1, check=_check
    )
    S3.TEST_AWS_ENDPOINT_URL = endpoint_url
    return endpoint_url


@pytest.fixture
def s3(s3_server, s3_fake_creds_file):
    workspace = S3(S3.get_url())
    workspace._s3.create_bucket(Bucket=TEST_AWS_REPO_BUCKET)
    yield workspace


@pytest.fixture
def real_s3(test_config):
    test_config.requires("s3")
    if not S3.should_test():
        pytest.skip("no real s3")
    yield S3(S3.get_url())
