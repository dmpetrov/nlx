# pylint:disable=abstract-method
import json
import os
import uuid
from functools import partialmethod

import pytest
from funcy import cached_property, retry

from dvc.fs.gdrive import GDriveFileSystem
from dvc.path_info import CloudURLInfo
from dvc.utils import tmp_fname

from .base import Base

TEST_GDRIVE_REPO_BUCKET = "root"


def _gdrive_retry(func):
    def should_retry(exc):
        from googleapiclient.errors import HttpError

        if not isinstance(exc, HttpError):
            return False

        if 500 <= exc.resp.status < 600:
            return True

        if exc.resp.status == 403:
            try:
                reason = json.loads(exc.content)["error"]["errors"][0][
                    "reason"
                ]
            except (ValueError, LookupError):
                return False

            return reason in ["userRateLimitExceeded", "rateLimitExceeded"]

    # 16 tries, start at 0.5s, multiply by golden ratio, cap at 20s
    return retry(
        16,
        timeout=lambda a: min(0.5 * 1.618 ** a, 20),
        filter_errors=should_retry,
    )(func)


class GDrive(Base, CloudURLInfo):
    @staticmethod
    def should_test():
        return bool(os.getenv(GDriveFileSystem.GDRIVE_CREDENTIALS_DATA))

    @cached_property
    def config(self):
        tmp_path = tmp_fname()
        with open(tmp_path, "w") as stream:
            raw_credentials = os.getenv(
                GDriveFileSystem.GDRIVE_CREDENTIALS_DATA
            )
            try:
                credentials = json.loads(raw_credentials)
            except ValueError:
                credentials = {}

            use_service_account = credentials.get("type") == "service_account"
            stream.write(raw_credentials)

        return {
            "url": self.url,
            "gdrive_service_account_json_file_path": tmp_path,
            "gdrive_use_service_account": use_service_account,
        }

    @staticmethod
    def _get_storagepath():
        return TEST_GDRIVE_REPO_BUCKET + "/" + str(uuid.uuid4())

    @staticmethod
    def get_url():
        # NOTE: `get_url` should always return new random url
        return "gdrive://" + GDrive._get_storagepath()

    @cached_property
    def client(self):
        try:
            from gdrivefs import GoogleDriveFileSystem
        except ImportError:
            pytest.skip("gdrivefs is not installed")

        return GoogleDriveFileSystem(
            token="cache",
            tokens_file=self.config["gdrive_service_account_json_file_path"],
            service_account=self.config["gdrive_use_service_account"],
        )

    @_gdrive_retry
    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        if not self.client.exists(self.path):
            self.client.mkdir(self.path)

    @_gdrive_retry
    def write_bytes(self, contents):
        with self.client.open(self.path, mode="wb") as stream:
            stream.write(contents)

    @_gdrive_retry
    def _read(self, mode):
        with self.client.open(self.path, mode=mode) as stream:
            return stream.read()

    read_text = partialmethod(_read, mode="r")
    read_bytes = partialmethod(_read, mode="rb")


@pytest.fixture
def gdrive(test_config, make_tmp_dir):
    test_config.requires("gdrive")
    if not GDrive.should_test():
        pytest.skip("no gdrive")

    # NOTE: temporary workaround
    tmp_dir = make_tmp_dir("gdrive", dvc=True)

    ret = GDrive(GDrive.get_url())
    fs = GDriveFileSystem(
        gdrive_credentials_tmp_dir=tmp_dir.dvc.tmp_dir, **ret.config
    )
    fs._gdrive_create_dir("root", fs.path_info.path)
    yield ret
