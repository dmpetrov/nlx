import locale
import os
import uuid

import pytest
from funcy import cached_property

from dvc.path_info import CloudURLInfo
from dvc.utils import env2bool

from .base import Base

TEST_GCP_REPO_BUCKET = os.environ.get("DVC_TEST_GCP_REPO_BUCKET", "dvc-test")

TEST_GCP_CREDS_FILE = os.path.abspath(
    os.environ.get(
        "GOOGLE_APPLICATION_CREDENTIALS",
        os.path.join("scripts", "ci", "gcp-creds.json"),
    )
)
# Ensure that absolute path is used
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = TEST_GCP_CREDS_FILE


class GCP(Base, CloudURLInfo):

    IS_OBJECT_STORAGE = True

    @staticmethod
    def should_test():
        do_test = env2bool("DVC_TEST_GCP", undefined=None)
        if do_test is not None:
            return do_test

        if not os.path.exists(TEST_GCP_CREDS_FILE):
            return False

        return True

    @cached_property
    def config(self):
        return {"url": self.url, "credentialpath": TEST_GCP_CREDS_FILE}

    @staticmethod
    def _get_storagepath():
        return TEST_GCP_REPO_BUCKET + "/" + str(uuid.uuid4())

    @staticmethod
    def get_url():
        return "gs://" + GCP._get_storagepath()

    @property
    def _gc(self):
        from google.cloud.storage import Client

        return Client.from_service_account_json(TEST_GCP_CREDS_FILE)

    @property
    def _bucket(self):
        return self._gc.bucket(self.bucket)

    @property
    def _blob(self):
        return self._bucket.blob(self.path)

    def is_file(self):
        if self.path.endswith("/"):
            return False

        return self._blob.exists()

    def is_dir(self):
        dir_path = self / ""
        return bool(list(self._bucket.list_blobs(prefix=dir_path.path)))

    def exists(self):
        return self.is_file() or self.is_dir()

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        assert mode == 0o777
        assert parents

    def write_bytes(self, contents):
        assert isinstance(contents, bytes)
        self._blob.upload_from_string(contents)

    def read_bytes(self):
        return self._blob.download_as_string()

    def read_text(self, encoding=None, errors=None):
        if not encoding:
            encoding = locale.getpreferredencoding(False)
        assert errors is None
        return self.read_bytes().decode(encoding)


@pytest.fixture
def gs(test_config):
    test_config.requires("gs")
    if not GCP.should_test():
        pytest.skip("no gs")
    yield GCP(GCP.get_url())
