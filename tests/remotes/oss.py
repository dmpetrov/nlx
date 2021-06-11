# pylint:disable=abstract-method
import os
import uuid

import pytest

from dvc.path_info import CloudURLInfo
from dvc.utils import env2bool

from .base import Base

TEST_OSS_REPO_BUCKET = "dvc-test"
EMULATOR_OSS_ENDPOINT = "127.0.0.1:{port}"
EMULATOR_OSS_ACCESS_KEY_ID = "AccessKeyID"
EMULATOR_OSS_ACCESS_KEY_SECRET = "AccessKeySecret"


class OSS(Base, CloudURLInfo):

    IS_OBJECT_STORAGE = True

    @staticmethod
    def get_url():
        return f"oss://{TEST_OSS_REPO_BUCKET}/{uuid.uuid4()}"

    @staticmethod
    def should_test():
        do_test = env2bool("DVC_TEST_OSS", undefined=None)
        if do_test is not None:
            return do_test

        if os.getenv("OSS_ACCESS_KEY_ID") and os.getenv(
            "OSS_ACCESS_KEY_SECRET"
        ):
            return True

        return False


@pytest.fixture(scope="session")
def oss_server(test_config, docker_compose, docker_services):
    import oss2

    test_config.requires("oss")
    port = docker_services.port_for("oss", 8880)
    endpoint = EMULATOR_OSS_ENDPOINT.format(port=port)

    def _check():
        try:
            auth = oss2.Auth(
                EMULATOR_OSS_ACCESS_KEY_ID, EMULATOR_OSS_ACCESS_KEY_SECRET
            )
            oss2.Bucket(auth, endpoint, "mybucket").get_bucket_info()
            return True
        except oss2.exceptions.NoSuchBucket:
            return True
        except oss2.exceptions.OssError:
            return False

    docker_services.wait_until_responsive(timeout=30.0, pause=5, check=_check)

    return endpoint


@pytest.fixture
def oss(oss_server):
    import oss2

    url = OSS.get_url()
    ret = OSS(url)
    ret.config = {
        "url": url,
        "oss_key_id": EMULATOR_OSS_ACCESS_KEY_ID,
        "oss_key_secret": EMULATOR_OSS_ACCESS_KEY_SECRET,
        "oss_endpoint": oss_server,
    }

    auth = oss2.Auth(
        EMULATOR_OSS_ACCESS_KEY_ID, EMULATOR_OSS_ACCESS_KEY_SECRET
    )
    bucket = oss2.Bucket(auth, oss_server, TEST_OSS_REPO_BUCKET)
    try:
        bucket.get_bucket_info()
    except oss2.exceptions.NoSuchBucket:
        bucket.create_bucket(
            oss2.BUCKET_ACL_PUBLIC_READ,
            oss2.models.BucketCreateConfig(oss2.BUCKET_STORAGE_CLASS_STANDARD),
        )

    return ret


@pytest.fixture
def real_oss(test_config):
    test_config.requires("oss")
    if not OSS.should_test():
        pytest.skip("no real OSS")

    url = OSS.get_url()
    ret = OSS(url)
    ret.config = {
        "url": url,
        "oss_key_id": os.environ.get("OSS_ACCESS_KEY_ID"),
        "oss_key_secret": os.environ.get("OSS_ACCESS_KEY_SECRET"),
        "oss_endpoint": os.environ.get("OSS_ENDPOINT"),
    }
    return ret
