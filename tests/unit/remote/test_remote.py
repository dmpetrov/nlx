import pytest

from dvc.fs import get_cloud_fs
from dvc.fs.gs import GSFileSystem
from dvc.fs.s3 import S3FileSystem


def test_remote_with_hash_jobs(dvc):
    dvc.config["remote"]["with_hash_jobs"] = {
        "url": "s3://bucket/name",
        "checksum_jobs": 100,
    }
    dvc.config["core"]["checksum_jobs"] = 200

    cls, config, _ = get_cloud_fs(dvc, name="with_hash_jobs")
    fs = cls(**config)
    assert fs.hash_jobs == 100


def test_remote_with_jobs(dvc):
    dvc.config["remote"]["with_jobs"] = {
        "url": "s3://bucket/name",
        "jobs": 100,
    }
    dvc.config["core"]["jobs"] = 200

    cls, config, _ = get_cloud_fs(dvc, name="with_jobs")
    fs = cls(**config)
    assert fs.jobs == 100


def test_remote_without_hash_jobs(dvc):
    dvc.config["remote"]["without_hash_jobs"] = {"url": "s3://bucket/name"}
    dvc.config["core"]["checksum_jobs"] = 200

    cls, config, _ = get_cloud_fs(dvc, name="without_hash_jobs")
    fs = cls(**config)
    assert fs.hash_jobs == 200


def test_remote_without_hash_jobs_default(dvc):
    dvc.config["remote"]["without_hash_jobs"] = {"url": "s3://bucket/name"}

    cls, config, _ = get_cloud_fs(dvc, name="without_hash_jobs")
    fs = cls(**config)
    assert fs.hash_jobs == fs.HASH_JOBS


@pytest.mark.parametrize("fs_cls", [GSFileSystem, S3FileSystem])
def test_makedirs_not_create_for_top_level_path(fs_cls, dvc, mocker):
    url = f"{fs_cls.scheme}://bucket/"
    fs = fs_cls(url=url)
    mocked_client = mocker.PropertyMock()
    mocker.patch.object(fs_cls, "fs", mocked_client)

    fs.makedirs(fs.PATH_CLS(url))
    assert not mocked_client.called
