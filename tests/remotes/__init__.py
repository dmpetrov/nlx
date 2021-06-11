import subprocess

import pytest

from .azure import Azure, azure, azure_server  # noqa: F401
from .gdrive import (  # noqa: F401; noqa: F401
    TEST_GDRIVE_REPO_BUCKET,
    GDrive,
    gdrive,
)
from .gs import (  # noqa: F401; noqa: F401
    GCP,
    TEST_GCP_CREDS_FILE,
    TEST_GCP_REPO_BUCKET,
    gs,
)
from .hdfs import HDFS, hadoop, hdfs, hdfs_server, real_hdfs  # noqa: F401
from .http import HTTP, http, http_server  # noqa: F401
from .local import Local, local_cloud, local_remote  # noqa: F401
from .oss import (  # noqa: F401
    OSS,
    TEST_OSS_REPO_BUCKET,
    oss,
    oss_server,
    real_oss,
)
from .s3 import (  # noqa: F401
    S3,
    TEST_AWS_REPO_BUCKET,
    real_s3,
    s3,
    s3_fake_creds_file,
    s3_server,
)
from .ssh import (  # noqa: F401; noqa: F401
    SSHMocked,
    ssh,
    ssh_connection,
    ssh_server,
)
from .webdav import Webdav, webdav, webdav_server  # noqa: F401
from .webhdfs import WebHDFS, real_webhdfs, webhdfs  # noqa: F401

TEST_REMOTE = "upstream"
TEST_CONFIG = {
    "cache": {},
    "core": {"remote": TEST_REMOTE},
    "remote": {TEST_REMOTE: {"url": ""}},
}


@pytest.fixture(scope="session")
def docker():
    import os

    # See https://travis-ci.community/t/docker-linux-containers-on-windows/301
    if os.environ.get("CI") and os.name == "nt":
        pytest.skip("disabled for Windows on Github Actions")

    try:
        subprocess.check_output("docker ps", shell=True)
    except (subprocess.CalledProcessError, OSError):
        pytest.skip("no docker installed")


@pytest.fixture(scope="session")
def docker_compose(docker):
    try:
        subprocess.check_output("docker-compose version", shell=True)
    except (subprocess.CalledProcessError, OSError):
        pytest.skip("no docker-compose installed")


@pytest.fixture(scope="session")
def docker_compose_project_name():
    return "pytest-dvc-test"


@pytest.fixture(scope="session")
def docker_services(
    docker_compose_file, docker_compose_project_name, tmp_path_factory
):
    # overriding `docker_services` fixture from `pytest_docker` plugin to
    # only launch docker images once.

    from filelock import FileLock
    from pytest_docker.plugin import DockerComposeExecutor, Services

    executor = DockerComposeExecutor(
        docker_compose_file, docker_compose_project_name
    )

    # making sure we don't accidentally launch docker-compose in parallel,
    # as it might result in network conflicts. Inspired by:
    # https://github.com/pytest-dev/pytest-xdist#making-session-scoped-fixtures-execute-only-once
    lockfile = tmp_path_factory.getbasetemp().parent / "docker-compose.lock"
    with FileLock(str(lockfile)):
        executor.execute("up --build -d")

    return Services(executor)


@pytest.fixture
def remote(tmp_dir, dvc, request):
    cloud = request.param
    assert cloud
    tmp_dir.add_remote(config=cloud.config)
    yield cloud


@pytest.fixture
def workspace(tmp_dir, dvc, request):
    from dvc.objects.db import ODBManager

    cloud = request.param

    assert cloud

    tmp_dir.add_remote(name="workspace", config=cloud.config, default=False)
    tmp_dir.add_remote(
        name="cache", url="remote://workspace/cache", default=False
    )

    scheme = getattr(cloud, "scheme", "local")
    if scheme != "http":
        with dvc.config.edit() as conf:
            conf["cache"][scheme] = "cache"

        dvc.odb = ODBManager(dvc)

    return cloud
