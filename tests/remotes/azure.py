# pylint:disable=abstract-method

import locale
import uuid

import pytest
from funcy import cached_property

from dvc.path_info import CloudURLInfo

from .base import Base

TEST_AZURE_CONTAINER = "tests"
TEST_AZURE_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSR"
    "Z6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:{port}/devstoreaccount1;"
)


class Azure(Base, CloudURLInfo):

    IS_OBJECT_STORAGE = True
    CONNECTION_STRING = None

    @cached_property
    def service_client(self):
        # pylint: disable=no-name-in-module
        from azure.storage.blob import BlobServiceClient

        service_client = BlobServiceClient.from_connection_string(
            self.CONNECTION_STRING
        )

        return service_client

    @property
    def blob_client(self):
        return self.service_client.get_blob_client(self.bucket, self.path)

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        assert mode == 0o777
        assert parents

    def write_bytes(self, contents):
        self.blob_client.upload_blob(contents, overwrite=True)

    def read_bytes(self):
        stream = self.blob_client.download_blob()
        return stream.readall()

    def read_text(self, encoding=None, errors=None):
        if not encoding:
            encoding = locale.getpreferredencoding(False)
        assert errors is None
        return self.read_bytes().decode(encoding)


@pytest.fixture(scope="session")
def azure_server(test_config, docker_compose, docker_services):
    test_config.requires("azure")

    from azure.core.exceptions import (  # pylint: disable=no-name-in-module
        AzureError,
    )
    from azure.storage.blob import (  # pylint: disable=no-name-in-module
        BlobServiceClient,
    )

    port = docker_services.port_for("azurite", 10000)
    connection_string = TEST_AZURE_CONNECTION_STRING.format(port=port)

    def _check():
        try:
            BlobServiceClient.from_connection_string(
                connection_string
            ).list_containers()
            return True
        except AzureError:
            return False

    docker_services.wait_until_responsive(
        timeout=60.0, pause=0.1, check=_check
    )

    Azure.CONNECTION_STRING = connection_string
    return connection_string


@pytest.fixture
def azure(azure_server):
    from azure.core.exceptions import ResourceNotFoundError

    url = f"azure://{TEST_AZURE_CONTAINER}/{uuid.uuid4()}"
    ret = Azure(url)
    ret.config = {"url": url, "connection_string": azure_server}

    container = ret.service_client.get_container_client(TEST_AZURE_CONTAINER)
    try:  # verify that container exists
        container.get_container_properties()
    except ResourceNotFoundError:
        container.create_container()

    return ret
