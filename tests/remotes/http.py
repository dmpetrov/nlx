# pylint:disable=abstract-method
import os
import uuid

import pytest
import requests

from dvc.path_info import HTTPURLInfo

from .base import Base


class HTTP(Base, HTTPURLInfo):
    @staticmethod
    def get_url(port):  # pylint: disable=arguments-differ
        dname = str(uuid.uuid4())
        return f"http://127.0.0.1:{port}/{dname}"

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        assert mode == 0o777
        assert parents

    def write_bytes(self, contents):
        assert isinstance(contents, bytes)
        response = requests.post(self.url, data=contents)
        assert response.status_code == 200


@pytest.fixture(scope="session")
def http_server(test_config, tmp_path_factory):
    from tests.utils.httpd import StaticFileServer

    test_config.requires("http")
    directory = os.fspath(tmp_path_factory.mktemp("http"))
    with StaticFileServer(directory=directory) as httpd:
        yield httpd


@pytest.fixture
def http(http_server):
    yield HTTP(HTTP.get_url(http_server.server_port))
