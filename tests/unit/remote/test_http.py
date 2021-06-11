import io

import pytest
import requests

from dvc.exceptions import HTTPError
from dvc.fs.http import HTTPFileSystem
from dvc.path_info import URLInfo


def test_download_fails_on_error_code(dvc, http):
    fs = HTTPFileSystem(**http.config)

    with pytest.raises(HTTPError):
        fs._download(http / "missing.txt", "missing.txt")


def test_public_auth_method(dvc):
    config = {
        "url": "http://example.com/",
        "path_info": "file.html",
        "user": "",
        "password": "",
    }

    fs = HTTPFileSystem(**config)

    assert fs._auth_method(config["url"]) is None


def test_basic_auth_method(dvc):
    from requests.auth import HTTPBasicAuth

    user = "username"
    password = "password"
    auth = HTTPBasicAuth(user, password)
    config = {
        "url": "http://example.com/",
        "path_info": "file.html",
        "auth": "basic",
        "user": user,
        "password": password,
    }

    fs = HTTPFileSystem(**config)

    assert fs._auth_method(config["url"]) == auth
    assert isinstance(fs._auth_method(config["url"]), HTTPBasicAuth)


def test_digest_auth_method(dvc):
    from requests.auth import HTTPDigestAuth

    user = "username"
    password = "password"
    auth = HTTPDigestAuth(user, password)
    config = {
        "url": "http://example.com/",
        "path_info": "file.html",
        "auth": "digest",
        "user": user,
        "password": password,
    }

    fs = HTTPFileSystem(**config)

    assert fs._auth_method(config["url"]) == auth
    assert isinstance(fs._auth_method(config["url"]), HTTPDigestAuth)


def test_custom_auth_method(dvc):
    header = "Custom-Header"
    password = "password"
    config = {
        "url": "http://example.com/",
        "path_info": "file.html",
        "auth": "custom",
        "custom_auth_header": header,
        "password": password,
    }

    fs = HTTPFileSystem(**config)

    assert fs._auth_method(config["url"]) is None
    assert header in fs.headers
    assert fs.headers[header] == password


def test_ssl_verify_is_enabled_by_default(dvc):
    config = {"url": "http://example.com/", "path_info": "file.html"}

    fs = HTTPFileSystem(**config)

    assert fs._session.verify is True


def test_ssl_verify_disable(dvc):
    config = {
        "url": "http://example.com/",
        "path_info": "file.html",
        "ssl_verify": False,
    }

    fs = HTTPFileSystem(**config)

    assert fs._session.verify is False


def test_ssl_verify_custom_cert(dvc):
    config = {
        "url": "http://example.com/",
        "path_info": "file.html",
        "ssl_verify": "/path/to/custom/cabundle.pem",
    }

    fs = HTTPFileSystem(**config)

    assert fs._session.verify == "/path/to/custom/cabundle.pem"


def test_http_method(dvc):
    from requests.auth import HTTPBasicAuth

    user = "username"
    password = "password"
    auth = HTTPBasicAuth(user, password)
    config = {
        "url": "http://example.com/",
        "path_info": "file.html",
        "auth": "basic",
        "user": user,
        "password": password,
        "method": "PUT",
    }

    fs = HTTPFileSystem(**config)

    assert fs._auth_method(config["url"]) == auth
    assert fs.method == "PUT"
    assert isinstance(fs._auth_method(config["url"]), HTTPBasicAuth)


def test_exists(mocker):
    res = requests.Response()
    # need to add `raw`, as `exists()` fallbacks to a streaming GET requests
    # on HEAD request failure.
    res.raw = io.StringIO("foo")

    fs = HTTPFileSystem()
    mocker.patch.object(fs, "request", return_value=res)

    url = URLInfo("https://example.org/file.txt")

    res.status_code = 200
    assert fs.exists(url) is True

    res.status_code = 404
    assert fs.exists(url) is False

    res.status_code = 403
    with pytest.raises(HTTPError):
        fs.exists(url)


@pytest.mark.parametrize(
    "headers, expected_size", [({"Content-Length": "3"}, 3), ({}, None)]
)
def test_content_length(mocker, headers, expected_size):
    res = requests.Response()
    res.headers.update(headers)
    res.status_code = 200

    fs = HTTPFileSystem()
    mocker.patch.object(fs, "request", return_value=res)

    url = URLInfo("https://example.org/file.txt")

    assert fs.info(url) == {"etag": None, "size": expected_size}
    assert fs._content_length(res) == expected_size
