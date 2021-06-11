import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest

from dvc.fs.azure import AzureAuthError, AzureFileSystem, _temp_event_loop
from dvc.path_info import PathInfo

container_name = "container-name"
connection_string = (
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsu"
    "Fq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)


def test_strip_protocol_env_var(monkeypatch, dvc):
    monkeypatch.setenv("AZURE_STORAGE_CONTAINER_NAME", container_name)
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", connection_string)

    assert (
        AzureFileSystem._strip_protocol("azure://")
        == f"azure://{container_name}"
    )


def test_strip_protocol(dvc):
    assert (
        AzureFileSystem._strip_protocol(f"azure://{container_name}")
        == f"azure://{container_name}"
    )


def test_init(dvc):
    config = {"connection_string": connection_string}
    fs = AzureFileSystem(**config)
    assert fs.fs_args["connection_string"] == connection_string


def test_info(tmp_dir, azure):
    tmp_dir.gen("foo", "foo")

    fs = AzureFileSystem(**azure.config)
    to_info = azure
    fs.upload(PathInfo("foo"), to_info)
    assert fs.exists(to_info)
    hash_ = fs.info(to_info)["etag"]
    assert isinstance(hash_, str)
    assert hash_.strip("'").strip('"') == hash_


def test_temp_event_loop():
    def procedure():
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.sleep(0))
        return "yeey"

    def wrapped_procedure():
        with _temp_event_loop():
            return procedure()

        # it should clean the loop after
        # exitting the context.
        with pytest.raises(RuntimeError):
            asyncio.get_event_loop()

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(procedure)

        with pytest.raises(RuntimeError):
            future.result()

        future = executor.submit(wrapped_procedure)
        assert future.result() == "yeey"


def test_azure_login_methods():
    def get_login_method(config):
        fs = AzureFileSystem(**config)
        # pylint: disable=pointless-statement
        return fs.login_method

    with pytest.raises(AzureAuthError):
        get_login_method({})

    assert (
        get_login_method({"connection_string": "test"}) == "connection string"
    )
    assert get_login_method({"account_name": "test"}).startswith(
        "default credentials"
    )
    assert (
        get_login_method(
            {"account_name": "test", "allow_anonymous_login": True}
        )
        == "anonymous login"
    )

    with pytest.raises(AzureAuthError):
        get_login_method(
            {"tenant_id": "test", "client_id": "test", "client_secret": "test"}
        )

    assert (
        get_login_method(
            {
                "account_name": "test",
                "tenant_id": "test",
                "client_id": "test",
                "client_secret": "test",
            }
        )
        == "AD service principal"
    )

    assert (
        get_login_method({"account_name": "test", "account_key": "test"})
        == "account key"
    )
    assert (
        get_login_method({"account_name": "test", "sas_token": "test"})
        == "SAS token"
    )
    assert (
        get_login_method(
            {
                "connection_string": "test",
                "account_name": "test",
                "sas_token": "test",
            }
        )
        == "connection string"
    )
    assert (
        get_login_method({"connection_string": "test", "sas_token": "test"})
        == "connection string"
    )
