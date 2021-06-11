import getpass
import os
import sys

import pytest
from mock import mock_open, patch

from dvc.fs.ssh import SSHFileSystem
from dvc.system import System


def test_get_kwargs_from_urls():
    user = "test"
    host = "123.45.67.89"
    port = 1234
    path = "/path/to/dir"

    # URL ssh://[user@]host.xz[:port]/path
    url = f"ssh://{user}@{host}:{port}{path}"

    assert SSHFileSystem._get_kwargs_from_urls(url) == {
        "user": user,
        "host": host,
        "port": port,
    }

    # SCP-like URL ssh://[user@]host.xz:/absolute/path
    url = f"ssh://{user}@{host}:{path}"

    assert SSHFileSystem._get_kwargs_from_urls(url) == {
        "user": user,
        "host": host,
    }


def test_init():
    fs = SSHFileSystem(user="test", host="12.34.56.78", port="1234")
    assert fs.user == "test"
    assert fs.host == "12.34.56.78"
    assert fs.port == "1234"


mock_ssh_config = """
Host example.com
   User ubuntu
   HostName 1.2.3.4
   Port 1234
   IdentityFile ~/.ssh/not_default.key
"""

if sys.version_info.major == 3:
    builtin_module_name = "builtins"
else:
    builtin_module_name = "__builtin__"


@pytest.mark.parametrize(
    "config,expected_host",
    [
        ({"host": "example.com"}, "1.2.3.4"),
        ({"host": "not_in_ssh_config.com"}, "not_in_ssh_config.com"),
    ],
)
@patch("os.path.exists", return_value=True)
@patch(
    f"{builtin_module_name}.open",
    new_callable=mock_open,
    read_data=mock_ssh_config,
)
def test_ssh_host_override_from_config(
    mock_file, mock_exists, config, expected_host
):
    fs = SSHFileSystem(**config)

    mock_exists.assert_called_with(SSHFileSystem.ssh_config_filename())
    mock_file.assert_called_with(SSHFileSystem.ssh_config_filename())
    assert fs.host == expected_host


@pytest.mark.parametrize(
    "config,expected_user",
    [
        ({"host": "example.com", "user": "test1"}, "test1"),
        ({"host": "example.com"}, "ubuntu"),
        ({"host": "not_in_ssh_config.com", "user": "test1"}, "test1"),
        ({"host": "not_in_ssh_config.com"}, getpass.getuser()),
    ],
)
@patch("os.path.exists", return_value=True)
@patch(
    f"{builtin_module_name}.open",
    new_callable=mock_open,
    read_data=mock_ssh_config,
)
def test_ssh_user(mock_file, mock_exists, config, expected_user):
    fs = SSHFileSystem(**config)

    mock_exists.assert_called_with(SSHFileSystem.ssh_config_filename())
    mock_file.assert_called_with(SSHFileSystem.ssh_config_filename())
    assert fs.user == expected_user


@pytest.mark.parametrize(
    "config,expected_port",
    [
        ({"host": "example.com"}, 1234),
        ({"host": "example.com", "port": 4321}, 4321),
        ({"host": "not_in_ssh_config.com"}, SSHFileSystem.DEFAULT_PORT),
        ({"host": "not_in_ssh_config.com", "port": 2222}, 2222),
    ],
)
@patch("os.path.exists", return_value=True)
@patch(
    f"{builtin_module_name}.open",
    new_callable=mock_open,
    read_data=mock_ssh_config,
)
def test_ssh_port(mock_file, mock_exists, config, expected_port):
    fs = SSHFileSystem(**config)

    mock_exists.assert_called_with(SSHFileSystem.ssh_config_filename())
    mock_file.assert_called_with(SSHFileSystem.ssh_config_filename())
    assert fs.port == expected_port


@pytest.mark.parametrize(
    "config,expected_keyfile",
    [
        (
            {"host": "example.com", "keyfile": "dvc_config.key"},
            "dvc_config.key",
        ),
        (
            {"host": "example.com"},
            os.path.expanduser("~/.ssh/not_default.key"),
        ),
        (
            {
                "host": "not_in_ssh_config.com",
                "keyfile": "dvc_config.key",
            },
            "dvc_config.key",
        ),
        ({"host": "not_in_ssh_config.com"}, None),
    ],
)
@patch("os.path.exists", return_value=True)
@patch(
    f"{builtin_module_name}.open",
    new_callable=mock_open,
    read_data=mock_ssh_config,
)
def test_ssh_keyfile(mock_file, mock_exists, config, expected_keyfile):
    fs = SSHFileSystem(**config)

    mock_exists.assert_called_with(SSHFileSystem.ssh_config_filename())
    mock_file.assert_called_with(SSHFileSystem.ssh_config_filename())
    assert fs.keyfile == expected_keyfile


@pytest.mark.parametrize(
    "config,expected_gss_auth",
    [
        ({"host": "example.com", "gss_auth": True}, True),
        ({"host": "example.com", "gss_auth": False}, False),
        ({"host": "not_in_ssh_config.com"}, False),
    ],
)
@patch("os.path.exists", return_value=True)
@patch(
    f"{builtin_module_name}.open",
    new_callable=mock_open,
    read_data=mock_ssh_config,
)
def test_ssh_gss_auth(mock_file, mock_exists, config, expected_gss_auth):
    fs = SSHFileSystem(**config)

    mock_exists.assert_called_with(SSHFileSystem.ssh_config_filename())
    mock_file.assert_called_with(SSHFileSystem.ssh_config_filename())
    assert fs.gss_auth == expected_gss_auth


@pytest.mark.parametrize(
    "config,expected_allow_agent",
    [
        ({"host": "example.com"}, True),
        ({"host": "not_in_ssh_config.com"}, True),
        ({"host": "example.com", "allow_agent": True}, True),
        ({"host": "example.com", "allow_agent": False}, False),
    ],
)
@patch("os.path.exists", return_value=True)
@patch(
    f"{builtin_module_name}.open",
    new_callable=mock_open,
    read_data=mock_ssh_config,
)
def test_ssh_allow_agent(mock_file, mock_exists, config, expected_allow_agent):
    fs = SSHFileSystem(**config)

    mock_exists.assert_called_with(SSHFileSystem.ssh_config_filename())
    mock_file.assert_called_with(SSHFileSystem.ssh_config_filename())
    assert fs.allow_agent == expected_allow_agent


def test_hardlink_optimization(tmp_dir, dvc, ssh):
    from dvc.fs import get_cloud_fs

    cls, config, path_info = get_cloud_fs(dvc, **ssh.config)
    fs = cls(**config)
    assert isinstance(fs, SSHFileSystem)

    from_info = path_info / "empty"
    to_info = path_info / "link"

    with fs.open(from_info, "wb"):
        pass

    if os.name == "nt":
        link_path = "c:" + to_info.path.replace("/", "\\")
    else:
        link_path = to_info.path

    fs.hardlink(from_info, to_info)
    assert not System.is_hardlink(link_path)
