import getpass
import os.path
import threading

from funcy import cached_property, first, memoize, silent, wrap_prop, wrap_with

import dvc.prompt as prompt
from dvc.scheme import Schemes

from .fsspec_wrapper import FSSpecWrapper

_SSH_TIMEOUT = 60 * 30
_SSH_CONFIG_FILE = os.path.expanduser(os.path.join("~", ".ssh", "config"))


@wrap_with(threading.Lock())
@memoize
def ask_password(host, user, port):
    return prompt.password(
        "Enter a private key passphrase or a password for "
        "host '{host}' port '{port}' user '{user}'".format(
            host=host, port=port, user=user
        )
    )


# pylint:disable=abstract-method
class SSHFileSystem(FSSpecWrapper):
    scheme = Schemes.SSH
    REQUIRES = {"sshfs": "sshfs"}

    DEFAULT_PORT = 22
    PARAM_CHECKSUM = "md5"
    SUPPORTS_CALLBACKS = True

    @staticmethod
    def _get_kwargs_from_urls(urlpath):
        from fsspec.implementations.sftp import SFTPFileSystem

        # pylint:disable=protected-access
        kwargs = SFTPFileSystem._get_kwargs_from_urls(urlpath)
        if "username" in kwargs:
            kwargs["user"] = kwargs.pop("username")
        return kwargs

    def _prepare_credentials(self, **config):
        from sshfs.config import parse_config

        login_info = {}

        user_ssh_config = parse_config(host=config["host"])

        login_info["host"] = user_ssh_config.get("Hostname", config["host"])

        login_info["username"] = (
            config.get("user")
            or user_ssh_config.get("User")
            or getpass.getuser()
        )

        login_info["port"] = (
            config.get("port")
            or silent(int)(user_ssh_config.get("Port"))
            or self.DEFAULT_PORT
        )

        login_info["password"] = config.get("password")

        if user_ssh_config.get("IdentityFile"):
            config.setdefault(
                "keyfile", first(user_ssh_config.get("IdentityFile"))
            )

        login_info["client_keys"] = [config.get("keyfile")]
        login_info["timeout"] = config.get("timeout", _SSH_TIMEOUT)

        # These two settings fine tune the asyncssh to use the
        # fastest encryption algortihm and disable compression
        # altogether (since it blocking, it is slowing down
        # the transfers in a considerable rate, and even for
        # compressible data it is making it extremely slow).
        # See: https://github.com/ronf/asyncssh/issues/374
        login_info["encryption_algs"] = [
            "aes128-gcm@openssh.com",
            "aes256-ctr",
            "aes192-ctr",
            "aes128-ctr",
        ]
        login_info["compression_algs"] = None

        login_info["gss_auth"] = config.get("gss_auth", False)
        login_info["agent_forwarding"] = config.get("agent_forwarding", True)
        login_info["proxy_command"] = user_ssh_config.get("ProxyCommand")

        if config.get("ask_password") and login_info["password"] is None:
            login_info["password"] = ask_password(
                login_info["host"], login_info["username"], login_info["port"]
            )

        # We are going to automatically add stuff to known_hosts
        # something like paramiko's AutoAddPolicy()
        login_info["known_hosts"] = None
        return login_info

    @wrap_prop(threading.Lock())
    @cached_property
    def fs(self):
        from sshfs import SSHFileSystem as _SSHFileSystem

        return _SSHFileSystem(**self.fs_args)
