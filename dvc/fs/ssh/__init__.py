import getpass
import io
import logging
import os
import shutil
import threading
from contextlib import closing, contextmanager

from funcy import first, memoize, silent, wrap_with

from dvc import prompt
from dvc.hash_info import HashInfo
from dvc.scheme import Schemes

from ..base import BaseFileSystem
from ..pool import get_connection

logger = logging.getLogger(__name__)


@wrap_with(threading.Lock())
@memoize
def ask_password(host, user, port):
    return prompt.password(
        "Enter a private key passphrase or a password for "
        "host '{host}' port '{port}' user '{user}'".format(
            host=host, port=port, user=user
        )
    )


class SSHFileSystem(BaseFileSystem):  # pylint:disable=abstract-method
    scheme = Schemes.SSH
    REQUIRES = {"paramiko": "paramiko"}
    _JOBS = 4

    PARAM_CHECKSUM = "md5"
    # At any given time some of the connections will go over network and
    # paramiko stuff, so we would ideally have it double of server processors.
    # We use conservative setting of 4 instead to not exhaust max sessions.
    CHECKSUM_JOBS = 4
    DEFAULT_CACHE_TYPES = ["copy"]
    TRAVERSE_PREFIX_LEN = 2

    DEFAULT_PORT = 22
    TIMEOUT = 1800

    def __init__(self, **config):
        super().__init__(**config)
        user_ssh_config = self._load_user_ssh_config(config["host"])

        self.host = user_ssh_config.get("hostname", config["host"])
        self.user = (
            config.get("user")
            or user_ssh_config.get("user")
            or getpass.getuser()
        )
        self.port = (
            config.get("port")
            or self._try_get_ssh_config_port(user_ssh_config)
            or self.DEFAULT_PORT
        )

        self.keyfile = config.get(
            "keyfile"
        ) or self._try_get_ssh_config_keyfile(user_ssh_config)
        self.timeout = config.get("timeout", self.TIMEOUT)
        self.password = config.get("password", None)
        self.ask_password = config.get("ask_password", False)
        self.gss_auth = config.get("gss_auth", False)
        proxy_command = user_ssh_config.get("proxycommand", False)
        if proxy_command:
            import paramiko

            self.sock = paramiko.ProxyCommand(proxy_command)
        else:
            self.sock = None
        self.allow_agent = config.get("allow_agent", True)

    @staticmethod
    def _get_kwargs_from_urls(urlpath):
        from fsspec.implementations.sftp import SFTPFileSystem

        # pylint:disable=protected-access
        kwargs = SFTPFileSystem._get_kwargs_from_urls(urlpath)
        if "username" in kwargs:
            kwargs["user"] = kwargs.pop("username")
        return kwargs

    @staticmethod
    def ssh_config_filename():
        return os.path.expanduser(os.path.join("~", ".ssh", "config"))

    @staticmethod
    def _load_user_ssh_config(hostname):
        import paramiko

        user_config_file = SSHFileSystem.ssh_config_filename()
        user_ssh_config = {}
        if hostname and os.path.exists(user_config_file):
            ssh_config = paramiko.SSHConfig()
            with open(user_config_file) as f:
                # For whatever reason parsing directly from f is unreliable
                f_copy = io.StringIO(f.read())
                ssh_config.parse(f_copy)
            user_ssh_config = ssh_config.lookup(hostname)
        return user_ssh_config

    @staticmethod
    def _try_get_ssh_config_port(user_ssh_config):
        return silent(int)(user_ssh_config.get("port"))

    @staticmethod
    def _try_get_ssh_config_keyfile(user_ssh_config):
        return first(user_ssh_config.get("identityfile") or ())

    def ensure_credentials(self):
        # NOTE: we use the same password regardless of the server :(
        if self.ask_password and self.password is None:
            self.password = ask_password(self.host, self.user, self.port)

    def ssh(self, path_info):
        self.ensure_credentials()

        from .connection import SSHConnection

        return get_connection(
            SSHConnection,
            path_info.host,
            username=path_info.user,
            port=path_info.port,
            key_filename=self.keyfile,
            timeout=self.timeout,
            password=self.password,
            gss_auth=self.gss_auth,
            sock=self.sock,
            allow_agent=self.allow_agent,
        )

    @contextmanager
    def open(self, path_info, mode="r", encoding=None, **kwargs):
        assert mode in {"r", "rt", "rb", "wb"}

        with self.ssh(path_info) as ssh, closing(
            ssh.sftp.open(path_info.path, mode)
        ) as fd:
            if "b" in mode:
                yield fd
            else:
                yield io.TextIOWrapper(fd, encoding=encoding)

    def exists(self, path_info) -> bool:
        with self.ssh(path_info) as ssh:
            return ssh.exists(path_info.path)

    def isdir(self, path_info):
        with self.ssh(path_info) as ssh:
            return ssh.isdir(path_info.path)

    def isfile(self, path_info):
        with self.ssh(path_info) as ssh:
            return ssh.isfile(path_info.path)

    def walk_files(self, path_info, **kwargs):
        with self.ssh(path_info) as ssh:
            for fname in ssh.walk_files(path_info.path):
                yield path_info.replace(path=fname)

    def remove(self, path_info):
        if path_info.scheme != self.scheme:
            raise NotImplementedError

        with self.ssh(path_info) as ssh:
            ssh.remove(path_info.path)

    def makedirs(self, path_info):
        with self.ssh(path_info) as ssh:
            ssh.makedirs(path_info.path)

    def move(self, from_info, to_info):
        if from_info.scheme != self.scheme or to_info.scheme != self.scheme:
            raise NotImplementedError

        with self.ssh(from_info) as ssh:
            ssh.move(from_info.path, to_info.path)

    def copy(self, from_info, to_info):
        if not from_info.scheme == to_info.scheme == self.scheme:
            raise NotImplementedError

        with self.ssh(from_info) as ssh:
            ssh.atomic_copy(from_info.path, to_info.path)

    def symlink(self, from_info, to_info):
        if not from_info.scheme == to_info.scheme == self.scheme:
            raise NotImplementedError

        with self.ssh(from_info) as ssh:
            ssh.symlink(from_info.path, to_info.path)

    def hardlink(self, from_info, to_info):
        if not from_info.scheme == to_info.scheme == self.scheme:
            raise NotImplementedError

        # See dvc/remote/local/__init__.py - hardlink()
        if self.getsize(from_info) == 0:

            with self.ssh(to_info) as ssh:
                ssh.sftp.open(to_info.path, "w").close()

            logger.debug(
                "Created empty file: {src} -> {dest}".format(
                    src=str(from_info), dest=str(to_info)
                )
            )
            return

        with self.ssh(from_info) as ssh:
            ssh.hardlink(from_info.path, to_info.path)

    def reflink(self, from_info, to_info):
        if from_info.scheme != self.scheme or to_info.scheme != self.scheme:
            raise NotImplementedError

        with self.ssh(from_info) as ssh:
            ssh.reflink(from_info.path, to_info.path)

    def md5(self, path_info):
        with self.ssh(path_info) as ssh:
            return HashInfo(
                "md5",
                ssh.md5(path_info.path),
                size=ssh.getsize(path_info.path),
            )

    def info(self, path_info):
        with self.ssh(path_info) as ssh:
            return ssh.info(path_info.path)

    def _upload_fobj(self, fobj, to_info):
        self.makedirs(to_info.parent)
        with self.open(to_info, mode="wb") as fdest:
            shutil.copyfileobj(fobj, fdest)

    def _download(self, from_info, to_file, name=None, no_progress_bar=False):
        with self.ssh(from_info) as ssh:
            ssh.download(
                from_info.path,
                to_file,
                progress_title=name,
                no_progress_bar=no_progress_bar,
            )

    def _upload(
        self, from_file, to_info, name=None, no_progress_bar=False, **_kwargs
    ):
        with self.ssh(to_info) as ssh:
            ssh.upload(
                from_file,
                to_info.path,
                progress_title=name,
                no_progress_bar=no_progress_bar,
            )
