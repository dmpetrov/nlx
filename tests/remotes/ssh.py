import locale
import os
from contextlib import contextmanager

import pytest
from funcy import cached_property

from dvc.path_info import URLInfo

from .base import Base
from .local import Local

TEST_SSH_USER = "user"
TEST_SSH_KEY_PATH = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), f"{TEST_SSH_USER}.key"
)


class SSHMocked(Base, URLInfo):
    @staticmethod
    def get_url(user, port):  # pylint: disable=arguments-differ
        path = Local.get_storagepath()
        if os.name == "nt":
            # NOTE: On Windows Local.get_storagepath() will return an
            # ntpath that looks something like `C:\some\path`, which is not
            # compatible with SFTP paths [1], so we need to convert it to
            # a proper posixpath.
            # To do that, we should construct a posixpath that would be
            # relative to the server's root.
            # Our URL format requires absolute paths, so the
            # resulting path would look like `/some/path`.
            #
            # [1]https://tools.ietf.org/html/draft-ietf-secsh-filexfer-13#section-6
            drive, path = os.path.splitdrive(path)

            # Hackish way to make sure SSH server runs on same drive as tests
            # and temporary directories are.
            # Context: https://github.com/iterative/dvc/pull/4660
            test_drive, _ = os.path.splitdrive(os.getcwd())
            if drive.lower() != test_drive.lower():
                raise Exception("Did you forget to use `tmp_dir?`")

            path = path.replace("\\", "/")
        url = f"ssh://{user}@127.0.0.1:{port}{path}"
        return url

    @cached_property
    def config(self):
        return {"url": self.url, "keyfile": TEST_SSH_KEY_PATH}

    @contextmanager
    def _ssh(self):
        from dvc.fs.ssh.connection import SSHConnection

        conn = SSHConnection(
            host=self.host,
            port=self.port,
            username=TEST_SSH_USER,
            key_filename=TEST_SSH_KEY_PATH,
        )
        try:
            yield conn
        finally:
            conn.close()

    def is_file(self):
        with self._ssh() as _ssh:
            return _ssh.isfile(self.path)

    def is_dir(self):
        with self._ssh() as _ssh:
            return _ssh.isdir(self.path)

    def exists(self):
        with self._ssh() as _ssh:
            return _ssh.exists(self.path)

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        assert mode == 0o777
        assert parents

        with self._ssh() as _ssh:
            _ssh.makedirs(self.path)

    def write_bytes(self, contents):
        assert isinstance(contents, bytes)
        with self._ssh() as _ssh:
            with _ssh.open(self.path, "w+") as fobj:
                # NOTE: accepts both str and bytes
                fobj.write(contents)

    def read_bytes(self):
        with self._ssh() as _ssh:
            # NOTE: sftp always reads in binary format
            with _ssh.open(self.path, "r") as fobj:
                return fobj.read()

    def read_text(self, encoding=None, errors=None):
        if not encoding:
            encoding = locale.getpreferredencoding(False)
        assert errors is None
        return self.read_bytes().decode(encoding)


@pytest.fixture
def ssh_server(test_config):
    import mockssh

    test_config.requires("ssh")
    users = {TEST_SSH_USER: TEST_SSH_KEY_PATH}
    with mockssh.Server(users) as s:
        yield s


@pytest.fixture
def ssh_connection(ssh_server):
    from dvc.fs.ssh.connection import SSHConnection

    yield SSHConnection(
        host=ssh_server.host,
        port=ssh_server.port,
        username=TEST_SSH_USER,
        key_filename=TEST_SSH_KEY_PATH,
    )


@pytest.fixture
def ssh(ssh_server, monkeypatch):
    from dvc.fs.ssh import SSHFileSystem

    # NOTE: see http://github.com/iterative/dvc/pull/3501
    monkeypatch.setattr(SSHFileSystem, "CAN_TRAVERSE", False)

    return SSHMocked(SSHMocked.get_url(TEST_SSH_USER, ssh_server.port))
