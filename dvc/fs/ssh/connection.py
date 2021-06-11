import errno
import logging
import os
import posixpath
import shlex
import stat
from contextlib import suppress

from funcy import cached_property

from dvc.exceptions import DvcException
from dvc.progress import Tqdm
from dvc.utils import tmp_fname

from ..base import RemoteCmdError

try:
    import paramiko
except ImportError:
    paramiko = None  # type: ignore


logger = logging.getLogger(__name__)


def sizeof_fmt(num, suffix="B"):
    """Convert number of bytes to human-readable string"""
    for unit in ["", "K", "M", "G", "T", "P", "E", "Z"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return "{:.1f}{}{}".format(num, "Y", suffix)


class SSHConnection:
    def __init__(self, host, *args, **kwargs):
        logger.debug(
            "Establishing ssh connection with '{host}' "
            "through port '{port}' as user '{username}'".format(
                host=host, **kwargs
            )
        )

        kwargs.setdefault("timeout", 1800)
        self.timeout = kwargs["timeout"]

        self._ssh = paramiko.SSHClient()

        # Explicitly disable paramiko logger. Due to how paramiko dynamically
        # loads loggers, it is not disabled by DVC disable_other_loggers().
        # See https://github.com/iterative/dvc/issues/3482
        self._ssh.set_log_channel("dvc.paramiko")
        logging.getLogger("dvc.paramiko").disabled = True
        logging.getLogger("dvc.paramiko.sftp").disabled = True

        self._ssh.load_system_host_keys()
        self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        self._ssh.connect(host, *args, **kwargs)
        self._ssh.get_transport().set_keepalive(10)
        self._sftp_channels = []

    @property
    def sftp(self):
        if not self._sftp_channels:
            self._sftp_channels = [self._ssh.open_sftp()]
        return self._sftp_channels[0]

    def close(self):
        for sftp in self._sftp_channels:
            sftp.close()
        self._ssh.close()

    def st_mode(self, path):
        lstat = None
        with suppress(FileNotFoundError):
            lstat = self.sftp.lstat(path)

        return lstat.st_mode if lstat else 0

    def info(self, path):
        st = self.sftp.stat(path)
        return {
            "size": st.st_size,
            "type": "dir" if stat.S_ISDIR(st.st_mode) else "file",
        }

    def getsize(self, path):
        return self.info(path)["size"]

    def exists(self, path):
        return bool(self.st_mode(path))

    def isdir(self, path):
        return stat.S_ISDIR(self.st_mode(path))

    def isfile(self, path):
        return stat.S_ISREG(self.st_mode(path))

    def islink(self, path):
        return stat.S_ISLNK(self.st_mode(path))

    def makedirs(self, path):
        # Single stat call will say whether this is a dir, a file or a link
        st_mode = self.st_mode(path)

        if stat.S_ISDIR(st_mode):
            return

        if stat.S_ISREG(st_mode) or stat.S_ISLNK(st_mode):
            raise DvcException(
                f"a file with the same name '{path}' already exists"
            )

        head, tail = posixpath.split(path)

        if head:
            self.makedirs(head)

        if tail:
            try:
                self.sftp.mkdir(path)
            except OSError as exc:
                # Since paramiko errors are very vague we need to recheck
                # whether it's because path already exists or something else
                if exc.errno == errno.EACCES or not self.exists(path):
                    raise DvcException(
                        f"unable to create remote directory '{path}'"
                    ) from exc

    def walk(self, directory, topdown=True):
        # NOTE: original os.walk() implementation [1] with default options was
        # used as a template.
        #
        # [1] https://github.com/python/cpython/blob/master/Lib/os.py
        try:
            dir_entries = self.sftp.listdir_attr(directory)
        except OSError as exc:
            raise DvcException(
                "couldn't get the '{}' remote directory files list".format(
                    directory
                )
            ) from exc

        dirs = []
        nondirs = []
        for entry in dir_entries:
            name = entry.filename
            if stat.S_ISDIR(entry.st_mode):
                dirs.append(name)
            else:
                nondirs.append(name)

        if topdown:
            yield directory, dirs, nondirs

        for dname in dirs:
            newpath = posixpath.join(directory, dname)
            yield from self.walk(newpath, topdown=topdown)

        if not topdown:
            yield directory, dirs, nondirs

    def walk_files(self, directory):
        for root, _, files in self.walk(directory):
            for fname in files:
                yield posixpath.join(root, fname)

    def _remove_file(self, path):
        with suppress(FileNotFoundError):
            self.sftp.remove(path)

    def _remove_dir(self, path):
        for root, dirs, files in self.walk(path, topdown=False):
            for fname in files:
                with suppress(FileNotFoundError):
                    self._remove_file(posixpath.join(root, fname))

            for dname in dirs:
                with suppress(FileNotFoundError):
                    self.sftp.rmdir(posixpath.join(root, dname))

        with suppress(FileNotFoundError):
            self.sftp.rmdir(path)

    def remove(self, path):
        if self.isdir(path):
            self._remove_dir(path)
        else:
            self._remove_file(path)

    def download(self, src, dest, no_progress_bar=False, progress_title=None):
        with Tqdm(
            desc=progress_title or os.path.basename(src),
            disable=no_progress_bar,
            bytes=True,
            total=self.getsize(src),
        ) as pbar:
            self.sftp.get(src, dest, callback=pbar.update_to)

    def move(self, src, dst):
        """Rename src to dst, if it is not possible (in case src and dst are
        on different filesystems) and actual physical copying of data is
        happening.
        """
        self.makedirs(posixpath.dirname(dst))

        try:
            self.sftp.rename(src, dst)
        except OSError:
            self.atomic_copy(src, dst)
            self.remove(src)

    def atomic_copy(self, src, dst):
        tmp = tmp_fname(dst)

        try:
            self.copy(src, tmp)
            self.sftp.rename(tmp, dst)
        finally:
            self.remove(tmp)

    def upload(self, src, dest, no_progress_bar=False, progress_title=None):
        self.makedirs(posixpath.dirname(dest))
        tmp_file = tmp_fname(dest)
        if not progress_title:
            progress_title = posixpath.basename(dest)

        with Tqdm(
            desc=progress_title,
            disable=no_progress_bar,
            bytes=True,
            total=os.path.getsize(src),
        ) as pbar:
            self.sftp.put(src, tmp_file, callback=pbar.update_to)

        self.sftp.rename(tmp_file, dest)

    def execute(self, cmd):
        stdin, stdout, stderr = self._ssh.exec_command(cmd)
        channel = stdout.channel

        stdin.close()
        channel.shutdown_write()

        stdout_chunks = []
        stderr_chunks = []
        while (
            not channel.closed
            or channel.recv_ready()
            or channel.recv_stderr_ready()
        ):
            import selectors

            selector = selectors.DefaultSelector()
            selector.register(stdout.channel, selectors.EVENT_READ)

            got_chunk = False
            events = selector.select(self.timeout)
            for key, _ in events:
                c = key.fileobj
                if c.recv_ready():
                    stdout_chunks.append(stdout.channel.recv(len(c.in_buffer)))
                    got_chunk = True

                if c.recv_stderr_ready():
                    stderr_len = len(c.in_stderr_buffer)
                    s = stderr.channel.recv_stderr(stderr_len)
                    stderr_chunks.append(s)
                    got_chunk = True

            if (
                not got_chunk
                and stdout.channel.exit_status_ready()
                and not stderr.channel.recv_stderr_ready()
                and not stdout.channel.recv_ready()
            ):
                stdout.channel.shutdown_read()
                stdout.channel.close()
                break

        stdout.close()
        stderr.close()

        ret = stdout.channel.recv_exit_status()
        if ret != 0:
            err = b"".join(stderr_chunks).decode("utf-8")
            raise RemoteCmdError("ssh", cmd, ret, err)

        return b"".join(stdout_chunks).decode("utf-8")

    @cached_property
    def uname(self):
        return self.execute("uname").strip()

    def md5(self, path):
        """
        Use different md5 commands depending on the OS:

         - Darwin's `md5` returns BSD-style checksums by default
         - Linux's `md5sum` needs the `--tag` flag for a similar output

         Example:
              MD5 (foo.txt) = f3d220a856b52aabbf294351e8a24300
        """
        path = shlex.quote(path)
        if self.uname == "Linux":
            md5 = self.execute("md5sum " + path).split()[0]
        elif self.uname == "Darwin":
            md5 = self.execute("md5 " + path).split()[-1]
        else:
            raise DvcException(
                f"'{self.uname}' is not supported as a SSH remote"
            )

        assert len(md5) == 32
        return md5

    def copy(self, src, dest):
        dest = shlex.quote(dest)
        src = shlex.quote(src)
        self.execute(f"cp {src} {dest}")

    def open_max_sftp_channels(self):
        # If there are more than 1 it means we've already opened max amount
        if len(self._sftp_channels) <= 1:
            while True:
                try:
                    self._sftp_channels.append(self._ssh.open_sftp())
                except paramiko.ssh_exception.ChannelException:
                    if not self._sftp_channels:
                        raise
                    break
        return self._sftp_channels

    def open(self, *args, **kwargs):
        return self.sftp.open(*args, **kwargs)

    def symlink(self, src, dest):
        self.sftp.symlink(src, dest)

    def reflink(self, src, dest):
        dest = shlex.quote(dest)
        src = shlex.quote(src)
        if self.uname == "Linux":
            return self.execute(f"cp --reflink {src} {dest}")

        if self.uname == "Darwin":
            return self.execute(f"cp -c {src} {dest}")

        raise DvcException(f"'{self.uname}' is not supported as a SSH remote")

    def hardlink(self, src, dest):
        dest = shlex.quote(dest)
        src = shlex.quote(src)
        self.execute(f"ln {src} {dest}")
