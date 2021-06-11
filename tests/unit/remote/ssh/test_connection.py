import filecmp
import os
import platform
import posixpath
import tempfile

import pytest

from dvc.fs.ssh.connection import SSHConnection
from dvc.info import get_fs_type
from dvc.system import System

here = os.path.abspath(os.path.dirname(__file__))

SRC_PATH_WITH_SPECIAL_CHARACTERS = "Escape me [' , ']"
ESCAPED_SRC_PATH_WITH_SPECIAL_CHARACTERS = "'Escape me ['\"'\"' , '\"'\"']'"

DEST_PATH_WITH_SPECIAL_CHARACTERS = "Escape me too [' , ']"
ESCAPED_DEST_PATH_WITH_SPECIAL_CHARACTERS = (
    "'Escape me too ['\"'\"' , '\"'\"']'"
)


def test_isdir(ssh_connection):
    assert ssh_connection.isdir(here)
    assert not ssh_connection.isdir(__file__)


def test_exists(ssh_connection):
    assert not ssh_connection.exists("/path/to/non/existent/file")
    assert ssh_connection.exists(__file__)


def test_isfile(ssh_connection):
    assert ssh_connection.isfile(__file__)
    assert not ssh_connection.isfile(here)


def test_makedirs(tmp_path, ssh_connection):
    tmp = tmp_path.absolute().as_posix()
    path = posixpath.join(tmp, "dir", "subdir")
    ssh_connection.makedirs(path)
    assert os.path.isdir(path)


def test_remove_dir(tmp_path, ssh_connection):
    dpath = tmp_path / "dir"
    dpath.mkdir()
    (dpath / "file").write_text("file")
    (dpath / "subdir").mkdir()
    (dpath / "subdir" / "subfile").write_text("subfile")
    ssh_connection.remove(dpath.absolute().as_posix())
    assert not dpath.exists()


def test_walk(tmp_path, ssh_connection):
    root_path = tmp_path
    dir_path = root_path / "dir"
    subdir_path = dir_path / "subdir"

    dir_path.mkdir()
    subdir_path.mkdir()

    root_data_path = root_path / "root_data"
    dir_data_path = dir_path / "dir_data"
    subdir_data_path = subdir_path / "subdir_data"

    with root_data_path.open("w+") as fobj:
        fobj.write("")

    with dir_data_path.open("w+") as fobj:
        fobj.write("")

    with subdir_data_path.open("w+") as fobj:
        fobj.write("")

    entries = [
        dir_path,
        subdir_path,
        root_data_path,
        dir_data_path,
        subdir_data_path,
    ]
    expected = {entry.absolute().as_posix() for entry in entries}

    paths = set()
    for root, dirs, files in ssh_connection.walk(
        root_path.absolute().as_posix()
    ):
        for entry in dirs + files:
            paths.add(posixpath.join(root, entry))

    assert paths == expected


@pytest.mark.skipif(
    get_fs_type(tempfile.gettempdir())[0] not in ["xfs", "apfs", "btrfs"],
    reason="Reflinks only work in specified file systems",
)
def test_reflink(tmp_dir, ssh_connection):
    tmp_dir.gen("foo", "foo content")
    ssh_connection.reflink("foo", "link")
    assert filecmp.cmp("foo", "link")
    assert not System.is_symlink("link")
    assert not System.is_hardlink("link")


@pytest.mark.skipif(
    platform.system() == "Windows",
    reason="sftp symlink is not supported on Windows",
)
def test_symlink(tmp_dir, ssh_connection):
    tmp_dir.gen("foo", "foo content")
    ssh_connection.symlink("foo", "link")
    assert System.is_symlink("link")


@pytest.mark.skipif(
    platform.system() == "Windows",
    reason="hardlink is temporarily not supported on Windows",
)
def test_hardlink(tmp_dir, ssh_connection):
    tmp_dir.gen("foo", "foo content")
    ssh_connection.hardlink("foo", "link")
    assert System.is_hardlink("link")


@pytest.mark.skipif(
    platform.system() == "Windows",
    reason="copy is temporarily not supported on Windows",
)
def test_copy(tmp_dir, ssh_connection):
    tmp_dir.gen("foo", "foo content")
    ssh_connection.copy("foo", "link")
    assert filecmp.cmp("foo", "link")


def test_move(tmp_dir, ssh_connection):
    tmp_dir.gen("foo", "foo content")
    ssh_connection.move("foo", "copy")
    assert os.path.exists("copy")
    assert not os.path.exists("foo")


@pytest.mark.parametrize(
    "uname,md5command", [("Linux", "md5sum"), ("Darwin", "md5")]
)
def test_escapes_filepaths_for_md5_calculation(
    ssh_connection, uname, md5command, mocker
):
    fake_md5 = "x" * 32
    uname_mock = mocker.PropertyMock(return_value=uname)
    mocker.patch.object(SSHConnection, "uname", new_callable=uname_mock)
    ssh_connection.execute = mocker.Mock(return_value=fake_md5)
    ssh_connection.md5(SRC_PATH_WITH_SPECIAL_CHARACTERS)
    ssh_connection.execute.assert_called_with(
        f"{md5command} {ESCAPED_SRC_PATH_WITH_SPECIAL_CHARACTERS}"
    )


def test_escapes_filepaths_for_copy(ssh_connection, mocker):
    ssh_connection.execute = mocker.Mock()
    ssh_connection.copy(
        SRC_PATH_WITH_SPECIAL_CHARACTERS, DEST_PATH_WITH_SPECIAL_CHARACTERS
    )
    ssh_connection.execute.assert_called_with(
        f"cp {ESCAPED_SRC_PATH_WITH_SPECIAL_CHARACTERS} "
        + f"{ESCAPED_DEST_PATH_WITH_SPECIAL_CHARACTERS}"
    )


@pytest.mark.parametrize(
    "uname,cp_command", [("Linux", "cp --reflink"), ("Darwin", "cp -c")]
)
def test_escapes_filepaths_for_reflink(
    ssh_connection, uname, cp_command, mocker
):
    uname_mock = mocker.PropertyMock(return_value=uname)
    mocker.patch.object(SSHConnection, "uname", new_callable=uname_mock)
    ssh_connection.execute = mocker.Mock()
    ssh_connection.reflink(
        SRC_PATH_WITH_SPECIAL_CHARACTERS, DEST_PATH_WITH_SPECIAL_CHARACTERS
    )
    ssh_connection.execute.assert_called_with(
        f"{cp_command} "
        + f"{ESCAPED_SRC_PATH_WITH_SPECIAL_CHARACTERS} "
        + f"{ESCAPED_DEST_PATH_WITH_SPECIAL_CHARACTERS}"
    )


def test_escapes_filepaths_for_hardlink(ssh_connection, mocker):
    ssh_connection.execute = mocker.Mock()
    ssh_connection.hardlink(
        SRC_PATH_WITH_SPECIAL_CHARACTERS, DEST_PATH_WITH_SPECIAL_CHARACTERS
    )
    ssh_connection.execute.assert_called_with(
        f"ln {ESCAPED_SRC_PATH_WITH_SPECIAL_CHARACTERS} "
        + f"{ESCAPED_DEST_PATH_WITH_SPECIAL_CHARACTERS}"
    )
