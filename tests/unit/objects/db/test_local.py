import errno
import os

import pytest

from dvc.fs.local import LocalFileSystem
from dvc.hash_info import HashInfo
from dvc.objects.db.local import LocalObjectDB
from dvc.objects.file import HashFile
from dvc.path_info import PathInfo
from dvc.remote.index import RemoteIndexNoop


def test_status_download_optimization(mocker, dvc):
    """When comparing the status to pull a remote cache,
    And the desired files to fetch are already on the local cache,
    Don't check the existence of the desired files on the remote cache
    """
    odb = LocalObjectDB(LocalFileSystem(), PathInfo("."))

    objs = {
        HashFile(
            None, odb.fs, HashInfo("md5", "acbd18db4cc2f85cedef654fccc4a4d8")
        ),
        HashFile(
            None, odb.fs, HashInfo("md5", "37b51d194a7513e45b56f6524f2d51f2")
        ),
    }

    local_exists = [obj.hash_info.value for obj in objs]
    mocker.patch.object(odb, "hashes_exist", return_value=local_exists)

    other_remote = mocker.Mock()
    other_remote.url = "other_remote"
    other_remote.hashes_exist.return_value = []
    other_remote.index = RemoteIndexNoop()

    other_remote.status(odb, objs, download=True)

    assert other_remote.hashes_exist.call_count == 0


@pytest.mark.parametrize("link_name", ["hardlink", "symlink"])
def test_is_protected(tmp_dir, dvc, link_name):
    odb = dvc.odb.local
    fs = odb.fs
    link_method = getattr(fs, link_name)

    (tmp_dir / "foo").write_text("foo")

    foo = PathInfo(tmp_dir / "foo")
    link = PathInfo(tmp_dir / "link")

    link_method(foo, link)

    assert not odb.is_protected(foo)
    assert not odb.is_protected(link)

    odb.protect(foo)

    assert odb.is_protected(foo)
    assert odb.is_protected(link)

    odb.unprotect(link)

    assert not odb.is_protected(link)
    if os.name == "nt" and link_name == "hardlink":
        # NOTE: NTFS doesn't allow deleting read-only files, which forces us to
        # set write perms on the link, which propagates to the source.
        assert not odb.is_protected(foo)
    else:
        assert odb.is_protected(foo)


@pytest.mark.parametrize("err", [errno.EPERM, errno.EACCES, errno.EROFS])
def test_protect_ignore_errors(tmp_dir, dvc, mocker, err):
    tmp_dir.gen("foo", "foo")

    mock_chmod = mocker.patch(
        "os.chmod", side_effect=OSError(err, "something")
    )
    dvc.odb.local.protect(PathInfo("foo"))
    assert mock_chmod.called


@pytest.mark.parametrize("err", [errno.EPERM, errno.EACCES, errno.EROFS])
def test_set_exec_ignore_errors(tmp_dir, dvc, mocker, err):
    tmp_dir.gen("foo", "foo")

    mock_chmod = mocker.patch(
        "os.chmod", side_effect=OSError(err, "something")
    )
    dvc.odb.local.set_exec(PathInfo("foo"))
    assert mock_chmod.called
