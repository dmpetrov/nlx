import os
import stat

import pytest

from dvc.utils.fs import copyfile, makedirs


@pytest.mark.skipif(os.name == "nt", reason="Not supported for Windows.")
def test_makedirs_permissions(tmp_dir):
    dir_mode = 0o755
    intermediate_dir = "тестовая-директория"
    test_dir = os.path.join(intermediate_dir, "data")

    assert not os.path.exists(intermediate_dir)

    makedirs(test_dir, mode=dir_mode)

    assert stat.S_IMODE(os.stat(test_dir).st_mode) == dir_mode
    assert stat.S_IMODE(os.stat(intermediate_dir).st_mode) == dir_mode


def test_copyfile(tmp_dir):
    src = "file1"
    dest = "file2"
    dest_dir = "testdir"

    tmp_dir.gen(src, "file1contents")

    os.mkdir(dest_dir)

    copyfile(src, dest)
    assert (tmp_dir / dest).read_text() == "file1contents"

    copyfile(src, dest_dir)
    assert (tmp_dir / dest_dir / src).read_text() == "file1contents"
