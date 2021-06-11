import os

import pytest

from dvc.system import System


@pytest.mark.skipif(os.name != "nt", reason="Windows specific")
def test_getdirinfo(tmp_path):
    # simulate someone holding an exclusive access
    locked = str(tmp_path / "locked")
    fd = os.open(locked, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
    try:
        # should not raise WinError
        System._getdirinfo(locked)
    finally:
        os.close(fd)
