import os

from dvc.fs.hdfs import _hadoop_fs_checksum
from dvc.path_info import URLInfo
from dvc.utils import fix_env


def test_hadoop_fs_checksum(mocker):
    mock_proc = mocker.Mock()

    out = b"/path/to/file\tMD5-of-0MD5-of-512CRC32C\t123456789"
    err = b""
    mock_proc.configure_mock(
        **{"communicate.return_value": (out, err), "returncode": 0}
    )
    mock_popen = mocker.patch("subprocess.Popen", return_value=mock_proc)

    path_info = URLInfo("hdfs://example.com:1234/path/to/file")

    assert _hadoop_fs_checksum(path_info) == "123456789"
    mock_popen.assert_called_once_with(
        "hadoop fs -checksum hdfs://example.com:1234/path/to/file",
        shell=True,
        close_fds=os.name != "nt",
        executable=os.getenv("SHELL") if os.name != "nt" else None,
        env=fix_env(os.environ),
        stdin=-1,
        stdout=-1,
        stderr=-1,
    )
    assert mock_proc.communicate.called
