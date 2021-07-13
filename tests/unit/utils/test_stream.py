import pytest

from dvc.fs.local import LocalFileSystem
from dvc.istextfile import DEFAULT_CHUNK_SIZE, istextfile
from dvc.utils import file_md5
from dvc.utils.stream import HashedStreamReader


def test_hashed_stream_reader(tmp_dir):
    tmp_dir.gen({"foo": "foo"})

    foo = tmp_dir / "foo"
    with open(foo, "rb") as fobj:
        stream_reader = HashedStreamReader(fobj)

        assert stream_reader.readable()
        assert not stream_reader.seekable()

        assert stream_reader.read(2) == b"fo"
        assert stream_reader.tell() == 2

        assert stream_reader.read(1) == b"o"
        assert stream_reader.tell() == 3

    hex_digest = file_md5(foo, LocalFileSystem())
    assert stream_reader.is_text_file
    assert hex_digest == stream_reader.hash_info.value


def test_hashed_stream_reader_as_chunks(tmp_dir):
    tmp_dir.gen({"foo": b"foo \x00" * 16})

    foo = tmp_dir / "foo"

    actual_size = len(foo.read_bytes())
    with open(foo, "rb") as fobj:
        stream_reader = HashedStreamReader(fobj)

        total_read = 0
        while True:
            chunk = stream_reader.read(16)
            total_read += len(chunk)
            assert stream_reader.tell() == total_read
            if not chunk:
                break

        assert stream_reader.tell() == actual_size == total_read

    hex_digest = file_md5(foo, LocalFileSystem())
    assert not stream_reader.is_text_file
    assert hex_digest == stream_reader.hash_info.value


@pytest.mark.parametrize(
    "contents",
    [b"x" * DEFAULT_CHUNK_SIZE + b"\x00", b"clean", b"not clean \x00"],
)
def test_hashed_stream_reader_compatibility(tmp_dir, contents):
    # Always read more than the DEFAULT_CHUNK_SIZE (512 bytes).
    # This imitates the read actions performed by upload_fobj.
    chunk_size = DEFAULT_CHUNK_SIZE * 2

    tmp_dir.gen("data", contents)
    data = tmp_dir / "data"

    with open(data, "rb") as fobj:
        stream_reader = HashedStreamReader(fobj)
        stream_reader.read(chunk_size)

    local_fs = LocalFileSystem()
    hex_digest = file_md5(data, local_fs)

    assert stream_reader.is_text_file is istextfile(data, local_fs)
    assert stream_reader.hash_info.value == hex_digest
