import hashlib
import io

from funcy import cached_property

from dvc.hash_info import HashInfo
from dvc.istextfile import istextblock
from dvc.utils import dos2unix


class IterStream(io.RawIOBase):
    """Wraps an iterator yielding bytes as a file object"""

    def __init__(self, iterator):  # pylint: disable=super-init-not-called
        self.iterator = iterator
        self.leftover = b""

    def readable(self):
        return True

    def writable(self) -> bool:
        return False

    # Python 3 requires only .readinto() method, it still uses other ones
    # under some circumstances and falls back if those are absent. Since
    # iterator already constructs byte strings for us, .readinto() is not the
    # most optimal, so we provide .read1() too.

    def readinto(self, b):
        try:
            n = len(b)  # We're supposed to return at most this much
            chunk = self.leftover or next(self.iterator)
            output, self.leftover = chunk[:n], chunk[n:]

            n_out = len(output)
            b[:n_out] = output
            return n_out
        except StopIteration:
            return 0  # indicate EOF

    readinto1 = readinto

    def read1(self, n=-1):
        try:
            chunk = self.leftover or next(self.iterator)
        except StopIteration:
            return b""

        # Return an arbitrary number or bytes
        if n <= 0:
            self.leftover = b""
            return chunk

        output, self.leftover = chunk[:n], chunk[n:]
        return output

    def peek(self, n):
        while len(self.leftover) < n:
            try:
                self.leftover += next(self.iterator)
            except StopIteration:
                break
        return self.leftover[:n]


class HashedStreamReader(io.IOBase):

    PARAM_CHECKSUM = "md5"

    def __init__(self, fobj):
        self.fobj = fobj
        self.md5 = hashlib.md5()
        self.is_text_file = None
        super().__init__()

    def readable(self):
        return True

    def tell(self):
        return self.fobj.tell()

    @cached_property
    def _reader(self):
        if hasattr(self.fobj, "read1"):
            return self.fobj.read1
        return self.fobj.read

    def read(self, n=-1):
        chunk = self._reader(n)
        if self.is_text_file is None:
            self.is_text_file = istextblock(chunk)

        if self.is_text_file:
            data = dos2unix(chunk)
        else:
            data = chunk
        self.md5.update(data)

        return chunk

    @property
    def hash_info(self):
        return HashInfo(self.PARAM_CHECKSUM, self.md5.hexdigest(), nfiles=1)
