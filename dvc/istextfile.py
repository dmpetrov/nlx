"""Use heuristics to guess if it is a text file or a binary file."""

# Based on https://eli.thegreenplace.net/2011/10/19/
# perls-guess-if-file-is-text-or-binary-implemented-in-python


TEXT_CHARS = bytes(range(32, 127)) + b"\n\r\t\f\b"
DEFAULT_CHUNK_SIZE = 512


def istextblock(block):
    if not block:
        # An empty file is considered a valid text file
        return True

    if b"\x00" in block:
        # Files with null bytes are binary
        return False

    # Use translate's 'deletechars' argument to efficiently remove all
    # occurrences of TEXT_CHARS from the block
    nontext = block.translate(None, TEXT_CHARS)
    return float(len(nontext)) / len(block) <= 0.30


def istextfile(fname, fs, blocksize=DEFAULT_CHUNK_SIZE):
    """Uses heuristics to guess whether the given file is text or binary,
    by reading a single block of bytes from the file.
    If more than 30% of the chars in the block are non-text, or there
    are NUL ('\x00') bytes in the block, assume this is a binary file.
    """
    with fs.open(fname, "rb") as fobj:
        block = fobj.read(blocksize)
    return istextblock(block)
