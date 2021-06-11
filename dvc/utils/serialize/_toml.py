from contextlib import contextmanager

from funcy import reraise

from ._common import ParseError, _dump_data, _load_data, _modify_data


class TOMLFileCorruptedError(ParseError):
    def __init__(self, path):
        super().__init__(path, "TOML file structure is corrupted")


def load_toml(path, fs=None):
    return _load_data(path, parser=parse_toml, fs=fs)


def parse_toml(text, path, decoder=None):
    from toml import TomlDecodeError, loads

    with reraise(TomlDecodeError, TOMLFileCorruptedError(path)):
        return loads(text, decoder=decoder)


def parse_toml_for_update(text, path):
    """Parses text into Python structure.

    NOTE: Python toml package does not currently use ordered dicts, so
    keys may be re-ordered between load/dump, but this function will at
    least preserve comments.
    """
    from toml import TomlPreserveCommentDecoder

    decoder = TomlPreserveCommentDecoder()
    return parse_toml(text, path, decoder=decoder)


def _dump(data, stream):
    import toml

    return toml.dump(data, stream, encoder=toml.TomlPreserveCommentEncoder())


def dump_toml(path, data, fs=None):
    return _dump_data(path, data, dumper=_dump, fs=fs)


@contextmanager
def modify_toml(path, fs=None):
    with _modify_data(path, parse_toml_for_update, dump_toml, fs=fs) as d:
        yield d
