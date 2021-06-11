import io
from collections import OrderedDict
from contextlib import contextmanager

from funcy import reraise

from ._common import ParseError, _dump_data, _load_data, _modify_data


class YAMLError(ParseError):
    pass


class YAMLFileCorruptedError(YAMLError):
    def __init__(self, path):
        super().__init__(path, "YAML file structure is corrupted")


def load_yaml(path, fs=None):
    return _load_data(path, parser=parse_yaml, fs=fs)


def parse_yaml(text, path, typ="safe"):
    from ruamel.yaml import YAML
    from ruamel.yaml import YAMLError as _YAMLError
    from ruamel.yaml.constructor import DuplicateKeyError

    yaml = YAML(typ=typ)
    try:
        with reraise(_YAMLError, YAMLFileCorruptedError(path)):
            return yaml.load(text) or {}
    except DuplicateKeyError as exc:
        # NOTE: unfortunately this one doesn't inherit from YAMLError, so we
        # have to catch it by-hand. See
        # https://yaml.readthedocs.io/en/latest/api.html#duplicate-keys
        raise YAMLError(path, exc.problem)


def parse_yaml_for_update(text, path):
    """Parses text into Python structure.

    Unlike `parse_yaml()` this returns ordered dicts, values have special
    attributes to store comments and line breaks. This allows us to preserve
    all of those upon dump.

    This one is, however, several times slower than simple `parse_yaml()`.
    """
    return parse_yaml(text, path, typ="rt")


def _get_yaml():
    from ruamel.yaml import YAML

    yaml = YAML()
    yaml.default_flow_style = False

    # tell Dumper to represent OrderedDict as normal dict
    yaml_repr_cls = yaml.Representer
    yaml_repr_cls.add_representer(OrderedDict, yaml_repr_cls.represent_dict)
    return yaml


def _dump(data, stream):
    yaml = _get_yaml()
    return yaml.dump(data, stream)


def dump_yaml(path, data, fs=None):
    return _dump_data(path, data, dumper=_dump, fs=fs)


def loads_yaml(s, typ="safe"):
    from ruamel.yaml import YAML

    return YAML(typ=typ).load(s)


def dumps_yaml(d):
    stream = io.StringIO()
    _dump(d, stream)
    return stream.getvalue()


@contextmanager
def modify_yaml(path, fs=None):
    with _modify_data(path, parse_yaml_for_update, dump_yaml, fs=fs) as d:
        yield d
