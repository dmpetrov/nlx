from collections import defaultdict
from typing import DefaultDict

from ._common import *  # noqa, pylint: disable=wildcard-import
from ._json import *  # noqa, pylint: disable=wildcard-import
from ._py import *  # noqa, pylint: disable=wildcard-import
from ._toml import *  # noqa, pylint: disable=wildcard-import
from ._yaml import *  # noqa, pylint: disable=wildcard-import

LOADERS: DefaultDict[str, LoaderFn] = defaultdict(  # noqa: F405
    lambda: load_yaml  # noqa: F405
)
LOADERS.update(
    {".toml": load_toml, ".json": load_json, ".py": load_py}  # noqa: F405
)

MODIFIERS: DefaultDict[str, ModifierFn] = defaultdict(  # noqa: F405
    lambda: modify_yaml  # noqa: F405
)
MODIFIERS.update(
    {
        ".toml": modify_toml,  # noqa: F405
        ".json": modify_json,  # noqa: F405
        ".py": modify_py,  # noqa: F405
    }
)
