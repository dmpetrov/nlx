import os
from collections.abc import Mapping
from typing import Dict, Iterable, List, TypeVar, Union

from pygtrie import StringTrie as _StringTrie


class PathStringTrie(_StringTrie):
    """Trie based on platform-dependent separator for pathname components."""

    def __init__(self, *args, **kwargs):
        kwargs["separator"] = os.sep
        super().__init__(*args, **kwargs)


def apply_diff(src, dest):
    """Recursively apply changes from src to dest.

    Preserves dest type and hidden info in dest structure,
    like ruamel.yaml leaves when parses files. This includes comments,
    ordering and line foldings.

    Used in Stage load/dump cycle to preserve comments and custom formatting.
    """
    Seq = (list, tuple)
    Container = (Mapping, list, tuple)

    def is_same_type(a, b):
        return any(
            isinstance(a, t) and isinstance(b, t)
            for t in [str, Mapping, Seq, bool]
        )

    if isinstance(src, Mapping) and isinstance(dest, Mapping):
        for key, value in src.items():
            if isinstance(value, Container) and is_same_type(
                value, dest.get(key)
            ):
                apply_diff(value, dest[key])
            elif key not in dest or value != dest[key]:
                dest[key] = value
        for key in set(dest) - set(src):
            del dest[key]
    elif isinstance(src, Seq) and isinstance(dest, Seq):
        if len(src) != len(dest):
            dest[:] = src
        else:
            for i, value in enumerate(src):
                if isinstance(value, Container) and is_same_type(
                    value, dest[i]
                ):
                    apply_diff(value, dest[i])
                elif value != dest[i]:
                    dest[i] = value
    else:
        raise AssertionError(
            "Can't apply diff from {} to {}".format(
                src.__class__.__name__, dest.__class__.__name__
            )
        )


def ensure_list(item: Union[Iterable[str], str, None]) -> List[str]:
    if item is None:
        return []
    if isinstance(item, str):
        return [item]
    return list(item)


_KT = TypeVar("_KT")
_VT = TypeVar("_VT")


def chunk_dict(d: Dict[_KT, _VT], size: int = 1) -> List[Dict[_KT, _VT]]:
    from funcy import chunks

    return [{key: d[key] for key in chunk} for chunk in chunks(size, d)]
