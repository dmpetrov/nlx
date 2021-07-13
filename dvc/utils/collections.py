import inspect
import os
from collections.abc import Mapping
from functools import wraps
from typing import Callable, Dict, Iterable, List, TypeVar, Union

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


class _NamespacedDict(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__dict__ = self


def validate(*validators: Callable, post: bool = False):
    """
    Validate and transform arguments and results from function calls.

    The validators functions are passed a dictionary of arguments, which
    supports dot notation access too.

    The key is derived from the function signature, and hence is the name of
    the argument, whereas the value is the one passed to the function
    (if it is not passed, default value from keyword arguments are provided).

    >>> def validator(args):
    ...    assert args["l"] >= 0 and args.b >= 0 and args.h >= 0

    >>> @validate(validator)
    ... def cuboid_area(l, b, h=1):
    ...   return 2*(l*b + l*h + b*h)

    >>> cuboid_area(5, 20)
    250
    >>> cuboid_area(-1, -2)
    Traceback (most recent call last):
      ...
    AssertionError
    """

    def wrapped(func: Callable):
        sig = inspect.signature(func)

        @wraps(func)
        def inner(*args, **kwargs):
            ba = sig.bind(*args, **kwargs)
            ba.apply_defaults()
            ba.arguments = _NamespacedDict(ba.arguments)

            if not post:
                for validator in validators:
                    validator(ba.arguments)

            result = func(*ba.args, **ba.kwargs)
            if post:
                for validator in validators:
                    result = validator(result)
            return result

        return inner

    return wrapped


def nested_contains(dictionary: Dict, phrase: str) -> bool:
    for key, val in dictionary.items():
        if key == phrase and val:
            return True

        if isinstance(val, dict):
            if nested_contains(val, phrase):
                return True
    return False
