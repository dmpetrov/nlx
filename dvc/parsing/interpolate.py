import re
import typing
from collections.abc import Mapping
from functools import singledispatch

from funcy import memoize, rpartial

from dvc.exceptions import DvcException

if typing.TYPE_CHECKING:
    from typing import List, Match

    from pyparsing import ParseException
    from typing_extensions import NoReturn

    from .context import Context

BRACE_OPEN = "${"
BRACE_CLOSE = "}"
LBRACK = "["
RBRACK = "]"
PERIOD = "."
KEYCRE = re.compile(
    r"""
    (?<!\\)                            # escape \${}
    \${                                # starts with ${
    (?P<inner>.*?)                     # match every char inside
    }                                  # end with {
""",
    re.VERBOSE,
)


@memoize
def get_parser():
    from pyparsing import CharsNotIn, ParserElement, Suppress, ZeroOrMore

    ParserElement.enablePackrat()

    word = CharsNotIn(f"{PERIOD}{LBRACK}{RBRACK}")
    idx = Suppress(LBRACK) + word + Suppress(RBRACK)
    attr = Suppress(PERIOD) + word
    parser = word + ZeroOrMore(attr ^ idx)
    parser.setParseAction(PERIOD.join)

    return parser


class ParseError(DvcException):
    pass


def get_matches(template: str):
    return list(KEYCRE.finditer(template))


def is_interpolated_string(val):
    return isinstance(val, str) and bool(get_matches(val))


def normalize_key(key: str):
    return key.replace(LBRACK, PERIOD).replace(RBRACK, "")


def format_and_raise_parse_error(exc) -> "NoReturn":
    raise ParseError(_format_exc_msg(exc))


def embrace(s: str):
    return BRACE_OPEN + s + BRACE_CLOSE


@singledispatch
def to_str(obj) -> str:
    return str(obj)


@to_str.register(bool)
def _(obj: bool):
    return "true" if obj else "false"


def _format_exc_msg(exc: "ParseException"):
    from pyparsing import ParseException

    from dvc.utils import colorize

    exc.loc += 2  # 2 because we append `${` at the start of expr below

    expr = exc.pstr
    exc.pstr = embrace(exc.pstr)
    error = ParseException.explain(exc, depth=0)

    _, pointer, *explains = error.splitlines()
    pstr = "{brace_open}{expr}{brace_close}".format(
        brace_open=colorize(BRACE_OPEN, color="blue"),
        expr=colorize(expr, color="magenta"),
        brace_close=colorize(BRACE_CLOSE, color="blue"),
    )
    msg = "\n".join(explains)
    pointer = colorize(pointer, color="red")
    return "\n".join([pstr, pointer, colorize(msg, color="red", style="bold")])


def recurse(f):
    seq = (list, tuple, set)

    def wrapper(data, *args):
        g = rpartial(wrapper, *args)
        if isinstance(data, Mapping):
            return {g(k): g(v) for k, v in data.items()}
        if isinstance(data, seq):
            return type(data)(map(g, data))
        if isinstance(data, str):
            return f(data, *args)
        return data

    return wrapper


def check_recursive_parse_errors(data):
    func = recurse(check_expression)
    return func(data)


def check_expression(s: str):
    matches = get_matches(s)
    for match in matches:
        get_expression(match)


def parse_expr(s: str):
    from pyparsing import ParseException

    try:
        result = get_parser().parseString(s, parseAll=True)
    except ParseException as exc:
        format_and_raise_parse_error(exc)
        raise AssertionError("unreachable")

    joined = result.asList()
    assert len(joined) == 1
    return joined[0]


def get_expression(match: "Match", skip_checks: bool = False):
    inner = match["inner"]
    return inner if skip_checks else parse_expr(inner)


def str_interpolate(
    template: str,
    matches: "List[Match]",
    context: "Context",
    skip_checks: bool = False,
):
    from .context import PRIMITIVES

    index, buf = 0, ""
    for match in matches:
        start, end = match.span(0)
        expr = get_expression(match, skip_checks=skip_checks)
        value = context.select(expr, unwrap=True)
        if value is not None and not isinstance(value, PRIMITIVES):
            raise ParseError(
                f"Cannot interpolate data of type '{type(value).__name__}'"
            )
        buf += template[index:start] + to_str(value)
        index = end
    buf += template[index:]
    # regex already backtracks and avoids any `${` starting with
    # backslashes(`\`). We just need to replace those by `${`.
    return buf.replace(r"\${", BRACE_OPEN)


def is_exact_string(src: str, matches: "List[Match]"):
    return len(matches) == 1 and src == matches[0].group(0)
