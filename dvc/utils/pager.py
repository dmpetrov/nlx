"""Draws DAG in ASCII."""

import logging
import os
import pydoc
import sys

from rich.pager import Pager

from dvc.env import DVC_PAGER
from dvc.utils import format_link

logger = logging.getLogger(__name__)


DEFAULT_PAGER = "less"
LESS = "LESS"
PAGER_ENV = "PAGER"


def prepare_default_pager(
    clear_screen: bool = False,
    quit_if_one_screen: bool = True,
    ansi_escapes: bool = True,
    chop_long_lines: bool = True,
    no_init: bool = True,
    no_tilde: bool = False,
) -> str:
    args = [DEFAULT_PAGER]
    if clear_screen:
        args.append("--clear-screen")  # -c
    if quit_if_one_screen:
        args.append("--quit-if-one-screen")  # -F
    if ansi_escapes:
        args.append("--RAW-CONTROL-CHARS")  # -R
    if chop_long_lines:
        args.append("--chop-long-lines")  # -S
    if no_init:
        args.append("--no-init")  # -X
    if no_tilde:
        args.append("--tilde")  # -~

    return " ".join(args)


def make_pager(cmd=None):
    def _pager(text):
        return pydoc.tempfilepager(pydoc.plain(text), cmd)

    return _pager if cmd else pydoc.plainpager


def find_pager():
    if not sys.stdout.isatty():
        return None

    # pylint: disable=redefined-outer-name
    pager = os.getenv(DVC_PAGER)
    if not pager:
        pager = os.getenv(PAGER_ENV)
    if not pager:
        if os.system(f"({DEFAULT_PAGER}) 2>{os.devnull}") != 0:
            logger.warning(
                "Unable to find `less` in the PATH. Check out "
                "{} for more info.".format(
                    format_link("https://man.dvc.org/pipeline/show")
                )
            )
        else:
            pager = DEFAULT_PAGER

    if pager == DEFAULT_PAGER:
        # if pager is less (i.e. default), regardless of `$LESS`, apply `-RS`.
        # `-R` is required to render ansi escape sequences for exp show
        # and, `-S` is required for horizontal scrolling.
        less_env = bool(os.getenv(LESS))
        pager = prepare_default_pager(
            ansi_escapes=True,
            chop_long_lines=True,
            quit_if_one_screen=not less_env,
            no_init=not less_env,
        )

    return pager


def pager(text: str) -> None:
    _pager = find_pager()
    logger.trace(f"Using pager: '{_pager}'")  # type: ignore[attr-defined]
    make_pager(_pager)(text)


class DvcPager(Pager):
    def show(self, content: str) -> None:
        pager(content)
