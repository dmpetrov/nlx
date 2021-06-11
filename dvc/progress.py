"""Manages progress bars for DVC repo."""

import logging
import sys
from threading import RLock

from tqdm import tqdm

from dvc.utils import env2bool

logger = logging.getLogger(__name__)
tqdm.set_lock(RLock())


class Tqdm(tqdm):
    """
    maximum-compatibility tqdm-based progressbars
    """

    BAR_FMT_DEFAULT = (
        "{percentage:3.0f}% {desc}|{bar}|"
        "{postfix[info]}{n_fmt}/{total_fmt}"
        " [{elapsed}<{remaining}, {rate_fmt:>11}]"
    )
    # nested bars should have fixed bar widths to align nicely
    BAR_FMT_DEFAULT_NESTED = (
        "{percentage:3.0f}%|{bar:10}|{desc:{ncols_desc}.{ncols_desc}}"
        "{postfix[info]}{n_fmt}/{total_fmt}"
        " [{elapsed}<{remaining}, {rate_fmt:>11}]"
    )
    BAR_FMT_NOTOTAL = (
        "{desc}{bar:b}|{postfix[info]}{n_fmt} [{elapsed}, {rate_fmt:>11}]"
    )
    BYTES_DEFAULTS = {
        "unit": "B",
        "unit_scale": True,
        "unit_divisor": 1024,
        "miniters": 1,
    }

    def __init__(
        self,
        iterable=None,
        disable=None,
        level=logging.ERROR,
        desc=None,
        leave=False,
        bar_format=None,
        bytes=False,  # pylint: disable=redefined-builtin
        file=None,
        total=None,
        postfix=None,
        **kwargs
    ):
        """
        bytes   : shortcut for
            `unit='B', unit_scale=True, unit_divisor=1024, miniters=1`
        desc  : persists after `close()`
        level  : effective logging level for determining `disable`;
            used only if `disable` is unspecified
        disable  : If (default: None) or False,
            will be determined by logging level.
            May be overridden to `True` due to non-TTY status.
            Skip override by specifying env var `DVC_IGNORE_ISATTY`.
        kwargs  : anything accepted by `tqdm.tqdm()`
        """
        kwargs = kwargs.copy()
        if bytes:
            kwargs = {**self.BYTES_DEFAULTS, **kwargs}
        else:
            kwargs.setdefault("unit_scale", total > 999 if total else True)
        if file is None:
            file = sys.stderr
        # auto-disable based on `logger.level`
        if not disable:
            disable = logger.getEffectiveLevel() > level
        # auto-disable based on TTY
        if (
            not disable
            and not env2bool("DVC_IGNORE_ISATTY")
            and hasattr(file, "isatty")
        ):
            disable = not file.isatty()
        super().__init__(
            iterable=iterable,
            disable=disable,
            leave=leave,
            desc=desc,
            bar_format="!",
            lock_args=(False,),
            total=total,
            **kwargs
        )
        self.postfix = postfix or {"info": ""}
        if bar_format is None:
            if self.__len__():
                self.bar_format = (
                    self.BAR_FMT_DEFAULT_NESTED
                    if self.pos
                    else self.BAR_FMT_DEFAULT
                )
            else:
                self.bar_format = self.BAR_FMT_NOTOTAL
        else:
            self.bar_format = bar_format
        self.refresh()

    def update_msg(self, msg, n=1):
        """
        Sets `msg` as a postfix and calls `update(n)`.
        """
        self.postfix["info"] = " %s |" % msg
        self.update(n)

    def update_to(self, current, total=None):
        if total:
            self.total = total
        self.update(current - self.n)

    def wrap_fn(self, fn, callback=None):
        """
        Returns a wrapped `fn` which calls `callback()` on each call.
        `callback` is `self.update` by default.
        """
        if callback is None:
            callback = self.update

        def wrapped(*args, **kwargs):
            res = fn(*args, **kwargs)
            callback()
            return res

        return wrapped

    def close(self):
        self.postfix["info"] = ""
        # remove ETA (either unknown or zero); remove completed bar
        self.bar_format = self.bar_format.replace("<{remaining}", "").replace(
            "|{bar:10}|", " "
        )
        super().close()

    @property
    def format_dict(self):
        """inject `ncols_desc` to fill the display width (`ncols`)"""
        d = super().format_dict
        ncols = d["ncols"] or 80
        # assumes `bar_format` has max one of ("ncols_desc" & "ncols_info")
        ncols_left = (
            ncols - len(self.format_meter(ncols_desc=1, ncols_info=1, **d)) + 1
        )
        ncols_left = max(ncols_left, 0)
        if ncols_left:
            d["ncols_desc"] = d["ncols_info"] = ncols_left
        else:
            # work-around for zero-width description
            d["ncols_desc"] = d["ncols_info"] = 1
            d["prefix"] = ""
        return d
