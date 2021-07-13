"""DVC command line interface"""
import argparse
import logging
import os
import sys

from ._debug import add_debugging_flags
from .command import (
    add,
    cache,
    check_ignore,
    checkout,
    commit,
    completion,
    config,
    daemon,
    dag,
    data_sync,
    destroy,
    diff,
    experiments,
    freeze,
    gc,
    get,
    get_url,
    git_hook,
    imp,
    imp_url,
    init,
    install,
    live,
    ls,
    metrics,
    move,
    params,
    plots,
    remote,
    remove,
    repro,
    root,
    run,
    stage,
    unprotect,
    update,
    version,
)
from .command.base import fix_subparsers
from .exceptions import DvcParserError
from .utils.string import fuzzy_match

logger = logging.getLogger(__name__)

COMMANDS = [
    init,
    get,
    get_url,
    destroy,
    add,
    remove,
    move,
    unprotect,
    run,
    repro,
    data_sync,
    gc,
    imp,
    imp_url,
    config,
    checkout,
    remote,
    cache,
    metrics,
    params,
    install,
    root,
    ls,
    freeze,
    dag,
    daemon,
    commit,
    completion,
    diff,
    version,
    update,
    git_hook,
    plots,
    stage,
    experiments,
    check_ignore,
    live,
]


def _find_parser(parser, cmd_cls):
    defaults = parser._defaults  # pylint: disable=protected-access
    if not cmd_cls or cmd_cls == defaults.get("func"):
        parser.print_help()
        raise DvcParserError()

    actions = parser._actions  # pylint: disable=protected-access
    for action in actions:
        if not isinstance(action.choices, dict):
            # NOTE: we are only interested in subparsers
            continue
        for subparser in action.choices.values():
            _find_parser(subparser, cmd_cls)


class DvcParser(argparse.ArgumentParser):
    """Custom parser class for dvc CLI."""

    last_invalid_command = None

    def error(self, message, cmd_cls=None):  # pylint: disable=arguments-differ

        if self.last_invalid_command and self.print_suggestions():
            raise DvcParserError()

        logger.error(message)
        _find_parser(self, cmd_cls)

    def print_suggestions(self):
        suggestions = fuzzy_match(
            self.last_invalid_command, self._get_choices()
        )

        subparser = self._subparsers
        if (
            not suggestions
            or not subparser._group_actions  # pylint: disable=protected-access
        ):
            return False

        help_cmd = (
            subparser._group_actions[  # pylint: disable=protected-access
                0
            ].help
        )

        parent_cmd = self.prog
        command_text = (
            "subcommand" if len(parent_cmd.split()) > 1 else "command"
        )
        multi_line_message = [
            f"dvc: '{self.last_invalid_command}' is "
            f"not a valid `{parent_cmd}` {command_text}. "
            f"{help_cmd}"
            "\t",
            "",
            "The most similar commands are:",
            f"\t{' '.join(suggestions)}",
        ]
        logger.info("\n".join(multi_line_message))
        return True

    def parse_args(self, args=None, namespace=None):
        # NOTE: overriding to provide a more granular help message.
        # E.g. `dvc plots diff --bad-flag` would result in a `dvc plots diff`
        # help message instead of generic `dvc` usage.
        args, argv = self.parse_known_args(args, namespace)
        if argv:
            msg = "unrecognized arguments: %s"
            self.error(msg % " ".join(argv), getattr(args, "func", None))
        return args

    def _get_choices(self) -> list:
        """Gets the list of choices for dvc and its sub-commands"""
        cmd_choices = [
            list(action.choices.keys())
            for action in self._actions
            if action.dest == "cmd"
            and isinstance(
                action.choices, dict
            )  # pylint: disable=protected-access
        ]
        if not cmd_choices:
            return []

        return cmd_choices[0]

    def _check_value(self, action, value):
        # NOTE: overriding to get the last invalid command
        try:
            self.last_invalid_command = None
            super()._check_value(action, value)
        except argparse.ArgumentError as arg_exc:
            self.last_invalid_command = value
            raise arg_exc


class VersionAction(argparse.Action):  # pragma: no cover
    # pylint: disable=too-few-public-methods
    """Shows DVC version and exits."""

    def __call__(self, parser, namespace, values, option_string=None):
        from dvc import __version__

        print(__version__)
        sys.exit(0)


def get_parent_parser():
    """Create instances of a parser containing common arguments shared among
    all the commands.

    When overwriting `-q` or `-v`, you need to instantiate a new object
    in order to prevent some weird behavior.
    """
    parent_parser = argparse.ArgumentParser(add_help=False)
    add_debugging_flags(parent_parser)
    log_level_group = parent_parser.add_mutually_exclusive_group()
    log_level_group.add_argument(
        "-q", "--quiet", action="count", default=0, help="Be quiet."
    )
    log_level_group.add_argument(
        "-v", "--verbose", action="count", default=0, help="Be verbose."
    )

    return parent_parser


def get_main_parser():
    parent_parser = get_parent_parser()

    # Main parser
    desc = "Data Version Control"
    parser = DvcParser(
        prog="dvc",
        description=desc,
        parents=[parent_parser],
        formatter_class=argparse.RawTextHelpFormatter,
        add_help=False,
    )

    # NOTE: We are doing this to capitalize help message.
    # Unfortunately, there is no easier and clearer way to do it,
    # as adding this argument in get_parent_parser() either in
    # log_level_group or on parent_parser itself will cause unexpected error.
    parser.add_argument(
        "-h",
        "--help",
        action="help",
        default=argparse.SUPPRESS,
        help="Show this help message and exit.",
    )

    # NOTE: On some python versions action='version' prints to stderr
    # instead of stdout https://bugs.python.org/issue18920
    parser.add_argument(
        "-V",
        "--version",
        action=VersionAction,
        nargs=0,
        help="Show program's version.",
    )

    parser.add_argument(
        "--cd",
        default=os.path.curdir,
        metavar="<path>",
        help="Change to directory before executing.",
        type=str,
    )

    # Sub commands
    subparsers = parser.add_subparsers(
        title="Available Commands",
        metavar="COMMAND",
        dest="cmd",
        help="Use `dvc COMMAND --help` for command-specific help.",
    )

    fix_subparsers(subparsers)

    for cmd in COMMANDS:
        cmd.add_parser(subparsers, parent_parser)

    return parser


def parse_args(argv=None):
    """Parses CLI arguments.

    Args:
        argv: optional list of arguments to parse. sys.argv is used by default.

    Raises:
        dvc.exceptions.DvcParserError: raised for argument parsing errors.
    """
    parser = get_main_parser()
    args = parser.parse_args(argv)
    return args
