"""
Disable certain checks for the tests.

To disable new checks, add to the `SUPPRESS_CHECKS` dictionary,
with `message_id` of the check to disable as a key and a list of
methods that check for that particular message.
"""
import os.path
from typing import TYPE_CHECKING

from pylint.checkers.base import NameChecker
from pylint.checkers.classes import ClassChecker

if TYPE_CHECKING:
    from astroid import node_classes, scoped_nodes  # noqa
    from pylint.lint import PyLinter


TESTS_FOLDER = os.path.join(os.path.abspath(os.path.dirname(__file__)), "")
SUPPRESS_CHECKS = {
    "protected-access": [
        ClassChecker.visit_assign,
        ClassChecker.visit_attribute,
    ],
    "blacklisted-name": [
        NameChecker.visit_global,
        NameChecker.visit_assignname,
    ],
}


def is_node_in_tests(node: "node_classes.NodeNG"):
    module: "scoped_nodes.Module" = node.root()
    return module.file.startswith(TESTS_FOLDER)


def register(linter: "PyLinter"):  # noqa
    try:
        from pylint_plugin_utils import suppress_message
    except ImportError:
        print("Cannot suppress message. 'pylint_plugin_utils' not installed.")
        return

    print("Registered custom plugin. Some checks will be disabled for tests.")
    for msg, checks in SUPPRESS_CHECKS.items():
        for checker_method in checks:
            suppress_message(linter, checker_method, msg, is_node_in_tests)
