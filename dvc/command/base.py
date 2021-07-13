import logging
import os
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


def fix_subparsers(subparsers):
    """Workaround for bug in Python 3. See more info at:
    https://bugs.python.org/issue16308
    https://github.com/iterative/dvc/issues/769

    Args:
        subparsers: subparsers to fix.
    """
    subparsers.required = True
    subparsers.dest = "cmd"


def append_doc_link(help_message, path):
    from dvc.utils import format_link

    if not path:
        return help_message
    doc_base = "https://man.dvc.org/"
    return "{message}\nDocumentation: {link}".format(
        message=help_message, link=format_link(doc_base + path)
    )


class CmdBase(ABC):
    UNINITIALIZED = False

    def __init__(self, args):
        from dvc.repo import Repo

        os.chdir(args.cd)

        self.repo = Repo(uninitialized=self.UNINITIALIZED)
        self.config = self.repo.config
        self.args = args

    def do_run(self):
        with self.repo:
            return self.run()

    @abstractmethod
    def run(self):
        pass


class CmdBaseNoRepo(CmdBase):
    def __init__(self, args):  # pylint: disable=super-init-not-called
        self.args = args

        os.chdir(args.cd)

    def do_run(self):
        return self.run()
