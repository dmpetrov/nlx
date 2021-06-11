import argparse
import logging

from dvc.command import completion
from dvc.command.base import CmdBase, append_doc_link
from dvc.exceptions import DvcException

logger = logging.getLogger(__name__)


class CmdUnprotect(CmdBase):
    def run(self):
        for target in self.args.targets:
            try:
                self.repo.unprotect(target)
            except DvcException:
                msg = f"failed to unprotect '{target}'"
                logger.exception(msg)
                return 1
        return 0


def add_parser(subparsers, parent_parser):
    UNPROTECT_HELP = (
        "Unprotect tracked files or directories (when hardlinks or symlinks "
        "have been enabled with `dvc config cache.type`)."
    )
    unprotect_parser = subparsers.add_parser(
        "unprotect",
        parents=[parent_parser],
        description=append_doc_link(UNPROTECT_HELP, "unprotect"),
        help=UNPROTECT_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    unprotect_parser.add_argument(
        "targets", nargs="+", help="Data files/directories to unprotect."
    ).complete = completion.FILE
    unprotect_parser.set_defaults(func=CmdUnprotect)
