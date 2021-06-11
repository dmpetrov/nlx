import argparse
import logging

from dvc.command import completion
from dvc.command.base import CmdBase, append_doc_link
from dvc.exceptions import DvcException

logger = logging.getLogger(__name__)


class CmdRemove(CmdBase):
    def run(self):
        for target in self.args.targets:
            try:
                self.repo.remove(target, outs=self.args.outs)
            except DvcException:
                logger.exception("")
                return 1
        return 0


def add_parser(subparsers, parent_parser):
    REMOVE_HELP = (
        "Remove stages from dvc.yaml and/or"
        " stop tracking files or directories."
    )
    remove_parser = subparsers.add_parser(
        "remove",
        parents=[parent_parser],
        description=append_doc_link(REMOVE_HELP, "remove"),
        help=REMOVE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    remove_parser.add_argument(
        "--outs",
        action="store_true",
        default=False,
        help="Remove outputs as well.",
    )
    remove_parser.add_argument(
        "targets",
        nargs="+",
        help=".dvc files or stages from dvc.yaml to remove.",
    ).complete = completion.DVC_FILE
    remove_parser.set_defaults(func=CmdRemove)
