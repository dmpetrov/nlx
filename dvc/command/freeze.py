import argparse
import logging

from dvc.command import completion
from dvc.command.base import CmdBase, append_doc_link
from dvc.exceptions import DvcException

logger = logging.getLogger(__name__)


class CmdFreezeBase(CmdBase):
    def _run(self, func, name):
        ret = 0
        for target in self.args.targets:
            try:
                func(target)
            except DvcException:
                logger.exception(f"failed to {name} '{target}'")
                ret = 1
        return ret


class CmdFreeze(CmdFreezeBase):
    def run(self):
        return self._run(self.repo.freeze, "freeze")


class CmdUnfreeze(CmdFreezeBase):
    def run(self):
        return self._run(self.repo.unfreeze, "unfreeze")


def add_parser(subparsers, parent_parser):
    FREEZE_HELP = "Freeze stages or .dvc files."
    freeze_parser = subparsers.add_parser(
        "freeze",
        parents=[parent_parser],
        description=append_doc_link(FREEZE_HELP, "freeze"),
        help=FREEZE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    freeze_parser.add_argument(
        "targets", nargs="+", help="Stages or .dvc files to freeze"
    ).complete = completion.DVC_FILE
    freeze_parser.set_defaults(func=CmdFreeze)

    UNFREEZE_HELP = "Unfreeze stages or .dvc files."
    unfreeze_parser = subparsers.add_parser(
        "unfreeze",
        parents=[parent_parser],
        description=append_doc_link(UNFREEZE_HELP, "unfreeze"),
        help=UNFREEZE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    unfreeze_parser.add_argument(
        "targets", nargs="+", help="Stages or .dvc files to unfreeze"
    ).complete = completion.DVC_FILE
    unfreeze_parser.set_defaults(func=CmdUnfreeze)
