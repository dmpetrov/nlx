import argparse
import logging

from dvc.command.base import CmdBaseNoRepo, append_doc_link
from dvc.utils import relpath

logger = logging.getLogger(__name__)


class CmdRoot(CmdBaseNoRepo):
    def run(self):
        from dvc.repo import Repo

        logger.info(relpath(Repo.find_root()))
        return 0


def add_parser(subparsers, parent_parser):
    ROOT_HELP = "Return the relative path to the root of the DVC project."
    root_parser = subparsers.add_parser(
        "root",
        parents=[parent_parser],
        description=append_doc_link(ROOT_HELP, "root"),
        help=ROOT_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    root_parser.set_defaults(func=CmdRoot)
