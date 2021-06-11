import argparse
import logging

from dvc.command.base import CmdBaseNoRepo, append_doc_link

logger = logging.getLogger(__name__)


class CmdVersion(CmdBaseNoRepo):
    def run(self):
        from dvc.info import get_dvc_info

        dvc_info = get_dvc_info()
        logger.info(dvc_info)
        return 0


def add_parser(subparsers, parent_parser):
    VERSION_HELP = (
        "Display the DVC version and system/environment information."
    )
    version_parser = subparsers.add_parser(
        "version",
        parents=[parent_parser],
        description=append_doc_link(VERSION_HELP, "version"),
        help=VERSION_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        aliases=["doctor"],
    )
    version_parser.set_defaults(func=CmdVersion)
