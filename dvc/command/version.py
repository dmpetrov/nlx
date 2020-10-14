import argparse
import logging

from dvc.command.base import CmdBaseNoRepo, append_doc_link
from dvc.info import get_dvc_info

logger = logging.getLogger(__name__)


class CmdVersion(CmdBaseNoRepo):
    def run(self):
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
    )
    version_parser.set_defaults(func=CmdVersion)

    DOCTOR_HELP = "Alias for 'dvc version'. " + VERSION_HELP
    doctor_parser = subparsers.add_parser(
        "doctor",
        parents=[parent_parser],
        description=append_doc_link(DOCTOR_HELP, "doctor"),
        help=DOCTOR_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    doctor_parser.set_defaults(func=CmdVersion)
