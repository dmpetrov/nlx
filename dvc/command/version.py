import argparse
import logging

from dvc.command.base import CmdBaseNoRepo, append_doc_link
from dvc.ui import ui

logger = logging.getLogger(__name__)


class CmdVersion(CmdBaseNoRepo):
    def run(self):
        from dvc.info import get_dvc_info
        from dvc.repo import NotDvcRepoError, Repo

        dvc_info = get_dvc_info()
        ui.write(dvc_info, force=True)

        try:
            with Repo() as repo:
                from dvc.updater import Updater

                hardlink_lock = repo.config["core"].get("hardlink_lock", False)
                updater = Updater(repo.tmp_dir, hardlink_lock=hardlink_lock)
                updater.check()
        except NotDvcRepoError:
            pass

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
