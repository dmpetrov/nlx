import argparse
import logging

from dvc.exceptions import DvcException

from . import completion
from .base import CmdBaseNoRepo, append_doc_link

logger = logging.getLogger(__name__)


class CmdGet(CmdBaseNoRepo):
    def _show_url(self):
        from dvc.api import get_url

        try:
            url = get_url(
                self.args.path, repo=self.args.url, rev=self.args.rev
            )
            logger.info(url)
        except DvcException:
            logger.exception("failed to show URL")
            return 1

        return 0

    def run(self):
        if self.args.show_url:
            return self._show_url()

        return self._get_file_from_repo()

    def _get_file_from_repo(self):
        from dvc.repo import Repo

        try:
            Repo.get(
                self.args.url,
                path=self.args.path,
                out=self.args.out,
                rev=self.args.rev,
                jobs=self.args.jobs,
            )
            return 0
        except DvcException:
            logger.exception(
                "failed to get '{}' from '{}'".format(
                    self.args.path, self.args.url
                )
            )
            return 1


def add_parser(subparsers, parent_parser):
    GET_HELP = "Download file or directory tracked by DVC or by Git."
    get_parser = subparsers.add_parser(
        "get",
        parents=[parent_parser],
        description=append_doc_link(GET_HELP, "get"),
        help=GET_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    get_parser.add_argument(
        "url", help="Location of DVC or Git repository to download from"
    )
    get_parser.add_argument(
        "path", help="Path to a file or directory within the repository"
    ).complete = completion.FILE
    get_parser.add_argument(
        "-o",
        "--out",
        nargs="?",
        help="Destination path to download files to",
        metavar="<path>",
    ).complete = completion.DIR
    get_parser.add_argument(
        "--rev",
        nargs="?",
        help="Git revision (e.g. SHA, branch, tag)",
        metavar="<commit>",
    )
    get_parser.add_argument(
        "--show-url",
        action="store_true",
        help="Print the storage location (URL) the target data would be "
        "downloaded from, and exit.",
    )
    get_parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        help=(
            "Number of jobs to run simultaneously. "
            "The default value is 4 * cpu_count(). "
            "For SSH remotes, the default is 4. "
        ),
        metavar="<number>",
    )
    get_parser.set_defaults(func=CmdGet)
