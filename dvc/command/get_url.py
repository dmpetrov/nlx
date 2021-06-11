import argparse
import logging

from dvc.exceptions import DvcException

from . import completion
from .base import CmdBaseNoRepo, append_doc_link

logger = logging.getLogger(__name__)


class CmdGetUrl(CmdBaseNoRepo):
    def run(self):
        from dvc.repo import Repo

        try:
            Repo.get_url(self.args.url, out=self.args.out, jobs=self.args.jobs)
            return 0
        except DvcException:
            logger.exception(f"failed to get '{self.args.url}'")
            return 1


def add_parser(subparsers, parent_parser):
    GET_HELP = "Download or copy files from URL."
    get_parser = subparsers.add_parser(
        "get-url",
        parents=[parent_parser],
        description=append_doc_link(GET_HELP, "get-url"),
        help=GET_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    get_parser.add_argument(
        "url", help="See `dvc import-url -h` for full list of supported URLs."
    )
    get_parser.add_argument(
        "out", nargs="?", help="Destination path to put data to."
    ).complete = completion.DIR
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
    get_parser.set_defaults(func=CmdGetUrl)
