import argparse
import logging

from dvc.command import completion
from dvc.command.base import CmdBase, append_doc_link
from dvc.exceptions import DvcException

logger = logging.getLogger(__name__)


class CmdImport(CmdBase):
    def run(self):
        try:
            self.repo.imp(
                self.args.url,
                self.args.path,
                out=self.args.out,
                fname=self.args.file,
                rev=self.args.rev,
                no_exec=self.args.no_exec,
                desc=self.args.desc,
                jobs=self.args.jobs,
            )
        except DvcException:
            logger.exception(
                "failed to import '{}' from '{}'.".format(
                    self.args.path, self.args.url
                )
            )
            return 1
        return 0


def add_parser(subparsers, parent_parser):
    IMPORT_HELP = (
        "Download file or directory tracked by DVC or by Git "
        "into the workspace, and track it."
    )

    import_parser = subparsers.add_parser(
        "import",
        parents=[parent_parser],
        description=append_doc_link(IMPORT_HELP, "import"),
        help=IMPORT_HELP,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    import_parser.add_argument(
        "url", help="Location of DVC or Git repository to download from"
    )
    import_parser.add_argument(
        "path", help="Path to a file or directory within the repository"
    ).complete = completion.FILE
    import_parser.add_argument(
        "-o",
        "--out",
        nargs="?",
        help="Destination path to download files to",
        metavar="<path>",
    ).complete = completion.DIR
    import_parser.add_argument(
        "--rev",
        nargs="?",
        help="Git revision (e.g. SHA, branch, tag)",
        metavar="<commit>",
    )
    import_parser.add_argument(
        "--file",
        help="Specify name of the .dvc file this command will generate.",
        metavar="<filename>",
    )
    import_parser.add_argument(
        "--no-exec",
        action="store_true",
        default=False,
        help="Only create .dvc file without actually downloading it.",
    )
    import_parser.add_argument(
        "--desc",
        type=str,
        metavar="<text>",
        help=(
            "User description of the data (optional). "
            "This doesn't affect any DVC operations."
        ),
    )
    import_parser.add_argument(
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
    import_parser.set_defaults(func=CmdImport)
