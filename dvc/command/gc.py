import argparse
import logging
import os

from dvc import prompt
from dvc.command.base import CmdBase, append_doc_link

logger = logging.getLogger(__name__)


class CmdGC(CmdBase):
    def run(self):
        from dvc.repo.gc import _raise_error_if_all_disabled

        _raise_error_if_all_disabled(
            all_branches=self.args.all_branches,
            all_tags=self.args.all_tags,
            all_commits=self.args.all_commits,
            workspace=self.args.workspace,
        )

        msg = "This will remove all cache except items used in "

        msg += "the workspace"
        if self.args.all_commits:
            msg += " and all git commits"
        elif self.args.all_branches and self.args.all_tags:
            msg += " and all git branches and tags"
        elif self.args.all_branches:
            msg += " and all git branches"
        elif self.args.all_tags:
            msg += " and all git tags"
        elif self.args.all_experiments:
            msg += " and all experiments"

        if self.args.repos:
            msg += " of the current and the following repos:"

            for repo_path in self.args.repos:
                msg += "\n  - %s" % os.path.abspath(repo_path)
        else:
            msg += " of the current repo."

        logger.warning(msg)

        msg = "Are you sure you want to proceed?"
        if not self.args.force and not prompt.confirm(msg):
            return 1

        self.repo.gc(
            all_branches=self.args.all_branches,
            all_tags=self.args.all_tags,
            all_commits=self.args.all_commits,
            all_experiments=self.args.all_experiments,
            cloud=self.args.cloud,
            remote=self.args.remote,
            force=self.args.force,
            jobs=self.args.jobs,
            repos=self.args.repos,
            workspace=self.args.workspace,
        )
        return 0


def add_parser(subparsers, parent_parser):
    GC_HELP = "Garbage collect unused objects from cache or remote storage."
    GC_DESCRIPTION = (
        "Removes all files in the cache or a remote which are not in\n"
        "use by the specified Git revisions (defaults to just HEAD)."
    )
    gc_parser = subparsers.add_parser(
        "gc",
        parents=[parent_parser],
        description=append_doc_link(GC_DESCRIPTION, "gc"),
        help=GC_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    gc_parser.add_argument(
        "-w",
        "--workspace",
        action="store_true",
        default=False,
        help="Keep data files used in the current workspace.",
    )
    gc_parser.add_argument(
        "-a",
        "--all-branches",
        action="store_true",
        default=False,
        help="Keep data files for the tips of all Git branches.",
    )
    gc_parser.add_argument(
        "-T",
        "--all-tags",
        action="store_true",
        default=False,
        help="Keep data files for all Git tags.",
    )
    gc_parser.add_argument(
        "-A",
        "--all-commits",
        action="store_true",
        default=False,
        help="Keep data files for all Git commits.",
    )
    gc_parser.add_argument(
        "--all-experiments",
        action="store_true",
        default=False,
        help="Keep data files for all experiments.",
    )
    gc_parser.add_argument(
        "-c",
        "--cloud",
        action="store_true",
        default=False,
        help="Collect garbage in remote repository.",
    )
    gc_parser.add_argument(
        "-r",
        "--remote",
        help="Remote storage to collect garbage in",
        metavar="<name>",
    )
    gc_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Force garbage collection - automatically agree to all prompts.",
    )
    gc_parser.add_argument(
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
    gc_parser.add_argument(
        "-p",
        "--projects",
        dest="repos",
        type=str,
        nargs="*",
        help="Keep data files required by these projects "
        "in addition to the current one. "
        "Useful if you share a single cache across repos.",
        metavar="<paths>",
    )
    gc_parser.set_defaults(func=CmdGC)
