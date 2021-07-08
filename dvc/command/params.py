import argparse
import logging

from dvc.command import completion
from dvc.command.base import CmdBase, append_doc_link, fix_subparsers
from dvc.exceptions import DvcException
from dvc.ui import ui

logger = logging.getLogger(__name__)


class CmdParamsDiff(CmdBase):
    UNINITIALIZED = True

    def run(self):
        try:
            diff = self.repo.params.diff(
                a_rev=self.args.a_rev,
                b_rev=self.args.b_rev,
                targets=self.args.targets,
                all=self.args.all,
                deps=self.args.deps,
            )
        except DvcException:
            logger.exception("failed to show params diff")
            return 1

        if self.args.show_json:
            import json

            ui.write(json.dumps(diff))
        else:
            from dvc.compare import show_diff

            show_diff(
                diff,
                title="Param",
                markdown=self.args.show_md,
                no_path=self.args.no_path,
                show_changes=False,
            )

        return 0


def add_parser(subparsers, parent_parser):
    PARAMS_HELP = "Commands to display params."

    params_parser = subparsers.add_parser(
        "params",
        parents=[parent_parser],
        description=append_doc_link(PARAMS_HELP, "params"),
        help=PARAMS_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    params_subparsers = params_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc params CMD --help` to display command-specific help.",
    )

    fix_subparsers(params_subparsers)

    PARAMS_DIFF_HELP = (
        "Show changes in params between commits in the DVC repository, or "
        "between a commit and the workspace."
    )
    params_diff_parser = params_subparsers.add_parser(
        "diff",
        parents=[parent_parser],
        description=append_doc_link(PARAMS_DIFF_HELP, "params/diff"),
        help=PARAMS_DIFF_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    params_diff_parser.add_argument(
        "a_rev", nargs="?", help="Old Git commit to compare (defaults to HEAD)"
    )
    params_diff_parser.add_argument(
        "b_rev",
        nargs="?",
        help=("New Git commit to compare (defaults to the current workspace)"),
    )
    params_diff_parser.add_argument(
        "--targets",
        nargs="*",
        help=(
            "Specific params file(s) to compare "
            "(even if not found as `params` in `dvc.yaml`). "
            "Shows all tracked params by default."
        ),
        metavar="<paths>",
    ).complete = completion.FILE
    params_diff_parser.add_argument(
        "--all",
        action="store_true",
        default=False,
        help="Show unchanged params as well.",
    )
    params_diff_parser.add_argument(
        "--deps",
        action="store_true",
        default=False,
        help="Show only params that are stage dependencies.",
    )
    params_diff_parser.add_argument(
        "--show-json",
        action="store_true",
        default=False,
        help="Show output in JSON format.",
    )
    params_diff_parser.add_argument(
        "--show-md",
        action="store_true",
        default=False,
        help="Show tabulated output in the Markdown format (GFM).",
    )
    params_diff_parser.add_argument(
        "--no-path",
        action="store_true",
        default=False,
        help="Don't show params path.",
    )
    params_diff_parser.set_defaults(func=CmdParamsDiff)
