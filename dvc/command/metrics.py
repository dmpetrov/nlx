import argparse
import logging

from dvc.command import completion
from dvc.command.base import CmdBase, append_doc_link, fix_subparsers
from dvc.exceptions import BadMetricError, DvcException
from dvc.ui import ui

logger = logging.getLogger(__name__)


DEFAULT_PRECISION = 5


class CmdMetricsBase(CmdBase):
    UNINITIALIZED = True


class CmdMetricsShow(CmdMetricsBase):
    def run(self):
        try:
            metrics = self.repo.metrics.show(
                self.args.targets,
                all_branches=self.args.all_branches,
                all_tags=self.args.all_tags,
                all_commits=self.args.all_commits,
                recursive=self.args.recursive,
            )
        except DvcException:
            logger.exception("")
            return 1

        if self.args.show_json:
            import json

            ui.write(json.dumps(metrics))
        else:
            from dvc.compare import show_metrics

            # When `metrics` contains a `None` key, it means that some files
            # specified as `targets` in `repo.metrics.show` didn't contain any
            # metrics.
            missing = metrics.pop(None, None)
            if missing:
                raise BadMetricError(missing)

            show_metrics(
                metrics,
                markdown=self.args.show_md,
                all_branches=self.args.all_branches,
                all_tags=self.args.all_tags,
                all_commits=self.args.all_commits,
                precision=self.args.precision or DEFAULT_PRECISION,
                round_digits=True,
            )

        return 0


class CmdMetricsDiff(CmdMetricsBase):
    def run(self):
        try:
            diff = self.repo.metrics.diff(
                a_rev=self.args.a_rev,
                b_rev=self.args.b_rev,
                targets=self.args.targets,
                recursive=self.args.recursive,
                all=self.args.all,
            )
        except DvcException:
            logger.exception("failed to show metrics diff")
            return 1

        if self.args.show_json:
            import json

            ui.write(json.dumps(diff))
        else:
            from dvc.compare import show_diff

            show_diff(
                diff,
                title="Metric",
                markdown=self.args.show_md,
                no_path=self.args.no_path,
                precision=self.args.precision or DEFAULT_PRECISION,
                round_digits=True,
            )

        return 0


def add_parser(subparsers, parent_parser):
    METRICS_HELP = "Commands to display and compare metrics."

    metrics_parser = subparsers.add_parser(
        "metrics",
        parents=[parent_parser],
        description=append_doc_link(METRICS_HELP, "metrics"),
        help=METRICS_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    metrics_subparsers = metrics_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc metrics CMD --help` to display command-specific help.",
    )

    fix_subparsers(metrics_subparsers)

    METRICS_SHOW_HELP = "Print metrics, with optional formatting."
    metrics_show_parser = metrics_subparsers.add_parser(
        "show",
        parents=[parent_parser],
        description=append_doc_link(METRICS_SHOW_HELP, "metrics/show"),
        help=METRICS_SHOW_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    metrics_show_parser.add_argument(
        "targets",
        nargs="*",
        help=(
            "Limit command scope to these metrics files. Using -R, "
            "directories to search metrics files in can also be given."
        ),
    ).complete = completion.FILE
    metrics_show_parser.add_argument(
        "-a",
        "--all-branches",
        action="store_true",
        default=False,
        help="Show metrics for all branches.",
    )
    metrics_show_parser.add_argument(
        "-T",
        "--all-tags",
        action="store_true",
        default=False,
        help="Show metrics for all tags.",
    )
    metrics_show_parser.add_argument(
        "-A",
        "--all-commits",
        action="store_true",
        default=False,
        help="Show metrics for all commits.",
    )
    metrics_show_parser.add_argument(
        "--show-json",
        action="store_true",
        default=False,
        help="Show output in JSON format.",
    )
    metrics_show_parser.add_argument(
        "--show-md",
        action="store_true",
        default=False,
        help="Show tabulated output in the Markdown format (GFM).",
    )
    metrics_show_parser.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        default=False,
        help=(
            "If any target is a directory, recursively search and process "
            "metrics files."
        ),
    )
    metrics_show_parser.add_argument(
        "--precision",
        type=int,
        help=(
            "Round metrics to `n` digits precision after the decimal point. "
            f"Rounds to {DEFAULT_PRECISION} digits by default."
        ),
        metavar="<n>",
    )
    metrics_show_parser.set_defaults(func=CmdMetricsShow)

    METRICS_DIFF_HELP = (
        "Show changes in metrics between commits in the DVC repository, or "
        "between a commit and the workspace."
    )
    metrics_diff_parser = metrics_subparsers.add_parser(
        "diff",
        parents=[parent_parser],
        description=append_doc_link(METRICS_DIFF_HELP, "metrics/diff"),
        help=METRICS_DIFF_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    metrics_diff_parser.add_argument(
        "a_rev", nargs="?", help="Old Git commit to compare (defaults to HEAD)"
    )
    metrics_diff_parser.add_argument(
        "b_rev",
        nargs="?",
        help="New Git commit to compare (defaults to the current workspace)",
    )
    metrics_diff_parser.add_argument(
        "--targets",
        nargs="*",
        help=(
            "Specific metrics file(s) to compare "
            "(even if not found as `metrics` in `dvc.yaml`). "
            "Using -R, directories to search metrics files in "
            "can also be given."
            "Shows all tracked metrics by default."
        ),
        metavar="<paths>",
    ).complete = completion.FILE
    metrics_diff_parser.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        default=False,
        help=(
            "If any target is a directory, recursively search and process "
            "metrics files."
        ),
    )
    metrics_diff_parser.add_argument(
        "--all",
        action="store_true",
        default=False,
        help="Show unchanged metrics as well.",
    )
    metrics_diff_parser.add_argument(
        "--show-json",
        action="store_true",
        default=False,
        help="Show output in JSON format.",
    )
    metrics_diff_parser.add_argument(
        "--show-md",
        action="store_true",
        default=False,
        help="Show tabulated output in the Markdown format (GFM).",
    )
    metrics_diff_parser.add_argument(
        "--no-path",
        action="store_true",
        default=False,
        help="Don't show metric path.",
    )
    metrics_diff_parser.add_argument(
        "--precision",
        type=int,
        help=(
            "Round metrics to `n` digits precision after the decimal point. "
            f"Rounds to {DEFAULT_PRECISION} digits by default."
        ),
        metavar="<n>",
    )
    metrics_diff_parser.set_defaults(func=CmdMetricsDiff)
