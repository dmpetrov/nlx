import argparse
import logging

from dvc.command.base import append_doc_link, CmdBase, fix_subparsers
from dvc.exceptions import DvcException
from dvc.utils import format_link

logger = logging.getLogger(__name__)


class CmdPlotShow(CmdBase):
    def run(self):
        try:
            self.repo.plot(
                datafile=self.args.datafile,
                template=self.args.template,
                file=self.args.file,
            )

        except DvcException:
            logger.exception("failed to plot metrics")
            return 1
        return 0


class CmdPlotDiff(CmdBase):
    def run(self):
        try:
            self.repo.plot(
                self.args.datafile,
                self.args.template,
                revisions=self.args.revisions,
                file=self.args.file,
            )

        except DvcException:
            logger.exception("failed to plot metrics diff")
            return 1

        return 0


def add_parser(subparsers, parent_parser):
    PLOT_HELP = (
        "For visualisation of metrics stored in structured files ("
        "json, csv, tsv)."
    )

    plot_parser = subparsers.add_parser(
        "plot",
        parents=[parent_parser],
        description=append_doc_link(PLOT_HELP, "plot"),
        help=PLOT_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    plot_subparsers = plot_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc plot CMD --help` to display command-specific help.",
    )

    fix_subparsers(plot_subparsers)

    SHOW_HELP = "Visualize target dvct file using {}.".format(
        format_link("https://vega.github.io")
    )
    plot_show_parser = plot_subparsers.add_parser(
        "show",
        parents=[parent_parser],
        description=append_doc_link(SHOW_HELP, "plot/show"),
        help=SHOW_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    plot_show_parser.add_argument(
        "-t",
        "--template",
        nargs="?",
        default=None,
        help="File to be injected with data.",
    )
    plot_show_parser.add_argument(
        "datafile", nargs="?", default=None, help="Data to be visualized."
    )
    plot_show_parser.add_argument(
        "-f", "--file", help="Specify name of the file it generates."
    )
    plot_show_parser.set_defaults(func=CmdPlotShow)

    PLOT_DIFF_HELP = "Plot changes in metrics between commits"
    " in the DVC repository, or between a commit and the workspace."
    plot_diff_parser = plot_subparsers.add_parser(
        "diff",
        parents=[parent_parser],
        description=append_doc_link(PLOT_DIFF_HELP, "plot/diff"),
        help=PLOT_DIFF_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    plot_diff_parser.add_argument(
        "-t",
        "--template",
        nargs="?",
        default=None,
        help=("File to be injected with data."),
    )
    plot_diff_parser.add_argument(
        "-d",
        "--datafile",
        nargs="?",
        default=None,
        help="Data to be visualized.",
    )
    plot_diff_parser.add_argument(
        "-f", "--file", help="Specify name of the file it generates."
    )
    plot_diff_parser.add_argument(
        "revisions",
        nargs="*",
        default=None,
        help="Git revisions to plot from",
    )
    plot_diff_parser.set_defaults(func=CmdPlotDiff)
