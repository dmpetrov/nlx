import argparse
import logging
from itertools import chain, filterfalse
from typing import TYPE_CHECKING, Dict, Iterable, List

from dvc.command import completion
from dvc.command.base import CmdBase, append_doc_link, fix_subparsers
from dvc.utils.cli_parse import parse_params
from dvc.utils.humanize import truncate_text

if TYPE_CHECKING:
    from dvc.output import Output
    from dvc.stage import Stage

logger = logging.getLogger(__name__)

MAX_TEXT_LENGTH = 80


def generate_description(stage: "Stage") -> str:
    def part_desc(outs: Iterable["Output"]) -> str:
        return ", ".join(out.def_path for out in outs)

    if not stage.deps and not stage.outs:
        return "No outputs or dependencies"

    if not stage.outs and stage.deps:
        return "Depends on " + part_desc(stage.deps)

    def is_plot_or_metric(out: "Output"):
        return bool(out.plot) or bool(out.metric)

    desc: List[str] = []

    outs = list(filterfalse(is_plot_or_metric, stage.outs))
    if outs:
        desc.append("Outputs " + part_desc(outs))

    plots_and_metrics = list(filter(is_plot_or_metric, stage.outs))
    if plots_and_metrics:
        desc.append("Reports " + part_desc(plots_and_metrics))

    return "; ".join(desc)


def prepare_description(
    stage: "Stage", max_length: int = MAX_TEXT_LENGTH
) -> str:
    desc = stage.short_description() or generate_description(stage)
    return truncate_text(desc, max_length)


def prepare_stages_data(
    stages: Iterable["Stage"],
    description: bool = True,
    max_length: int = MAX_TEXT_LENGTH,
) -> Dict[str, str]:
    return {
        stage.addressing: prepare_description(stage, max_length=max_length)
        if description
        else ""
        for stage in stages
    }


class CmdStageList(CmdBase):
    def _get_stages(self) -> Iterable["Stage"]:
        if self.args.all:
            stages: List["Stage"] = self.repo.stages  # type: ignore
            logger.trace(  # type: ignore[attr-defined]
                "%d no. of stages found", len(stages)
            )
            return stages

        # removing duplicates while maintaining order
        collected = chain.from_iterable(
            self.repo.stage.collect(
                target=target, recursive=self.args.recursive, accept_group=True
            )
            for target in self.args.targets
        )
        return dict.fromkeys(collected).keys()

    def run(self):
        from dvc.ui import ui

        def log_error(relpath: str, exc: Exception):
            if self.args.fail:
                raise exc
            logger.debug("Stages from %s failed to load", relpath)

        # silence stage collection error by default
        self.repo.stage_collection_error_handler = log_error

        stages = self._get_stages()
        names_only = self.args.names_only

        data = prepare_stages_data(stages, description=not names_only)
        ui.table(data.items())

        return 0


def parse_cmd(commands: List[str]) -> str:
    """
    We need to take into account two cases:

    - ['python code.py foo bar']: Used mainly with dvc as a library
    - ['echo', 'foo bar']: List of arguments received from the CLI

    The second case would need quoting, as it was passed through:
            dvc run echo "foo bar"
    """

    def quote_argument(arg: str):
        if not arg:
            return '""'
        if " " in arg and '"' not in arg:
            return f'"{arg}"'
        return arg

    if len(commands) < 2:
        return " ".join(commands)
    return " ".join(map(quote_argument, commands))


class CmdStageAdd(CmdBase):
    def run(self):
        kwargs = vars(self.args)
        kwargs.update(
            {
                "cmd": parse_cmd(kwargs.pop("cmd")),
                "params": parse_params(self.args.params),
            }
        )
        self.repo.stage.add(**kwargs)
        return 0


def _add_common_args(parser):
    parser.add_argument(
        "-d",
        "--deps",
        action="append",
        default=[],
        help="Declare dependencies for reproducible cmd.",
        metavar="<path>",
    ).complete = completion.FILE
    parser.add_argument(
        "-o",
        "--outs",
        action="append",
        default=[],
        help="Declare output file or directory.",
        metavar="<filename>",
    ).complete = completion.FILE
    parser.add_argument(
        "-O",
        "--outs-no-cache",
        action="append",
        default=[],
        help="Declare output file or directory "
        "(do not put into DVC cache).",
        metavar="<filename>",
    ).complete = completion.FILE
    parser.add_argument(
        "-p",
        "--params",
        action="append",
        default=[],
        help="Declare parameter to use as additional dependency.",
        metavar="[<filename>:]<params_list>",
    ).complete = completion.FILE
    parser.add_argument(
        "-m",
        "--metrics",
        action="append",
        default=[],
        help="Declare output metrics file.",
        metavar="<path>",
    )
    parser.add_argument(
        "-M",
        "--metrics-no-cache",
        action="append",
        default=[],
        help="Declare output metrics file (do not put into DVC cache).",
        metavar="<path>",
    )
    parser.add_argument(
        "--plots",
        action="append",
        default=[],
        help="Declare output plot file.",
        metavar="<path>",
    )
    parser.add_argument(
        "--plots-no-cache",
        action="append",
        default=[],
        help="Declare output plot file (do not put into DVC cache).",
        metavar="<path>",
    )
    parser.add_argument(
        "--live", help="Declare output as dvclive.", metavar="<path>"
    )
    parser.add_argument(
        "--live-no-cache",
        help="Declare output as dvclive (do not put into DVC cache).",
        metavar="<path>",
    )
    parser.add_argument(
        "--live-no-summary",
        action="store_true",
        default=False,
        help="Signal dvclive logger to not dump latest metrics file.",
    )
    parser.add_argument(
        "--live-no-html",
        action="store_true",
        default=False,
        help="Signal dvclive logger to not produce training report.",
    )
    parser.add_argument(
        "-w",
        "--wdir",
        help="Directory within your repo to run your command in.",
        metavar="<path>",
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Overwrite existing stage",
    )
    parser.add_argument(
        "--outs-persist",
        action="append",
        default=[],
        help="Declare output file or directory that will not be "
        "removed upon repro.",
        metavar="<filename>",
    )
    parser.add_argument(
        "--outs-persist-no-cache",
        action="append",
        default=[],
        help="Declare output file or directory that will not be "
        "removed upon repro (do not put into DVC cache).",
        metavar="<filename>",
    )
    parser.add_argument(
        "-c",
        "--checkpoints",
        action="append",
        default=[],
        help="Declare checkpoint output file or directory for 'dvc exp run'. "
        "Not compatible with 'dvc repro'.",
        metavar="<filename>",
    ).complete = completion.FILE
    parser.add_argument(
        "--always-changed",
        action="store_true",
        default=False,
        help="Always consider this DVC-file as changed.",
    )
    parser.add_argument(
        "--external",
        action="store_true",
        default=False,
        help="Allow outputs that are outside of the DVC repository.",
    )
    parser.add_argument(
        "--desc",
        type=str,
        metavar="<text>",
        help=(
            "User description of the stage (optional). "
            "This doesn't affect any DVC operations."
        ),
    )
    parser.add_argument(
        "cmd",
        nargs=argparse.REMAINDER,
        help="Command to execute.",
        metavar="command",
    )


def add_parser(subparsers, parent_parser):
    STAGES_HELP = "Commands to list and create stages."

    stage_parser = subparsers.add_parser(
        "stage",
        parents=[parent_parser],
        description=append_doc_link(STAGES_HELP, "stage"),
        help=STAGES_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    stage_subparsers = stage_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc stage CMD --help` to display command-specific help.",
    )

    fix_subparsers(stage_subparsers)

    STAGE_ADD_HELP = "Create stage"
    stage_add_parser = stage_subparsers.add_parser(
        "add",
        parents=[parent_parser],
        description=append_doc_link(STAGE_ADD_HELP, "stage/add"),
        help=STAGE_ADD_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    stage_add_parser.add_argument(
        "-n", "--name", help="Name of the stage to add", required=True
    )
    _add_common_args(stage_add_parser)
    stage_add_parser.set_defaults(func=CmdStageAdd)

    STAGE_LIST_HELP = "List stages."
    stage_list_parser = stage_subparsers.add_parser(
        "list",
        parents=[parent_parser],
        description=append_doc_link(STAGE_LIST_HELP, "stage/list"),
        help=STAGE_LIST_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    stage_list_parser.add_argument(
        "targets",
        nargs="*",
        default=["dvc.yaml"],
        help=(
            "Show stages from a dvc.yaml/.dvc file or a directory. "
            "'dvc.yaml' by default"
        ),
    )
    stage_list_parser.add_argument(
        "--all",
        action="store_true",
        default=False,
        help="List all of the stages in the repo.",
    )
    stage_list_parser.add_argument(
        "--fail",
        action="store_true",
        default=False,
        help="Fail immediately, do not suppress any syntax errors.",
    )
    stage_list_parser.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        default=False,
        help="List all stages inside the specified directory.",
    )
    stage_list_parser.add_argument(
        "--names-only",
        action="store_true",
        default=False,
        help="List only stage names.",
    )
    stage_list_parser.set_defaults(func=CmdStageList)
