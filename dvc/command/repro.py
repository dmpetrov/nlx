import argparse
import logging

from dvc.command import completion
from dvc.command.base import CmdBase, append_doc_link
from dvc.command.status import CmdDataStatus

logger = logging.getLogger(__name__)


class CmdRepro(CmdBase):
    def run(self):
        stages = self.repo.reproduce(**self._repro_kwargs)
        if len(stages) == 0:
            logger.info(CmdDataStatus.UP_TO_DATE_MSG)
        else:
            logger.info(
                "Use `dvc push` to send your updates to " "remote storage."
            )

        if self.args.metrics:
            from dvc.compare import show_metrics

            metrics = self.repo.metrics.show()
            show_metrics(metrics)

        return 0

    @property
    def _repro_kwargs(self):
        return {
            "targets": self.args.targets,
            "single_item": self.args.single_item,
            "force": self.args.force,
            "dry": self.args.dry,
            "interactive": self.args.interactive,
            "pipeline": self.args.pipeline,
            "all_pipelines": self.args.all_pipelines,
            "run_cache": not self.args.no_run_cache,
            "no_commit": self.args.no_commit,
            "downstream": self.args.downstream,
            "recursive": self.args.recursive,
            "force_downstream": self.args.force_downstream,
            "pull": self.args.pull,
            "glob": self.args.glob,
        }


def add_arguments(repro_parser):
    repro_parser.add_argument(
        "targets",
        nargs="*",
        help="Stages to reproduce. 'dvc.yaml' by default.",
    ).complete = completion.DVCFILES_AND_STAGE
    repro_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Reproduce even if dependencies were not changed.",
    )
    repro_parser.add_argument(
        "-s",
        "--single-item",
        action="store_true",
        default=False,
        help="Reproduce only single data item without recursive dependencies "
        "check.",
    )
    repro_parser.add_argument(
        "-m",
        "--metrics",
        action="store_true",
        default=False,
        help="Show metrics after reproduction.",
    )
    repro_parser.add_argument(
        "--dry",
        action="store_true",
        default=False,
        help="Only print the commands that would be executed without "
        "actually executing.",
    )
    repro_parser.add_argument(
        "-i",
        "--interactive",
        action="store_true",
        default=False,
        help="Ask for confirmation before reproducing each stage.",
    )
    repro_parser.add_argument(
        "-p",
        "--pipeline",
        action="store_true",
        default=False,
        help="Reproduce the whole pipeline that the specified targets "
        "belong to.",
    )
    repro_parser.add_argument(
        "-P",
        "--all-pipelines",
        action="store_true",
        default=False,
        help="Reproduce all pipelines in the repo.",
    )
    repro_parser.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        default=False,
        help="Reproduce all stages in the specified directory.",
    )
    repro_parser.add_argument(
        "--no-run-cache",
        action="store_true",
        default=False,
        help=(
            "Execute stage commands even if they have already been run with "
            "the same command/dependencies/outputs/etc before."
        ),
    )
    repro_parser.add_argument(
        "--force-downstream",
        action="store_true",
        default=False,
        help="Reproduce all descendants of a changed stage even if their "
        "direct dependencies didn't change.",
    )
    repro_parser.add_argument(
        "--no-commit",
        action="store_true",
        default=False,
        help="Don't put files/directories into cache.",
    )
    repro_parser.add_argument(
        "--downstream",
        action="store_true",
        default=False,
        help="Start from the specified stages when reproducing pipelines.",
    )
    repro_parser.add_argument(
        "--pull",
        action="store_true",
        default=False,
        help=(
            "Try automatically pulling missing cache for outputs restored "
            "from the run-cache."
        ),
    )
    repro_parser.add_argument(
        "--glob",
        action="store_true",
        default=False,
        help="Allows targets containing shell-style wildcards.",
    )


def add_parser(subparsers, parent_parser):
    REPRO_HELP = (
        "Reproduce complete or partial pipelines by executing their stages."
    )
    repro_parser = subparsers.add_parser(
        "repro",
        parents=[parent_parser],
        description=append_doc_link(REPRO_HELP, "repro"),
        help=REPRO_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    add_arguments(repro_parser)
    repro_parser.set_defaults(func=CmdRepro)
