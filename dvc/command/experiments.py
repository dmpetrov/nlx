import argparse
import io
import logging
import os
from collections import OrderedDict
from datetime import date
from itertools import groupby
from typing import Iterable, Optional

from dvc.command import completion
from dvc.command.base import CmdBase, append_doc_link, fix_subparsers
from dvc.command.metrics import DEFAULT_PRECISION, _show_metrics
from dvc.command.status import CmdDataStatus
from dvc.dvcfile import PIPELINE_FILE
from dvc.exceptions import DvcException, InvalidArgumentError
from dvc.utils.flatten import flatten

logger = logging.getLogger(__name__)


def _filter_names(
    names: Iterable,
    label: str,
    include: Optional[Iterable],
    exclude: Optional[Iterable],
):
    if include and exclude:
        intersection = set(include) & set(exclude)
        if intersection:
            values = ", ".join(intersection)
            raise InvalidArgumentError(
                f"'{values}' specified in both --include-{label} and"
                f" --exclude-{label}"
            )

    names = [tuple(name.split(".")) for name in names]

    def _filter(filters, update_func):
        filters = [tuple(name.split(".")) for name in filters]
        for length, groups in groupby(filters, len):
            for group in groups:
                matches = [name for name in names if name[:length] == group]
                if not matches:
                    name = ".".join(group)
                    raise InvalidArgumentError(
                        f"'{name}' does not match any known {label}"
                    )
                update_func({match: None for match in matches})

    if include:
        ret = OrderedDict()
        _filter(include, ret.update)
    else:
        ret = OrderedDict({name: None for name in names})

    if exclude:
        _filter(exclude, ret.difference_update)

    return [".".join(name) for name in ret]


def _update_names(names, items):
    for name, item in items:
        if isinstance(item, dict):
            item = flatten(item)
            names.update(item.keys())
        else:
            names[name] = None


def _collect_names(all_experiments, **kwargs):
    metric_names = set()
    param_names = set()

    for _, experiments in all_experiments.items():
        for exp in experiments.values():
            _update_names(metric_names, exp.get("metrics", {}).items())
            _update_names(param_names, exp.get("params", {}).items())

    metric_names = _filter_names(
        sorted(metric_names),
        "metrics",
        kwargs.get("include_metrics"),
        kwargs.get("exclude_metrics"),
    )
    param_names = _filter_names(
        sorted(param_names),
        "params",
        kwargs.get("include_params"),
        kwargs.get("exclude_params"),
    )

    return metric_names, param_names


def _collect_rows(
    base_rev,
    experiments,
    metric_names,
    param_names,
    precision=DEFAULT_PRECISION,
    no_timestamp=False,
    sort_by=None,
    sort_order=None,
):
    if sort_by:
        if sort_by in metric_names:
            sort_type = "metrics"
        elif sort_by in param_names:
            sort_type = "params"
        else:
            raise InvalidArgumentError(f"Unknown sort column '{sort_by}'")
        reverse = sort_order == "desc"
        experiments = _sort_exp(experiments, sort_by, sort_type, reverse)

    for i, (rev, exp) in enumerate(experiments.items()):
        row = []
        style = None
        queued = "*" if exp.get("queued", False) else ""

        if rev == "baseline":
            name = exp.get("name", base_rev)
            row.append(f"{name}")
            style = "bold"
        elif i < len(experiments) - 1:
            row.append(f"├── {queued}{rev[:7]}")
        else:
            row.append(f"└── {queued}{rev[:7]}")

        if not no_timestamp:
            row.append(_format_time(exp.get("timestamp")))

        _extend_row(
            row, metric_names, exp.get("metrics", {}).items(), precision
        )
        _extend_row(row, param_names, exp.get("params", {}).items(), precision)

        yield row, style


def _sort_exp(experiments, sort_by, typ, reverse):
    if "baseline" in experiments:
        ret = OrderedDict({"baseline": experiments.pop("baseline")})
    else:
        ret = OrderedDict()

    def _sort(item):
        _, exp = item
        for fname, item in exp.get(typ, {}).items():
            if isinstance(item, dict):
                item = flatten(item)
            else:
                item = {fname: item}
            if sort_by in item:
                val = item[sort_by]
                return (val is None, val)
        return (True, None)

    ret.update(sorted(experiments.items(), key=_sort, reverse=reverse))
    return ret


def _format_time(timestamp):
    if timestamp is None:
        return "-"
    if timestamp.date() == date.today():
        fmt = "%I:%M %p"
    else:
        fmt = "%b %d, %Y"
    return timestamp.strftime(fmt)


def _extend_row(row, names, items, precision):
    def _round(val):
        if isinstance(val, float):
            return round(val, precision)

        return val

    if not items:
        row.extend(["-"] * len(names))
        return

    for fname, item in items:
        if isinstance(item, dict):
            item = flatten(item)
        else:
            item = {fname: item}
        for name in names:
            if name in item:
                value = item[name]
                text = str(_round(value)) if value is not None else "-"
                row.append(text)
            else:
                row.append("-")


def _parse_list(param_list):
    ret = []
    for param_str in param_list:
        # we don't care about filename prefixes for show, silently
        # ignore it if provided to keep usage consistent with other
        # metric/param list command options
        _, _, param_str = param_str.rpartition(":")
        ret.extend(param_str.split(","))
    return ret


def _show_experiments(all_experiments, console, **kwargs):
    from rich.table import Table

    from dvc.scm.git import Git

    include_metrics = _parse_list(kwargs.pop("include_metrics", []))
    exclude_metrics = _parse_list(kwargs.pop("exclude_metrics", []))
    include_params = _parse_list(kwargs.pop("include_params", []))
    exclude_params = _parse_list(kwargs.pop("exclude_params", []))

    metric_names, param_names = _collect_names(
        all_experiments,
        include_metrics=include_metrics,
        exclude_metrics=exclude_metrics,
        include_params=include_params,
        exclude_params=exclude_params,
    )

    table = Table()
    table.add_column("Experiment", no_wrap=True)
    if not kwargs.get("no_timestamp", False):
        table.add_column("Created")
    for name in metric_names:
        table.add_column(name, justify="right", no_wrap=True)
    for name in param_names:
        table.add_column(name, justify="left")

    for base_rev, experiments in all_experiments.items():
        if Git.is_sha(base_rev):
            base_rev = base_rev[:7]

        for row, _, in _collect_rows(
            base_rev, experiments, metric_names, param_names, **kwargs,
        ):
            table.add_row(*row)

    console.print(table)


class CmdExperimentsShow(CmdBase):
    def run(self):
        from rich.console import Console

        from dvc.utils.pager import pager

        if not self.repo.experiments:
            return 0

        try:
            all_experiments = self.repo.experiments.show(
                all_branches=self.args.all_branches,
                all_tags=self.args.all_tags,
                all_commits=self.args.all_commits,
                sha_only=self.args.sha,
            )

            if self.args.no_pager:
                console = Console()
            else:
                # Note: rich does not currently include a native way to force
                # infinite width for use with a pager
                console = Console(
                    file=io.StringIO(), force_terminal=True, width=9999
                )

            _show_experiments(
                all_experiments,
                console,
                include_metrics=self.args.include_metrics,
                exclude_metrics=self.args.exclude_metrics,
                include_params=self.args.include_params,
                exclude_params=self.args.exclude_params,
                no_timestamp=self.args.no_timestamp,
                sort_by=self.args.sort_by,
                sort_order=self.args.sort_order,
            )

            if not self.args.no_pager:
                pager(console.file.getvalue())
        except DvcException:
            logger.exception("failed to show experiments")
            return 1

        return 0


class CmdExperimentsCheckout(CmdBase):
    def run(self):
        if not self.repo.experiments:
            return 0

        self.repo.experiments.checkout(self.args.experiment)

        return 0


def _show_diff(
    diff, title="", markdown=False, no_path=False, old=False, precision=None
):
    from dvc.utils.diff import table

    if precision is None:
        precision = DEFAULT_PRECISION

    def _round(val):
        if isinstance(val, float):
            return round(val, precision)

        return val

    rows = []
    for fname, diff_ in diff.items():
        sorted_diff = OrderedDict(sorted(diff_.items()))
        for item, change in sorted_diff.items():
            row = [] if no_path else [fname]
            row.append(item)
            if old:
                row.append(_round(change.get("old")))
            row.append(_round(change["new"]))
            row.append(_round(change.get("diff", "diff not supported")))
            rows.append(row)

    header = [] if no_path else ["Path"]
    header.append(title)
    if old:
        header.extend(["Old", "New"])
    else:
        header.append("Value")
    header.append("Change")

    return table(header, rows, markdown)


class CmdExperimentsDiff(CmdBase):
    def run(self):
        if not self.repo.experiments:
            return 0

        try:
            diff = self.repo.experiments.diff(
                a_rev=self.args.a_rev,
                b_rev=self.args.b_rev,
                all=self.args.all,
            )

            if self.args.show_json:
                import json

                logger.info(json.dumps(diff))
            else:
                diffs = [("metrics", "Metric"), ("params", "Param")]
                for key, title in diffs:
                    table = _show_diff(
                        diff[key],
                        title=title,
                        markdown=self.args.show_md,
                        no_path=self.args.no_path,
                        old=self.args.old,
                        precision=self.args.precision,
                    )
                    if table:
                        logger.info(table)
                        logger.info("")

        except DvcException:
            logger.exception("failed to show experiments diff")
            return 1

        return 0


class CmdExperimentsRun(CmdBase):
    def run(self):
        if not self.repo.experiments:
            return 0

        saved_dir = os.path.realpath(os.curdir)
        os.chdir(self.args.cwd)

        # Dirty hack so the for loop below can at least enter once
        if self.args.all_pipelines:
            self.args.targets = [None]
        elif not self.args.targets:
            self.args.targets = self.default_targets

        ret = 0
        for target in self.args.targets:
            try:
                stages = self.repo.reproduce(
                    target,
                    single_item=self.args.single_item,
                    force=self.args.force,
                    dry=self.args.dry,
                    interactive=self.args.interactive,
                    pipeline=self.args.pipeline,
                    all_pipelines=self.args.all_pipelines,
                    run_cache=not self.args.no_run_cache,
                    no_commit=self.args.no_commit,
                    downstream=self.args.downstream,
                    recursive=self.args.recursive,
                    force_downstream=self.args.force_downstream,
                    experiment=True,
                    queue=self.args.queue,
                    run_all=self.args.run_all,
                    jobs=self.args.jobs,
                    params=self.args.params,
                    checkpoint=(
                        self.args.checkpoint or self.args.checkpoint_continue
                    ),
                    checkpoint_continue=self.args.checkpoint_continue,
                )

                if len(stages) == 0:
                    logger.info(CmdDataStatus.UP_TO_DATE_MSG)

                if self.args.metrics:
                    metrics = self.repo.metrics.show()
                    logger.info(_show_metrics(metrics))

            except DvcException:
                logger.exception("")
                ret = 1
                break

        os.chdir(saved_dir)
        return ret


def add_parser(subparsers, parent_parser):
    EXPERIMENTS_HELP = "Commands to display and compare experiments."

    experiments_parser = subparsers.add_parser(
        "experiments",
        parents=[parent_parser],
        aliases=["exp"],
        description=append_doc_link(EXPERIMENTS_HELP, "experiments"),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    experiments_subparsers = experiments_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc experiments CMD --help` to display "
        "command-specific help.",
    )

    fix_subparsers(experiments_subparsers)

    EXPERIMENTS_SHOW_HELP = "Print experiments."
    experiments_show_parser = experiments_subparsers.add_parser(
        "show",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_SHOW_HELP, "experiments/show"),
        help=EXPERIMENTS_SHOW_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    experiments_show_parser.add_argument(
        "-a",
        "--all-branches",
        action="store_true",
        default=False,
        help="Show metrics for all branches.",
    )
    experiments_show_parser.add_argument(
        "-T",
        "--all-tags",
        action="store_true",
        default=False,
        help="Show metrics for all tags.",
    )
    experiments_show_parser.add_argument(
        "--all-commits",
        action="store_true",
        default=False,
        help="Show metrics for all commits.",
    )
    experiments_show_parser.add_argument(
        "--no-pager",
        action="store_true",
        default=False,
        help="Do not pipe output into a pager.",
    )
    experiments_show_parser.add_argument(
        "--include-metrics",
        action="append",
        default=[],
        help="Include the specified metrics in output table.",
        metavar="<metrics_list>",
    )
    experiments_show_parser.add_argument(
        "--exclude-metrics",
        action="append",
        default=[],
        help="Exclude the specified metrics from output table.",
        metavar="<metrics_list>",
    )
    experiments_show_parser.add_argument(
        "--include-params",
        action="append",
        default=[],
        help="Include the specified params in output table.",
        metavar="<params_list>",
    )
    experiments_show_parser.add_argument(
        "--exclude-params",
        action="append",
        default=[],
        help="Exclude the specified params from output table.",
        metavar="<params_list>",
    )
    experiments_show_parser.add_argument(
        "--sort-by",
        help="Sort related experiments by the specified metric or param.",
        metavar="<metric/param>",
    )
    experiments_show_parser.add_argument(
        "--sort-order",
        help="Sort order to use with --sort-by.",
        choices=("asc", "desc"),
        default="asc",
    )
    experiments_show_parser.add_argument(
        "--no-timestamp",
        action="store_true",
        default=False,
        help="Do not show experiment timestamps.",
    )
    experiments_show_parser.add_argument(
        "--sha",
        action="store_true",
        default=False,
        help="Always show git commit SHAs instead of branch/tag names.",
    )
    experiments_show_parser.set_defaults(func=CmdExperimentsShow)

    EXPERIMENTS_CHECKOUT_HELP = "Checkout experiments."
    experiments_checkout_parser = experiments_subparsers.add_parser(
        "checkout",
        parents=[parent_parser],
        description=append_doc_link(
            EXPERIMENTS_CHECKOUT_HELP, "experiments/checkout"
        ),
        help=EXPERIMENTS_CHECKOUT_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    experiments_checkout_parser.add_argument(
        "experiment", help="Checkout this experiment.",
    )
    experiments_checkout_parser.set_defaults(func=CmdExperimentsCheckout)

    EXPERIMENTS_DIFF_HELP = (
        "Show changes between experiments in the DVC repository."
    )
    experiments_diff_parser = experiments_subparsers.add_parser(
        "diff",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_DIFF_HELP, "experiments/diff"),
        help=EXPERIMENTS_DIFF_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    experiments_diff_parser.add_argument(
        "a_rev", nargs="?", help="Old experiment to compare (defaults to HEAD)"
    )
    experiments_diff_parser.add_argument(
        "b_rev",
        nargs="?",
        help="New experiment to compare (defaults to the current workspace)",
    )
    experiments_diff_parser.add_argument(
        "--all",
        action="store_true",
        default=False,
        help="Show unchanged metrics/params as well.",
    )
    experiments_diff_parser.add_argument(
        "--show-json",
        action="store_true",
        default=False,
        help="Show output in JSON format.",
    )
    experiments_diff_parser.add_argument(
        "--show-md",
        action="store_true",
        default=False,
        help="Show tabulated output in the Markdown format (GFM).",
    )
    experiments_diff_parser.add_argument(
        "--old",
        action="store_true",
        default=False,
        help="Show old metric/param value.",
    )
    experiments_diff_parser.add_argument(
        "--no-path",
        action="store_true",
        default=False,
        help="Don't show metric/param path.",
    )
    experiments_diff_parser.add_argument(
        "--precision",
        type=int,
        help=(
            "Round metrics/params to `n` digits precision after the decimal "
            f"point. Rounds to {DEFAULT_PRECISION} digits by default."
        ),
        metavar="<n>",
    )
    experiments_diff_parser.set_defaults(func=CmdExperimentsDiff)

    EXPERIMENTS_RUN_HELP = (
        "Reproduce complete or partial experiment pipelines."
    )
    experiments_run_parser = experiments_subparsers.add_parser(
        "run",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_RUN_HELP, "experiments/run"),
        help=EXPERIMENTS_RUN_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    experiments_run_parser.add_argument(
        "targets",
        nargs="*",
        help=f"Stages to reproduce. '{PIPELINE_FILE}' by default.",
    ).complete = completion.DVC_FILE
    experiments_run_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Reproduce even if dependencies were not changed.",
    )
    experiments_run_parser.add_argument(
        "-s",
        "--single-item",
        action="store_true",
        default=False,
        help="Reproduce only single data item without recursive dependencies "
        "check.",
    )
    experiments_run_parser.add_argument(
        "-c",
        "--cwd",
        default=os.path.curdir,
        help="Directory within your repo to reproduce from. Note: deprecated "
        "by `dvc --cd <path>`.",
        metavar="<path>",
    )
    experiments_run_parser.add_argument(
        "-m",
        "--metrics",
        action="store_true",
        default=False,
        help="Show metrics after reproduction.",
    )
    experiments_run_parser.add_argument(
        "--dry",
        action="store_true",
        default=False,
        help="Only print the commands that would be executed without "
        "actually executing.",
    )
    experiments_run_parser.add_argument(
        "-i",
        "--interactive",
        action="store_true",
        default=False,
        help="Ask for confirmation before reproducing each stage.",
    )
    experiments_run_parser.add_argument(
        "-p",
        "--pipeline",
        action="store_true",
        default=False,
        help="Reproduce the whole pipeline that the specified stage file "
        "belongs to.",
    )
    experiments_run_parser.add_argument(
        "-P",
        "--all-pipelines",
        action="store_true",
        default=False,
        help="Reproduce all pipelines in the repo.",
    )
    experiments_run_parser.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        default=False,
        help="Reproduce all stages in the specified directory.",
    )
    experiments_run_parser.add_argument(
        "--no-run-cache",
        action="store_true",
        default=False,
        help=(
            "Execute stage commands even if they have already been run with "
            "the same command/dependencies/outputs/etc before."
        ),
    )
    experiments_run_parser.add_argument(
        "--force-downstream",
        action="store_true",
        default=False,
        help="Reproduce all descendants of a changed stage even if their "
        "direct dependencies didn't change.",
    )
    experiments_run_parser.add_argument(
        "--no-commit",
        action="store_true",
        default=False,
        help="Don't put files/directories into cache.",
    )
    experiments_run_parser.add_argument(
        "--downstream",
        action="store_true",
        default=False,
        help="Start from the specified stages when reproducing pipelines.",
    )
    experiments_run_parser.add_argument(
        "--params",
        action="append",
        default=[],
        help="Use the specified param values when reproducing pipelines.",
        metavar="[<filename>:]<params_list>",
    )
    experiments_run_parser.add_argument(
        "--queue",
        action="store_true",
        default=False,
        help="Stage this experiment in the run queue for future execution.",
    )
    experiments_run_parser.add_argument(
        "--run-all",
        action="store_true",
        default=False,
        help="Execute all experiments in the run queue.",
    )
    experiments_run_parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        help="Run the specified number of experiments at a time in parallel.",
        metavar="<number>",
    )
    experiments_run_parser.add_argument(
        "--checkpoint",
        action="store_true",
        default=False,
        help="Reproduce pipelines as a checkpoint experiment.",
    )
    experiments_run_parser.add_argument(
        "--continue",
        nargs=1,
        default=None,
        dest="checkpoint_continue",
        help=(
            "Continue from the specified checkpoint experiment"
            "(implies --checkpoint)."
        ),
    )
    experiments_run_parser.set_defaults(func=CmdExperimentsRun)
