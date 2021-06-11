import logging
from typing import List

from dvc.exceptions import (
    MetricDoesNotExistError,
    NoMetricsFoundError,
    NoMetricsParsedError,
)
from dvc.fs.repo import RepoFileSystem
from dvc.output import Output
from dvc.path_info import PathInfo
from dvc.repo import locked
from dvc.repo.collect import collect
from dvc.repo.live import summary_path_info
from dvc.scm.base import SCMError
from dvc.utils.serialize import YAMLFileCorruptedError, load_yaml

logger = logging.getLogger(__name__)


def _is_metric(out: Output) -> bool:
    return bool(out.metric) or bool(out.live)


def _to_path_infos(metrics: List[Output]) -> List[PathInfo]:
    result = []
    for out in metrics:
        if out.metric:
            result.append(out.path_info)
        elif out.live:
            path_info = summary_path_info(out)
            if path_info:
                result.append(path_info)
    return result


def _collect_metrics(repo, targets, revision, recursive):
    metrics, path_infos = collect(
        repo,
        targets=targets,
        output_filter=_is_metric,
        recursive=recursive,
        rev=revision,
    )
    return _to_path_infos(metrics) + list(path_infos)


def _extract_metrics(metrics, path, rev):
    if isinstance(metrics, (int, float)):
        return metrics

    if not isinstance(metrics, dict):
        return None

    ret = {}
    for key, val in metrics.items():
        m = _extract_metrics(val, path, rev)
        if m not in (None, {}):
            ret[key] = m
        else:
            logger.debug(
                "Could not parse '%s' metric from '%s' at '%s' "
                "due to its unsupported type: '%s'",
                key,
                path,
                rev,
                type(val).__name__,
            )

    return ret


def _read_metrics(repo, metrics, rev):
    fs = RepoFileSystem(repo)

    res = {}
    for metric in metrics:
        if not fs.isfile(metric):
            continue

        try:
            val = load_yaml(metric, fs=fs)
        except (FileNotFoundError, YAMLFileCorruptedError):
            logger.debug(
                "failed to read '%s' on '%s'", metric, rev, exc_info=True
            )
            continue

        val = _extract_metrics(val, metric, rev)
        if val not in (None, {}):
            res[str(metric)] = val

    return res


@locked
def show(
    repo,
    targets=None,
    all_branches=False,
    all_tags=False,
    recursive=False,
    revs=None,
    all_commits=False,
):
    res = {}
    metrics_found = False

    for rev in repo.brancher(
        revs=revs,
        all_branches=all_branches,
        all_tags=all_tags,
        all_commits=all_commits,
    ):
        metrics = _collect_metrics(repo, targets, rev, recursive)

        if not metrics_found and metrics:
            metrics_found = True

        vals = _read_metrics(repo, metrics, rev)

        if vals:
            res[rev] = vals

    if not res:
        if metrics_found:
            raise NoMetricsParsedError("metrics")
        elif targets:
            raise MetricDoesNotExistError(targets)
        else:
            raise NoMetricsFoundError("metrics", "-m/-M")

    # Hide workspace metrics if they are the same as in the active branch
    try:
        active_branch = repo.scm.active_branch()
    except (TypeError, SCMError):
        # TypeError - detached head
        # SCMError - no repo case
        pass
    else:
        if res.get("workspace") == res.get(active_branch):
            res.pop("workspace", None)

    return res
