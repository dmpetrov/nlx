import contextlib
import logging
import os
from typing import TYPE_CHECKING, List, Optional

from dvc.exceptions import MetricDoesNotExistError, MetricsError

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from dvc.output import Output
    from dvc.path_info import PathInfo
    from dvc.repo import Repo


def create_summary(out):
    assert out.live and out.live["html"]

    metrics, plots = out.repo.live.show(str(out.path_info))

    html_path = out.path_info.with_suffix(".html")

    out.repo.plots.write_html(html_path, plots, metrics)
    logger.info(f"\nfile://{os.path.abspath(html_path)}")


def summary_path_info(out: "Output") -> Optional["PathInfo"]:
    from dvc.output import Output

    assert out.live
    has_summary = True
    if isinstance(out.live, dict):
        has_summary = out.live.get(Output.PARAM_LIVE_SUMMARY, True)
    if has_summary:
        return out.path_info.with_suffix(".json")
    return None


class Live:
    def __init__(self, repo: "Repo"):
        self.repo = repo

    def show(self, target: str, revs: List[str] = None):
        if revs:
            revs = ["workspace", *revs]

        if not os.path.exists(target):
            raise MetricDoesNotExistError([target])

        metrics_path = target + ".json"

        metrics = None
        with contextlib.suppress(MetricsError):
            metrics = self.repo.metrics.show(targets=[metrics_path])

        plots = self.repo.plots.show(target, recursive=True, revs=revs)

        return metrics, plots
