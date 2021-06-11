import logging
from typing import Iterable, Optional

from dvc.repo import locked
from dvc.utils.cli_parse import loads_param_overrides

logger = logging.getLogger(__name__)


@locked
def run(
    repo,
    targets: Optional[Iterable[str]] = None,
    params: Optional[Iterable[str]] = None,
    run_all: bool = False,
    jobs: int = 1,
    tmp_dir: bool = False,
    **kwargs,
) -> dict:
    """Reproduce the specified targets as an experiment.

    Accepts the same additional kwargs as Repo.reproduce.

    Returns a dict mapping new experiment SHAs to the results
    of `repro` for that experiment.
    """
    if run_all:
        return repo.experiments.reproduce_queued(jobs=jobs)

    if params:
        params = loads_param_overrides(params)
    return repo.experiments.reproduce_one(
        targets=targets, params=params, tmp_dir=tmp_dir, **kwargs
    )
