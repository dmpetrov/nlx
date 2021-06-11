from dvc.exceptions import MetricsError
from dvc.repo.experiments.utils import fix_exp_head
from dvc.utils.diff import diff as _diff
from dvc.utils.diff import format_dict


def _get_metrics(repo, *args, revs=None, **kwargs):
    try:
        metrics = repo.metrics.show(*args, **kwargs, revs=revs)
        return metrics
    except MetricsError:
        return {}


def diff(repo, *args, a_rev=None, b_rev=None, **kwargs):
    if repo.scm.no_commits:
        return {}

    with_unchanged = kwargs.pop("all", False)

    a_rev = a_rev or "HEAD"
    a_rev = fix_exp_head(repo.scm, a_rev)
    b_rev = fix_exp_head(repo.scm, b_rev) or "workspace"

    metrics = _get_metrics(repo, *args, **kwargs, revs=[a_rev, b_rev])
    old = metrics.get(a_rev, {})
    new = metrics.get(b_rev, {})

    return _diff(
        format_dict(old), format_dict(new), with_unchanged=with_unchanged
    )
