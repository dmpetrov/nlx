import logging

from dvc.repo.experiments.utils import fix_exp_head
from dvc.utils.diff import diff as _diff
from dvc.utils.diff import format_dict

logger = logging.getLogger(__name__)


def diff(repo, *args, a_rev=None, b_rev=None, param_deps=False, **kwargs):
    from dvc.repo.experiments.show import _collect_experiment_commit

    if repo.scm.no_commits:
        return {}

    if a_rev:
        a_rev = fix_exp_head(repo.scm, a_rev)
        rev = repo.scm.resolve_rev(a_rev)
        old = _collect_experiment_commit(repo, rev, param_deps=param_deps)
    else:
        old = _collect_experiment_commit(
            repo, fix_exp_head(repo.scm, "HEAD"), param_deps=param_deps
        )

    if b_rev:
        b_rev = fix_exp_head(repo.scm, b_rev)
        rev = repo.scm.resolve_rev(b_rev)
        new = _collect_experiment_commit(repo, rev, param_deps=param_deps)
    else:
        new = _collect_experiment_commit(
            repo, "workspace", param_deps=param_deps
        )

    with_unchanged = kwargs.pop("all", False)

    return {
        key: _diff(
            format_dict(old[key]),
            format_dict(new[key]),
            with_unchanged=with_unchanged,
        )
        for key in ["metrics", "params"]
    }
