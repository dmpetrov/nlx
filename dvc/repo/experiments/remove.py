import logging

from dvc.exceptions import InvalidArgumentError
from dvc.repo import locked
from dvc.repo.scm_context import scm_context

from .base import EXPS_NAMESPACE, ExpRefInfo
from .utils import exp_refs_by_name, remove_exp_refs

logger = logging.getLogger(__name__)


@locked
@scm_context
def remove(repo, exp_names=None, queue=False, **kwargs):
    if not exp_names and not queue:
        return 0

    removed = 0
    if queue:
        removed += len(repo.experiments.stash)
        repo.experiments.stash.clear()
    if exp_names:
        ref_infos = list(_get_exp_refs(repo, exp_names))
        remove_exp_refs(repo.scm, ref_infos)
        removed += len(ref_infos)
    return removed


def _get_exp_refs(repo, exp_names):
    cur_rev = repo.scm.get_rev()
    for name in exp_names:
        if name.startswith(EXPS_NAMESPACE):
            if not repo.scm.get_ref(name):
                raise InvalidArgumentError(
                    f"'{name}' is not a valid experiment name"
                )
            yield ExpRefInfo.from_ref(name)
        else:

            exp_refs = list(exp_refs_by_name(repo.scm, name))
            if not exp_refs:
                raise InvalidArgumentError(
                    f"'{name}' is not a valid experiment name"
                )
            yield _get_ref(exp_refs, name, cur_rev)


def _get_ref(ref_infos, name, cur_rev):
    if len(ref_infos) > 1:
        for info in ref_infos:
            if info.baseline_sha == cur_rev:
                return info
        msg = [
            (
                f"Ambiguous name '{name}' refers to multiple "
                "experiments. Use full refname to remove one of "
                "the following:"
            )
        ]
        msg.extend([f"\t{info}" for info in ref_infos])
        raise InvalidArgumentError("\n".join(msg))
    return ref_infos[0]
