import logging

from dvc.exceptions import InvalidArgumentError
from dvc.repo import locked
from dvc.repo.scm_context import scm_context
from dvc.scm.base import RevError

from .base import InvalidExpRevError
from .utils import exp_refs_by_rev

logger = logging.getLogger(__name__)


@locked
@scm_context
def branch(repo, exp_rev, branch_name, *args, **kwargs):
    try:
        rev = repo.scm.resolve_rev(exp_rev)
    except RevError:
        raise InvalidArgumentError(exp_rev)
    ref_info = None

    ref_infos = list(exp_refs_by_rev(repo.scm, rev))
    if len(ref_infos) == 1:
        ref_info = ref_infos[0]
    elif len(ref_infos) > 1:
        current_rev = repo.scm.get_rev()
        for info in ref_infos:
            if info.baseline_sha == current_rev:
                ref_info = info
                break
        if not ref_info:
            msg = [
                f"Ambiguous experiment name '{exp_rev}' can refer to "
                "multiple experiments. To create a branch use a full "
                "experiment ref:",
                "",
            ]
            msg.extend([str(info) for info in ref_infos])
            raise InvalidArgumentError("\n".join(msg))

    if not ref_info:
        raise InvalidExpRevError(exp_rev)

    branch_ref = f"refs/heads/{branch_name}"
    if repo.scm.get_ref(branch_ref):
        raise InvalidArgumentError(
            f"Git branch '{branch_name}' already exists."
        )

    target = repo.scm.get_ref(str(ref_info))
    repo.scm.set_ref(
        branch_ref,
        target,
        message=f"dvc: Created from experiment '{ref_info.name}'",
    )
    fmt = (
        "Git branch '%s' has been created from experiment '%s'.\n"
        "To switch to the new branch run:\n\n"
        "\tgit checkout %s"
    )
    logger.info(fmt, branch_name, ref_info.name, branch_name)
