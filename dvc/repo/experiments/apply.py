import logging
import os

from dvc.repo import locked
from dvc.repo.scm_context import scm_context
from dvc.scm.base import RevError
from dvc.utils.fs import remove

from .base import (
    EXEC_APPLY,
    ApplyConflictError,
    BaselineMismatchError,
    InvalidExpRevError,
)
from .executor.base import BaseExecutor

logger = logging.getLogger(__name__)


@locked
@scm_context
def apply(repo, rev, force=True, **kwargs):
    from dvc.repo.checkout import checkout as dvc_checkout
    from dvc.scm.base import MergeConflictError, SCMError

    exps = repo.experiments

    try:
        exp_rev = repo.scm.resolve_rev(rev)
        exps.check_baseline(exp_rev)
    except (RevError, BaselineMismatchError) as exc:
        raise InvalidExpRevError(rev) from exc

    stash_rev = exp_rev in exps.stash_revs
    if not stash_rev and not exps.get_branch_by_rev(
        exp_rev, allow_multiple=True
    ):
        raise InvalidExpRevError(exp_rev)

    # Note that we don't use stash_workspace() here since we need finer control
    # over the merge behavior when we unstash everything
    if repo.scm.is_dirty(untracked_files=True):
        logger.debug("Stashing workspace")
        workspace = repo.scm.stash.push(include_untracked=True)
    else:
        workspace = None

    repo.scm.merge(exp_rev, commit=False)

    if workspace:
        try:
            repo.scm.stash.apply(workspace)
        except MergeConflictError as exc:
            # Applied experiment conflicts with user's workspace changes
            if force:
                # prefer applied experiment changes over prior stashed changes
                repo.scm.checkout_index(ours=True)
            else:
                # revert applied changes and restore user's workspace
                repo.scm.reset(hard=True)
                repo.scm.stash.pop()
                raise ApplyConflictError(rev) from exc
        except SCMError as exc:
            raise ApplyConflictError(rev) from exc
        repo.scm.stash.drop()
    repo.scm.reset()

    if stash_rev:
        args_path = os.path.join(repo.tmp_dir, BaseExecutor.PACKED_ARGS_FILE)
        if os.path.exists(args_path):
            remove(args_path)

    dvc_checkout(repo, **kwargs)

    repo.scm.set_ref(EXEC_APPLY, exp_rev)
    logger.info(
        "Changes for experiment '%s' have been applied to your current "
        "workspace.",
        rev,
    )
