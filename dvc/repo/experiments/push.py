import logging

from dvc.exceptions import DvcException, InvalidArgumentError
from dvc.repo import locked
from dvc.repo.scm_context import scm_context

from .utils import exp_commits, exp_refs_by_name

logger = logging.getLogger(__name__)


@locked
@scm_context
def push(
    repo, git_remote, exp_name, *args, force=False, push_cache=False, **kwargs
):
    exp_ref = _get_exp_ref(repo, exp_name)

    def on_diverged(refname: str, rev: str) -> bool:
        if repo.scm.get_ref(refname) == rev:
            return True
        raise DvcException(
            f"Local experiment '{exp_name}' has diverged from remote "
            "experiment with the same name. To override the remote experiment "
            "re-run with '--force'."
        )

    refname = str(exp_ref)
    logger.debug("git push experiment '%s' -> '%s'", exp_ref, git_remote)
    repo.scm.push_refspec(
        git_remote, refname, refname, force=force, on_diverged=on_diverged
    )

    if push_cache:
        _push_cache(repo, exp_ref, **kwargs)


def _get_exp_ref(repo, exp_name):
    if exp_name.startswith("refs/"):
        return exp_name

    exp_refs = list(exp_refs_by_name(repo.scm, exp_name))
    if not exp_refs:
        raise InvalidArgumentError(
            f"'{exp_name}' is not a valid experiment name"
        )
    if len(exp_refs) > 1:
        cur_rev = repo.scm.get_rev()
        for info in exp_refs:
            if info.baseline_sha == cur_rev:
                return info
        msg = [
            (
                f"Ambiguous name '{exp_name}' refers to multiple "
                "experiments. Use full refname to push one of the "
                "following:"
            ),
            "",
        ]
        msg.extend([f"\t{info}" for info in exp_refs])
        raise InvalidArgumentError("\n".join(msg))
    return exp_refs[0]


def _push_cache(repo, exp_ref, dvc_remote=None, jobs=None, run_cache=False):
    revs = list(exp_commits(repo.scm, [exp_ref]))
    logger.debug("dvc push experiment '%s'", exp_ref)
    repo.push(jobs=jobs, remote=dvc_remote, run_cache=run_cache, revs=revs)
