from typing import Generator, Iterable, Optional, Set

from dvc.scm.git import Git

from .base import (
    EXEC_BASELINE,
    EXEC_NAMESPACE,
    EXPS_NAMESPACE,
    EXPS_STASH,
    ExpRefInfo,
)


def exp_refs(scm: "Git") -> Generator["ExpRefInfo", None, None]:
    """Iterate over all experiment refs."""
    for ref in scm.iter_refs(base=EXPS_NAMESPACE):
        if ref.startswith(EXEC_NAMESPACE) or ref == EXPS_STASH:
            continue
        yield ExpRefInfo.from_ref(ref)


def exp_refs_by_rev(
    scm: "Git", rev: str
) -> Generator["ExpRefInfo", None, None]:
    """Iterate over all experiment refs pointing to the specified revision."""
    for ref in scm.get_refs_containing(rev, EXPS_NAMESPACE):
        if not (ref.startswith(EXEC_NAMESPACE) or ref == EXPS_STASH):
            yield ExpRefInfo.from_ref(ref)


def exp_refs_by_name(
    scm: "Git", name: str
) -> Generator["ExpRefInfo", None, None]:
    """Iterate over all experiment refs matching the specified name."""
    for ref_info in exp_refs(scm):
        if ref_info.name == name:
            yield ref_info


def exp_refs_by_baseline(
    scm: "Git", rev: str
) -> Generator["ExpRefInfo", None, None]:
    """Iterate over all experiment refs with the specified baseline."""
    ref_info = ExpRefInfo(baseline_sha=rev)
    for ref in scm.iter_refs(base=str(ref_info)):
        if ref.startswith(EXEC_NAMESPACE) or ref == EXPS_STASH:
            continue
        yield ExpRefInfo.from_ref(ref)


def remote_exp_refs(
    scm: "Git", url: str
) -> Generator["ExpRefInfo", None, None]:
    """Iterate over all remote experiment refs."""
    for ref in scm.iter_remote_refs(url, base=EXPS_NAMESPACE):
        if ref.startswith(EXEC_NAMESPACE) or ref == EXPS_STASH:
            continue
        yield ExpRefInfo.from_ref(ref)


def remote_exp_refs_by_name(
    scm: "Git", url: str, name: str
) -> Generator["ExpRefInfo", None, None]:
    """Iterate over all remote experiment refs matching the specified name."""
    for ref_info in remote_exp_refs(scm, url):
        if ref_info.name == name:
            yield ref_info


def remote_exp_refs_by_baseline(
    scm: "Git", url: str, rev: str
) -> Generator["ExpRefInfo", None, None]:
    """Iterate over all remote experiment refs with the specified baseline."""
    ref_info = ExpRefInfo(baseline_sha=rev)
    for ref in scm.iter_remote_refs(url, base=str(ref_info)):
        if ref.startswith(EXEC_NAMESPACE) or ref == EXPS_STASH:
            continue
        yield ExpRefInfo.from_ref(ref)


def exp_commits(
    scm: "Git", ref_infos: Iterable["ExpRefInfo"] = None
) -> Iterable[str]:
    """Iterate over all experiment commits."""
    shas: Set["str"] = set()
    refs = ref_infos if ref_infos else exp_refs(scm)
    for ref_info in refs:
        shas.update(scm.branch_revs(str(ref_info), ref_info.baseline_sha))
        if ref_info.baseline_sha:
            shas.add(ref_info.baseline_sha)
    yield from shas


def remove_exp_refs(scm: "Git", ref_infos: Iterable["ExpRefInfo"]):
    from .base import EXEC_APPLY, EXEC_BRANCH, EXEC_CHECKPOINT

    exec_branch = scm.get_ref(EXEC_BRANCH, follow=False)
    exec_apply = scm.get_ref(EXEC_APPLY)
    exec_checkpoint = scm.get_ref(EXEC_CHECKPOINT)

    for ref_info in ref_infos:
        ref = scm.get_ref(str(ref_info))
        if exec_branch and str(ref_info):
            scm.remove_ref(EXEC_BRANCH)
        if exec_apply and exec_apply == ref:
            scm.remove_ref(EXEC_APPLY)
        if exec_checkpoint and exec_checkpoint == ref:
            scm.remove_ref(EXEC_CHECKPOINT)
        scm.remove_ref(str(ref_info))


def fix_exp_head(scm: "Git", ref: Optional[str]) -> Optional[str]:
    if ref:
        name, tail = Git.split_ref_pattern(ref)
        if name == "HEAD" and scm.get_ref(EXEC_BASELINE):
            return "".join((EXEC_BASELINE, tail))
    return ref
