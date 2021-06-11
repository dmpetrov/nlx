from dvc.repo.experiments.utils import fix_exp_head


def _revisions(repo, revs, experiment):
    revisions = revs or []
    if experiment and len(revisions) == 1:
        baseline = repo.experiments.get_baseline(revisions[0])
        if baseline:
            revisions.append(baseline[:7])
    if len(revisions) <= 1:
        if len(revisions) == 0 and repo.scm.is_dirty():
            revisions.append(fix_exp_head(repo.scm, "HEAD"))
        revisions.append("workspace")
    return revisions


def diff(repo, *args, revs=None, experiment=False, **kwargs):
    return repo.plots.show(
        *args, revs=_revisions(repo, revs, experiment), **kwargs
    )
