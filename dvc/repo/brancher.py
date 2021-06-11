from funcy import group_by


def brancher(  # noqa: E302
    self,
    revs=None,
    all_branches=False,
    all_tags=False,
    all_commits=False,
    all_experiments=False,
    sha_only=False,
):
    """Generator that iterates over specified revisions.

    Args:
        revs (list): a list of revisions to iterate over.
        all_branches (bool): iterate over all available branches.
        all_commits (bool): iterate over all commits.
        all_tags (bool): iterate over all available tags.
        sha_only (bool): only return git SHA for a revision.

    Yields:
        str: the display name for the currently selected fs, it could be:
            - a git revision identifier
            - empty string it there is no branches to iterate over
            - "workspace" if there are uncommitted changes in the SCM repo
    """
    if not any([revs, all_branches, all_tags, all_commits, all_experiments]):
        yield ""
        return

    from dvc.fs.local import LocalFileSystem

    saved_fs = self.fs
    revs = revs.copy() if revs else []

    scm = self.scm

    self.fs = LocalFileSystem(url=self.root_dir)
    yield "workspace"

    if revs and "workspace" in revs:
        revs.remove("workspace")

    if all_commits:
        revs = scm.list_all_commits()
    else:
        if all_branches:
            revs.extend(scm.list_branches())

        if all_tags:
            revs.extend(scm.list_tags())

    if all_experiments:
        from dvc.repo.experiments.utils import exp_commits

        revs.extend(exp_commits(scm))

    try:
        if revs:
            for sha, names in group_by(scm.resolve_rev, revs).items():
                self.fs = scm.get_fs(sha)
                # ignore revs that don't contain repo root
                # (i.e. revs from before a subdir=True repo was init'ed)
                if self.fs.exists(self.root_dir):
                    if sha_only:
                        yield sha
                    else:
                        yield ", ".join(names)
    finally:
        self.fs = saved_fs
