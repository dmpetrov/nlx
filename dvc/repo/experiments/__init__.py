import logging
import os
import re
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from typing import Iterable, Optional

from funcy import cached_property

from dvc.exceptions import DvcException
from dvc.repo.experiments.executor import ExperimentExecutor, LocalExecutor
from dvc.scm.git import Git
from dvc.stage.serialize import to_lockfile
from dvc.utils import dict_sha256, env2bool, relpath
from dvc.utils.fs import copyfile, remove

logger = logging.getLogger(__name__)


class UnchangedExperimentError(DvcException):
    def __init__(self, rev):
        super().__init__("Experiment identical to baseline '{rev[:7]}'.")
        self.rev = rev


class Experiments:
    """Class that manages experiments in a DVC repo.

    Args:
        repo (dvc.repo.Repo): repo instance that these experiments belong to.
    """

    EXPERIMENTS_DIR = "experiments"
    PACKED_ARGS_FILE = "repro.dat"
    STASH_MSG_PREFIX = "dvc-exp-"
    STASH_EXPERIMENT_RE = re.compile(
        r"(?:On \(.*\): )dvc-exp-(?P<baseline_rev>[0-9a-f]+)$"
    )

    def __init__(self, repo):
        if not (
            env2bool("DVC_TEST")
            or repo.config["core"].get("experiments", False)
        ):
            raise NotImplementedError

        self.repo = repo

    @cached_property
    def exp_dir(self):
        return os.path.join(self.repo.dvc_dir, self.EXPERIMENTS_DIR)

    @cached_property
    def scm(self):
        """Experiments clone scm instance."""
        if os.path.exists(self.exp_dir):
            return Git(self.exp_dir)
        return self._init_clone()

    @cached_property
    def dvc_dir(self):
        return relpath(self.repo.dvc_dir, self.repo.scm.root_dir)

    @cached_property
    def exp_dvc_dir(self):
        return os.path.join(self.exp_dir, self.dvc_dir)

    @cached_property
    def exp_dvc(self):
        """Return clone dvc Repo instance."""
        from dvc.repo import Repo

        return Repo(self.exp_dvc_dir)

    @cached_property
    def args_file(self):
        return os.path.join(self.exp_dvc.tmp_dir, self.PACKED_ARGS_FILE)

    @property
    def stash_reflog(self):
        if "refs/stash" in self.scm.repo.refs:
            return self.scm.repo.refs["refs/stash"].log()
        return []

    @property
    def stash_revs(self):
        revs = {}
        for i, entry in enumerate(self.stash_reflog):
            m = self.STASH_EXPERIMENT_RE.match(entry.message)
            if m:
                revs[entry.newhexsha] = (i, m.group("baseline_rev"))
        return revs

    @staticmethod
    def exp_hash(stages):
        exp_data = {}
        for stage in stages:
            exp_data.update(to_lockfile(stage))
        return dict_sha256(exp_data)

    @contextmanager
    def chdir(self):
        cwd = os.getcwd()
        os.chdir(self.exp_dvc.root_dir)
        yield
        os.chdir(cwd)

    def _init_clone(self):
        src_dir = self.repo.scm.root_dir
        logger.debug("Initializing experiments clone")
        git = Git.clone(src_dir, self.exp_dir)
        self._config_clone()
        return git

    def _config_clone(self):
        dvc_dir = relpath(self.repo.dvc_dir, self.repo.scm.root_dir)
        local_config = os.path.join(self.exp_dir, dvc_dir, "config.local")
        cache_dir = self.repo.cache.local.cache_dir
        logger.debug("Writing experiments local config '%s'", local_config)
        with open(local_config, "w") as fobj:
            fobj.write(f"[cache]\n    dir = {cache_dir}")

    def _scm_checkout(self, rev):
        self.scm.repo.git.reset(hard=True)
        if self.scm.repo.head.is_detached:
            # switch back to default branch
            self.scm.repo.heads[0].checkout()
        if not Git.is_sha(rev) or not self.scm.has_rev(rev):
            self.scm.pull()
        logger.debug("Checking out base experiment commit '%s'", rev)
        self.scm.checkout(rev)

    def _stash_exp(self, *args, **kwargs):
        """Stash changes from the current (parent) workspace as an experiment.
        """
        rev = self.scm.get_rev()
        tmp = tempfile.NamedTemporaryFile(delete=False).name
        try:
            self.repo.scm.repo.git.diff(patch=True, output=tmp)
            if os.path.getsize(tmp):
                logger.debug("Patching experiment workspace")
                self.scm.repo.git.apply(tmp)
            else:
                raise UnchangedExperimentError(rev)
        finally:
            remove(tmp)
        self._pack_args(*args, **kwargs)
        msg = f"{self.STASH_MSG_PREFIX}{rev}"
        self.scm.repo.git.stash("push", "-m", msg)
        return self.scm.resolve_rev("stash@{0}")

    def _pack_args(self, *args, **kwargs):
        ExperimentExecutor.pack_repro_args(self.args_file, *args, **kwargs)
        self.scm.add(self.args_file)

    def _unpack_args(self, tree=None):
        args_file = os.path.join(self.exp_dvc.tmp_dir, self.PACKED_ARGS_FILE)
        return ExperimentExecutor.unpack_repro_args(args_file, tree=tree)

    def _commit(self, stages, check_exists=True, branch=True):
        """Commit stages as an experiment and return the commit SHA."""
        if not self.scm.is_dirty():
            raise UnchangedExperimentError(self.scm.get_rev())
        rev = self.scm.get_rev()
        hash_ = self.exp_hash(stages)
        exp_name = f"{rev[:7]}-{hash_}"
        if branch:
            if check_exists and exp_name in self.scm.list_branches():
                logger.debug("Using existing experiment branch '%s'", exp_name)
                return self.scm.resolve_rev(exp_name)
            self.scm.checkout(exp_name, create_new=True)
        logger.debug("Commit new experiment branch '%s'", exp_name)
        self.scm.repo.git.add(A=True)
        self.scm.commit(f"Add experiment {exp_name}")
        return self.scm.get_rev()

    def reproduce_one(self, queue=False, **kwargs):
        """Reproduce and checkout a single experiment."""
        stash_rev = self.new(**kwargs)
        if queue:
            logger.info(
                "Queued experiment '%s' for future execution.", stash_rev[:7]
            )
            return []
        results = self.reproduce([stash_rev], keep_stash=False)
        if results:
            exp_rev, (stages, _) = results.items()[0]
            self.checkout_exp(exp_rev, force=True)
            return stages
        return []

    def reproduce_queued(self, **kwargs):
        results = self.reproduce(**kwargs)
        if results:
            revs = [f"{rev[:7]}" for rev in results]
            logger.info(
                "Successfully reproduced experiment(s) '%s'.\n"
                "Use `dvc exp checkout <exp_rev>` to apply the results of "
                "a specific experiment to your workspace.",
                ", ".join(revs),
            )
        return []

    def new(self, *args, workspace=True, **kwargs):
        """Create a new experiment.

        Experiment will be reproduced and checked out into the user's
        workspace.
        """
        rev = self.repo.scm.get_rev()
        self._scm_checkout(rev)
        if workspace:
            try:
                stash_rev = self._stash_exp(*args, **kwargs)
            except UnchangedExperimentError as exc:
                logger.info("Reproducing existing experiment '%s'.", rev[:7])
                raise exc
        else:
            # configure params via command line here
            pass
        logger.debug(
            "Stashed experiment '%s' for future execution.", stash_rev[:7]
        )
        return stash_rev

    def reproduce(
        self,
        revs: Optional[Iterable] = None,
        keep_stash: Optional[bool] = True,
        **kwargs,
    ):
        """Reproduce the specified experiments.

        Args:
            revs: If revs is not specified, all stashed experiments will be
                reproduced.
            keep_stash: If True, stashed experiments will be preserved if they
                fail to reproduce successfully.
        """
        stash_revs = self.stash_revs

        # to_run contains mapping of:
        #   input_rev: (stash_index, baseline_rev)
        # where input_rev contains the changes to execute (usually a stash
        # commit) and baseline_rev is the baseline to compare output against.
        # The final experiment commit will be branched from baseline_rev.
        if revs is None:
            to_run = {
                rev: baseline_rev
                for rev, (_, baseline_rev) in stash_revs.items()
            }
        else:
            to_run = {
                rev: stash_revs[rev][1] if rev in stash_revs else rev
                for rev in revs
            }

        # setup executors
        executors = {}
        for rev, baseline_rev in to_run.items():
            tree = self.scm.get_tree(rev)
            repro_args, repro_kwargs = self._unpack_args(tree)
            executor = LocalExecutor(
                tree,
                baseline_rev,
                repro_args=repro_args,
                repro_kwargs=repro_kwargs,
                dvc_dir=self.dvc_dir,
                cache_dir=self.repo.cache.local.cache_dir,
            )
            executors[rev] = executor

        exec_results = self._reproduce(executors, **kwargs)

        if keep_stash:
            # only drop successfully run stashed experiments
            to_drop = sorted(
                (
                    stash_revs[rev][0]
                    for rev in exec_results
                    if rev in stash_revs
                ),
                reverse=True,
            )
        else:
            # drop all stashed experiments
            to_drop = sorted(
                (stash_revs[rev][0] for rev in to_run if rev in stash_revs),
                reverse=True,
            )
        for index in to_drop:
            self.scm.repo.git.stash("drop", index)

        result = {}
        for _, exp_result in exec_results.items():
            result.update(exp_result)
        return result

    def _reproduce(self, executors: dict, jobs: Optional[int] = 1) -> dict:
        """Run dvc repro for the specified ExperimentExecutors in parallel.

        Returns dict containing successfully executed experiments.
        """
        result = {}

        with ThreadPoolExecutor(max_workers=jobs) as workers:
            futures = {
                workers.submit(executor.reproduce): (rev, executor)
                for rev, executor in executors.items()
            }
            for future in as_completed(futures):
                rev, executor = futures[future]
                exc = future.exception()
                if exc is None:
                    stages, unchanged = future.result()
                    logger.debug(f"ran exp based on {executor.baseline_rev}")
                    self._scm_checkout(executor.baseline_rev)
                    self._collect_output(executor.baseline_rev, executor)
                    remove(self.args_file)
                    try:
                        exp_rev = self._commit(stages + unchanged)
                    except UnchangedExperimentError:
                        logger.debug(
                            "Experiment '%s' identical to baseline '%s'",
                            rev,
                            executor.baseline_rev,
                        )
                        exp_rev = executor.baseline_rev
                    logger.info("Reproduced experiment '%s'.", exp_rev[:7])
                    result[rev] = {exp_rev: (stages, unchanged)}
                else:
                    logger.exception(
                        "Failed to reproduce experiment '%s'", rev
                    )
                executor.cleanup()

        return result

    def _collect_output(self, rev: str, executor: ExperimentExecutor):
        logger.debug("copying tmp output from '%s'", executor.tmp_dir)
        tree = self.scm.get_tree(rev)
        for fname in tree.walk_files(tree.tree_root):
            src = executor.path_info / relpath(fname, tree.tree_root)
            copyfile(src, fname)

    def checkout_exp(self, rev, force=False):
        """Checkout an experiment to the user's workspace."""
        from git.exc import GitCommandError
        from dvc.repo.checkout import _checkout as dvc_checkout

        if force:
            self.repo.scm.repo.git.reset(hard=True)
        self._scm_checkout(rev)

        tmp = tempfile.NamedTemporaryFile(delete=False).name
        self.scm.repo.head.commit.diff("HEAD~1", patch=True, output=tmp)
        try:
            if os.path.getsize(tmp):
                logger.debug("Patching local workspace")
                self.repo.scm.repo.git.apply(tmp, reverse=True)
            dvc_checkout(self.repo)
        except GitCommandError:
            raise DvcException(
                "Checkout failed, experiment contains changes which "
                "conflict with your current workspace. To overwrite "
                "your workspace, use `dvc experiments checkout --force`."
            )
        finally:
            remove(tmp)

    def checkout(self, *args, **kwargs):
        from dvc.repo.experiments.checkout import checkout

        return checkout(self.repo, *args, **kwargs)

    def diff(self, *args, **kwargs):
        from dvc.repo.experiments.diff import diff

        return diff(self.repo, *args, **kwargs)

    def show(self, *args, **kwargs):
        from dvc.repo.experiments.show import show

        return show(self.repo, *args, **kwargs)
