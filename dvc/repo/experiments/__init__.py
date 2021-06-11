import logging
import os
import re
import signal
from collections import defaultdict, namedtuple
from concurrent.futures import CancelledError, ProcessPoolExecutor, wait
from functools import wraps
from multiprocessing import Manager
from typing import Dict, Iterable, Mapping, Optional

from funcy import cached_property, first

from dvc.env import DVCLIVE_RESUME
from dvc.exceptions import DvcException
from dvc.path_info import PathInfo
from dvc.stage.monitor import CheckpointKilledError
from dvc.utils import relpath

from .base import (
    EXEC_APPLY,
    EXEC_BASELINE,
    EXEC_BRANCH,
    EXEC_CHECKPOINT,
    EXEC_HEAD,
    EXEC_MERGE,
    EXEC_NAMESPACE,
    EXPS_NAMESPACE,
    EXPS_STASH,
    BaselineMismatchError,
    CheckpointExistsError,
    ExperimentExistsError,
    ExpRefInfo,
    MultipleBranchError,
)
from .utils import exp_refs_by_rev

logger = logging.getLogger(__name__)


def scm_locked(f):
    # Lock the experiments workspace so that we don't try to perform two
    # different sequences of git operations at once
    @wraps(f)
    def wrapper(exp, *args, **kwargs):
        with exp.scm_lock:
            return f(exp, *args, **kwargs)

    return wrapper


def unlocked_repo(f):
    @wraps(f)
    def wrapper(exp, *args, **kwargs):
        exp.repo.lock.unlock()
        exp.repo._reset()  # pylint: disable=protected-access
        try:
            ret = f(exp, *args, **kwargs)
        finally:
            exp.repo.lock.lock()
        return ret

    return wrapper


class Experiments:
    """Class that manages experiments in a DVC repo.

    Args:
        repo (dvc.repo.Repo): repo instance that these experiments belong to.
    """

    STASH_EXPERIMENT_FORMAT = "dvc-exp:{rev}:{baseline_rev}:{name}"
    STASH_EXPERIMENT_RE = re.compile(
        r"(?:commit: )"
        r"dvc-exp:(?P<rev>[0-9a-f]+):(?P<baseline_rev>[0-9a-f]+)"
        r":(?P<name>[^~^:\\?\[\]*]*)"
        r"(:(?P<branch>.+))?$"
    )
    BRANCH_RE = re.compile(
        r"^(?P<baseline_rev>[a-f0-9]{7})-(?P<exp_sha>[a-f0-9]+)"
        r"(?P<checkpoint>-checkpoint)?$"
    )
    EXEC_TMP_DIR = "exps"

    StashEntry = namedtuple(
        "StashEntry", ["index", "rev", "baseline_rev", "branch", "name"]
    )

    def __init__(self, repo):
        from dvc.lock import make_lock
        from dvc.scm.base import NoSCMError

        if repo.config["core"].get("no_scm", False):
            raise NoSCMError

        self.repo = repo
        self.scm_lock = make_lock(
            os.path.join(self.repo.tmp_dir, "exp_scm_lock"),
            tmp_dir=self.repo.tmp_dir,
        )

    @property
    def scm(self):
        return self.repo.scm

    @cached_property
    def dvc_dir(self):
        return relpath(self.repo.dvc_dir, self.repo.scm.root_dir)

    @cached_property
    def args_file(self):
        from .executor.base import BaseExecutor

        return os.path.join(self.repo.tmp_dir, BaseExecutor.PACKED_ARGS_FILE)

    @cached_property
    def stash(self):
        from dvc.scm.git import Stash

        return Stash(self.scm, EXPS_STASH)

    @property
    def stash_revs(self):
        revs = {}
        for i, entry in enumerate(self.stash):
            msg = entry.message.decode("utf-8").strip()
            m = self.STASH_EXPERIMENT_RE.match(msg)
            if m:
                revs[entry.new_sha.decode("utf-8")] = self.StashEntry(
                    i,
                    m.group("rev"),
                    m.group("baseline_rev"),
                    m.group("branch"),
                    m.group("name"),
                )
        return revs

    def _stash_exp(
        self,
        *args,
        params: Optional[dict] = None,
        resume_rev: Optional[str] = None,
        baseline_rev: Optional[str] = None,
        branch: Optional[str] = None,
        name: Optional[str] = None,
        **kwargs,
    ):
        """Stash changes from the workspace as an experiment.

        Args:
            params: Optional dictionary of parameter values to be used.
                Values take priority over any parameters specified in the
                user's workspace.
            resume_rev: Optional checkpoint resume rev.
            baseline_rev: Optional baseline rev for this experiment, defaults
                to the current SCM rev.
            branch: Optional experiment branch name. If specified, the
                experiment will be added to `branch` instead of creating
                a new branch.
            name: Optional experiment name. If specified this will be used as
                the human-readable name in the experiment branch ref. Has no
                effect of branch is specified.
        """
        with self.scm.detach_head() as orig_head:
            stash_head = orig_head
            if baseline_rev is None:
                baseline_rev = orig_head

            with self.scm.stash_workspace() as workspace:
                try:
                    if workspace:
                        self.stash.apply(workspace)

                    if resume_rev:
                        # move HEAD to the resume rev so that the stashed diff
                        # only contains changes relative to resume rev
                        stash_head = resume_rev
                        self.scm.set_ref(
                            "HEAD",
                            resume_rev,
                            message=f"dvc: resume from HEAD {resume_rev[:7]}",
                        )
                        self.scm.reset()

                    # update experiment params from command line
                    if params:
                        self._update_params(params)

                    # DVC commit data deps to preserve state across workspace
                    # & tempdir runs
                    self._stash_commit_deps(*args, **kwargs)

                    if resume_rev:
                        if branch:
                            branch_name = ExpRefInfo.from_ref(branch).name
                        else:
                            branch_name = f"{resume_rev[:7]}"
                        if self.scm.is_dirty():
                            logger.info(
                                "Modified checkpoint experiment based on "
                                "'%s' will be created",
                                branch_name,
                            )
                            branch = None
                        elif (
                            not branch
                            or self.scm.get_ref(branch) != resume_rev
                        ):
                            msg = [
                                (
                                    "Nothing to do for unchanged checkpoint "
                                    f"'{resume_rev[:7]}'. "
                                )
                            ]
                            if branch:
                                msg.append(
                                    "To resume from the head of this "
                                    "experiment, use "
                                    f"'dvc exp apply {branch_name}'."
                                )
                            else:
                                names = [
                                    ref_info.name
                                    for ref_info in exp_refs_by_rev(
                                        self.scm, resume_rev
                                    )
                                ]
                                if len(names) > 3:
                                    names[3:] = [
                                        f"... ({len(names) - 3} more)"
                                    ]
                                msg.append(
                                    "To resume an experiment containing this "
                                    "checkpoint, apply one of these heads:\n"
                                    "\t{}".format(", ".join(names))
                                )
                            raise DvcException("".join(msg))
                        else:
                            logger.info(
                                "Existing checkpoint experiment '%s' will be "
                                "resumed",
                                branch_name,
                            )
                        if name:
                            logger.warning(
                                "Ignoring option '--name %s' for resumed "
                                "experiment. Existing experiment name will"
                                "be preserved instead.",
                                name,
                            )

                    # save additional repro command line arguments
                    run_env = {DVCLIVE_RESUME: "1"} if resume_rev else {}
                    self._pack_args(*args, run_env=run_env, **kwargs)

                    # save experiment as a stash commit
                    msg = self._stash_msg(
                        stash_head,
                        baseline_rev=baseline_rev,
                        branch=branch,
                        name=name,
                    )
                    stash_rev = self.stash.push(message=msg)
                    logger.debug(
                        (
                            "Stashed experiment '%s' with baseline '%s' "
                            "for future execution."
                        ),
                        stash_rev[:7],
                        baseline_rev[:7],
                    )
                finally:
                    if resume_rev:
                        # NOTE: this set_ref + reset() is equivalent to
                        # `git reset orig_head` (our SCM reset() only operates
                        # on HEAD rather than any arbitrary commit)
                        self.scm.set_ref(
                            "HEAD", orig_head, message="dvc: restore HEAD"
                        )
                        self.scm.reset()
                    # Revert any of our changes before prior unstashing
                    self.scm.reset(hard=True)

        return stash_rev

    def _stash_commit_deps(self, *args, **kwargs):
        if len(args):
            targets = args[0]
        else:
            targets = kwargs.get("targets")
        if isinstance(targets, str):
            targets = [targets]
        elif not targets:
            targets = [None]
        for target in targets:
            self.repo.commit(
                target,
                with_deps=True,
                recursive=kwargs.get("recursive", False),
                force=True,
                allow_missing=True,
                data_only=True,
            )

    def _stash_msg(
        self,
        rev: str,
        baseline_rev: str,
        branch: Optional[str] = None,
        name: Optional[str] = None,
    ):
        if not baseline_rev:
            baseline_rev = rev
        msg = self.STASH_EXPERIMENT_FORMAT.format(
            rev=rev, baseline_rev=baseline_rev, name=name if name else ""
        )
        if branch:
            return f"{msg}:{branch}"
        return msg

    def _pack_args(self, *args, **kwargs):
        import pickle

        from .executor.base import BaseExecutor

        if os.path.exists(self.args_file) and self.scm.is_tracked(
            self.args_file
        ):
            logger.warning(
                (
                    "Temporary DVC file '.dvc/tmp/%s' exists and was "
                    "likely committed to Git by mistake. It should be removed "
                    "with:\n"
                    "\tgit rm .dvc/tmp/%s"
                ),
                BaseExecutor.PACKED_ARGS_FILE,
                BaseExecutor.PACKED_ARGS_FILE,
            )
            with open(self.args_file, "rb") as fobj:
                try:
                    data = pickle.load(fobj)
                except Exception:  # pylint: disable=broad-except
                    data = {}
            extra = int(data.get("extra", 0)) + 1
        else:
            extra = None
        BaseExecutor.pack_repro_args(
            self.args_file, *args, extra=extra, **kwargs
        )
        self.scm.add(self.args_file)

    def _update_params(self, params: dict):
        """Update experiment params files with the specified values."""
        from benedict import benedict

        from dvc.utils.serialize import MODIFIERS

        logger.debug("Using experiment params '%s'", params)

        for params_fname in params:
            path = PathInfo(params_fname)
            suffix = path.suffix.lower()
            modify_data = MODIFIERS[suffix]
            with modify_data(path, fs=self.repo.fs) as data:
                benedict(data).merge(params[params_fname], overwrite=True)

        # Force params file changes to be staged in git
        # Otherwise in certain situations the changes to params file may be
        # ignored when we `git stash` them since mtime is used to determine
        # whether the file is dirty
        self.scm.add(list(params.keys()))

    def reproduce_one(
        self,
        queue: bool = False,
        tmp_dir: bool = False,
        checkpoint_resume: Optional[str] = None,
        reset: bool = False,
        **kwargs,
    ):
        """Reproduce and checkout a single experiment."""
        if queue and not checkpoint_resume:
            reset = True

        if reset:
            self.reset_checkpoints()

        if not (queue or tmp_dir):
            staged, _, _ = self.scm.status()
            if staged:
                logger.warning(
                    "Your workspace contains staged Git changes which will be "
                    "unstaged before running this experiment."
                )
                self.scm.reset()

        if checkpoint_resume:
            resume_rev = self.scm.resolve_rev(checkpoint_resume)
            try:
                self.check_baseline(resume_rev)
                checkpoint_resume = resume_rev
            except BaselineMismatchError as exc:
                raise DvcException(
                    f"Cannot resume from '{checkpoint_resume}' as it is not "
                    "derived from your current workspace."
                ) from exc
        else:
            checkpoint_resume = self._workspace_resume_rev()

        stash_rev = self.new(
            checkpoint_resume=checkpoint_resume, reset=reset, **kwargs
        )
        if queue:
            logger.info(
                "Queued experiment '%s' for future execution.", stash_rev[:7]
            )
            return [stash_rev]
        if tmp_dir or queue:
            results = self._reproduce_revs(revs=[stash_rev], keep_stash=False)
        else:
            results = self._workspace_repro()
        exp_rev = first(results)
        if exp_rev is not None:
            self._log_reproduced(results, tmp_dir=tmp_dir)
        return results

    def _workspace_resume_rev(self) -> Optional[str]:
        last_checkpoint = self._get_last_checkpoint()
        last_applied = self._get_last_applied()
        if last_checkpoint and last_applied:
            return last_applied
        return None

    def reproduce_queued(self, **kwargs):
        results = self._reproduce_revs(**kwargs)
        if results:
            self._log_reproduced(results, tmp_dir=True)
        return results

    def _log_reproduced(self, revs: Iterable[str], tmp_dir: bool = False):
        names = []
        for rev in revs:
            name = self.get_exact_name(rev)
            names.append(name if name else rev[:7])
        logger.info("\nReproduced experiment(s): %s", ", ".join(names))
        if tmp_dir:
            logger.info(
                "To apply the results of an experiment to your workspace "
                "run:\n\n"
                "\tdvc exp apply <exp>"
            )
        else:
            logger.info(
                "Experiment results have been applied to your workspace."
            )
        logger.info(
            "\nTo promote an experiment to a Git branch run:\n\n"
            "\tdvc exp branch <exp>\n"
        )

    @scm_locked
    def new(self, *args, checkpoint_resume: Optional[str] = None, **kwargs):
        """Create a new experiment.

        Experiment will be reproduced and checked out into the user's
        workspace.
        """
        if checkpoint_resume is not None:
            return self._resume_checkpoint(
                *args, resume_rev=checkpoint_resume, **kwargs
            )

        return self._stash_exp(*args, **kwargs)

    def _resume_checkpoint(
        self, *args, resume_rev: Optional[str] = None, **kwargs
    ):
        """Resume an existing (checkpoint) experiment.

        Experiment will be reproduced and checked out into the user's
        workspace.
        """
        assert resume_rev

        branch: Optional[str] = None
        try:
            allow_multiple = bool(kwargs.get("params", None))
            branch = self.get_branch_by_rev(
                resume_rev, allow_multiple=allow_multiple
            )
            if not branch:
                raise DvcException(
                    "Could not find checkpoint experiment "
                    f"'{resume_rev[:7]}'"
                )
            baseline_rev = self._get_baseline(branch)
        except MultipleBranchError as exc:
            baselines = {
                info.baseline_sha
                for info in exc.ref_infos
                if info.baseline_sha
            }
            if len(baselines) == 1:
                baseline_rev = baselines.pop()
            else:
                raise

        logger.debug(
            "Checkpoint run from '%s' with baseline '%s'",
            resume_rev[:7],
            baseline_rev,
        )
        return self._stash_exp(
            *args,
            resume_rev=resume_rev,
            baseline_rev=baseline_rev,
            branch=branch,
            **kwargs,
        )

    def _get_last_checkpoint(self) -> Optional[str]:
        try:
            last_checkpoint = self.scm.get_ref(EXEC_CHECKPOINT)
            if last_checkpoint:
                self.check_baseline(last_checkpoint)
            return last_checkpoint
        except BaselineMismatchError:
            # If HEAD has moved since the the last checkpoint run,
            # the specified checkpoint is no longer relevant
            self.scm.remove_ref(EXEC_CHECKPOINT)
        return None

    def _get_last_applied(self) -> Optional[str]:
        try:
            last_applied = self.scm.get_ref(EXEC_APPLY)
            if last_applied:
                self.check_baseline(last_applied)
            return last_applied
        except BaselineMismatchError:
            # If HEAD has moved since the the last applied experiment,
            # the applied experiment is no longer relevant
            self.scm.remove_ref(EXEC_APPLY)
        return None

    def reset_checkpoints(self):
        self.scm.remove_ref(EXEC_CHECKPOINT)
        self.scm.remove_ref(EXEC_APPLY)

    @scm_locked
    def _reproduce_revs(
        self,
        revs: Optional[Iterable] = None,
        keep_stash: Optional[bool] = True,
        **kwargs,
    ) -> Mapping[str, str]:
        """Reproduce the specified experiments.

        Args:
            revs: If revs is not specified, all stashed experiments will be
                reproduced.
            keep_stash: If True, stashed experiments will be preserved if they
                fail to reproduce successfully.

        Returns:
            dict mapping successfully reproduced experiment revs to their
            hashes.
        """
        stash_revs = self.stash_revs

        # to_run contains mapping of:
        #   input_rev: (stash_index, rev, baseline_rev)
        # where input_rev contains the changes to execute (usually a stash
        # commit), rev is the original SCM commit to be checked out, and
        # baseline_rev is the experiment baseline.
        if revs is None:
            to_run = dict(stash_revs)
        else:
            to_run = {
                rev: stash_revs[rev]
                if rev in stash_revs
                else self.StashEntry(None, rev, rev, None, None)
                for rev in revs
            }

        logger.debug(
            "Reproducing experiment revs '%s'",
            ", ".join((rev[:7] for rev in to_run)),
        )

        executors = self._init_executors(to_run)
        exec_results = self._executors_repro(executors, **kwargs)

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
            self.stash.drop(index)

        result: Dict[str, str] = {}
        for _, exp_result in exec_results.items():
            result.update(exp_result)
        return result

    def _init_executors(self, to_run):
        from dvc.utils.fs import makedirs

        from .executor.local import TempDirExecutor

        executors = {}
        base_tmp_dir = os.path.join(self.repo.tmp_dir, self.EXEC_TMP_DIR)
        if not os.path.exists(base_tmp_dir):
            makedirs(base_tmp_dir)
        for stash_rev, item in to_run.items():
            self.scm.set_ref(EXEC_HEAD, item.rev)
            self.scm.set_ref(EXEC_MERGE, stash_rev)
            self.scm.set_ref(EXEC_BASELINE, item.baseline_rev)

            # Executor will be initialized with an empty git repo that
            # we populate by pushing:
            #   EXEC_HEAD - the base commit for this experiment
            #   EXEC_MERGE - the unmerged changes (from our stash)
            #       to be reproduced
            #   EXEC_BASELINE - the baseline commit for this experiment
            executor = TempDirExecutor(
                self.scm,
                self.dvc_dir,
                name=item.name,
                branch=item.branch,
                tmp_dir=base_tmp_dir,
                cache_dir=self.repo.odb.local.cache_dir,
            )
            executors[stash_rev] = executor

        for ref in (EXEC_HEAD, EXEC_MERGE, EXEC_BASELINE):
            self.scm.remove_ref(ref)

        return executors

    def _executors_repro(
        self, executors: dict, jobs: Optional[int] = 1
    ) -> Mapping[str, Mapping[str, str]]:
        """Run dvc repro for the specified BaseExecutors in parallel.

        Returns:
            dict mapping stash revs to the successfully executed experiments
            for each stash rev.
        """
        result: Dict[str, Dict[str, str]] = defaultdict(dict)

        manager = Manager()
        pid_q = manager.Queue()

        rel_cwd = relpath(os.getcwd(), self.repo.root_dir)
        with ProcessPoolExecutor(max_workers=jobs) as workers:
            futures = {}
            for rev, executor in executors.items():
                future = workers.submit(
                    executor.reproduce,
                    executor.dvc_dir,
                    rev,
                    queue=pid_q,
                    name=executor.name,
                    rel_cwd=rel_cwd,
                    log_level=logger.getEffectiveLevel(),
                )
                futures[future] = (rev, executor)

            try:
                wait(futures)
            except KeyboardInterrupt:
                # forward SIGINT to any running executor processes and
                # cancel any remaining futures
                pids = {}
                while not pid_q.empty():
                    rev, pid = pid_q.get()
                    pids[rev] = pid
                for future, (rev, _) in futures.items():
                    if future.running():
                        os.kill(pids[rev], signal.SIGINT)
                    elif not future.done():
                        future.cancel()

            for future, (rev, executor) in futures.items():
                rev, executor = futures[future]

                try:
                    exc = future.exception()
                    if exc is None:
                        exec_result = future.result()
                        result[rev].update(
                            self._collect_executor(executor, exec_result)
                        )
                    elif not isinstance(exc, CheckpointKilledError):
                        logger.error(
                            "Failed to reproduce experiment '%s'", rev[:7]
                        )
                except CancelledError:
                    logger.error(
                        "Cancelled before attempting to reproduce experiment "
                        "'%s'",
                        rev[:7],
                    )
                finally:
                    executor.cleanup()

        return result

    def _collect_executor(self, executor, exec_result) -> Mapping[str, str]:
        # NOTE: GitPython Repo instances cannot be re-used
        # after process has received SIGINT or SIGTERM, so we
        # need this hack to re-instantiate git instances after
        # checkpoint runs. See:
        # https://github.com/gitpython-developers/GitPython/issues/427
        del self.repo.scm

        results = {}

        def on_diverged(ref: str, checkpoint: bool):
            ref_info = ExpRefInfo.from_ref(ref)
            if checkpoint:
                raise CheckpointExistsError(ref_info.name)
            raise ExperimentExistsError(ref_info.name)

        for ref in executor.fetch_exps(
            self.scm, force=exec_result.force, on_diverged=on_diverged
        ):
            exp_rev = self.scm.get_ref(ref)
            if exp_rev:
                logger.debug("Collected experiment '%s'.", exp_rev[:7])
                results[exp_rev] = exec_result.exp_hash

        return results

    @unlocked_repo
    def _workspace_repro(self) -> Mapping[str, str]:
        """Run the most recently stashed experiment in the workspace."""
        from .executor.base import BaseExecutor

        entry = first(self.stash_revs.values())
        assert entry.index == 0

        # NOTE: the stash commit to be popped already contains all the current
        # workspace changes plus CLI modifed --params changes.
        # `checkout --force` here will not lose any data (popping stash commit
        # will result in conflict between workspace params and stashed CLI
        # params, but we always want the stashed version).
        with self.scm.detach_head(entry.rev, force=True):
            rev = self.stash.pop()
            self.scm.set_ref(EXEC_BASELINE, entry.baseline_rev)
            if entry.branch:
                self.scm.set_ref(EXEC_BRANCH, entry.branch, symbolic=True)
            elif self.scm.get_ref(EXEC_BRANCH):
                self.scm.remove_ref(EXEC_BRANCH)
            try:
                orig_checkpoint = self.scm.get_ref(EXEC_CHECKPOINT)
                exec_result = BaseExecutor.reproduce(
                    None,
                    rev,
                    name=entry.name,
                    rel_cwd=relpath(os.getcwd(), self.scm.root_dir),
                    log_errors=False,
                )

                if not exec_result.exp_hash:
                    raise DvcException(
                        f"Failed to reproduce experiment '{rev[:7]}'"
                    )
                if not exec_result.ref_info:
                    # repro succeeded but result matches baseline
                    # (no experiment generated or applied)
                    return {}
                exp_rev = self.scm.get_ref(str(exec_result.ref_info))
                self.scm.set_ref(EXEC_APPLY, exp_rev)
                return {exp_rev: exec_result.exp_hash}
            except CheckpointKilledError:
                # Checkpoint errors have already been logged
                return {}
            except DvcException:
                raise
            except Exception as exc:
                raise DvcException(
                    f"Failed to reproduce experiment '{rev[:7]}'"
                ) from exc
            finally:
                self.scm.remove_ref(EXEC_BASELINE)
                if entry.branch:
                    self.scm.remove_ref(EXEC_BRANCH)
                checkpoint = self.scm.get_ref(EXEC_CHECKPOINT)
                if checkpoint and checkpoint != orig_checkpoint:
                    self.scm.set_ref(EXEC_APPLY, checkpoint)

    def check_baseline(self, exp_rev):
        baseline_sha = self.repo.scm.get_rev()
        if exp_rev == baseline_sha:
            return exp_rev

        exp_baseline = self._get_baseline(exp_rev)
        if exp_baseline is None:
            # if we can't tell from branch name, fall back to parent commit
            exp_commit = self.scm.resolve_commit(exp_rev)
            if exp_commit:
                exp_baseline = first(exp_commit.parents)
        if exp_baseline == baseline_sha:
            return exp_baseline
        raise BaselineMismatchError(exp_baseline, baseline_sha)

    @scm_locked
    def get_baseline(self, rev):
        """Return the baseline rev for an experiment rev."""
        return self._get_baseline(rev)

    def _get_baseline(self, rev):
        rev = self.scm.resolve_rev(rev)

        if rev in self.stash_revs:
            entry = self.stash_revs.get(rev)
            if entry:
                return entry.baseline_rev
            return None

        ref_info = first(exp_refs_by_rev(self.scm, rev))
        if ref_info:
            return ref_info.baseline_sha
        return None

    def get_branch_by_rev(
        self, rev: str, allow_multiple: bool = False
    ) -> Optional[str]:
        """Returns full refname for the experiment branch containing rev."""
        ref_infos = list(exp_refs_by_rev(self.scm, rev))
        if not ref_infos:
            return None
        if len(ref_infos) > 1 and not allow_multiple:
            for ref_info in ref_infos:
                if self.scm.get_ref(str(ref_info)) == rev:
                    return str(ref_info)
            raise MultipleBranchError(rev, ref_infos)
        return str(ref_infos[0])

    def get_exact_name(self, rev: str):
        """Returns preferred name for the specified revision.

        Prefers tags, branches (heads), experiments in that orer.
        """
        exclude = f"{EXEC_NAMESPACE}/*"
        ref = self.scm.describe(rev, base=EXPS_NAMESPACE, exclude=exclude)
        if ref:
            return ExpRefInfo.from_ref(ref).name
        return None

    def apply(self, *args, **kwargs):
        from dvc.repo.experiments.apply import apply

        return apply(self.repo, *args, **kwargs)

    def branch(self, *args, **kwargs):
        from dvc.repo.experiments.branch import branch

        return branch(self.repo, *args, **kwargs)

    def diff(self, *args, **kwargs):
        from dvc.repo.experiments.diff import diff

        return diff(self.repo, *args, **kwargs)

    def show(self, *args, **kwargs):
        from dvc.repo.experiments.show import show

        return show(self.repo, *args, **kwargs)

    def run(self, *args, **kwargs):
        from dvc.repo.experiments.run import run

        return run(self.repo, *args, **kwargs)

    def gc(self, *args, **kwargs):
        from dvc.repo.experiments.gc import gc

        return gc(self.repo, *args, **kwargs)

    def push(self, *args, **kwargs):
        from dvc.repo.experiments.push import push

        return push(self.repo, *args, **kwargs)

    def pull(self, *args, **kwargs):
        from dvc.repo.experiments.pull import pull

        return pull(self.repo, *args, **kwargs)

    def ls(self, *args, **kwargs):
        from dvc.repo.experiments.ls import ls

        return ls(self.repo, *args, **kwargs)

    def remove(self, *args, **kwargs):
        from dvc.repo.experiments.remove import remove

        return remove(self.repo, *args, **kwargs)
