import logging
import os
import pickle
from abc import ABC, abstractmethod
from contextlib import contextmanager
from functools import partial
from typing import (
    TYPE_CHECKING,
    Callable,
    Iterable,
    NamedTuple,
    Optional,
    Union,
)

from funcy import cached_property

from dvc.exceptions import DvcException
from dvc.path_info import PathInfo
from dvc.repo.experiments.base import (
    EXEC_BASELINE,
    EXEC_BRANCH,
    EXEC_CHECKPOINT,
    EXEC_HEAD,
    EXEC_MERGE,
    EXEC_NAMESPACE,
    EXPS_NAMESPACE,
    EXPS_STASH,
    CheckpointExistsError,
    ExperimentExistsError,
    ExpRefInfo,
    UnchangedExperimentError,
)
from dvc.scm import SCM
from dvc.stage import PipelineStage
from dvc.stage.monitor import CheckpointKilledError
from dvc.stage.serialize import to_lockfile
from dvc.utils import dict_sha256
from dvc.utils.fs import remove

if TYPE_CHECKING:
    from multiprocessing import Queue

    from dvc.scm.git import Git

logger = logging.getLogger(__name__)


class ExecutorResult(NamedTuple):
    exp_hash: Optional[str]
    ref_info: Optional["ExpRefInfo"]
    force: bool


class BaseExecutor(ABC):
    """Base class for executing experiments in parallel.

    Args:
        src: source Git SCM instance.
        dvc_dir: relpath to DVC root from SCM root.

    Optional keyword args:
        branch: Existing git branch for this experiment.
    """

    PACKED_ARGS_FILE = "repro.dat"
    WARN_UNTRACKED = False
    QUIET = False

    def __init__(
        self,
        src: "Git",
        dvc_dir: str,
        root_dir: Optional[Union[str, PathInfo]] = None,
        branch: Optional[str] = None,
        name: Optional[str] = None,
        **kwargs,
    ):
        assert root_dir is not None
        self._dvc_dir = dvc_dir
        self.root_dir = root_dir
        self._init_git(src, branch)
        self.name = name

    def _init_git(self, scm: "Git", branch: Optional[str] = None):
        """Init git repo and collect executor refs from the specified SCM."""
        from dulwich.repo import Repo as DulwichRepo

        DulwichRepo.init(os.fspath(self.root_dir))

        cwd = os.getcwd()
        os.chdir(self.root_dir)
        try:
            refspec = f"{EXEC_NAMESPACE}/"
            scm.push_refspec(self.git_url, refspec, refspec)
            if branch:
                scm.push_refspec(self.git_url, branch, branch)
                self.scm.set_ref(EXEC_BRANCH, branch, symbolic=True)
            elif self.scm.get_ref(EXEC_BRANCH):
                self.scm.remove_ref(EXEC_BRANCH)

            if self.scm.get_ref(EXEC_CHECKPOINT):
                self.scm.remove_ref(EXEC_CHECKPOINT)

            # checkout EXEC_HEAD and apply EXEC_MERGE on top of it without
            # committing
            head = EXEC_BRANCH if branch else EXEC_HEAD
            self.scm.checkout(head, detach=True)
            merge_rev = self.scm.get_ref(EXEC_MERGE)
            self.scm.merge(merge_rev, squash=True, commit=False)
        finally:
            os.chdir(cwd)

    @cached_property
    def scm(self):
        return SCM(self.root_dir)

    @property
    @abstractmethod
    def git_url(self) -> str:
        pass

    @property
    def dvc_dir(self) -> str:
        return os.path.join(self.root_dir, self._dvc_dir)

    @staticmethod
    def hash_exp(stages: Iterable["PipelineStage"]) -> str:
        exp_data = {}
        for stage in stages:
            if isinstance(stage, PipelineStage):
                exp_data.update(to_lockfile(stage))
        return dict_sha256(exp_data)

    def cleanup(self):
        self.scm.close()
        del self.scm

    # TODO: come up with better way to stash repro arguments
    @staticmethod
    def pack_repro_args(path, *args, fs=None, extra=None, **kwargs):
        dpath = os.path.dirname(path)
        if fs:
            open_func = fs.open
            fs.makedirs(dpath)
        else:
            from dvc.utils.fs import makedirs

            open_func = open
            makedirs(dpath, exist_ok=True)

        data = {"args": args, "kwargs": kwargs}
        if extra is not None:
            data["extra"] = extra
        with open_func(path, "wb") as fobj:
            pickle.dump(data, fobj)

    @staticmethod
    def unpack_repro_args(path):
        with open(path, "rb") as fobj:
            data = pickle.load(fobj)
        return data["args"], data["kwargs"]

    def fetch_exps(
        self,
        dest_scm: "Git",
        force: bool = False,
        on_diverged: Callable[[str, bool], None] = None,
    ) -> Iterable[str]:
        """Fetch reproduced experiments into the specified SCM.

        Args:
            dest_scm: Destination Git instance.
            force: If True, diverged refs will be overwritten
            on_diverged: Callback in the form on_diverged(ref, is_checkpoint)
                to be called when an experiment ref has diverged.
        """
        refs = []
        for ref in self.scm.iter_refs(base=EXPS_NAMESPACE):
            if not ref.startswith(EXEC_NAMESPACE) and ref != EXPS_STASH:
                refs.append(ref)

        def on_diverged_ref(orig_ref: str, new_rev: str):
            if force:
                logger.debug("Replacing existing experiment '%s'", orig_ref)
                return True

            checkpoint = self.scm.get_ref(EXEC_CHECKPOINT) is not None
            self._raise_ref_conflict(dest_scm, orig_ref, new_rev, checkpoint)
            if on_diverged:
                on_diverged(orig_ref, checkpoint)
            logger.debug("Reproduced existing experiment '%s'", orig_ref)
            return False

        # fetch experiments
        dest_scm.fetch_refspecs(
            self.git_url,
            [f"{ref}:{ref}" for ref in refs],
            on_diverged=on_diverged_ref,
            force=force,
        )
        # update last run checkpoint (if it exists)
        if self.scm.get_ref(EXEC_CHECKPOINT):
            dest_scm.fetch_refspecs(
                self.git_url,
                [f"{EXEC_CHECKPOINT}:{EXEC_CHECKPOINT}"],
                force=True,
            )
        return refs

    @classmethod
    def reproduce(
        cls,
        dvc_dir: Optional[str],
        rev: str,
        queue: Optional["Queue"] = None,
        rel_cwd: Optional[str] = None,
        name: Optional[str] = None,
        log_errors: bool = True,
        log_level: Optional[int] = None,
    ) -> "ExecutorResult":
        """Run dvc repro and return the result.

        Returns tuple of (exp_hash, exp_ref, force) where exp_hash is the
            experiment hash (or None on error), exp_ref is the experiment ref,
            and force is a bool specifying whether or not this experiment
            should force overwrite any existing duplicates.
        """
        from dvc.repo.checkout import checkout as dvc_checkout
        from dvc.repo.reproduce import reproduce as dvc_reproduce

        unchanged = []

        if queue is not None:
            queue.put((rev, os.getpid()))
        if log_errors and log_level is not None:
            cls._set_log_level(log_level)

        def filter_pipeline(stages):
            unchanged.extend(
                [stage for stage in stages if isinstance(stage, PipelineStage)]
            )

        exp_hash: Optional[str] = None
        exp_ref: Optional["ExpRefInfo"] = None
        repro_force: bool = False

        with cls._repro_dvc(dvc_dir, rel_cwd, log_errors) as dvc:
            args, kwargs = cls._repro_args(dvc)
            if args:
                targets: Optional[Union[list, str]] = args[0]
            else:
                targets = kwargs.get("targets")

            repro_force = kwargs.get("force", False)
            logger.trace(  # type: ignore[attr-defined]
                "Executor repro with force = '%s'", str(repro_force)
            )

            repro_dry = kwargs.get("dry")

            # NOTE: checkpoint outs are handled as a special type of persist
            # out:
            #
            # - checkpoint out may not yet exist if this is the first time this
            #   experiment has been run, this is not an error condition for
            #   experiments
            # - if experiment was run with --reset, the checkpoint out will be
            #   removed at the start of the experiment (regardless of any
            #   dvc.lock entry for the checkpoint out)
            # - if run without --reset, the checkpoint out will be checked out
            #   using any hash present in dvc.lock (or removed if no entry
            #   exists in dvc.lock)
            checkpoint_reset: bool = kwargs.pop("reset", False)
            if not repro_dry:
                dvc_checkout(
                    dvc,
                    targets=targets,
                    with_deps=targets is not None,
                    force=True,
                    quiet=True,
                    allow_missing=True,
                    checkpoint_reset=checkpoint_reset,
                )

            checkpoint_func = partial(
                cls.checkpoint_callback,
                dvc.scm,
                name,
                repro_force or checkpoint_reset,
            )
            stages = dvc_reproduce(
                dvc,
                *args,
                on_unchanged=filter_pipeline,
                checkpoint_func=checkpoint_func,
                **kwargs,
            )

            exp_hash = cls.hash_exp(stages)
            if not repro_dry:
                try:
                    is_checkpoint = any(
                        stage.is_checkpoint for stage in stages
                    )
                    if is_checkpoint and checkpoint_reset:
                        # For reset checkpoint stages, we need to force
                        # overwriting existing checkpoint refs even though
                        # repro may not have actually been run with --force
                        repro_force = True
                    cls.commit(
                        dvc.scm,
                        exp_hash,
                        exp_name=name,
                        force=repro_force,
                        checkpoint=is_checkpoint,
                    )
                except UnchangedExperimentError:
                    pass
                ref = dvc.scm.get_ref(EXEC_BRANCH, follow=False)
                if ref:
                    exp_ref = ExpRefInfo.from_ref(ref)
                if cls.WARN_UNTRACKED:
                    untracked = dvc.scm.untracked_files()
                    if untracked:
                        logger.warning(
                            "The following untracked files were present in "
                            "the experiment directory after reproduction but "
                            "will not be included in experiment commits:\n"
                            "\t%s",
                            ", ".join(untracked),
                        )

        # ideally we would return stages here like a normal repro() call, but
        # stages is not currently picklable and cannot be returned across
        # multiprocessing calls
        return ExecutorResult(exp_hash, exp_ref, repro_force)

    @classmethod
    @contextmanager
    def _repro_dvc(
        cls, dvc_dir: Optional[str], rel_cwd: Optional[str], log_errors: bool
    ):
        from dvc.repo import Repo

        dvc = Repo(dvc_dir)
        if cls.QUIET:
            dvc.scm.quiet = cls.QUIET
        if dvc_dir is not None:
            old_cwd: Optional[str] = os.getcwd()
            if rel_cwd:
                os.chdir(os.path.join(dvc.root_dir, rel_cwd))
            else:
                os.chdir(dvc.root_dir)
        else:
            old_cwd = None
        logger.debug("Running repro in '%s'", os.getcwd())

        try:
            yield dvc
        except CheckpointKilledError:
            raise
        except DvcException:
            if log_errors:
                logger.exception("")
            raise
        except Exception:
            if log_errors:
                logger.exception("unexpected error")
            raise
        finally:
            dvc.close()
            if old_cwd:
                os.chdir(old_cwd)

    @classmethod
    def _repro_args(cls, dvc):
        args_path = os.path.join(dvc.tmp_dir, cls.PACKED_ARGS_FILE)
        if os.path.exists(args_path):
            args, kwargs = cls.unpack_repro_args(args_path)
            remove(args_path)
            # explicitly git rm/unstage the args file
            dvc.scm.add([args_path])
        else:
            args = []
            kwargs = {}
        return args, kwargs

    @classmethod
    def checkpoint_callback(
        cls,
        scm: "Git",
        name: Optional[str],
        force: bool,
        unchanged: Iterable["PipelineStage"],
        stages: Iterable["PipelineStage"],
    ):
        try:
            exp_hash = cls.hash_exp(list(stages) + list(unchanged))
            exp_rev = cls.commit(
                scm, exp_hash, exp_name=name, force=force, checkpoint=True
            )
            logger.info("Checkpoint experiment iteration '%s'.", exp_rev[:7])
        except UnchangedExperimentError:
            pass

    @classmethod
    def commit(
        cls,
        scm: "Git",
        exp_hash: str,
        exp_name: Optional[str] = None,
        force: bool = False,
        checkpoint: bool = False,
    ):
        """Commit stages as an experiment and return the commit SHA."""
        rev = scm.get_rev()
        if not scm.is_dirty():
            logger.debug("No changes to commit")
            raise UnchangedExperimentError(rev)

        check_conflict = False
        branch = scm.get_ref(EXEC_BRANCH, follow=False)
        if branch:
            old_ref = rev
            logger.debug("Commit to current experiment branch '%s'", branch)
        else:
            baseline_rev = scm.get_ref(EXEC_BASELINE)
            name = exp_name if exp_name else f"exp-{exp_hash[:5]}"
            ref_info = ExpRefInfo(baseline_rev, name)
            branch = str(ref_info)
            old_ref = None
            if scm.get_ref(branch):
                if not force:
                    check_conflict = True
                logger.debug(
                    "%s existing experiment branch '%s'",
                    "Replace" if force else "Reuse",
                    branch,
                )
            else:
                logger.debug("Commit to new experiment branch '%s'", branch)

        scm.add([], update=True)
        scm.commit(f"dvc: commit experiment {exp_hash}", no_verify=True)
        new_rev = scm.get_rev()
        if check_conflict:
            new_rev = cls._raise_ref_conflict(scm, branch, new_rev, checkpoint)
        else:
            scm.set_ref(branch, new_rev, old_ref=old_ref)
        scm.set_ref(EXEC_BRANCH, branch, symbolic=True)
        if checkpoint:
            scm.set_ref(EXEC_CHECKPOINT, new_rev)
        return new_rev

    @staticmethod
    def _raise_ref_conflict(scm, ref, new_rev, checkpoint):
        # If this commit is a duplicate of the existing commit at 'ref', return
        # the existing commit. Otherwise, error out and require user to re-run
        # with --force as needed
        orig_rev = scm.get_ref(ref)
        if scm.diff(orig_rev, new_rev):
            if checkpoint:
                raise CheckpointExistsError(ref)
            raise ExperimentExistsError(ref)
        return orig_rev

    @staticmethod
    def _set_log_level(level):
        from dvc.logger import disable_other_loggers

        # When executor.reproduce is run in a multiprocessing child process,
        # dvc.main will not be called for that child process so we need to
        # setup logging ourselves
        dvc_logger = logging.getLogger("dvc")
        disable_other_loggers()
        if level is not None:
            dvc_logger.setLevel(level)
