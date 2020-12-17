import logging
import os
import pickle
from abc import ABC, abstractmethod
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
    ExperimentExistsError,
    ExpRefInfo,
    UnchangedExperimentError,
)
from dvc.scm import SCM
from dvc.stage import PipelineStage
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

            if self.scm.get_ref(EXEC_CHECKPOINT):
                self.scm.remove_ref(EXEC_CHECKPOINT)

            # checkout EXEC_HEAD and apply EXEC_MERGE on top of it without
            # committing
            head = EXEC_BRANCH if branch else EXEC_HEAD
            self.scm.checkout(head, detach=True)
            self.scm.gitpython.repo.git.merge(
                EXEC_MERGE, squash=True, no_commit=True
            )
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
    def hash_exp(stages: Iterable["PipelineStage"]):
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
    def pack_repro_args(path, *args, tree=None, extra=None, **kwargs):
        dpath = os.path.dirname(path)
        if tree:
            open_func = tree.open
            tree.makedirs(dpath)
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
            orig_rev = dest_scm.get_ref(orig_ref)
            if dest_scm.diff(orig_rev, new_rev):
                if force:
                    logger.debug(
                        "Replacing existing experiment '%s'", orig_ref,
                    )
                    return True
                if on_diverged:
                    checkpoint = self.scm.get_ref(EXEC_CHECKPOINT) is not None
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
        log_level: Optional[int] = None,
    ) -> "ExecutorResult":
        """Run dvc repro and return the result.

        Returns tuple of (exp_hash, exp_ref, force) where exp_hash is the
            experiment hash (or None on error), exp_ref is the experiment ref,
            and force is a bool specifying whether or not this experiment
            should force overwrite any existing duplicates.
        """
        from dvc.repo import Repo
        from dvc.repo.checkout import checkout as dvc_checkout
        from dvc.repo.reproduce import reproduce as dvc_reproduce

        unchanged = []

        if queue is not None:
            queue.put((rev, os.getpid()))
        if log_level is not None:
            cls._set_log_level(log_level)

        def filter_pipeline(stages):
            unchanged.extend(
                [stage for stage in stages if isinstance(stage, PipelineStage)]
            )

        exp_hash: Optional[str] = None
        exp_ref: Optional["ExpRefInfo"] = None
        repro_force: bool = False

        try:
            dvc = Repo(dvc_dir)
            if dvc_dir is not None:
                old_cwd = os.getcwd()
                if rel_cwd:
                    os.chdir(os.path.join(dvc.root_dir, rel_cwd))
                else:
                    os.chdir(dvc.root_dir)
            else:
                old_cwd = None
            logger.debug("Running repro in '%s'", os.getcwd())

            args_path = os.path.join(
                dvc.tmp_dir, BaseExecutor.PACKED_ARGS_FILE
            )
            if os.path.exists(args_path):
                args, kwargs = BaseExecutor.unpack_repro_args(args_path)
                remove(args_path)
            else:
                args = []
                kwargs = {}

            repro_force = kwargs.get("force", False)

            # NOTE: for checkpoint experiments we handle persist outs slightly
            # differently than normal:
            #
            # - checkpoint out may not yet exist if this is the first time this
            #   experiment has been run, this is not an error condition for
            #   experiments
            # - at the start of a repro run, we need to remove the persist out
            #   and restore it to its last known (committed) state (which may
            #   be removed/does not yet exist) so that our executor workspace
            #   is not polluted with the (persistent) out from an unrelated
            #   experiment run
            dvc_checkout(dvc, force=True, quiet=True)

            checkpoint_func = partial(
                cls.checkpoint_callback, dvc.scm, name, repro_force
            )
            stages = dvc_reproduce(
                dvc,
                *args,
                on_unchanged=filter_pipeline,
                checkpoint_func=checkpoint_func,
                **kwargs,
            )

            exp_hash = cls.hash_exp(stages)
            exp_rev = cls.commit(
                dvc.scm, exp_hash, exp_name=name, force=repro_force
            )
            ref = dvc.scm.get_ref(EXEC_BRANCH, follow=False)
            if ref:
                exp_ref = ExpRefInfo.from_ref(ref)
            if dvc.scm.get_ref(EXEC_CHECKPOINT):
                dvc.scm.set_ref(EXEC_CHECKPOINT, exp_rev)
        except UnchangedExperimentError:
            pass
        finally:
            if dvc:
                dvc.scm.close()
            if old_cwd:
                os.chdir(old_cwd)

        # ideally we would return stages here like a normal repro() call, but
        # stages is not currently picklable and cannot be returned across
        # multiprocessing calls
        return ExecutorResult(exp_hash, exp_ref, repro_force)

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
            exp_rev = cls.commit(scm, exp_hash, exp_name=name, force=force)
            scm.set_ref(EXEC_CHECKPOINT, exp_rev)
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
    ):
        """Commit stages as an experiment and return the commit SHA."""
        rev = scm.get_rev()
        if not scm.is_dirty(untracked_files=True):
            logger.debug("No changes to commit")
            raise UnchangedExperimentError(rev)

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
            if not force and scm.get_ref(branch):
                raise ExperimentExistsError(ref_info.name)
            logger.debug("Commit to new experiment branch '%s'", branch)

        scm.gitpython.repo.git.add(update=True)
        scm.commit(f"dvc: commit experiment {exp_hash}")
        new_rev = scm.get_rev()
        scm.set_ref(branch, new_rev, old_ref=old_ref)
        scm.set_ref(EXEC_BRANCH, branch, symbolic=True)
        return new_rev

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
