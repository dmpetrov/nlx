import logging
import os
from contextlib import contextmanager
from tempfile import TemporaryDirectory

from funcy import cached_property

from dvc.path_info import PathInfo
from dvc.stage import PipelineStage
from dvc.tree.base import BaseTree
from dvc.utils import relpath
from dvc.utils.fs import copyfile, makedirs

logger = logging.getLogger(__name__)


class ExperimentExecutor:
    """Base class for executing experiments in parallel."""

    def __init__(self, src_tree: BaseTree, **kwargs):
        pass

    def run(self, *args, **kwargs):
        pass

    def cleanup(self):
        pass


class LocalExecutor(ExperimentExecutor):
    def __init__(self, src_tree: BaseTree, **kwargs):
        dvc_dir = kwargs.pop("dvc_dir")
        cache_dir = kwargs.pop("cache_dir")
        super().__init__(src_tree, **kwargs)
        self.tmp_dir = TemporaryDirectory()
        logger.debug("Init local executor in dir '%s'.", self.tmp_dir)
        self.dvc_dir = os.path.join(self.tmp_dir.name, dvc_dir)
        try:
            for fname in src_tree.walk_files(src_tree.tree_root):
                dest = self.path_info / relpath(fname, src_tree.tree_root)
                if not os.path.exists(dest.parent):
                    makedirs(dest.parent)
                copyfile(fname, dest)
        except Exception:
            self.tmp_dir.cleanup()
            raise
        self._config(cache_dir)
        self._stages = None
        self._unchanged = None

    def _config(self, cache_dir):
        local_config = os.path.join(self.dvc_dir, "config.local")
        logger.debug("Writing experiments local config '%s'", local_config)
        with open(local_config, "w") as fobj:
            fobj.write("[core]\n    no_scm = true\n")
            fobj.write(f"[cache]\n    dir = {cache_dir}")

    @cached_property
    def dvc(self):
        from dvc.repo import Repo

        return Repo(self.dvc_dir)

    @cached_property
    def path_info(self):
        return PathInfo(self.tmp_dir.name)

    @property
    def result(self):
        return self._stages, self._unchanged

    @contextmanager
    def chdir(self):
        cwd = os.getcwd()
        os.chdir(self.dvc.root_dir)
        yield
        os.chdir(cwd)

    def run(self, *args, **kwargs):
        unchanged = []

        def filter_pipeline(stage):
            if isinstance(stage, PipelineStage):
                unchanged.append(stage)

        logger.debug("Running repro in '%s'", self.tmp_dir)
        with self.chdir():
            self.dvc.checkout()
            stages = self.dvc.reproduce(
                *args, on_unchanged=filter_pipeline, **kwargs,
            )
        self._stages = stages
        self._unchanged = unchanged

    def cleanup(self):
        logger.debug("Removing tmpdir '%s'", self.tmp_dir)
        self.tmp_dir.cleanup()
        super().cleanup()