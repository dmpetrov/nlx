import logging
import os
from tempfile import TemporaryDirectory
from typing import Optional

from .base import BaseExecutor

logger = logging.getLogger(__name__)


class ExpTemporaryDirectory(TemporaryDirectory):
    # Python's TemporaryDirectory cleanup shutil.rmtree wrapper does not handle
    # git read-only dirs cleanly in Windows on Python <3.8, so we use our own
    # remove(). See:
    # https://github.com/iterative/dvc/pull/5425
    # https://bugs.python.org/issue26660

    @classmethod
    def _rmtree(cls, name):
        from dvc.utils.fs import remove

        remove(name)

    def cleanup(self):
        if self._finalizer.detach():
            self._rmtree(self.name)


class BaseLocalExecutor(BaseExecutor):
    """Base local machine executor."""

    @property
    def git_url(self) -> str:
        root_dir = os.path.abspath(self.root_dir)
        if os.name == "nt":
            root_dir = root_dir.replace(os.sep, "/")
        return f"file://{root_dir}"


class TempDirExecutor(BaseLocalExecutor):
    """Temp directory experiment executor."""

    # Temp dir executors should warn if untracked files exist (to help with
    # debugging user code), and suppress other DVC hints (like `git add`
    # suggestions) that are not applicable outside of workspace runs
    WARN_UNTRACKED = True
    QUIET = True

    def __init__(
        self,
        *args,
        tmp_dir: Optional[str] = None,
        cache_dir: Optional[str] = None,
        **kwargs,
    ):
        self._tmp_dir = ExpTemporaryDirectory(dir=tmp_dir)
        kwargs["root_dir"] = self._tmp_dir.name
        super().__init__(*args, **kwargs)
        if cache_dir:
            self._config(cache_dir)
        logger.debug("Init temp dir executor in dir '%s'", self._tmp_dir)

    def _config(self, cache_dir):
        local_config = os.path.join(self.dvc_dir, "config.local")
        logger.debug("Writing experiments local config '%s'", local_config)
        with open(local_config, "w") as fobj:
            fobj.write(f"[cache]\n    dir = {cache_dir}")

    def cleanup(self):
        super().cleanup()
        logger.debug("Removing tmpdir '%s'", self._tmp_dir)
        self._tmp_dir.cleanup()
