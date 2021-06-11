import logging
import os

from dvc.config import Config
from dvc.exceptions import InitError, InvalidArgumentError
from dvc.ignore import init as init_dvcignore
from dvc.repo import Repo
from dvc.scm import SCM
from dvc.scm.base import SCMError
from dvc.utils import relpath
from dvc.utils.fs import remove

logger = logging.getLogger(__name__)


def init(root_dir=os.curdir, no_scm=False, force=False, subdir=False):
    """
    Creates an empty repo on the given directory -- basically a
    `.dvc` directory with subdirectories for configuration and cache.

    It should be tracked by a SCM or use the `--no-scm` flag.

    If the given directory is not empty, you must use the `--force`
    flag to override it.

    Args:
        root_dir: Path to repo's root directory.

    Returns:
        Repo instance.

    Raises:
        KeyError: Raises an exception.
    """

    if no_scm and subdir:
        raise InvalidArgumentError(
            "Cannot initialize repo with `--no-scm` and `--subdir`"
        )

    root_dir = os.path.realpath(root_dir)
    dvc_dir = os.path.join(root_dir, Repo.DVC_DIR)

    try:
        scm = SCM(root_dir, search_parent_directories=subdir, no_scm=no_scm)
    except SCMError:
        raise InitError(
            f"{root_dir} is not tracked by any supported SCM tool (e.g. Git). "
            "Use `--no-scm` if you don't want to use any SCM or "
            "`--subdir` if initializing inside a subdirectory of a parent SCM "
            "repository."
        )

    if scm.is_ignored(dvc_dir):
        raise InitError(
            f"{dvc_dir} is ignored by your SCM tool. \n"
            "Make sure that it's tracked, "
            "for example, by adding '!.dvc' to .gitignore."
        )

    if os.path.isdir(dvc_dir):
        if not force:
            raise InitError(f"'{relpath(dvc_dir)}' exists. Use `-f` to force.")

        remove(dvc_dir)

    os.mkdir(dvc_dir)

    config = Config.init(dvc_dir)

    if no_scm:
        with config.edit() as conf:
            conf["core"]["no_scm"] = True

    dvcignore = init_dvcignore(root_dir)

    proj = Repo(root_dir)

    proj.plots.templates.init()

    scm.add(
        [config.files["repo"], dvcignore, proj.plots.templates.templates_dir]
    )

    logger.info("Initialized DVC repository.\n")

    if scm.ignore_file:
        scm.add([os.path.join(dvc_dir, scm.ignore_file)])
        logger.info("You can now commit the changes to git.\n")

    return proj
