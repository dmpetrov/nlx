"""Manages source control systems (e.g. Git)."""

from dvc.scm.base import Base, NoSCMError
from dvc.scm.git import Git


# Syntactic sugar to signal that this is an actual implementation for a DVC
# project under no SCM control.
class NoSCM(Base):
    def __getattr__(self, name):
        raise NoSCMError


def SCM(
    root_dir, search_parent_directories=True, no_scm=False
):  # pylint: disable=invalid-name
    """Returns SCM instance that corresponds to a repo at the specified
    path.

    Args:
        root_dir (str): path to a root directory of the repo.
        search_parent_directories (bool): whether to look for repo root in
        parent directories.
        no_scm (bool): return NoSCM if True.

    Returns:
        dvc.scm.base.Base: SCM instance.
    """

    if no_scm:
        return NoSCM(root_dir)

    return Git(root_dir, search_parent_directories=search_parent_directories)
