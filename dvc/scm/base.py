"""Manages source control systems (e.g. Git) in DVC."""

import os
from contextlib import contextmanager

from funcy import get_in

from dvc.exceptions import DvcException


class SCMError(DvcException):
    """Base class for source control management errors."""


class FileNotInRepoError(SCMError):
    """Thrown when trying to find .gitignore for a file that is not in a scm
    repository.
    """


class CloneError(SCMError):
    def __init__(self, url, path):
        super().__init__(f"Failed to clone repo '{url}' to '{path}'")


class RevError(SCMError):
    pass


class NoSCMError(SCMError):
    def __init__(self):
        msg = (
            "Only supported for Git repositories. If you're "
            "seeing this error in a Git repo, try updating the DVC "
            "configuration with `dvc config core.no_scm false`."
        )
        super().__init__(msg)


class MergeConflictError(SCMError):
    pass


class Base:
    """Base class for source control management driver implementations."""

    def __init__(self, root_dir=os.curdir):
        self._root_dir = os.path.realpath(root_dir)

    @property
    def root_dir(self) -> str:
        return self._root_dir

    def __repr__(self):
        return "{class_name}: '{directory}'".format(
            class_name=type(self).__name__, directory=self.dir
        )

    @property
    def dir(self):
        """Path to a directory with SCM specific information."""
        return None

    @staticmethod
    def is_repo(root_dir):  # pylint: disable=unused-argument
        """Returns whether or not root_dir is a valid SCM repository."""
        return True

    @staticmethod
    def is_submodule(root_dir):  # pylint: disable=unused-argument
        """Returns whether or not root_dir is a valid SCM repository
        submodule.
        """
        return True

    def is_ignored(self, path):  # pylint: disable=unused-argument
        """Returns whether or not path is ignored by SCM."""
        return False

    def ignore(self, path):  # pylint: disable=unused-argument
        """Makes SCM ignore a specified path."""

    def ignore_remove(self, path):  # pylint: disable=unused-argument
        """Makes SCM stop ignoring a specified path."""

    @property
    def ignore_file(self):
        """Filename for a file that contains ignored paths for this SCM."""

    def ignore_list(self, p_list):
        """Makes SCM ignore all paths specified in a list."""
        return [self.ignore(path) for path in p_list]

    def add(self, paths):
        """Makes SCM track every path from a specified list of paths."""

    def commit(self, msg):
        """Makes SCM create a commit."""

    def checkout(self, branch, create_new=False):
        """Makes SCM checkout a branch."""

    def branch(self, branch):
        """Makes SCM create a branch with a specified name."""

    def tag(self, tag):
        """Makes SCM create a tag with a specified name."""

    def untracked_files(self):  # pylint: disable=no-self-use
        """Returns a list of untracked files."""
        return []

    def is_tracked(self, path):  # pylint: disable=no-self-use, unused-argument
        """Returns whether or not a specified path is tracked."""
        return False

    def is_dirty(self):
        """Return whether the SCM contains uncommitted changes."""
        return False

    def active_branch(self):  # pylint: disable=no-self-use
        """Returns current branch in the repo."""
        return ""

    def list_branches(self):  # pylint: disable=no-self-use
        """Returns a list of available branches in the repo."""
        return []

    def list_tags(self):  # pylint: disable=no-self-use
        """Returns a list of available tags in the repo."""
        return []

    def list_all_commits(self):  # pylint: disable=no-self-use
        """Returns a list of commits in the repo."""
        return []

    def install(self, use_pre_commit_tool=False):
        """
        Adds dvc commands to SCM hooks for the repo.

        If use_pre_commit_tool is set and pre-commit is
        installed it will be used to install the hooks.
        """

    def cleanup_ignores(self):
        """
        This method should clean up ignores (eg. entries in .gitignore),
        use, when method editing ignores (eg. add, run, import) fails to
        perform its task.
        """

    def reset_ignores(self):
        """
        Method to reset in-memory ignore storing mechanism.
        """

    def reset_tracked_files(self):
        """
        Method to reset in-memory tracked files storing mechanism.
        """

    def remind_to_track(self):
        """
        Method to remind user to track newly created files handled by scm
        """

    def track_changed_files(self):
        """
        Method to stage files that have changed
        """

    def track_file(self, path):
        """
        Method to add file to mechanism that will remind user
        to track new files
        """

    @contextmanager
    def track_file_changes(self, config=None):
        autostage = get_in(config or {}, ["core", "autostage"])

        try:
            yield
        except Exception:
            self.cleanup_ignores()
            raise

        self.reset_ignores()
        if autostage:
            self.track_changed_files()
        else:
            self.remind_to_track()
        self.reset_tracked_files()

    def belongs_to_scm(self, path):
        """Return boolean whether file belongs to scm"""

    def close(self):
        """Method to close the files"""

    def _reset(self) -> None:
        pass
