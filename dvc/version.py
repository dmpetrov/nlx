# Used in setup.py, so don't pull any additional dependencies
#
# Based on:
#   - https://github.com/python/mypy/blob/master/mypy/version.py
#   - https://github.com/python/mypy/blob/master/mypy/git.py
import os
import subprocess

_BASE_VERSION = "2.3.0"


def _generate_version(base_version):
    """Generate a version with information about the Git repository."""
    pkg_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    if not _is_git_repo(pkg_dir) or not _have_git():
        return base_version

    if _is_release(pkg_dir, base_version) and not _is_dirty(pkg_dir):
        return base_version

    return "{base_version}+{short_sha}{dirty}".format(
        base_version=base_version,
        short_sha=_git_revision(pkg_dir).decode("utf-8")[0:6],
        dirty=".mod" if _is_dirty(pkg_dir) else "",
    )


def _is_git_repo(dir_path):
    """Is the given directory version-controlled with Git?"""
    return os.path.exists(os.path.join(dir_path, ".git"))


def _have_git():
    """Can we run the git executable?"""
    try:
        subprocess.check_output(["git", "--help"])
        return True
    except subprocess.CalledProcessError:
        return False
    except OSError:
        return False


def _is_release(dir_path, base_version):
    try:
        output = subprocess.check_output(
            ["git", "describe", "--tags", "--exact-match"],
            cwd=dir_path,
            stderr=subprocess.STDOUT,
        ).decode("utf-8")
        tag = output.strip()
        return tag == base_version
    except subprocess.CalledProcessError:
        return False


def _git_revision(dir_path):
    """Get SHA of the HEAD of a Git repository."""
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=dir_path
    ).strip()


def _is_dirty(dir_path):
    """Check whether a git repository has uncommitted changes."""
    try:
        subprocess.check_call(["git", "diff", "--quiet"], cwd=dir_path)
        return False
    except subprocess.CalledProcessError:
        return True


__version__ = _generate_version(_BASE_VERSION)
