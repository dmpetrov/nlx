"""Manages Git."""

import logging
import os
import re
import shlex
from collections.abc import Mapping
from contextlib import contextmanager
from functools import partialmethod
from typing import Dict, Iterable, List, Optional, Set, Tuple, Type

from funcy import cached_property, first
from pathspec.patterns import GitWildMatchPattern

from dvc.exceptions import GitHookAlreadyExistsError
from dvc.scm.base import Base, FileNotInRepoError, RevError
from dvc.utils import relpath
from dvc.utils.fs import path_isin

from .backend.base import BaseGitBackend, NoGitBackendError
from .backend.dulwich import DulwichBackend
from .backend.gitpython import GitPythonBackend
from .backend.pygit2 import Pygit2Backend
from .stash import Stash

logger = logging.getLogger(__name__)

BackendCls = Type[BaseGitBackend]


class GitBackends(Mapping):
    DEFAULT: Dict[str, BackendCls] = {
        "dulwich": DulwichBackend,
        "pygit2": Pygit2Backend,
        "gitpython": GitPythonBackend,
    }

    def __getitem__(self, key: str) -> BaseGitBackend:
        """Lazily initialize backends and cache it afterwards"""
        initialized = self.initialized.get(key)
        if not initialized:
            backend = self.backends[key]
            initialized = backend(*self.args, **self.kwargs)
            self.initialized[key] = initialized
        return initialized

    def __init__(
        self, selected: Optional[Iterable[str]], *args, **kwargs
    ) -> None:
        selected = selected or list(self.DEFAULT)
        self.backends = {key: self.DEFAULT[key] for key in selected}

        self.initialized: Dict[str, BaseGitBackend] = {}

        self.args = args
        self.kwargs = kwargs

    def __iter__(self):
        return iter(self.backends)

    def __len__(self) -> int:
        return len(self.backends)

    def close_initialized(self) -> None:
        for backend in self.initialized.values():
            backend.close()

    def reset_all(self) -> None:
        for backend in self.initialized.values():
            backend._reset()  # pylint: disable=protected-access


class Git(Base):
    """Class for managing Git."""

    GITIGNORE = ".gitignore"
    GIT_DIR = ".git"
    LOCAL_BRANCH_PREFIX = "refs/heads/"
    RE_HEXSHA = re.compile(r"^[0-9A-Fa-f]{4,40}$")
    BAD_REF_CHARS_RE = re.compile("[\177\\s~^:?*\\[]")

    def __init__(
        self, *args, backends: Optional[Iterable[str]] = None, **kwargs
    ):
        self.ignored_paths: List[str] = []
        self.files_to_track: Set[str] = set()
        self.quiet: bool = False

        self.backends = GitBackends(backends, *args, **kwargs)
        first_ = first(self.backends.values())
        super().__init__(first_.root_dir)

    @property
    def dir(self):
        return first(self.backends.values()).dir

    @property
    def gitpython(self):
        return self.backends["gitpython"]

    @property
    def dulwich(self):
        return self.backends["dulwich"]

    @property
    def pygit2(self):
        return self.backends["pygit2"]

    @cached_property
    def stash(self):
        return Stash(self)

    @classmethod
    def clone(cls, url, to_path, **kwargs):
        for _, backend in GitBackends.DEFAULT.items():
            try:
                backend.clone(url, to_path, **kwargs)
                return Git(to_path)
            except NotImplementedError:
                pass
        raise NoGitBackendError("clone")

    @classmethod
    def is_sha(cls, rev):
        return rev and cls.RE_HEXSHA.search(rev)

    @classmethod
    def split_ref_pattern(cls, ref: str) -> Tuple[str, str]:
        name = cls.BAD_REF_CHARS_RE.split(ref, maxsplit=1)[0]
        return name, ref[len(name) :]

    @staticmethod
    def _get_git_dir(root_dir):
        return os.path.join(root_dir, Git.GIT_DIR)

    @property
    def ignore_file(self):
        return self.GITIGNORE

    def _get_gitignore(self, path):
        ignore_file_dir = os.path.dirname(path)

        assert os.path.isabs(path)
        assert os.path.isabs(ignore_file_dir)

        entry = relpath(path, ignore_file_dir).replace(os.sep, "/")
        # NOTE: using '/' prefix to make path unambiguous
        if len(entry) > 0 and entry[0] != "/":
            entry = "/" + entry

        gitignore = os.path.join(ignore_file_dir, self.GITIGNORE)

        if not path_isin(os.path.realpath(gitignore), self.root_dir):
            raise FileNotInRepoError(
                f"'{path}' is outside of git repository '{self.root_dir}'"
            )

        return entry, gitignore

    def ignore(self, path):
        entry, gitignore = self._get_gitignore(path)

        if self.is_ignored(path):
            return

        msg = "Adding '{}' to '{}'.".format(relpath(path), relpath(gitignore))
        logger.debug(msg)

        self._add_entry_to_gitignore(entry, gitignore)

        self.track_file(relpath(gitignore))

        self.ignored_paths.append(path)

    def _add_entry_to_gitignore(self, entry, gitignore):
        entry = GitWildMatchPattern.escape(entry)

        with open(gitignore, "a+", encoding="utf-8") as fobj:
            unique_lines = set(fobj.readlines())
            fobj.seek(0, os.SEEK_END)
            if fobj.tell() == 0:
                # Empty file
                prefix = ""
            else:
                fobj.seek(fobj.tell() - 1, os.SEEK_SET)
                last = fobj.read(1)
                prefix = "" if last == "\n" else "\n"
            new_entry = f"{prefix}{entry}\n"
            if new_entry not in unique_lines:
                fobj.write(new_entry)

    def ignore_remove(self, path):
        entry, gitignore = self._get_gitignore(path)

        if not os.path.exists(gitignore):
            return

        with open(gitignore) as fobj:
            lines = fobj.readlines()

        filtered = list(filter(lambda x: x.strip() != entry.strip(), lines))

        if not filtered:
            os.unlink(gitignore)
            return

        with open(gitignore, "w") as fobj:
            fobj.writelines(filtered)

        self.track_file(relpath(gitignore))

    def _install_hook(self, name):
        hook = self._hook_path(name)
        with open(hook, "w+") as fobj:
            fobj.write(f"#!/bin/sh\nexec dvc git-hook {name} $@\n")

        os.chmod(hook, 0o777)

    def _install_merge_driver(self):
        self.gitpython.repo.git.config("merge.dvc.name", "DVC merge driver")
        self.gitpython.repo.git.config(
            "merge.dvc.driver",
            (
                "dvc git-hook merge-driver "
                "--ancestor %O "
                "--our %A "
                "--their %B "
            ),
        )

    def install(self, use_pre_commit_tool=False):
        self._install_merge_driver()

        if not use_pre_commit_tool:
            self._verify_dvc_hooks()
            self._install_hook("post-checkout")
            self._install_hook("pre-commit")
            self._install_hook("pre-push")
            return

        from dvc.utils.serialize import modify_yaml

        config_path = os.path.join(self.root_dir, ".pre-commit-config.yaml")
        with modify_yaml(config_path) as config:
            entry = {
                "repo": "https://github.com/iterative/dvc",
                "rev": "master",
                "hooks": [
                    {
                        "id": "dvc-pre-commit",
                        "language_version": "python3",
                        "stages": ["commit"],
                    },
                    {
                        "id": "dvc-pre-push",
                        "language_version": "python3",
                        "stages": ["push"],
                    },
                    {
                        "id": "dvc-post-checkout",
                        "language_version": "python3",
                        "stages": ["post-checkout"],
                        "always_run": True,
                    },
                ],
            }

            if entry not in config["repos"]:
                config["repos"].append(entry)

    def cleanup_ignores(self):
        for path in self.ignored_paths:
            self.ignore_remove(path)
        self.reset_ignores()

    def reset_ignores(self):
        self.ignored_paths = []

    def reset_tracked_files(self):
        self.files_to_track = set()

    def remind_to_track(self):
        if self.quiet or not self.files_to_track:
            return

        files = " ".join(shlex.quote(path) for path in self.files_to_track)

        logger.info(
            "\n"
            "To track the changes with git, run:\n"
            "\n"
            "\tgit add {files}".format(files=files)
        )

    def track_changed_files(self):
        if not self.files_to_track:
            return

        self.add(self.files_to_track)

    def track_file(self, path: str):
        self.files_to_track.add(path)

    def belongs_to_scm(self, path):
        basename = os.path.basename(path)
        path_parts = os.path.normpath(path).split(os.path.sep)
        return basename == self.ignore_file or Git.GIT_DIR in path_parts

    def has_rev(self, rev):
        try:
            self.resolve_rev(rev)
            return True
        except RevError:
            return False

    def close(self):
        self.backends.close_initialized()

    @cached_property
    def _hooks_home(self):
        return os.path.join(self.root_dir, self.GIT_DIR, "hooks")

    def _hook_path(self, name):
        return os.path.join(self._hooks_home, name)

    def _verify_hook(self, name):
        if os.path.exists(self._hook_path(name)):
            raise GitHookAlreadyExistsError(name)

    def _verify_dvc_hooks(self):
        self._verify_hook("post-checkout")
        self._verify_hook("pre-commit")
        self._verify_hook("pre-push")

    @property
    def no_commits(self):
        return not bool(self.get_ref("HEAD"))

    def _backend_func(self, name, *args, **kwargs):
        for backend in self.backends.values():
            try:
                func = getattr(backend, name)
                return func(*args, **kwargs)
            except NotImplementedError:
                pass
        raise NoGitBackendError(name)

    def get_fs(self, rev: str):
        from dvc.fs.git import GitFileSystem

        from .objects import GitTrie

        resolved = self.resolve_rev(rev)
        tree_obj = self.pygit2.get_tree_obj(rev=resolved)
        trie = GitTrie(tree_obj, resolved)
        return GitFileSystem(self.root_dir, trie)

    is_ignored = partialmethod(_backend_func, "is_ignored")
    add = partialmethod(_backend_func, "add")
    commit = partialmethod(_backend_func, "commit")
    checkout = partialmethod(_backend_func, "checkout")
    pull = partialmethod(_backend_func, "pull")
    push = partialmethod(_backend_func, "push")
    branch = partialmethod(_backend_func, "branch")
    tag = partialmethod(_backend_func, "tag")
    untracked_files = partialmethod(_backend_func, "untracked_files")
    is_tracked = partialmethod(_backend_func, "is_tracked")
    is_dirty = partialmethod(_backend_func, "is_dirty")
    active_branch = partialmethod(_backend_func, "active_branch")
    list_branches = partialmethod(_backend_func, "list_branches")
    list_tags = partialmethod(_backend_func, "list_tags")
    list_all_commits = partialmethod(_backend_func, "list_all_commits")
    get_rev = partialmethod(_backend_func, "get_rev")
    _resolve_rev = partialmethod(_backend_func, "resolve_rev")
    resolve_commit = partialmethod(_backend_func, "resolve_commit")

    set_ref = partialmethod(_backend_func, "set_ref")
    get_ref = partialmethod(_backend_func, "get_ref")
    remove_ref = partialmethod(_backend_func, "remove_ref")
    iter_refs = partialmethod(_backend_func, "iter_refs")
    iter_remote_refs = partialmethod(_backend_func, "iter_remote_refs")
    get_refs_containing = partialmethod(_backend_func, "get_refs_containing")
    push_refspec = partialmethod(_backend_func, "push_refspec")
    fetch_refspecs = partialmethod(_backend_func, "fetch_refspecs")
    _stash_iter = partialmethod(_backend_func, "_stash_iter")
    _stash_push = partialmethod(_backend_func, "_stash_push")
    _stash_apply = partialmethod(_backend_func, "_stash_apply")
    _stash_drop = partialmethod(_backend_func, "_stash_drop")
    describe = partialmethod(_backend_func, "describe")
    diff = partialmethod(_backend_func, "diff")
    reset = partialmethod(_backend_func, "reset")
    checkout_index = partialmethod(_backend_func, "checkout_index")
    status = partialmethod(_backend_func, "status")
    merge = partialmethod(_backend_func, "merge")

    def resolve_rev(self, rev: str) -> str:
        from dvc.repo.experiments.utils import exp_refs_by_name

        try:
            return self._resolve_rev(rev)
        except RevError:
            # backends will only resolve git branch and tag names,
            # if rev is not a sha it may be an abbreviated experiment name
            if not self.is_sha(rev) and not rev.startswith("refs/"):
                ref_infos = list(exp_refs_by_name(self, rev))
                if len(ref_infos) == 1:
                    return self.get_ref(str(ref_infos[0]))
                if len(ref_infos) > 1:
                    raise RevError(f"ambiguous Git revision '{rev}'")
            raise

    def branch_revs(
        self, branch: str, end_rev: Optional[str] = None
    ) -> Iterable[str]:
        """Iterate over revisions in a given branch (from newest to oldest).

        If end_rev is set, iterator will stop when the specified revision is
        reached.
        """
        commit = self.resolve_commit(branch)
        while commit is not None:
            yield commit.hexsha
            parent = first(commit.parents)
            if parent is None or parent == end_rev:
                return
            commit = self.resolve_commit(parent)

    @contextmanager
    def detach_head(self, rev: Optional[str] = None, force: bool = False):
        """Context manager for performing detached HEAD SCM operations.

        Detaches and restores HEAD similar to interactive git rebase.
        Restore is equivalent to 'reset --soft', meaning the caller is
        is responsible for preserving & restoring working tree state
        (i.e. via stash) when applicable.

        Yields revision of detached head.
        """
        if not rev:
            rev = "HEAD"
        orig_head = self.get_ref("HEAD", follow=False)
        logger.debug("Detaching HEAD at '%s'", rev)
        self.checkout(rev, detach=True, force=force)
        try:
            yield self.get_ref("HEAD")
        finally:
            prefix = self.LOCAL_BRANCH_PREFIX
            if orig_head.startswith(prefix):
                symbolic = True
                name = orig_head[len(prefix) :]
            else:
                symbolic = False
                name = orig_head
            self.set_ref(
                "HEAD",
                orig_head,
                symbolic=symbolic,
                message=f"dvc: Restore HEAD to '{name}'",
            )
            logger.debug("Restore HEAD to '%s'", name)
            self.reset()

    @contextmanager
    def stash_workspace(self, **kwargs):
        """Stash and restore any workspace changes.

        Yields revision of the stash commit. Yields None if there were no
        changes to stash.
        """
        logger.debug("Stashing workspace")
        rev = self.stash.push(**kwargs)
        try:
            yield rev
        finally:
            if rev:
                logger.debug("Restoring stashed workspace")
                self.stash.pop()

    def _reset(self) -> None:
        self.backends.reset_all()
