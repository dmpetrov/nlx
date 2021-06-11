import locale
import logging
import os
import stat
from contextlib import contextmanager
from io import BytesIO, StringIO
from typing import Callable, Iterable, List, Mapping, Optional, Tuple, Union

from funcy import cached_property

from dvc.scm.base import MergeConflictError, RevError, SCMError
from dvc.utils import relpath

from ..objects import GitCommit, GitObject
from .base import BaseGitBackend

logger = logging.getLogger(__name__)


class Pygit2Object(GitObject):
    def __init__(self, obj):
        self.obj = obj

    def open(self, mode: str = "r", encoding: str = None):
        if not encoding:
            encoding = locale.getpreferredencoding(False)
        data = self.obj.read_raw()
        if mode == "rb":
            return BytesIO(data)
        return StringIO(data.decode(encoding))

    @property
    def name(self) -> str:
        return self.obj.name

    @property
    def mode(self):
        if not self.obj.filemode and self.obj.type_str == "tree":
            return stat.S_IFDIR
        return self.obj.filemode

    def scandir(self) -> Iterable["Pygit2Object"]:
        for entry in self.obj:  # noqa: B301
            yield Pygit2Object(entry)


class Pygit2Backend(BaseGitBackend):  # pylint:disable=abstract-method
    def __init__(  # pylint:disable=W0231
        self, root_dir=os.curdir, search_parent_directories=True
    ):
        import pygit2

        if search_parent_directories:
            ceiling_dirs = ""
        else:
            ceiling_dirs = os.path.abspath(root_dir)

        # NOTE: discover_repository will return path/.git/
        path = pygit2.discover_repository(  # pylint:disable=no-member
            root_dir, True, ceiling_dirs
        )
        if not path:
            raise SCMError(f"{root_dir} is not a git repository")

        self.repo = pygit2.Repository(path)

        self._stashes: dict = {}

    def close(self):
        if hasattr(self, "_refdb"):
            del self._refdb
        self.repo.free()

    @property
    def root_dir(self) -> str:
        return self.repo.workdir

    @cached_property
    def _refdb(self):
        from pygit2 import RefdbFsBackend

        return RefdbFsBackend(self.repo)

    @property
    def default_signature(self):
        try:
            return self.repo.default_signature
        except KeyError as exc:
            raise SCMError(
                "Git username and email must be configured"
            ) from exc

    # Workaround to force git_backend_odb_pack to release open file handles
    # in DVC's mixed git-backend environment.
    # See https://github.com/iterative/dvc/issues/5641
    @contextmanager
    def release_odb_handles(self):
        yield
        # It is safe to free the libgit2 repo context multiple times - free
        # just forces libgit/pygit to release git ODB related contexts which
        # can be reacquired later as needed.
        self.repo.free()

    @staticmethod
    def clone(
        url: str,
        to_path: str,
        rev: Optional[str] = None,
        shallow_branch: Optional[str] = None,
    ):
        raise NotImplementedError

    @property
    def dir(self) -> str:
        raise NotImplementedError

    def add(self, paths: Union[str, Iterable[str]], update=False):
        raise NotImplementedError

    def commit(self, msg: str, no_verify: bool = False):
        raise NotImplementedError

    def checkout(
        self,
        branch: str,
        create_new: Optional[bool] = False,
        force: bool = False,
        **kwargs,
    ):
        from pygit2 import GIT_CHECKOUT_FORCE, GitError

        checkout_strategy = GIT_CHECKOUT_FORCE if force else None

        with self.release_odb_handles():
            if create_new:
                commit = self.repo.revparse_single("HEAD")
                new_branch = self.repo.branches.local.create(branch, commit)
                self.repo.checkout(new_branch, strategy=checkout_strategy)
            else:
                if branch == "-":
                    branch = "@{-1}"
                try:
                    commit, ref = self.repo.resolve_refish(branch)
                except (KeyError, GitError):
                    raise RevError(f"unknown Git revision '{branch}'")
                self.repo.checkout_tree(commit, strategy=checkout_strategy)
                detach = kwargs.get("detach", False)
                if ref and not detach:
                    self.repo.set_head(ref.name)
                else:
                    self.repo.set_head(commit.id)

    def pull(self, **kwargs):
        raise NotImplementedError

    def push(self):
        raise NotImplementedError

    def branch(self, branch: str):
        from pygit2 import GitError

        try:
            commit = self.repo[self.repo.head.target]
            self.repo.create_branch(branch, commit)
        except GitError as exc:
            raise SCMError(f"Failed to create branch '{branch}'") from exc

    def tag(self, tag: str):
        raise NotImplementedError

    def untracked_files(self) -> Iterable[str]:
        raise NotImplementedError

    def is_tracked(self, path: str) -> bool:
        raise NotImplementedError

    def is_dirty(self, untracked_files: bool = False) -> bool:
        raise NotImplementedError

    def active_branch(self) -> str:
        raise NotImplementedError

    def list_branches(self) -> Iterable[str]:
        raise NotImplementedError

    def list_tags(self) -> Iterable[str]:
        raise NotImplementedError

    def list_all_commits(self) -> Iterable[str]:
        raise NotImplementedError

    def get_tree_obj(self, rev: str, **kwargs) -> Pygit2Object:
        tree = self.repo[rev].tree
        return Pygit2Object(tree)

    def get_rev(self) -> str:
        raise NotImplementedError

    def resolve_rev(self, rev: str) -> str:
        from pygit2 import GitError

        try:
            commit, _ref = self.repo.resolve_refish(rev)
            return str(commit.id)
        except (KeyError, GitError):
            pass

        # Look for single exact match in remote refs
        shas = {
            self.get_ref(f"refs/remotes/{remote.name}/{rev}")
            for remote in self.repo.remotes
        } - {None}
        if len(shas) > 1:
            raise RevError(f"ambiguous Git revision '{rev}'")
        if len(shas) == 1:
            return shas.pop()  # type: ignore
        raise RevError(f"unknown Git revision '{rev}'")

    def resolve_commit(self, rev: str) -> "GitCommit":
        from pygit2 import GitError

        try:
            commit, _ref = self.repo.resolve_refish(rev)
        except (KeyError, GitError):
            raise SCMError(f"Invalid commit '{rev}'")
        return GitCommit(
            str(commit.id),
            commit.commit_time,
            commit.commit_time_offset,
            commit.message,
            [str(parent) for parent in commit.parent_ids],
        )

    def _get_stash(self, ref: str):
        raise NotImplementedError

    def is_ignored(self, path: str) -> bool:
        rel = relpath(path, self.root_dir)
        if os.name == "nt":
            rel.replace("\\", "/")
        return self.repo.path_is_ignored(rel)

    def set_ref(
        self,
        name: str,
        new_ref: str,
        old_ref: Optional[str] = None,
        message: Optional[str] = None,
        symbolic: Optional[bool] = False,
    ):
        if old_ref and old_ref != self.get_ref(name, follow=False):
            raise SCMError(f"Failed to set '{name}'")

        if message:
            self._refdb.ensure_log(name)
        if symbolic:
            self.repo.create_reference_symbolic(
                name, new_ref, True, message=message
            )
        else:
            self.repo.create_reference_direct(
                name, new_ref, True, message=message
            )

    def get_ref(self, name, follow: bool = True) -> Optional[str]:
        from pygit2 import GIT_REF_SYMBOLIC

        ref = self.repo.references.get(name)
        if not ref:
            return None
        if follow and ref.type == GIT_REF_SYMBOLIC:
            ref = ref.resolve()
        return str(ref.target)

    def remove_ref(self, name: str, old_ref: Optional[str] = None):
        ref = self.repo.references.get(name)
        if not ref:
            raise SCMError(f"Ref '{name}' does not exist")
        if old_ref and old_ref != str(ref.target):
            raise SCMError(f"Failed to remove '{name}'")
        ref.delete()

    def iter_refs(self, base: Optional[str] = None):
        if base:
            for ref in self.repo.references:
                if ref.startswith(base):
                    yield ref
        else:
            yield from self.repo.references

    def get_refs_containing(self, rev: str, pattern: Optional[str] = None):
        import fnmatch

        from pygit2 import GitError

        def _contains(repo, ref, search_commit):
            commit, _ref = self.repo.resolve_refish(ref)
            base = repo.merge_base(search_commit.id, commit.id)
            return base == search_commit.id

        try:
            search_commit, _ref = self.repo.resolve_refish(rev)
        except (KeyError, GitError):
            raise SCMError(f"Invalid rev '{rev}'")

        if not pattern:
            yield from (
                ref
                for ref in self.iter_refs()
                if _contains(self.repo, ref, search_commit)
            )
            return

        literal = pattern.rstrip("/").split("/")
        for ref in self.iter_refs():
            if (
                ref.split("/")[: len(literal)] == literal
                or fnmatch.fnmatch(ref, pattern)
            ) and _contains(self.repo, ref, search_commit):
                yield ref

    def push_refspec(
        self,
        url: str,
        src: Optional[str],
        dest: str,
        force: bool = False,
        on_diverged: Optional[Callable[[str, str], bool]] = None,
    ):
        raise NotImplementedError

    def fetch_refspecs(
        self,
        url: str,
        refspecs: Iterable[str],
        force: Optional[bool] = False,
        on_diverged: Optional[Callable[[str, str], bool]] = None,
    ):
        raise NotImplementedError

    def _stash_iter(self, ref: str):
        raise NotImplementedError

    def _stash_push(
        self,
        ref: str,
        message: Optional[str] = None,
        include_untracked: Optional[bool] = False,
    ) -> Tuple[Optional[str], bool]:
        from dvc.scm.git import Stash

        oid = self.repo.stash(
            self.default_signature,
            message=message,
            include_untracked=include_untracked,
        )
        commit = self.repo[oid]

        if ref != Stash.DEFAULT_STASH:
            self.set_ref(ref, commit.id, message=commit.message)
            self.repo.stash_drop()
        return str(oid), False

    def _stash_apply(self, rev: str):
        from pygit2 import (
            GIT_CHECKOUT_RECREATE_MISSING,
            GIT_CHECKOUT_SAFE,
            GitError,
        )

        from dvc.scm.git import Stash

        def _apply(index):
            try:
                self.repo.index.read(False)
                self.repo.stash_apply(
                    index,
                    strategy=GIT_CHECKOUT_SAFE | GIT_CHECKOUT_RECREATE_MISSING,
                )
            except GitError as exc:
                raise MergeConflictError(
                    "Stash apply resulted in merge conflicts"
                ) from exc

        # libgit2 stash apply only accepts refs/stash items by index. If rev is
        # not in refs/stash, we will push it onto the stash, and then pop it
        commit, _ref = self.repo.resolve_refish(rev)
        stash = self.repo.references.get(Stash.DEFAULT_STASH)
        if stash:
            for i, entry in enumerate(stash.log()):
                if entry.oid_new == commit.id:
                    _apply(i)
                    return

        self.set_ref(Stash.DEFAULT_STASH, commit.id, message=commit.message)
        try:
            _apply(0)
        finally:
            self.repo.stash_drop()

    def _stash_drop(self, ref: str, index: int):
        from dvc.scm.git import Stash

        if ref != Stash.DEFAULT_STASH:
            raise NotImplementedError

        self.repo.stash_drop(index)

    def describe(
        self,
        rev: str,
        base: Optional[str] = None,
        match: Optional[str] = None,
        exclude: Optional[str] = None,
    ) -> Optional[str]:
        raise NotImplementedError

    def diff(self, rev_a: str, rev_b: str, binary=False) -> str:
        raise NotImplementedError

    def reset(self, hard: bool = False, paths: Iterable[str] = None):
        from pygit2 import GIT_RESET_HARD, GIT_RESET_MIXED, IndexEntry

        self.repo.index.read(False)
        if paths is not None:
            tree = self.repo.revparse_single("HEAD").tree
            for path in paths:
                rel = relpath(path, self.root_dir)
                if os.name == "nt":
                    rel = rel.replace("\\", "/")
                obj = tree[rel]
                self.repo.index.add(IndexEntry(rel, obj.oid, obj.filemode))
            self.repo.index.write()
        elif hard:
            self.repo.reset(self.repo.head.target, GIT_RESET_HARD)
        else:
            self.repo.reset(self.repo.head.target, GIT_RESET_MIXED)

    def checkout_index(
        self,
        paths: Optional[Iterable[str]] = None,
        force: bool = False,
        ours: bool = False,
        theirs: bool = False,
    ):
        from pygit2 import (
            GIT_CHECKOUT_ALLOW_CONFLICTS,
            GIT_CHECKOUT_FORCE,
            GIT_CHECKOUT_RECREATE_MISSING,
            GIT_CHECKOUT_SAFE,
        )

        assert not (ours and theirs)
        strategy = GIT_CHECKOUT_RECREATE_MISSING
        if force or ours or theirs:
            strategy |= GIT_CHECKOUT_FORCE
        else:
            strategy |= GIT_CHECKOUT_SAFE

        if ours or theirs:
            strategy |= GIT_CHECKOUT_ALLOW_CONFLICTS

        index = self.repo.index
        if paths:
            path_list: Optional[List[str]] = [
                relpath(path, self.root_dir) for path in paths
            ]
            if os.name == "nt":
                path_list = [
                    path.replace("\\", "/")
                    for path in path_list  # type: ignore[union-attr]
                ]
        else:
            path_list = None

        with self.release_odb_handles():
            self.repo.checkout_index(
                index=index, paths=path_list, strategy=strategy
            )

            if index.conflicts and (ours or theirs):
                for ancestor, ours_entry, theirs_entry in index.conflicts:
                    if not ancestor:
                        continue
                    if ours:
                        entry = ours_entry
                        index.add(ours_entry)
                    else:
                        entry = theirs_entry
                    path = os.path.join(self.root_dir, entry.path)
                    with open(path, "wb") as fobj:
                        fobj.write(self.repo.get(entry.id).read_raw())
                    index.add(entry.path)
                index.write()

    def iter_remote_refs(self, url: str, base: Optional[str] = None):
        raise NotImplementedError

    def status(
        self, ignored: bool = False
    ) -> Tuple[Mapping[str, Iterable[str]], Iterable[str], Iterable[str]]:
        raise NotImplementedError

    def merge(
        self,
        rev: str,
        commit: bool = True,
        msg: Optional[str] = None,
        squash: bool = False,
    ) -> Optional[str]:
        from pygit2 import GIT_RESET_MIXED, GitError

        if commit and squash:
            raise SCMError("Cannot merge with 'squash' and 'commit'")

        if commit and not msg:
            raise SCMError("Merge commit message is required")

        with self.release_odb_handles():
            try:
                self.repo.index.read(False)
                self.repo.merge(rev)
                self.repo.index.write()
            except GitError as exc:
                raise SCMError("Merge failed") from exc

            if self.repo.index.conflicts:
                raise MergeConflictError("Merge contained conflicts")

            if commit:
                user = self.default_signature
                tree = self.repo.index.write_tree()
                merge_commit = self.repo.create_commit(
                    "HEAD", user, user, msg, tree, [self.repo.head.target, rev]
                )
                return str(merge_commit)
            if squash:
                self.repo.reset(self.repo.head.target, GIT_RESET_MIXED)
                self.repo.state_cleanup()
                self.repo.index.write()
        return None
