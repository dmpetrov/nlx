import io
import locale
import logging
import os
from functools import partial
from typing import Callable, Iterable, List, Mapping, Optional, Tuple, Union

from funcy import ignore

from dvc.progress import Tqdm
from dvc.scm.base import CloneError, MergeConflictError, RevError, SCMError
from dvc.utils import fix_env, is_binary, relpath

from ..objects import GitCommit, GitObject
from .base import BaseGitBackend

logger = logging.getLogger(__name__)


class TqdmGit(Tqdm):
    def update_git(self, op_code, cur_count, max_count=None, message=""):
        op_code = self.code2desc(op_code)
        if op_code:
            message = (op_code + " | " + message) if message else op_code
        if message:
            self.postfix["info"] = f" {message} | "
        self.update_to(cur_count, max_count)

    @staticmethod
    def code2desc(op_code):
        from git import RootUpdateProgress as OP

        ops = {
            OP.COUNTING: "Counting",
            OP.COMPRESSING: "Compressing",
            OP.WRITING: "Writing",
            OP.RECEIVING: "Receiving",
            OP.RESOLVING: "Resolving",
            OP.FINDING_SOURCES: "Finding sources",
            OP.CHECKING_OUT: "Checking out",
            OP.CLONE: "Cloning",
            OP.FETCH: "Fetching",
            OP.UPDWKTREE: "Updating working tree",
            OP.REMOVE: "Removing",
            OP.PATHCHANGE: "Changing path",
            OP.URLCHANGE: "Changing URL",
            OP.BRANCHCHANGE: "Changing branch",
        }
        return ops.get(op_code & OP.OP_MASK, "")


class GitPythonObject(GitObject):
    def __init__(self, obj):
        self.obj = obj

    def open(self, mode: str = "r", encoding: str = None):
        if not encoding:
            encoding = locale.getpreferredencoding(False)
        # GitPython's obj.data_stream is a fragile thing, it is better to
        # read it immediately, also it needs to be to decoded if we follow
        # the `open()` behavior (since data_stream.read() returns bytes,
        # and `open` with default "r" mode returns str)
        data = self.obj.data_stream.read()
        if mode == "rb":
            return io.BytesIO(data)
        return io.StringIO(data.decode(encoding))

    @property
    def name(self) -> str:
        # NOTE: `obj.name` is not always a basename. See [1] for more details.
        #
        # [1] https://github.com/iterative/dvc/issues/3481
        return os.path.basename(self.obj.path)

    @property
    def mode(self) -> int:
        return self.obj.mode

    def scandir(self) -> Iterable["GitPythonObject"]:
        for obj in self.obj:
            yield GitPythonObject(obj)


class GitPythonBackend(BaseGitBackend):  # pylint:disable=abstract-method
    """git-python Git backend."""

    def __init__(  # pylint:disable=W0231
        self, root_dir=os.curdir, search_parent_directories=True
    ):
        import git
        from git.exc import InvalidGitRepositoryError

        try:
            self.repo = git.Repo(
                root_dir, search_parent_directories=search_parent_directories
            )
        except InvalidGitRepositoryError:
            msg = "{} is not a git repository"
            raise SCMError(msg.format(root_dir))

        # NOTE: fixing LD_LIBRARY_PATH for binary built by PyInstaller.
        # http://pyinstaller.readthedocs.io/en/stable/runtime-information.html
        env = fix_env(None)
        libpath = env.get("LD_LIBRARY_PATH", None)
        self.repo.git.update_environment(LD_LIBRARY_PATH=libpath)

    def close(self):
        self.repo.close()

    @property
    def git(self):
        return self.repo.git

    def is_ignored(self, path: str) -> bool:
        from git.exc import GitCommandError

        func = ignore(GitCommandError)(self.repo.git.check_ignore)
        return bool(func(path))

    @property
    def root_dir(self) -> str:
        return self.repo.working_tree_dir

    @staticmethod
    def clone(
        url: str,
        to_path: str,
        rev: Optional[str] = None,
        shallow_branch: Optional[str] = None,
    ):
        import git

        ld_key = "LD_LIBRARY_PATH"

        env = fix_env(None)
        if is_binary() and ld_key not in env.keys():
            # In fix_env, we delete LD_LIBRARY_PATH key if it was empty before
            # PyInstaller modified it. GitPython, in git.Repo.clone_from, uses
            # env to update its own internal state. When there is no key in
            # env, this value is not updated and GitPython re-uses
            # LD_LIBRARY_PATH that has been set by PyInstaller.
            # See [1] for more info.
            # [1] https://github.com/gitpython-developers/GitPython/issues/924
            env[ld_key] = ""

        try:
            if shallow_branch is not None and os.path.exists(url):
                # git disables --depth for local clones unless file:// url
                # scheme is used
                url = f"file://{url}"
            with TqdmGit(desc="Cloning", unit="obj") as pbar:
                clone_from = partial(
                    git.Repo.clone_from,
                    url,
                    to_path,
                    env=env,  # needed before we can fix it in __init__
                    no_single_branch=True,
                    progress=pbar.update_git,
                )
                if shallow_branch is None:
                    tmp_repo = clone_from()
                else:
                    tmp_repo = clone_from(branch=shallow_branch, depth=1)
            tmp_repo.close()
        except git.exc.GitCommandError as exc:  # pylint: disable=no-member
            raise CloneError(url, to_path) from exc

        # NOTE: using our wrapper to make sure that env is fixed in __init__
        repo = GitPythonBackend(to_path)

        if rev:
            try:
                repo.checkout(rev)
            except git.exc.GitCommandError as exc:  # pylint: disable=no-member
                raise RevError(
                    "failed to access revision '{}' for repo '{}'".format(
                        rev, url
                    )
                ) from exc

    @staticmethod
    def is_sha(rev):
        import git

        return rev and git.Repo.re_hexsha_shortened.search(rev)

    @property
    def dir(self) -> str:
        return self.repo.git_dir

    def add(self, paths: Union[str, Iterable[str]], update=False):
        # NOTE: GitPython is not currently able to handle index version >= 3.
        # See https://github.com/iterative/dvc/issues/610 for more details.
        try:
            if update:
                if isinstance(paths, str):
                    paths = [paths]
                self.git.add(*paths, update=True)
            else:
                self.repo.index.add(paths)
        except AssertionError:
            msg = (
                "failed to add '{}' to git. You can add those files "
                "manually using `git add`. See "
                "https://github.com/iterative/dvc/issues/610 for more "
                "details.".format(str(paths))
            )

            logger.exception(msg)

    def commit(self, msg: str, no_verify: bool = False):
        from git.exc import HookExecutionError

        try:
            self.repo.index.commit(msg, skip_hooks=no_verify)
        except HookExecutionError as exc:
            raise SCMError("Git pre-commit hook failed") from exc

    def checkout(
        self,
        branch: str,
        create_new: Optional[bool] = False,
        force: bool = False,
        **kwargs,
    ):
        if create_new:
            self.repo.git.checkout("HEAD", b=branch, force=force, **kwargs)
        else:
            self.repo.git.checkout(branch, force=force, **kwargs)

    def pull(self, **kwargs):
        infos = self.repo.remote().pull(**kwargs)
        for info in infos:
            if info.flags & info.ERROR:
                raise SCMError(f"pull failed: {info.note}")

    def push(self):
        infos = self.repo.remote().push()
        for info in infos:
            if info.flags & info.ERROR:
                raise SCMError(f"push failed: {info.summary}")

    def branch(self, branch):
        self.repo.git.branch(branch)

    def tag(self, tag):
        self.repo.git.tag(tag)

    def untracked_files(self):
        files = self.repo.untracked_files
        return [os.path.join(self.repo.working_dir, fname) for fname in files]

    def is_tracked(self, path):
        return bool(self.repo.git.ls_files(path))

    def is_dirty(self, untracked_files: bool = False) -> bool:
        return self.repo.is_dirty(untracked_files=untracked_files)

    def active_branch(self):
        return self.repo.active_branch.name

    def list_branches(self):
        return [h.name for h in self.repo.heads]

    def list_tags(self):
        return [t.name for t in self.repo.tags]

    def list_all_commits(self):
        head = self.get_ref("HEAD")
        if not head:
            # Empty repo
            return []

        return [
            c.hexsha
            for c in self.repo.iter_commits(
                rev=head, branches=True, tags=True, remotes=True
            )
        ]

    def get_tree_obj(self, rev: str, **kwargs) -> GitPythonObject:
        tree = self.repo.tree(rev)
        return GitPythonObject(tree)

    def get_rev(self):
        return self.repo.rev_parse("HEAD").hexsha

    def resolve_rev(self, rev):
        from contextlib import suppress

        from git.exc import BadName, GitCommandError

        def _resolve_rev(name):
            with suppress(BadName, GitCommandError):
                try:
                    # Try python implementation of rev-parse first, it's faster
                    return self.repo.rev_parse(name).hexsha
                except NotImplementedError:
                    # Fall back to `git rev-parse` for advanced features
                    return self.repo.git.rev_parse(name)
                except ValueError:
                    raise RevError(f"unknown Git revision '{name}'")

        # Resolve across local names
        sha = _resolve_rev(rev)
        if sha:
            return sha

        # Try all the remotes and if it resolves unambiguously then take it
        if not self.is_sha(rev):
            shas = {
                _resolve_rev(f"{remote.name}/{rev}")
                for remote in self.repo.remotes
            } - {None}
            if len(shas) > 1:
                raise RevError(f"ambiguous Git revision '{rev}'")
            if len(shas) == 1:
                return shas.pop()

        raise RevError(f"unknown Git revision '{rev}'")

    def resolve_commit(self, rev: str) -> "GitCommit":
        """Return Commit object for the specified revision."""
        from git.exc import BadName, GitCommandError
        from git.objects.tag import TagObject

        try:
            commit = self.repo.rev_parse(rev)
        except (BadName, GitCommandError):
            raise SCMError(f"Invalid commit '{rev}'")
        if isinstance(commit, TagObject):
            commit = commit.object
        return GitCommit(
            commit.hexsha,
            commit.committed_date,
            commit.committer_tz_offset,
            commit.message,
            [str(parent) for parent in commit.parents],
        )

    def set_ref(
        self,
        name: str,
        new_ref: str,
        old_ref: Optional[str] = None,
        message: Optional[str] = None,
        symbolic: Optional[bool] = False,
    ):
        from git.exc import GitCommandError

        if old_ref and self.get_ref(name) != old_ref:
            raise SCMError(f"Failed to set ref '{name}'")
        try:
            if symbolic:
                if message:
                    self.git.symbolic_ref(name, new_ref, m=message)
                else:
                    self.git.symbolic_ref(name, new_ref)
            else:
                args = [name, new_ref]
                if old_ref:
                    args.append(old_ref)
                if message:
                    self.git.update_ref(*args, m=message, create_reflog=True)
                else:
                    self.git.update_ref(*args)
        except GitCommandError as exc:
            raise SCMError(f"Failed to set ref '{name}'") from exc

    def get_ref(self, name: str, follow: bool = True) -> Optional[str]:
        from git.exc import GitCommandError

        if name == "HEAD":
            try:
                if follow or self.repo.head.is_detached:
                    return self.repo.head.commit.hexsha
                return f"refs/heads/{self.repo.active_branch}"
            except (GitCommandError, ValueError):
                return None
        elif name.startswith("refs/heads/"):
            name = name[11:]
            if name in self.repo.heads:
                return self.repo.heads[name].commit.hexsha
        elif name.startswith("refs/tags/"):
            name = name[10:]
            if name in self.repo.tags:
                return self.repo.tags[name].commit.hexsha
        else:
            if not follow:
                try:
                    rev = self.git.symbolic_ref(name).strip()
                    return rev if rev else None
                except GitCommandError:
                    pass
            try:
                rev = self.git.show_ref(name, hash=True).strip()
                return rev if rev else None
            except GitCommandError:
                pass
        return None

    def remove_ref(self, name: str, old_ref: Optional[str] = None):
        from git.exc import GitCommandError

        if old_ref and self.get_ref(name) != old_ref:
            raise SCMError(f"Failed to set ref '{name}'")
        try:
            args = [name]
            if old_ref:
                args.append(old_ref)
            self.git.update_ref(*args, d=True)
        except GitCommandError as exc:
            raise SCMError(f"Failed to set ref '{name}'") from exc

    def iter_refs(self, base: Optional[str] = None):
        from git import Reference

        for ref in Reference.iter_items(self.repo, common_path=base):
            yield ref.path

    def iter_remote_refs(self, url: str, base: Optional[str] = None):
        raise NotImplementedError

    def get_refs_containing(self, rev: str, pattern: Optional[str] = None):
        from git.exc import GitCommandError

        try:
            if pattern:
                args = [pattern]
            else:
                args = []
            for line in self.git.for_each_ref(
                *args, contains=rev, format=r"%(refname)"
            ).splitlines():
                line = line.strip()
                if line:
                    yield line
        except GitCommandError:
            pass

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

        args = ["push"]
        if message:
            args.extend(["-m", message])
        if include_untracked:
            args.append("--include-untracked")
        self.git.stash(*args)
        commit = self.resolve_commit("stash@{0}")
        if ref != Stash.DEFAULT_STASH:
            # `git stash` CLI doesn't support using custom refspecs,
            # so we push a commit onto refs/stash, make our refspec
            # point to the new commit, then pop it from refs/stash
            # `git stash create` is intended to be used for this kind of
            # behavior but it doesn't support --include-untracked so we need to
            # use push
            self.set_ref(ref, commit.hexsha, message=commit.message)
            self.git.stash("drop")
        return commit.hexsha, False

    def _stash_apply(self, rev: str):
        from git.exc import GitCommandError

        try:
            self.git.stash("apply", rev)
        except GitCommandError as exc:
            out = str(exc)
            if "CONFLICT" in out or "already exists" in out:
                raise MergeConflictError(
                    "Stash apply resulted in merge conflicts"
                ) from exc
            raise SCMError("Could not apply stash") from exc

    def _stash_drop(self, ref: str, index: int):
        from git.exc import GitCommandError

        from dvc.scm.git import Stash

        if ref == Stash.DEFAULT_STASH:
            self.git.stash("drop", index)
            return

        self.git.reflog(
            "delete", "--updateref", "--rewrite", f"{ref}@{{{index}}}"
        )
        try:
            self.git.reflog("exists", ref)
        except GitCommandError:
            self.remove_ref(ref)

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
        if paths:
            paths_list: Optional[List[str]] = [
                relpath(path, self.root_dir) for path in paths
            ]
            if os.name == "nt":
                paths_list = [
                    path.replace("\\", "/")
                    for path in paths_list  # type: ignore[union-attr]
                ]
        else:
            paths_list = None
        self.repo.head.reset(index=True, working_tree=hard, paths=paths_list)

    def checkout_index(
        self,
        paths: Optional[Iterable[str]] = None,
        force: bool = False,
        ours: bool = False,
        theirs: bool = False,
    ):
        """Checkout the specified paths from HEAD index."""
        assert not (ours and theirs)
        if ours or theirs:
            args = ["--ours"] if ours else ["--theirs"]
            if force:
                args.append("--force")
            args.append("--")
            if paths:
                args.extend(list(paths))
            else:
                args.append(".")
            self.repo.git.checkout(*args)
        else:
            if paths:
                paths_list: Optional[List[str]] = [
                    relpath(path, self.root_dir) for path in paths
                ]
                if os.name == "nt":
                    paths_list = [
                        path.replace("\\", "/")
                        for path in paths_list  # type: ignore[union-attr]
                    ]
            else:
                paths_list = None
            self.repo.index.checkout(paths=paths_list, force=force)

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
        from git.exc import GitCommandError

        if commit and squash:
            raise SCMError("Cannot merge with 'squash' and 'commit'")

        if commit and not msg:
            raise SCMError("Merge commit message is required")

        merge = partial(self.git.merge, rev)
        try:
            if commit:
                merge(m=msg)
                return self.get_rev()
            merge(no_commit=True, squash=True)
        except GitCommandError as exc:
            if "CONFLICT" in str(exc):
                raise MergeConflictError("Merge contained conflicts") from exc
            raise SCMError("Merge failed") from exc
        return None
