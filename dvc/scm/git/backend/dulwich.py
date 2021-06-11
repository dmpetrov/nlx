import fnmatch
import locale
import logging
import os
import stat
from io import BytesIO, StringIO
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Tuple,
    Union,
)

from funcy import cached_property

from dvc.path_info import PathInfo
from dvc.progress import Tqdm
from dvc.scm.base import SCMError
from dvc.utils import relpath

from ..objects import GitObject
from .base import BaseGitBackend

if TYPE_CHECKING:
    from ..objects import GitCommit

logger = logging.getLogger(__name__)


class DulwichObject(GitObject):
    def __init__(self, repo, name, mode, sha):
        self.repo = repo
        self._name = name
        self._mode = mode
        self.sha = sha

    def open(self, mode: str = "r", encoding: str = None):
        if not encoding:
            encoding = locale.getpreferredencoding(False)
        # NOTE: we didn't load the object before as Dulwich will also try to
        # load the contents of it into memory, which will slow down Trie
        # building considerably.
        obj = self.repo[self.sha]
        data = obj.as_raw_string()
        if mode == "rb":
            return BytesIO(data)
        return StringIO(data.decode(encoding))

    @property
    def name(self) -> str:
        return self._name

    @property
    def mode(self) -> int:
        return self._mode

    def scandir(self) -> Iterable["DulwichObject"]:
        tree = self.repo[self.sha]
        for entry in tree.iteritems():  # noqa: B301
            yield DulwichObject(
                self.repo, entry.path.decode(), entry.mode, entry.sha
            )


class DulwichBackend(BaseGitBackend):  # pylint:disable=abstract-method
    """Dulwich Git backend."""

    # Dulwich progress will return messages equivalent to git CLI,
    # our pbars should just display the messages as formatted by dulwich
    BAR_FMT_NOTOTAL = "{desc}{bar:b}|{postfix[info]} [{elapsed}]"

    def __init__(  # pylint:disable=W0231
        self, root_dir=os.curdir, search_parent_directories=True
    ):
        from dulwich.errors import NotGitRepository
        from dulwich.repo import Repo

        try:
            if search_parent_directories:
                self.repo = Repo.discover(start=root_dir)
            else:
                self.repo = Repo(root_dir)
        except NotGitRepository as exc:
            raise SCMError(f"{root_dir} is not a git repository") from exc

        self._submodules: Dict[str, "PathInfo"] = self._find_submodules()
        self._stashes: dict = {}

    def _find_submodules(self) -> Dict[str, "PathInfo"]:
        """Return dict mapping submodule names to submodule paths.

        Submodule paths will be relative to Git repo root.
        """
        from dulwich.config import ConfigFile, parse_submodules

        submodules: Dict[str, "PathInfo"] = {}
        config_path = os.path.join(self.root_dir, ".gitmodules")
        if os.path.isfile(config_path):
            config = ConfigFile.from_path(config_path)
            for path, _url, section in parse_submodules(config):
                submodules[os.fsdecode(section)] = PathInfo(os.fsdecode(path))
        return submodules

    def close(self):
        self.repo.close()

    @property
    def root_dir(self) -> str:
        return self.repo.path

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
        return self.repo.commondir()

    def add(self, paths: Union[str, Iterable[str]], update=False):
        from dvc.utils.fs import walk_files

        assert paths or update

        if isinstance(paths, str):
            paths = [paths]

        if update and not paths:
            self.repo.stage(list(self.repo.open_index()))
            return

        files: List[bytes] = []
        for path in paths:
            if not os.path.isabs(path) and self._submodules:
                # NOTE: If path is inside a submodule, Dulwich expects the
                # staged paths to be relative to the submodule root (not the
                # parent git repo root). We append path to root_dir here so
                # that the result of relpath(path, root_dir) is actually the
                # path relative to the submodule root.
                path_info = PathInfo(path).relative_to(self.root_dir)
                for sm_path in self._submodules.values():
                    if path_info.isin(sm_path):
                        path = os.path.join(
                            self.root_dir, path_info.relative_to(sm_path)
                        )
                        break
            if os.path.isdir(path):
                files.extend(
                    os.fsencode(relpath(fpath, self.root_dir))
                    for fpath in walk_files(path)
                )
            else:
                files.append(os.fsencode(relpath(path, self.root_dir)))

        # NOTE: this doesn't check gitignore, same as GitPythonBackend.add
        if update:
            index = self.repo.open_index()
            if os.name == "nt":
                # NOTE: we need git/unix separator to compare against index
                # paths but repo.stage() expects to be called with OS paths
                self.repo.stage(
                    [
                        fname
                        for fname in files
                        if fname.replace(b"\\", b"/") in index
                    ]
                )
            else:
                self.repo.stage([fname for fname in files if fname in index])
        else:
            self.repo.stage(files)

    def commit(self, msg: str, no_verify: bool = False):
        from dulwich.errors import CommitError
        from dulwich.porcelain import commit
        from dulwich.repo import InvalidUserIdentity

        try:
            commit(self.root_dir, message=msg, no_verify=no_verify)
        except CommitError as exc:
            raise SCMError("Git commit failed") from exc
        except InvalidUserIdentity as exc:
            raise SCMError(
                "Git username and email must be configured"
            ) from exc

    def checkout(
        self,
        branch: str,
        create_new: Optional[bool] = False,
        force: bool = False,
        **kwargs,
    ):
        raise NotImplementedError

    def pull(self, **kwargs):
        raise NotImplementedError

    def push(self):
        raise NotImplementedError

    def branch(self, branch: str):
        from dulwich.porcelain import Error, branch_create

        try:
            branch_create(self.root_dir, branch)
        except Error as exc:
            raise SCMError(f"Failed to create branch '{branch}'") from exc

    def tag(self, tag: str):
        raise NotImplementedError

    def untracked_files(self) -> Iterable[str]:
        _staged, _unstaged, untracked = self.status()
        return untracked

    def is_tracked(self, path: str) -> bool:
        rel = PathInfo(path).relative_to(self.root_dir).as_posix().encode()
        rel_dir = rel + b"/"
        for path in self.repo.open_index():
            if path == rel or path.startswith(rel_dir):
                return True
        return False

    def is_dirty(self, untracked_files: bool = False) -> bool:
        staged, unstaged, untracked = self.status()
        return bool(staged or unstaged or (untracked_files and untracked))

    def active_branch(self) -> str:
        raise NotImplementedError

    def list_branches(self) -> Iterable[str]:
        raise NotImplementedError

    def list_tags(self) -> Iterable[str]:
        raise NotImplementedError

    def list_all_commits(self) -> Iterable[str]:
        raise NotImplementedError

    def get_tree_obj(self, rev: str, **kwargs) -> DulwichObject:
        from dulwich.objectspec import parse_tree

        tree = parse_tree(self.repo, rev)
        return DulwichObject(self.repo, ".", stat.S_IFDIR, tree.id)

    def get_rev(self) -> str:
        rev = self.get_ref("HEAD")
        if rev:
            return rev
        raise SCMError("Empty git repo")

    def resolve_rev(self, rev: str) -> str:
        raise NotImplementedError

    def resolve_commit(self, rev: str) -> "GitCommit":
        raise NotImplementedError

    def _get_stash(self, ref: str):
        from dulwich.stash import Stash as DulwichStash

        if ref not in self._stashes:
            self._stashes[ref] = DulwichStash(self.repo, ref=os.fsencode(ref))
        return self._stashes[ref]

    @cached_property
    def ignore_manager(self):
        from dulwich.ignore import IgnoreFilterManager

        return IgnoreFilterManager.from_repo(self.repo)

    def is_ignored(self, path: str) -> bool:
        # `is_ignored` returns `false` if excluded in `.gitignore` and
        # `None` if it's not mentioned at all. `True` if it is ignored.
        return bool(
            self.ignore_manager.is_ignored(relpath(path, self.root_dir))
        )

    def set_ref(
        self,
        name: str,
        new_ref: str,
        old_ref: Optional[str] = None,
        message: Optional[str] = None,
        symbolic: Optional[bool] = False,
    ):
        name_b = os.fsencode(name)
        new_ref_b = os.fsencode(new_ref)
        old_ref_b = os.fsencode(old_ref) if old_ref else None
        message_b = message.encode("utf-8") if message else None
        if symbolic:
            return self.repo.refs.set_symbolic_ref(
                name_b, new_ref_b, message=message_b
            )
        if not self.repo.refs.set_if_equals(
            name_b, old_ref_b, new_ref_b, message=message_b
        ):
            raise SCMError(f"Failed to set '{name}'")

    def get_ref(self, name, follow: bool = True) -> Optional[str]:
        from dulwich.refs import parse_symref_value

        name_b = os.fsencode(name)
        if follow:
            try:
                ref = self.repo.refs[name_b]
            except KeyError:
                ref = None
        else:
            ref = self.repo.refs.read_ref(name_b)
            try:
                if ref:
                    ref = parse_symref_value(ref)
            except ValueError:
                pass
        if ref:
            return os.fsdecode(ref)
        return None

    def remove_ref(self, name: str, old_ref: Optional[str] = None):
        name_b = name.encode("utf-8")
        old_ref_b = old_ref.encode("utf-8") if old_ref else None
        if not self.repo.refs.remove_if_equals(name_b, old_ref_b):
            raise SCMError(f"Failed to remove '{name}'")

    def iter_refs(self, base: Optional[str] = None):
        base_b = os.fsencode(base) if base else None
        for key in self.repo.refs.keys(base=base_b):
            if base:
                if base.endswith("/"):
                    base = base[:-1]
                yield "/".join([base, os.fsdecode(key)])
            else:
                yield os.fsdecode(key)

    def iter_remote_refs(self, url: str, base: Optional[str] = None):
        from dulwich.client import get_transport_and_path
        from dulwich.porcelain import get_remote_repo

        try:
            _remote, location = get_remote_repo(self.repo, url)
            client, path = get_transport_and_path(location)
        except Exception as exc:
            raise SCMError(
                f"'{url}' is not a valid Git remote or URL"
            ) from exc

        if base:
            yield from (
                os.fsdecode(ref)
                for ref in client.get_refs(path)
                if ref.startswith(os.fsencode(base))
            )
        else:
            yield from (os.fsdecode(ref) for ref in client.get_refs(path))

    def get_refs_containing(self, rev: str, pattern: Optional[str] = None):
        raise NotImplementedError

    def push_refspec(
        self,
        url: str,
        src: Optional[str],
        dest: str,
        force: bool = False,
        on_diverged: Optional[Callable[[str, str], bool]] = None,
    ):
        from dulwich.client import get_transport_and_path
        from dulwich.errors import NotGitRepository, SendPackError
        from dulwich.porcelain import (
            DivergedBranches,
            check_diverged,
            get_remote_repo,
        )

        dest_refs, values = self._push_dest_refs(src, dest)

        try:
            _remote, location = get_remote_repo(self.repo, url)
            client, path = get_transport_and_path(location)
        except Exception as exc:
            raise SCMError(
                f"'{url}' is not a valid Git remote or URL"
            ) from exc

        def update_refs(refs):
            new_refs = {}
            for ref, value in zip(dest_refs, values):
                if ref in refs:
                    local_sha = self.repo.refs[ref]
                    remote_sha = refs[ref]
                    try:
                        check_diverged(self.repo, remote_sha, local_sha)
                    except DivergedBranches:
                        if not force:
                            overwrite = False
                            if on_diverged:
                                overwrite = on_diverged(
                                    os.fsdecode(ref), os.fsdecode(remote_sha)
                                )
                            if not overwrite:
                                continue
                new_refs[ref] = value
            return new_refs

        try:
            with Tqdm(
                desc="Pushing git refs", bar_format=self.BAR_FMT_NOTOTAL
            ) as pbar:

                def progress(msg_b):
                    msg = msg_b.decode("ascii").strip()
                    pbar.update_msg(msg)
                    pbar.refresh()
                    logger.trace(msg)

                client.send_pack(
                    path,
                    update_refs,
                    self.repo.object_store.generate_pack_data,
                    progress=progress,
                )
        except (NotGitRepository, SendPackError) as exc:
            raise SCMError("Git failed to push '{src}' to '{url}'") from exc

    def _push_dest_refs(
        self, src: Optional[str], dest: str
    ) -> Tuple[Iterable[bytes], Iterable[bytes]]:
        from dulwich.objects import ZERO_SHA

        if src is not None and src.endswith("/"):
            src_b = os.fsencode(src)
            keys = self.repo.refs.subkeys(src_b)
            values = [self.repo.refs[b"".join([src_b, key])] for key in keys]
            dest_refs = [b"".join([os.fsencode(dest), key]) for key in keys]
        else:
            if src is None:
                values = [ZERO_SHA]
            else:
                values = [self.repo.refs[os.fsencode(src)]]
            dest_refs = [os.fsencode(dest)]
        return dest_refs, values

    def fetch_refspecs(
        self,
        url: str,
        refspecs: Iterable[str],
        force: Optional[bool] = False,
        on_diverged: Optional[Callable[[str, str], bool]] = None,
    ):
        from dulwich.client import get_transport_and_path
        from dulwich.objectspec import parse_reftuples
        from dulwich.porcelain import (
            DivergedBranches,
            check_diverged,
            get_remote_repo,
        )

        fetch_refs = []

        def determine_wants(remote_refs):
            fetch_refs.extend(
                parse_reftuples(
                    remote_refs,
                    self.repo.refs,
                    [os.fsencode(refspec) for refspec in refspecs],
                    force=force,
                )
            )
            return [
                remote_refs[lh]
                for (lh, _, _) in fetch_refs
                if remote_refs[lh] not in self.repo.object_store
            ]

        try:
            _remote, location = get_remote_repo(self.repo, url)
            client, path = get_transport_and_path(location)
        except Exception as exc:
            raise SCMError(
                f"'{url}' is not a valid Git remote or URL"
            ) from exc

        with Tqdm(
            desc="Fetching git refs", bar_format=self.BAR_FMT_NOTOTAL
        ) as pbar:

            def progress(msg_b):
                msg = msg_b.decode("ascii").strip()
                pbar.update_msg(msg)
                pbar.refresh()
                logger.trace(msg)

            fetch_result = client.fetch(
                path,
                self.repo,
                progress=progress,
                determine_wants=determine_wants,
            )
        for (lh, rh, _) in fetch_refs:
            try:
                if rh in self.repo.refs:
                    check_diverged(
                        self.repo, self.repo.refs[rh], fetch_result.refs[lh]
                    )
            except DivergedBranches:
                if not force:
                    overwrite = False
                    if on_diverged:
                        overwrite = on_diverged(
                            os.fsdecode(rh), os.fsdecode(fetch_result.refs[lh])
                        )
                    if not overwrite:
                        continue
            self.repo.refs[rh] = fetch_result.refs[lh]

    def _stash_iter(self, ref: str):
        stash = self._get_stash(ref)
        yield from stash.stashes()

    def _stash_push(
        self,
        ref: str,
        message: Optional[str] = None,
        include_untracked: Optional[bool] = False,
    ) -> Tuple[Optional[str], bool]:
        from dulwich.repo import InvalidUserIdentity

        from dvc.scm.git import Stash

        if include_untracked or ref == Stash.DEFAULT_STASH:
            # dulwich stash.push does not support include_untracked and does
            # not touch working tree
            raise NotImplementedError

        stash = self._get_stash(ref)
        message_b = message.encode("utf-8") if message else None
        try:
            rev = stash.push(message=message_b)
        except InvalidUserIdentity as exc:
            raise SCMError(
                "Git username and email must be configured"
            ) from exc
        return os.fsdecode(rev), True

    def _stash_apply(self, rev: str):
        raise NotImplementedError

    def _stash_drop(self, ref: str, index: int):
        from dvc.scm.git import Stash

        if ref == Stash.DEFAULT_STASH:
            raise NotImplementedError

        stash = self._get_stash(ref)
        try:
            stash.drop(index)
        except ValueError as exc:
            raise SCMError("Failed to drop stash entry") from exc

    def describe(
        self,
        rev: str,
        base: Optional[str] = None,
        match: Optional[str] = None,
        exclude: Optional[str] = None,
    ) -> Optional[str]:
        if not base:
            base = "refs/tags"
        for ref in self.iter_refs(base=base):
            if (match and not fnmatch.fnmatch(ref, match)) or (
                exclude and fnmatch.fnmatch(ref, exclude)
            ):
                continue
            if self.get_ref(ref, follow=False) == rev:
                return ref
        return None

    def diff(self, rev_a: str, rev_b: str, binary=False) -> str:
        from dulwich.patch import write_tree_diff

        commit_a = self.repo[os.fsencode(rev_a)]
        commit_b = self.repo[os.fsencode(rev_b)]

        buf = BytesIO()
        write_tree_diff(
            buf, self.repo.object_store, commit_a.tree, commit_b.tree
        )
        return buf.getvalue().decode("utf-8")

    def reset(self, hard: bool = False, paths: Iterable[str] = None):
        raise NotImplementedError

    def checkout_index(
        self,
        paths: Optional[Iterable[str]] = None,
        force: bool = False,
        ours: bool = False,
        theirs: bool = False,
    ):
        raise NotImplementedError

    def status(
        self, ignored: bool = False
    ) -> Tuple[Mapping[str, Iterable[str]], Iterable[str], Iterable[str]]:
        from dulwich.porcelain import status as git_status

        staged, unstaged, untracked = git_status(
            self.root_dir, ignored=ignored
        )
        return (
            {
                status: [os.fsdecode(name) for name in paths]
                for status, paths in staged.items()
                if paths
            },
            [os.fsdecode(name) for name in unstaged],
            [os.fsdecode(name) for name in untracked],
        )

    def _reset(self) -> None:
        self.__dict__.pop("ignore_manager", None)

    def merge(
        self,
        rev: str,
        commit: bool = True,
        msg: Optional[str] = None,
        squash: bool = False,
    ) -> Optional[str]:
        raise NotImplementedError
