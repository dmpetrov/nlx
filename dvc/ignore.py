import logging
import os
import re
from collections import namedtuple
from itertools import groupby, takewhile

from pathspec.patterns import GitWildMatchPattern
from pathspec.util import normalize_file

from dvc.fs.base import BaseFileSystem
from dvc.path_info import PathInfo
from dvc.pathspec_math import PatternInfo, merge_patterns
from dvc.scheme import Schemes
from dvc.system import System
from dvc.types import AnyPath, List, Optional
from dvc.utils import relpath
from dvc.utils.collections import PathStringTrie

logger = logging.getLogger(__name__)


DVCIGNORE_FILE = ".dvcignore"


class DvcIgnorePatterns:
    def __init__(self, pattern_list, dirname):
        if pattern_list:
            if isinstance(pattern_list[0], str):
                pattern_list = [
                    PatternInfo(pattern, "") for pattern in pattern_list
                ]

        self.pattern_list = pattern_list
        self.dirname = dirname
        self.prefix = self.dirname + os.sep

        self.regex_pattern_list = [
            GitWildMatchPattern.pattern_to_regex(pattern_info.patterns)
            for pattern_info in pattern_list
        ]

        self.ignore_spec = [
            (ignore, re.compile("|".join(item[0] for item in group)))
            for ignore, group in groupby(
                self.regex_pattern_list, lambda x: x[1]
            )
            if ignore is not None
        ]

    @classmethod
    def from_file(cls, path, fs, name):
        assert os.path.isabs(path)
        dirname = os.path.normpath(os.path.dirname(path))
        with fs.open(path, encoding="utf-8") as fobj:
            path_spec_lines = [
                PatternInfo(line, "{}:{}:{}".format(name, line_no + 1, line))
                for line_no, line in enumerate(
                    map(str.strip, fobj.readlines())
                )
                if line and not (line.strip().startswith("#"))
            ]

        return cls(path_spec_lines, dirname)

    def __call__(self, root: str, dirs: List[str], files: List[str]):
        if not self.pattern_list:
            return dirs, files
        files = [f for f in files if not self.matches(root, f)]
        dirs = [d for d in dirs if not self.matches(root, d, True)]

        return dirs, files

    def _get_normalize_path(self, dirname, basename):
        # NOTE: `relpath` is too slow, so we have to assume that both
        # `dirname` and `self.dirname` are relative or absolute together.
        if dirname == self.dirname:
            path = basename
        elif dirname.startswith(self.prefix):
            rel = dirname[len(self.prefix) :]
            # NOTE: `os.path.join` is ~x5.5 slower
            path = f"{rel}{os.sep}{basename}"
        else:
            return False

        if not System.is_unix():
            path = normalize_file(path)
        return path

    def matches(self, dirname, basename, is_dir=False, details: bool = False):
        if not self.pattern_list:
            return False
        path = self._get_normalize_path(dirname, basename)
        if not path:
            return False

        if details:
            return self._ignore_details(path, is_dir)
        return self._ignore(path, is_dir)

    def _ignore(self, path, is_dir):
        def matches(pattern, path, is_dir) -> bool:
            matches_ = bool(pattern.match(path))

            if is_dir:
                matches_ |= bool(pattern.match(f"{path}/"))

            return matches_

        result = False

        for ignore, pattern in self.ignore_spec[::-1]:
            if matches(pattern, path, is_dir):
                result = ignore
                break
        return result

    def _ignore_details(self, path, is_dir: bool):
        result = []
        for (regex, _), pattern_info in list(
            zip(self.regex_pattern_list, self.pattern_list)
        ):
            # skip system pattern
            if not pattern_info.file_info:
                continue

            regex = re.compile(regex)

            matches = bool(regex.match(path))
            if is_dir:
                matches |= bool(regex.match(f"{path}/"))

            if matches:
                result.append(pattern_info.file_info)

        return result

    def __hash__(self):
        return hash(self.dirname + ":" + str(self.pattern_list))

    def __eq__(self, other):
        if not isinstance(other, DvcIgnorePatterns):
            return NotImplemented
        return (self.dirname == other.dirname) & (str(self) == str(other))

    def __bool__(self):
        return bool(self.pattern_list)

    def __str__(self):
        return ",".join([pattern.patterns for pattern in self.pattern_list])


def _merge_two_patterns(first: DvcIgnorePatterns, second: DvcIgnorePatterns):
    return DvcIgnorePatterns(
        *merge_patterns(
            first.pattern_list,
            first.dirname,
            second.pattern_list,
            second.dirname,
        )
    )


CheckIgnoreResult = namedtuple(
    "CheckIgnoreResult", ["file", "match", "patterns"]
)


def _no_match(path):
    return CheckIgnoreResult(path, False, ["::"])


class DvcIgnoreTrieNode:
    def __init__(self, dirname: str):
        self.file_patterns = DvcIgnorePatterns([], dirname)
        self.subrepo_patterns = DvcIgnorePatterns([], dirname)

    def add_new_file_patterns(self, new_patterns: DvcIgnorePatterns):
        self.file_patterns = _merge_two_patterns(
            self.file_patterns, new_patterns
        )

    def add_new_subrepo_patterns(self, new_patterns: DvcIgnorePatterns):
        self.subrepo_patterns = _merge_two_patterns(
            self.subrepo_patterns, new_patterns
        )

    @classmethod
    def copy_node(cls, dirname: str, old_node):
        new_node = cls(dirname)
        new_node.file_patterns = old_node.file_patterns
        new_node.subrepo_patterns = old_node.subrepo_patterns
        return new_node


class DvcIgnoreFilter:
    def __init__(self, fs, root_dir):
        from dvc.repo import Repo

        default_ignore_patterns = [
            ".hg/",
            ".git/",
            ".git",
            "{}/".format(Repo.DVC_DIR),
        ]

        self.fs = fs
        self.root_dir = root_dir
        self._ignore_trie = PathStringTrie()
        self._ignore_trie[root_dir] = DvcIgnoreTrieNode(self.root_dir)
        self._ignore_trie[root_dir].file_patterns = DvcIgnorePatterns(
            default_ignore_patterns, root_dir
        )
        self._update_trie(root_dir, None)

    def _update_pattern_trie(self, dirname: str) -> None:
        path = os.path.join(dirname, DVCIGNORE_FILE)
        node = self._ignore_trie.get(dirname)
        dvcignore_file_is_ignored = node.file_patterns.matches(
            dirname, DVCIGNORE_FILE
        ) | node.subrepo_patterns.matches(dirname, DVCIGNORE_FILE)

        if self.fs.exists(path) and not dvcignore_file_is_ignored:
            name = os.path.relpath(path, self.root_dir)
            new_pattern = DvcIgnorePatterns.from_file(path, self.fs, name)
            node.add_new_file_patterns(new_pattern)

    def _update_subrepo_trie(
        self, dirname: str, dnames: Optional["List"]
    ) -> None:
        if dnames is None:
            try:
                _, dnames, _ = next(self.fs.walk(dirname))
            except StopIteration:
                dnames = []

        for dname in dnames:
            self._update_sub_repo(os.path.join(dirname, dname))

    def _update_trie(
        self,
        dirname: str,
        dnames: Optional["List"],
    ) -> None:
        old_trie_node = self._ignore_trie.longest_prefix(dirname).value
        self._ignore_trie[dirname] = DvcIgnoreTrieNode.copy_node(
            dirname, old_trie_node
        )

        self._update_pattern_trie(dirname)
        self._update_subrepo_trie(dirname, dnames)

    def _update_sub_repo(self, path: str) -> None:
        from dvc.repo import Repo

        if path == self.root_dir:
            return

        dvc_dir = os.path.join(path, Repo.DVC_DIR)
        if not os.path.exists(dvc_dir):
            return

        root, dname = os.path.split(path)
        pattern_info = PatternInfo(
            f"/{dname}/",
            "fatal: Pathspec '{}' is in a subrepo " + f"'{dname}'",
        )
        new_pattern = DvcIgnorePatterns([pattern_info], root)
        self._ignore_trie[root].add_new_subrepo_patterns(new_pattern)

    def __call__(
        self,
        root: str,
        dirs: List[str],
        files: List[str],
        ignore_subrepos: bool = True,
    ):
        abs_root = os.path.abspath(root)
        trie_node = self._get_trie_node(abs_root, dnames=dirs)
        if trie_node:
            if ignore_subrepos:
                dirs, files = trie_node.subrepo_patterns(abs_root, dirs, files)
            dirs, files = trie_node.file_patterns(abs_root, dirs, files)
        return dirs, files

    def walk(self, fs: BaseFileSystem, path_info: AnyPath, **kwargs):
        ignore_subrepos = kwargs.pop("ignore_subrepos", True)
        if fs.scheme == Schemes.LOCAL:
            for root, dirs, files in fs.walk(path_info, **kwargs):
                dirs[:], files[:] = self(
                    root, dirs, files, ignore_subrepos=ignore_subrepos
                )
                yield root, dirs, files
        else:
            yield from fs.walk(path_info, **kwargs)

    def walk_files(self, fs: BaseFileSystem, path_info: AnyPath, **kwargs):
        if fs.scheme == Schemes.LOCAL:
            for root, _, files in self.walk(fs, path_info, **kwargs):
                for file in files:
                    # NOTE: os.path.join is ~5.5 times slower
                    yield PathInfo(f"{root}{os.sep}{file}")
        else:
            yield from fs.walk_files(path_info)

    def _get_trie_node(
        self, dirname, dnames: Optional["List"] = None
    ) -> Optional["DvcIgnoreTrieNode"]:
        ignore_node = self._ignore_trie.get(dirname)
        if ignore_node:
            return ignore_node

        prefix = self._ignore_trie.longest_prefix(dirname).key
        if not prefix:
            # outside of the repo
            return None

        dirs = list(
            takewhile(
                lambda path: path != prefix,
                (parent.fspath for parent in PathInfo(dirname).parents),
            )
        )
        dirs.reverse()
        dirs.append(dirname)

        for parent in dirs:
            self._update_trie(parent, dnames)

        return self._ignore_trie.get(dirname)

    def _is_ignored(
        self, path: str, is_dir: bool = False, ignore_subrepos: bool = True
    ) -> bool:
        if self._outside_repo(path):
            return False
        dirname, basename = os.path.split(os.path.normpath(path))
        trie_node = self._get_trie_node(dirname, None)
        if trie_node:
            ret = trie_node.file_patterns.matches(dirname, basename, is_dir)
            if ignore_subrepos:
                ret |= trie_node.subrepo_patterns.matches(
                    dirname, basename, is_dir
                )
            return ret
        return False

    def is_ignored_dir(self, path: str, ignore_subrepos: bool = True) -> bool:
        "Only used in LocalFileSystem"
        path = os.path.abspath(path)
        if path == self.root_dir:
            return False

        return self._is_ignored(path, True, ignore_subrepos=ignore_subrepos)

    def is_ignored_file(self, path: str) -> bool:
        "Only used in LocalFileSystem"
        path = os.path.abspath(path)
        return self._is_ignored(path, False)

    def _outside_repo(self, path):
        path = PathInfo(path)

        # paths outside of the repo should be ignored
        path = relpath(path, self.root_dir)
        if path.startswith("..") or (
            os.name == "nt"
            and not os.path.commonprefix(
                [os.path.abspath(path), self.root_dir]
            )
        ):
            return True
        return False

    def check_ignore(self, target) -> CheckIgnoreResult:
        # NOTE: can only be used in `dvc check-ignore`, see
        # https://github.com/iterative/dvc/issues/5046
        from dvc.exceptions import DvcException

        full_target = os.path.abspath(target)
        if self._outside_repo(full_target):
            raise DvcException(
                f"fatal: '{target}' is outside repository at '{self.root_dir}'"
            )

        dirname, basename = os.path.split(os.path.normpath(full_target))
        trie_node = self._get_trie_node(dirname)
        if trie_node:
            subrepo_match = trie_node.subrepo_patterns.matches(
                dirname,
                basename,
                os.path.isdir(full_target),
                True,
            )
            if subrepo_match:
                subrepo_info = subrepo_match[0].format(target)
                raise DvcException(subrepo_info.format(target))

            matches = trie_node.file_patterns.matches(
                dirname, basename, os.path.isdir(full_target), True
            )
            if matches:
                return CheckIgnoreResult(target, True, matches)
        return _no_match(target)

    def is_ignored(
        self, fs: BaseFileSystem, path: str, ignore_subrepos: bool = True
    ) -> bool:
        # NOTE: can't use self.check_ignore(path).match for now, see
        # https://github.com/iterative/dvc/issues/4555
        if fs.scheme != Schemes.LOCAL:
            return False
        if fs.isfile(path):
            return self.is_ignored_file(path)
        if fs.isdir(path):
            return self.is_ignored_dir(path, ignore_subrepos)
        return self.is_ignored_file(path) or self.is_ignored_dir(
            path, ignore_subrepos
        )


def init(path):
    dvcignore = os.path.join(path, DVCIGNORE_FILE)
    if os.path.exists(dvcignore):
        return dvcignore

    with open(dvcignore, "w") as fobj:
        fobj.write(
            "# Add patterns of files dvc should ignore, which could improve\n"
            "# the performance. Learn more at\n"
            "# https://dvc.org/doc/user-guide/dvcignore\n"
        )

    return dvcignore
