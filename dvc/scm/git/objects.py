import os
import stat
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterable, List, Optional

from pygtrie import Trie

S_IFGITLINK = 0o160000


def S_ISGITLINK(m: int) -> bool:
    return stat.S_IFMT(m) == S_IFGITLINK


class GitObject(ABC):
    @abstractmethod
    def open(self, mode: str = "r", encoding: str = None):
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    @abstractmethod
    def mode(self) -> int:
        pass

    @abstractmethod
    def scandir(self) -> Iterable["GitObject"]:
        pass

    @property
    def isfile(self) -> bool:
        return stat.S_ISREG(self.mode)

    @property
    def isdir(self) -> bool:
        return stat.S_ISDIR(self.mode)

    @property
    def issubmodule(self) -> bool:
        return S_ISGITLINK(self.mode)


class GitTrie:
    def __init__(self, tree: GitObject, rev: str):
        self.tree = tree
        self.rev = rev
        self.trie = Trie()

        self.trie[()] = tree
        self._build(tree, ())

    def _build(self, tree: GitObject, path: tuple):
        for obj in tree.scandir():
            obj_path = path + (obj.name,)
            self.trie[obj_path] = obj

            if obj.isdir:
                self._build(obj, obj_path)

    def open(
        self,
        key: tuple,
        mode: Optional[str] = "r",
        encoding: Optional[str] = None,
    ):
        obj = self.trie[key]
        if obj.isdir:
            raise IsADirectoryError

        return obj.open(mode=mode, encoding=encoding)

    def exists(self, key: tuple) -> bool:
        return bool(self.trie.has_node(key))

    def isdir(self, key: tuple) -> bool:
        try:
            obj = self.trie[key]
        except KeyError:
            return False
        return obj.isdir

    def isfile(self, key: tuple) -> bool:
        try:
            obj = self.trie[key]
        except KeyError:
            return False

        return obj.isfile

    def walk(self, top: tuple, topdown: Optional[bool] = True):
        dirs = []
        nondirs = []

        def node_factory(_, path, children, obj):
            if path == top:
                assert obj.isdir
                list(filter(None, children))
            elif obj.isdir:
                dirs.append(obj.name)
            else:
                nondirs.append(obj.name)

        self.trie.traverse(node_factory, prefix=top)

        if topdown:
            yield top, dirs, nondirs

        for dname in dirs:
            yield from self.walk(top + (dname,), topdown=topdown)

        if not topdown:
            yield top, dirs, nondirs

    def stat(self, key: tuple) -> os.stat_result:
        obj = self.trie[key]
        return os.stat_result((obj.mode, 0, 0, 0, 0, 0, 0, 0, 0, 0))


@dataclass
class GitCommit:
    hexsha: str
    commit_time: int
    commit_time_offset: int
    message: str
    parents: List[str]
