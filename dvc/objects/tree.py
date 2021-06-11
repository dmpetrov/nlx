import json
import logging
import posixpath
from typing import TYPE_CHECKING, Optional, Tuple

from funcy import cached_property

from .errors import ObjectFormatError
from .file import HashFile
from .stage import get_file_hash

if TYPE_CHECKING:
    from .db.base import ObjectDB

logger = logging.getLogger(__name__)


class Tree(HashFile):
    PARAM_RELPATH = "relpath"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._dict = {}

    @cached_property
    def trie(self):
        from pygtrie import Trie

        return Trie(self._dict)

    @property
    def size(self):
        try:
            return sum(obj.size for _, obj in self)
        except TypeError:
            return None

    def add(self, key, obj):
        self.__dict__.pop("trie", None)
        self._dict[key] = obj

    def digest(self):
        from dvc.fs.memory import MemoryFileSystem
        from dvc.path_info import PathInfo
        from dvc.utils import tmp_fname

        memfs = MemoryFileSystem()
        path_info = PathInfo(tmp_fname(""))
        with memfs.open(path_info, "wb") as fobj:
            fobj.write(self.as_bytes())
        self.fs = memfs
        self.path_info = path_info
        self.hash_info = get_file_hash(path_info, memfs, "md5")
        self.hash_info.value += ".dir"
        self.hash_info.size = self.size
        self.hash_info.nfiles = len(self)

    def __len__(self):
        return len(self._dict)

    def __iter__(self):
        yield from self._dict.items()

    def as_dict(self):
        return self._dict.copy()

    def as_list(self):
        from operator import itemgetter

        # Sorting the list by path to ensure reproducibility
        return sorted(
            (
                {
                    # NOTE: not using hash_info.to_dict() because we don't want
                    # size/nfiles fields at this point.
                    obj.hash_info.name: obj.hash_info.value,
                    self.PARAM_RELPATH: posixpath.sep.join(parts),
                }
                for parts, obj in self._dict.items()  # noqa: B301
            ),
            key=itemgetter(self.PARAM_RELPATH),
        )

    def as_bytes(self):
        return json.dumps(self.as_list(), sort_keys=True).encode("utf-8")

    @classmethod
    def from_list(cls, lst):
        from dvc.hash_info import HashInfo

        tree = cls(None, None, None)
        for _entry in lst:
            entry = _entry.copy()
            relpath = entry.pop(cls.PARAM_RELPATH)
            parts = tuple(relpath.split(posixpath.sep))
            hash_info = HashInfo.from_dict(entry)
            obj = HashFile(None, None, hash_info)
            tree.add(parts, obj)
        return tree

    @classmethod
    def load(cls, odb, hash_info):

        obj = odb.get(hash_info)

        try:
            with obj.fs.open(obj.path_info, "r") as fobj:
                raw = json.load(fobj)
        except ValueError as exc:
            raise ObjectFormatError(f"{obj} is corrupted") from exc

        if not isinstance(raw, list):
            logger.error(
                "dir cache file format error '%s' [skipping the file]",
                obj.path_info,
            )
            raise ObjectFormatError(f"{obj} is corrupted")

        tree = cls.from_list(raw)
        tree.path_info = obj.path_info
        tree.fs = obj.fs
        for _, entry_obj in tree:
            entry_obj.fs = obj.fs
        tree.hash_info = hash_info

        return tree

    def filter(
        self, odb: "ObjectDB", prefix: Tuple[str], copy: bool = False
    ) -> Optional[HashFile]:
        """Return filter object(s) for this tree.

        If copy is True, returned object will be a Tree containing
        filtered entries, but with hash_info copied from the original tree.

        If copy is False, returned object will be a raw HashFile or Tree with
        newly computed hash_info for the filtered object.
        """
        obj = self._dict.get(prefix)
        if obj:
            if copy:
                tree = Tree(self.path_info, self.fs, self.hash_info)
                tree.add(prefix, obj)
                return tree
            return obj

        if copy:
            tree = Tree(self.path_info, self.fs, self.hash_info)
            depth = 0
        else:
            tree = Tree(None, None, None)
            depth = len(prefix)
        try:
            for key, obj in self.trie.items(prefix):
                tree.add(key[depth:], obj)
        except KeyError:
            return None
        if not copy:
            tree.digest()
            odb.add(tree.path_info, tree.fs, tree.hash_info)
        return tree


def _get_dir_size(odb, tree):
    try:
        return sum(
            odb.fs.getsize(odb.hash_to_path_info(obj.hash_info.value))
            for _, obj in tree
        )
    except FileNotFoundError:
        return None


def _diff(ancestor, other, allow_removed=False):
    from dictdiffer import diff

    from dvc.exceptions import MergeError

    allowed = ["add"]
    if allow_removed:
        allowed.append("remove")

    result = list(diff(ancestor, other))
    for typ, _, _ in result:
        if typ not in allowed:
            raise MergeError(
                "unable to auto-merge directories with diff that contains "
                f"'{typ}'ed files"
            )
    return result


def _merge(ancestor, our, their):
    import copy

    from dictdiffer import patch

    our_diff = _diff(ancestor, our)
    if not our_diff:
        return copy.deepcopy(their)

    their_diff = _diff(ancestor, their)
    if not their_diff:
        return copy.deepcopy(our)

    # make sure there are no conflicting files
    _diff(our, their, allow_removed=True)

    return patch(our_diff + their_diff, ancestor)


def merge(odb, ancestor_info, our_info, their_info):
    from . import load

    assert our_info
    assert their_info

    if ancestor_info:
        ancestor = load(odb, ancestor_info)
    else:
        ancestor = Tree(None, None, None)

    our = load(odb, our_info)
    their = load(odb, their_info)

    merged_dict = _merge(ancestor.as_dict(), our.as_dict(), their.as_dict())

    merged = Tree(None, None, None)
    for key, hi in merged_dict.items():
        merged.add(key, hi)
    merged.digest()

    odb.add(merged.path_info, merged.fs, merged.hash_info)
    hash_info = merged.hash_info
    hash_info.size = _get_dir_size(odb, merged)
    return hash_info
