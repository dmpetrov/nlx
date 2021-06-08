import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, NamedTuple, Optional, Set

from dvc.progress import Tqdm

from .tree import Tree

if TYPE_CHECKING:
    from dvc.remote.base import Remote

    from .db.base import ObjectDB
    from .file import HashFile

logger = logging.getLogger(__name__)


class UsedObjectsPair(NamedTuple):
    remote: Optional["Remote"]
    objs: Set["HashFile"]


def save(
    odb: "ObjectDB",
    obj: "HashFile",
    jobs: Optional[int] = None,
    **kwargs,
):
    if isinstance(obj, Tree):
        with ThreadPoolExecutor(max_workers=jobs) as executor:
            for future in Tqdm(
                as_completed(
                    executor.submit(
                        odb.add,
                        entry.path_info,
                        entry.fs,
                        entry.hash_info,
                        **kwargs,
                    )
                    for _, entry in obj
                ),
                desc="Saving files",
                total=len(obj),
                unit="file",
            ):
                future.result()

    odb.add(obj.path_info, obj.fs, obj.hash_info, **kwargs)


def check(odb, obj):
    odb.check(obj.hash_info)

    if isinstance(obj, Tree):
        for _, entry in obj:
            odb.check(entry.hash_info)


def load(odb, hash_info):
    if hash_info.isdir:
        return Tree.load(odb, hash_info)
    return odb.get(hash_info)
