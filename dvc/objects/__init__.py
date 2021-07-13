import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, Optional

from dvc.progress import Tqdm

from .tree import Tree

if TYPE_CHECKING:
    from dvc.hash_info import HashInfo

    from .db.base import ObjectDB
    from .file import HashFile

logger = logging.getLogger(__name__)


def save(
    odb: "ObjectDB",
    obj: "HashFile",
    jobs: Optional[int] = None,
    **kwargs,
):
    if isinstance(obj, Tree):
        from .stage import get_staging

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

            # if dir cache for this tree has already been staged, move the
            # staged object when possible
            staging = get_staging(odb)
            if staging.exists(obj.hash_info):
                obj = staging.get(obj.hash_info)
                kwargs["move"] = True

    assert obj.path_info and obj.fs and obj.hash_info
    odb.add(obj.path_info, obj.fs, obj.hash_info, **kwargs)


def check(odb: "ObjectDB", obj: "HashFile"):
    if isinstance(obj, Tree):
        for _, entry in obj:
            odb.check(entry.hash_info)

    odb.check(obj.hash_info)


def load(odb: "ObjectDB", hash_info: "HashInfo") -> "HashFile":
    if hash_info.isdir:
        return Tree.load(odb, hash_info)
    return odb.get(hash_info)


def iterobjs(obj):
    if isinstance(obj, Tree):
        yield from (entry_obj for _, entry_obj in obj)
    yield obj
