import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from dvc.progress import Tqdm

from .tree import Tree

logger = logging.getLogger(__name__)


def save(odb, obj, jobs=None, **kwargs):
    if isinstance(obj, Tree):
        with ThreadPoolExecutor(max_workers=jobs) as executor:
            for future in Tqdm(
                as_completed(
                    executor.submit(
                        odb.add,
                        entry.path_info,
                        entry.fs,
                        entry.hash_info,
                        **kwargs
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
