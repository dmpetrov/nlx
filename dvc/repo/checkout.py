import logging
import os
from typing import TYPE_CHECKING, Set

from dvc.exceptions import (
    CheckoutError,
    CheckoutErrorSuggestGit,
    NoOutputOrStageError,
)
from dvc.progress import Tqdm
from dvc.utils import relpath

from . import locked

if TYPE_CHECKING:
    from . import Repo
    from .stage import StageInfo

logger = logging.getLogger(__name__)


def _get_unused_links(repo):
    used = [
        out.fspath
        for stage in repo.stages
        for out in stage.outs
        if out.scheme == "local"
    ]
    return repo.state.get_unused_links(used)


def _fspath_dir(path):
    if not os.path.exists(str(path)):
        return str(path)

    path = relpath(path)
    return os.path.join(path, "") if os.path.isdir(path) else path


def get_all_files_numbers(pairs):
    return sum(
        stage.get_all_files_number(filter_info) for stage, filter_info in pairs
    )


def _collect_pairs(
    self: "Repo", targets, with_deps: bool, recursive: bool
) -> Set["StageInfo"]:
    from dvc.stage.exceptions import (
        StageFileBadNameError,
        StageFileDoesNotExistError,
    )

    pairs: Set["StageInfo"] = set()
    for target in targets:
        try:
            pairs.update(
                self.stage.collect_granular(
                    target, with_deps=with_deps, recursive=recursive
                )
            )
        except (
            StageFileDoesNotExistError,
            StageFileBadNameError,
            NoOutputOrStageError,
        ) as exc:
            if not target:
                raise
            raise CheckoutErrorSuggestGit(target) from exc

    return pairs


@locked
def checkout(
    self,
    targets=None,
    with_deps=False,
    force=False,
    relink=False,
    recursive=False,
    allow_missing=False,
    **kwargs,
):

    unused = []
    stats = {
        "added": [],
        "deleted": [],
        "modified": [],
        "failed": [],
        "failed_not_stored": [],
    }
    if not targets:
        targets = [None]
        unused = _get_unused_links(self)

    stats["deleted"] = [_fspath_dir(u) for u in unused]
    self.state.remove_links(unused)

    if isinstance(targets, str):
        targets = [targets]

    pairs = _collect_pairs(self, targets, with_deps, recursive)
    total = get_all_files_numbers(pairs)
    with Tqdm(
        total=total, unit="file", desc="Checkout", disable=total == 0
    ) as pbar:
        for stage, filter_info in pairs:
            result = stage.checkout(
                force=force,
                progress_callback=pbar.update_msg,
                relink=relink,
                filter_info=filter_info,
                allow_missing=allow_missing,
                **kwargs,
            )
            for key, items in result.items():
                stats[key].extend(_fspath_dir(path) for path in items)

    if stats.get("failed") or stats.get("failed_not_stored"):
        raise CheckoutError(
            stats["failed"] + stats["failed_not_stored"],
            stats,
            failed_not_stored=stats["failed_not_stored"],
        )

    del stats["failed"]
    del stats["failed_not_stored"]
    return stats
