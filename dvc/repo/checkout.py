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


def _fspath_dir(path):
    if not os.path.exists(str(path)):
        return str(path)

    path = relpath(path)
    return os.path.join(path, "") if os.path.isdir(path) else path


def _remove_unused_links(repo):
    used = [
        out.fspath
        for stage in repo.stages
        for out in stage.outs
        if out.scheme == "local"
    ]

    unused = repo.state.get_unused_links(used, repo.fs)
    ret = [_fspath_dir(u) for u in unused]
    repo.state.remove_links(unused, repo.fs)
    return ret


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

    stats = {"added": [], "deleted": [], "modified": [], "failed": []}
    if not targets:
        targets = [None]
        stats["deleted"] = _remove_unused_links(self)

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

    if stats.get("failed"):
        raise CheckoutError(stats["failed"], stats)

    del stats["failed"]
    return stats
