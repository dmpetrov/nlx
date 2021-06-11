import logging
import typing

from . import locked

if typing.TYPE_CHECKING:
    from dvc.repo import Repo

logger = logging.getLogger(__name__)


@locked
def remove(self: "Repo", target: str, outs: bool = False):
    stages = self.stage.from_target(target)

    for stage in stages:
        stage.remove(remove_outs=outs, force=outs)

    return stages
