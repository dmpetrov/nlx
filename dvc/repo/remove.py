import logging
import typing

from dvc.dvcfile import PipelineFile
from dvc.exceptions import InvalidArgumentError

from . import locked

if typing.TYPE_CHECKING:
    from dvc.repo import Repo


logger = logging.getLogger(__name__)


PARENT_TRACKED_ERROR_MSG = """
    DVC is already tracking '{parent}'
    and can't remove files within that directory.
    You can run 'dvc remove {parent}'
    in order to remove '{child}' and all
    the other files within that directory.
"""


@locked
def remove(self: "Repo", target: str, outs: bool = False):
    stages_info = self.stage.collect_granular(target)

    for stage, filter_info in stages_info:
        if filter_info is not None:
            # target is stage's output
            if isinstance(stage.dvcfile, PipelineFile):
                for out in stage.filter_outs(filter_info):
                    if out.path_info != filter_info:
                        raise InvalidArgumentError(
                            PARENT_TRACKED_ERROR_MSG.format(
                                parent=out.path_info, child=filter_info
                            )
                        )
                    out.remove(True)
            # target is tracked file or dir
            else:
                if stage.outs[0].path_info != filter_info:
                    raise InvalidArgumentError(
                        PARENT_TRACKED_ERROR_MSG.format(
                            parent=stage.outs[0].path_info, child=filter_info
                        )
                    )
                stage.remove(remove_outs=outs, force=outs)
        # target is stage name or `.dvc` file
        else:
            stage.remove(remove_outs=outs, force=outs)

    return [x.stage for x in stages_info]
