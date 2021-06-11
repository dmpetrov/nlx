import typing

from . import locked

if typing.TYPE_CHECKING:
    from . import Repo


@locked
def _set(repo: "Repo", target, frozen):
    stage = repo.stage.get_target(target)
    stage.frozen = frozen
    stage.dvcfile.dump(stage, update_lock=False)

    return stage


def freeze(repo, target):
    return _set(repo, target, True)


def unfreeze(repo, target):
    return _set(repo, target, False)
