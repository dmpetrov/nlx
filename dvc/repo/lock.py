from . import locked


@locked
def lock(self, target, unlock=False):
    from .. import dvcfile
    from dvc.utils import parse_target

    path, target = parse_target(target)
    dvcfile = dvcfile.Dvcfile(self, path)
    stage = dvcfile.stages[target]
    stage.locked = False if unlock else True
    dvcfile.dump(stage, update_pipeline=True)

    return stage
