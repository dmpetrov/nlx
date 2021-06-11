from dvc import prompt

from . import locked


def _prepare_message(stage, changes):
    changed_deps, changed_outs, changed_stage = changes
    if changed_deps and changed_outs:
        msg = "dependencies {deps} and outputs {outs} of {stage} changed."
    elif changed_deps:
        msg = "dependencies {deps} of {stage} changed."
    elif changed_outs:
        msg = "outputs {outs} of {stage} changed."
    else:
        msg = "{stage_changed}"

    msg += " Are you sure you want to commit it?"

    kw = {
        "stage": stage,
        "deps": changed_deps,
        "outs": changed_outs,
        "stage_changed": changed_stage,
    }
    return msg.format_map(kw)


def prompt_to_commit(stage, changes, force=False):
    from dvc.stage.exceptions import StageCommitError

    if not (force or prompt.confirm(_prepare_message(stage, changes))):
        raise StageCommitError(
            "unable to commit changed {}. Use `-f|--force` to "
            "force.".format(stage)
        )


@locked
def commit(
    self,
    target,
    with_deps=False,
    recursive=False,
    force=False,
    allow_missing=False,
    data_only=False,
):
    from dvc.dvcfile import Dvcfile

    stages_info = [
        info
        for info in self.stage.collect_granular(
            target, with_deps=with_deps, recursive=recursive
        )
        if not data_only or info.stage.is_data_source
    ]
    for stage_info in stages_info:
        stage = stage_info.stage
        changes = stage.changed_entries()
        if any(changes):
            prompt_to_commit(stage, changes, force=force)
            stage.save(allow_missing=allow_missing)
        stage.commit(
            filter_info=stage_info.filter_info, allow_missing=allow_missing
        )

        Dvcfile(self, stage.path).dump(stage, update_pipeline=False)
    return [s.stage for s in stages_info]
