from __future__ import unicode_literals

import dvc.logger as logger
from dvc.exceptions import DvcException
from dvc.progress import ProgressCallback


def _cleanup_unused_links(self, all_stages):
    used = []
    for stage in all_stages:
        for out in stage.outs:
            used.append(out.path)
    self.state.remove_unused_links(used)


def get_all_files_numbers(stages):
    return sum(stage.get_all_files_number() for stage in stages)


def get_progress_callback(stages):
    try:
        total_files_num = get_all_files_numbers(stages)
        return ProgressCallback(total_files_num)
    except Exception:
        return None


def checkout(self, target=None, with_deps=False, force=False, recursive=False):
    from dvc.stage import StageFileDoesNotExistError, StageFileBadNameError

    if target and not recursive:
        all_stages = self.active_stages()
        try:
            stages = self.collect(target, with_deps=with_deps)
        except (StageFileDoesNotExistError, StageFileBadNameError) as exc:
            raise DvcException(
                str(exc) + " Did you mean 'git checkout {}'?".format(target)
            )
    else:
        all_stages = self.active_stages(target)
        stages = all_stages

    with self.state:
        _cleanup_unused_links(self, all_stages)
        progress_callback = get_progress_callback(stages)

        for stage in stages:
            if stage.locked:
                logger.warning(
                    "DVC file '{path}' is locked. Its dependencies are"
                    " not going to be checked out.".format(path=stage.relpath)
                )

            stage.checkout(force=force, progress_callback=progress_callback)
        if progress_callback:
            progress_callback.finish("Checkout finished!")
