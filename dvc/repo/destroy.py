from dvc.utils.fs import remove

from . import locked


@locked
def _destroy_stages(repo):
    for stage in repo.stages:
        stage.unprotect_outs()
        stage.dvcfile.remove(force=True)


# NOTE: not locking `destroy`, as `remove` will need to delete `.dvc` dir,
# which will cause issues on Windows, as `.dvc/lock` will be busy.
def destroy(repo):
    _destroy_stages(repo)
    repo.close()
    remove(repo.dvc_dir)
