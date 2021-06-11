import logging

from dvc.exceptions import InvalidArgumentError

logger = logging.getLogger(__name__)


def _update_import_on_remote(stage, remote, jobs):
    if stage.is_repo_import:
        raise InvalidArgumentError(
            "Data imported from other DVC or Git repositories can't "
            "be updated with --to-remote"
        )

    url = stage.deps[0].def_path
    remote = stage.repo.cloud.get_remote(remote, "update")
    stage.outs[0].transfer(url, odb=remote.odb, jobs=jobs, update=True)


def update_import(stage, rev=None, to_remote=False, remote=None, jobs=None):
    stage.deps[0].update(rev=rev)
    frozen = stage.frozen
    stage.frozen = False
    try:
        if to_remote:
            _update_import_on_remote(stage, remote, jobs)
        else:
            stage.reproduce(jobs=jobs)
    finally:
        stage.frozen = frozen


def sync_import(stage, dry=False, force=False, jobs=None):
    """Synchronize import's outs to the workspace."""
    logger.info(
        "Importing '{dep}' -> '{out}'".format(
            dep=stage.deps[0], out=stage.outs[0]
        )
    )
    if dry:
        return

    if not force and stage.already_cached():
        stage.outs[0].checkout()
    else:
        stage.save_deps()
        stage.deps[0].download(stage.outs[0], jobs=jobs)
