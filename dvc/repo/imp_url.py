import os

from dvc.repo.scm_context import scm_context
from dvc.utils import relpath, resolve_output, resolve_paths
from dvc.utils.fs import path_isin

from ..exceptions import InvalidArgumentError, OutputDuplicationError
from . import locked


@locked
@scm_context
def imp_url(
    self,
    url,
    out=None,
    fname=None,
    erepo=None,
    frozen=True,
    no_exec=False,
    remote=None,
    to_remote=False,
    desc=None,
    jobs=None,
):
    from dvc.dvcfile import Dvcfile
    from dvc.stage import Stage, create_stage, restore_meta

    out = resolve_output(url, out)
    path, wdir, out = resolve_paths(
        self, out, always_local=to_remote and not out
    )

    if to_remote and no_exec:
        raise InvalidArgumentError(
            "--no-exec can't be combined with --to-remote"
        )

    if not to_remote and remote:
        raise InvalidArgumentError(
            "--remote can't be used without --to-remote"
        )

    # NOTE: when user is importing something from within their own repository
    if (
        erepo is None
        and os.path.exists(url)
        and path_isin(os.path.abspath(url), self.root_dir)
    ):
        url = relpath(url, wdir)

    stage = create_stage(
        Stage,
        self,
        fname or path,
        wdir=wdir,
        deps=[url],
        outs=[out],
        erepo=erepo,
    )
    restore_meta(stage)

    if desc:
        stage.outs[0].desc = desc

    dvcfile = Dvcfile(self, stage.path)
    dvcfile.remove()

    try:
        self.check_modified_graph([stage])
    except OutputDuplicationError as exc:
        raise OutputDuplicationError(exc.output, set(exc.stages) - {stage})

    if no_exec:
        stage.ignore_outs()
    elif to_remote:
        remote = self.cloud.get_remote(remote, "import-url")
        stage.outs[0].transfer(url, odb=remote.odb, jobs=jobs)
    else:
        stage.run(jobs=jobs)

    stage.frozen = frozen

    dvcfile.dump(stage)

    return stage
