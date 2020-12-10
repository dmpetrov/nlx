from collections import defaultdict
from urllib.parse import urlparse

from funcy import collecting, project
from voluptuous import And, Any, Coerce, Length, Lower, Required, SetTo

from dvc.hash_info import HashInfo
from dvc.output.base import BaseOutput
from dvc.output.gs import GSOutput
from dvc.output.hdfs import HDFSOutput
from dvc.output.local import LocalOutput
from dvc.output.s3 import S3Output
from dvc.output.ssh import SSHOutput
from dvc.output.webhdfs import WebHDFSOutput
from dvc.scheme import Schemes

from ..tree import get_cloud_tree
from ..tree.hdfs import HDFSTree
from ..tree.local import LocalTree
from ..tree.s3 import S3Tree
from ..tree.webhdfs import WebHDFSTree

OUTS = [
    HDFSOutput,
    S3Output,
    GSOutput,
    SSHOutput,
    WebHDFSOutput,
    # NOTE: LocalOutput is the default choice
]

OUTS_MAP = {
    Schemes.HDFS: HDFSOutput,
    Schemes.S3: S3Output,
    Schemes.GS: GSOutput,
    Schemes.SSH: SSHOutput,
    Schemes.LOCAL: LocalOutput,
    Schemes.WEBHDFS: WebHDFSOutput,
}

CHECKSUM_SCHEMA = Any(
    None,
    And(str, Length(max=0), SetTo(None)),
    And(Any(str, And(int, Coerce(str))), Length(min=3), Lower),
)

# NOTE: currently there are only 3 possible checksum names:
#
#    1) md5 (LOCAL, SSH, GS);
#    2) etag (S3);
#    3) checksum (HDFS);
#
# so when a few types of outputs share the same name, we only need
# specify it once.
CHECKSUMS_SCHEMA = {
    LocalTree.PARAM_CHECKSUM: CHECKSUM_SCHEMA,
    S3Tree.PARAM_CHECKSUM: CHECKSUM_SCHEMA,
    HDFSTree.PARAM_CHECKSUM: CHECKSUM_SCHEMA,
    WebHDFSTree.PARAM_CHECKSUM: CHECKSUM_SCHEMA,
}

SCHEMA = CHECKSUMS_SCHEMA.copy()
SCHEMA[Required(BaseOutput.PARAM_PATH)] = str
SCHEMA[BaseOutput.PARAM_CACHE] = bool
SCHEMA[BaseOutput.PARAM_METRIC] = BaseOutput.METRIC_SCHEMA
SCHEMA[BaseOutput.PARAM_PLOT] = bool
SCHEMA[BaseOutput.PARAM_PERSIST] = bool
SCHEMA[BaseOutput.PARAM_CHECKPOINT] = bool
SCHEMA[HashInfo.PARAM_SIZE] = int
SCHEMA[HashInfo.PARAM_NFILES] = int
SCHEMA[BaseOutput.PARAM_DESC] = str
SCHEMA[BaseOutput.PARAM_STORE] = bool


def _get(
    stage,
    p,
    info=None,
    cache=True,
    metric=False,
    plot=False,
    persist=False,
    checkpoint=False,
    desc=None,
    store=True,
):
    parsed = urlparse(p)

    if parsed.scheme == "remote":
        tree = get_cloud_tree(stage.repo, name=parsed.netloc)
        return OUTS_MAP[tree.scheme](
            stage,
            p,
            info,
            cache=cache,
            tree=tree,
            metric=metric,
            plot=plot,
            persist=persist,
            checkpoint=checkpoint,
            desc=desc,
            store=store,
        )

    for o in OUTS:
        if o.supported(p):
            return o(
                stage,
                p,
                info,
                cache=cache,
                tree=None,
                metric=metric,
                plot=plot,
                persist=persist,
                checkpoint=checkpoint,
                desc=desc,
                store=store,
            )
    return LocalOutput(
        stage,
        p,
        info,
        cache=cache,
        tree=None,
        metric=metric,
        plot=plot,
        persist=persist,
        checkpoint=checkpoint,
        desc=desc,
        store=store,
    )


def loadd_from(stage, d_list):
    ret = []
    for d in d_list:
        p = d.pop(BaseOutput.PARAM_PATH)
        store = d.pop(BaseOutput.PARAM_STORE, not stage.is_repo_import)
        cache = d.pop(BaseOutput.PARAM_CACHE, True)
        metric = d.pop(BaseOutput.PARAM_METRIC, False)
        plot = d.pop(BaseOutput.PARAM_PLOT, False)
        persist = d.pop(BaseOutput.PARAM_PERSIST, False)
        checkpoint = d.pop(BaseOutput.PARAM_CHECKPOINT, False)
        desc = d.pop(BaseOutput.PARAM_DESC, False)
        ret.append(
            _get(
                stage,
                p,
                info=d,
                store=store,
                cache=cache,
                metric=metric,
                plot=plot,
                persist=persist,
                checkpoint=checkpoint,
                desc=desc,
            )
        )
    return ret


def loads_from(
    stage,
    s_list,
    use_cache=True,
    metric=False,
    plot=False,
    persist=False,
    checkpoint=False,
    store=True,
):
    return [
        _get(
            stage,
            s,
            info={},
            cache=use_cache,
            metric=metric,
            plot=plot,
            persist=persist,
            checkpoint=checkpoint,
            store=store,
        )
        for s in s_list
    ]


def _split_dict(d, keys):
    return project(d, keys), project(d, d.keys() - keys)


def _merge_data(s_list):
    d = defaultdict(dict)
    for key in s_list:
        if isinstance(key, str):
            d[key].update({})
            continue
        if not isinstance(key, dict):
            raise ValueError(f"'{type(key).__name__}' not supported.")

        for k, flags in key.items():
            if not isinstance(flags, dict):
                raise ValueError(
                    f"Expected dict for '{k}', got: '{type(flags).__name__}'"
                )
            d[k].update(flags)
    return d


@collecting
def load_from_pipeline(stage, s_list, typ="outs"):
    if typ not in (stage.PARAM_OUTS, stage.PARAM_METRICS, stage.PARAM_PLOTS):
        raise ValueError(f"'{typ}' key is not allowed for pipeline files.")

    metric = typ == stage.PARAM_METRICS
    plot = typ == stage.PARAM_PLOTS

    d = _merge_data(s_list)

    for path, flags in d.items():
        plt_d = {}
        if plot:
            from dvc.schema import PLOT_PROPS

            plt_d, flags = _split_dict(flags, keys=PLOT_PROPS.keys())
        extra = project(
            flags,
            [
                BaseOutput.PARAM_CACHE,
                BaseOutput.PARAM_PERSIST,
                BaseOutput.PARAM_CHECKPOINT,
            ],
        )
        yield _get(stage, path, {}, plot=plt_d or plot, metric=metric, **extra)
