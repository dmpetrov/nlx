from voluptuous import Any, Optional, Required, Schema

from dvc import dependency, output
from dvc.hash_info import HashInfo
from dvc.output import CHECKSUMS_SCHEMA, Output
from dvc.parsing import DO_KWD, FOREACH_KWD, VARS_KWD
from dvc.parsing.versions import SCHEMA_KWD, lockfile_version_schema
from dvc.stage.params import StageParams

STAGES = "stages"
SINGLE_STAGE_SCHEMA = {
    StageParams.PARAM_MD5: output.CHECKSUM_SCHEMA,
    StageParams.PARAM_CMD: Any(str, list, None),
    StageParams.PARAM_WDIR: Any(str, None),
    StageParams.PARAM_DEPS: Any([dependency.SCHEMA], None),
    StageParams.PARAM_OUTS: Any([output.SCHEMA], None),
    StageParams.PARAM_LOCKED: bool,  # backward compatibility
    StageParams.PARAM_FROZEN: bool,
    StageParams.PARAM_META: object,
    StageParams.PARAM_ALWAYS_CHANGED: bool,
    StageParams.PARAM_DESC: str,
}

DATA_SCHEMA = {
    **CHECKSUMS_SCHEMA,
    Required("path"): str,
    HashInfo.PARAM_SIZE: int,
    HashInfo.PARAM_NFILES: int,
    Output.PARAM_ISEXEC: bool,
}
LOCK_FILE_STAGE_SCHEMA = {
    Required(StageParams.PARAM_CMD): Any(str, list),
    StageParams.PARAM_DEPS: [DATA_SCHEMA],
    StageParams.PARAM_PARAMS: {str: {str: object}},
    StageParams.PARAM_OUTS: [DATA_SCHEMA],
}

LOCKFILE_STAGES_SCHEMA = {str: LOCK_FILE_STAGE_SCHEMA}

LOCKFILE_V1_SCHEMA = LOCKFILE_STAGES_SCHEMA
LOCKFILE_V2_SCHEMA = {
    STAGES: LOCKFILE_STAGES_SCHEMA,
    Required(SCHEMA_KWD): lockfile_version_schema,
}

OUT_PSTAGE_DETAILED_SCHEMA = {
    str: {
        Output.PARAM_CACHE: bool,
        Output.PARAM_PERSIST: bool,
        Output.PARAM_CHECKPOINT: bool,
        Output.PARAM_DESC: str,
    }
}

PLOT_PROPS = {
    Output.PARAM_PLOT_TEMPLATE: str,
    Output.PARAM_PLOT_X: str,
    Output.PARAM_PLOT_Y: str,
    Output.PARAM_PLOT_X_LABEL: str,
    Output.PARAM_PLOT_Y_LABEL: str,
    Output.PARAM_PLOT_TITLE: str,
    Output.PARAM_PLOT_HEADER: bool,
}
PLOT_PROPS_SCHEMA = {**OUT_PSTAGE_DETAILED_SCHEMA[str], **PLOT_PROPS}
PLOT_PSTAGE_SCHEMA = {str: Any(PLOT_PROPS_SCHEMA, [PLOT_PROPS_SCHEMA])}

LIVE_PROPS = {
    Output.PARAM_LIVE_SUMMARY: bool,
    Output.PARAM_LIVE_HTML: bool,
}
LIVE_PROPS_SCHEMA = {**PLOT_PROPS_SCHEMA, **LIVE_PROPS}
LIVE_PSTAGE_SCHEMA = {str: LIVE_PROPS_SCHEMA}

PARAM_PSTAGE_NON_DEFAULT_SCHEMA = {str: [str]}

VARS_SCHEMA = [str, dict]

STAGE_DEFINITION = {
    Required(StageParams.PARAM_CMD): Any(str, list),
    Optional(StageParams.PARAM_WDIR): str,
    Optional(StageParams.PARAM_DEPS): [str],
    Optional(StageParams.PARAM_PARAMS): [Any(str, dict)],
    Optional(VARS_KWD): VARS_SCHEMA,
    Optional(StageParams.PARAM_FROZEN): bool,
    Optional(StageParams.PARAM_META): object,
    Optional(StageParams.PARAM_DESC): str,
    Optional(StageParams.PARAM_ALWAYS_CHANGED): bool,
    Optional(StageParams.PARAM_OUTS): [Any(str, OUT_PSTAGE_DETAILED_SCHEMA)],
    Optional(StageParams.PARAM_METRICS): [
        Any(str, OUT_PSTAGE_DETAILED_SCHEMA)
    ],
    Optional(StageParams.PARAM_PLOTS): [Any(str, PLOT_PSTAGE_SCHEMA)],
    Optional(StageParams.PARAM_LIVE): Any(str, LIVE_PSTAGE_SCHEMA),
}

FOREACH_IN = {
    Required(FOREACH_KWD): Any(dict, list, str),
    Required(DO_KWD): STAGE_DEFINITION,
}
SINGLE_PIPELINE_STAGE_SCHEMA = {str: Any(FOREACH_IN, STAGE_DEFINITION)}
MULTI_STAGE_SCHEMA = {
    STAGES: SINGLE_PIPELINE_STAGE_SCHEMA,
    VARS_KWD: VARS_SCHEMA,
}

COMPILED_SINGLE_STAGE_SCHEMA = Schema(SINGLE_STAGE_SCHEMA)
COMPILED_MULTI_STAGE_SCHEMA = Schema(MULTI_STAGE_SCHEMA)
COMPILED_LOCK_FILE_STAGE_SCHEMA = Schema(LOCK_FILE_STAGE_SCHEMA)
COMPILED_LOCKFILE_V1_SCHEMA = Schema(LOCKFILE_V1_SCHEMA)
COMPILED_LOCKFILE_V2_SCHEMA = Schema(LOCKFILE_V2_SCHEMA)
