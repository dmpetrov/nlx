import logging
import os
from copy import deepcopy
from itertools import starmap
from typing import TYPE_CHECKING

from funcy import join
from funcy.seqs import first

from dvc.dependency.param import ParamsDependency
from dvc.path_info import PathInfo
from dvc.utils.serialize import dumps_yaml

from .context import Context
from .interpolate import resolve

if TYPE_CHECKING:
    from dvc.repo import Repo

logger = logging.getLogger(__name__)

STAGES_KWD = "stages"
USE_KWD = "use"
VARS_KWD = "vars"
WDIR_KWD = "wdir"
DEFAULT_PARAMS_FILE = ParamsDependency.DEFAULT_PARAMS_FILE
PARAMS_KWD = "params"

DEFAULT_SENTINEL = object()


class DataResolver:
    def __init__(self, repo: "Repo", yaml_wdir: PathInfo, d: dict):
        to_import: PathInfo = yaml_wdir / d.get(USE_KWD, DEFAULT_PARAMS_FILE)
        vars_ = d.get(VARS_KWD, {})
        if os.path.exists(to_import):
            self.global_ctx_source = to_import
            self.global_ctx = Context.load_from(repo.tree, str(to_import))
        else:
            self.global_ctx = Context()
            self.global_ctx_source = None
            logger.debug(
                "%s does not exist, it won't be used in parametrization",
                to_import,
            )

        self.global_ctx.merge_update(vars_)
        self.data: dict = d
        self._yaml_wdir = yaml_wdir
        self.repo = repo

    def _resolve_entry(self, name: str, definition):
        context = Context.clone(self.global_ctx)
        return self._resolve_stage(context, name, definition)

    def resolve(self):
        stages = self.data.get(STAGES_KWD, {})
        data = join(starmap(self._resolve_entry, stages.items()))
        logger.trace("Resolved dvc.yaml:\n%s", dumps_yaml(data))
        return {STAGES_KWD: data}

    def _resolve_stage(self, context: Context, name: str, definition) -> dict:
        definition = deepcopy(definition)
        wdir = self._resolve_wdir(context, definition.get(WDIR_KWD))
        if self._yaml_wdir != wdir:
            logger.debug(
                "Stage %s has different wdir than dvc.yaml file", name
            )

        contexts = []
        params_yaml_file = wdir / DEFAULT_PARAMS_FILE
        if self.global_ctx_source != params_yaml_file:
            if os.path.exists(params_yaml_file):
                contexts.append(
                    Context.load_from(self.repo.tree, str(params_yaml_file))
                )
            else:
                logger.debug(
                    "%s does not exist for stage %s", params_yaml_file, name
                )

        params_file = definition.get(PARAMS_KWD, [])
        for item in params_file:
            if item and isinstance(item, dict):
                contexts.append(
                    Context.load_from(self.repo.tree, str(wdir / first(item)))
                )

        context.merge_update(*contexts)

        logger.trace(  # pytype: disable=attribute-error
            "Context during resolution of stage %s:\n%s", name, context
        )

        with context.track():
            stage_d = resolve(definition, context)

        params = stage_d.get(PARAMS_KWD, []) + context.tracked

        if params:
            stage_d[PARAMS_KWD] = params
        return {name: stage_d}

    def _resolve_wdir(self, context: Context, wdir: str = None) -> PathInfo:
        if not wdir:
            return self._yaml_wdir
        wdir = resolve(wdir, context)
        return self._yaml_wdir / str(wdir)
