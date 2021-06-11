import logging
from collections.abc import Mapping
from copy import deepcopy
from itertools import chain

from funcy import cached_property, get_in, lcat, once, project

from dvc import dependency, output
from dvc.hash_info import HashInfo
from dvc.parsing import FOREACH_KWD, JOIN, DataResolver, EntryNotFound
from dvc.parsing.versions import LOCKFILE_VERSION
from dvc.path_info import PathInfo

from . import PipelineStage, Stage, loads_from
from .exceptions import StageNameUnspecified, StageNotFound
from .params import StageParams
from .utils import fill_stage_dependencies, resolve_paths

logger = logging.getLogger(__name__)


class StageLoader(Mapping):
    def __init__(self, dvcfile, data, lockfile_data=None):
        self.dvcfile = dvcfile
        self.data = data or {}
        self.stages_data = self.data.get("stages", {})
        self.repo = self.dvcfile.repo

        lockfile_data = lockfile_data or {}
        version = LOCKFILE_VERSION.from_dict(lockfile_data)
        if version == LOCKFILE_VERSION.V1:
            self._lockfile_data = lockfile_data
        else:
            self._lockfile_data = lockfile_data.get("stages", {})

    @cached_property
    def resolver(self):
        wdir = PathInfo(self.dvcfile.path).parent
        return DataResolver(self.repo, wdir, self.data)

    @cached_property
    def lockfile_data(self):
        if not self._lockfile_data:
            logger.debug("Lockfile for '%s' not found", self.dvcfile.relpath)
        return self._lockfile_data

    @staticmethod
    def fill_from_lock(stage, lock_data=None):
        """Fill values for params, checksums for outs and deps from lock."""
        if not lock_data:
            return

        assert isinstance(lock_data, dict)
        items = chain(
            ((StageParams.PARAM_DEPS, dep) for dep in stage.deps),
            ((StageParams.PARAM_OUTS, out) for out in stage.outs),
        )

        checksums = {
            key: {item["path"]: item for item in lock_data.get(key, {})}
            for key in [StageParams.PARAM_DEPS, StageParams.PARAM_OUTS]
        }
        for key, item in items:
            path = item.def_path
            if isinstance(item, dependency.ParamsDependency):
                item.fill_values(get_in(lock_data, [stage.PARAM_PARAMS, path]))
                continue
            info = get_in(checksums, [key, path], {})
            info = info.copy()
            info.pop("path", None)
            item.isexec = info.pop("isexec", None)
            item.hash_info = HashInfo.from_dict(info)

    @classmethod
    def load_stage(cls, dvcfile, name, stage_data, lock_data=None):
        assert all([name, dvcfile, dvcfile.repo, dvcfile.path])
        assert stage_data and isinstance(stage_data, dict)

        path, wdir = resolve_paths(
            dvcfile.path, stage_data.get(Stage.PARAM_WDIR)
        )
        stage = loads_from(PipelineStage, dvcfile.repo, path, wdir, stage_data)
        stage.name = name
        stage.desc = stage_data.get(Stage.PARAM_DESC)
        stage.meta = stage_data.get(Stage.PARAM_META)

        deps = project(stage_data, [stage.PARAM_DEPS, stage.PARAM_PARAMS])
        fill_stage_dependencies(stage, **deps)

        outs = project(
            stage_data,
            [
                stage.PARAM_OUTS,
                stage.PARAM_METRICS,
                stage.PARAM_PLOTS,
                stage.PARAM_LIVE,
            ],
        )
        stage.outs = lcat(
            output.load_from_pipeline(stage, data, typ=key)
            for key, data in outs.items()
        )

        if lock_data:
            stage.cmd_changed = lock_data.get(Stage.PARAM_CMD) != stage.cmd

        cls.fill_from_lock(stage, lock_data)
        return stage

    @once
    def lockfile_needs_update(self):
        # if lockfile does not have all of the entries that dvc.yaml says it
        # should have, provide a debug message once
        # pylint: disable=protected-access
        lockfile = self.dvcfile._lockfile.relpath
        logger.debug("Lockfile '%s' needs to be updated.", lockfile)

    def __getitem__(self, name):
        if not name:
            raise StageNameUnspecified(self.dvcfile)

        try:
            resolved_data = self.resolver.resolve_one(name)
        except EntryNotFound:
            raise StageNotFound(self.dvcfile, name)

        if self.lockfile_data and name not in self.lockfile_data:
            self.lockfile_needs_update()
            logger.trace(  # type: ignore[attr-defined]
                "No lock entry found for '%s:%s'", self.dvcfile.relpath, name
            )

        resolved_stage = resolved_data[name]
        stage = self.load_stage(
            self.dvcfile,
            name,
            resolved_stage,
            self.lockfile_data.get(name, {}),
        )

        stage.tracked_vars = self.resolver.tracked_vars.get(name, {})
        group, *keys = name.rsplit(JOIN, maxsplit=1)
        if group and keys and name not in self.stages_data:
            stage.raw_data.generated_from = group

        stage.raw_data.parametrized = (
            self.stages_data.get(name, {}) != resolved_stage
        )
        return stage

    def __iter__(self):
        return iter(self.resolver.get_keys())

    def __len__(self):
        return len(self.resolver.get_keys())

    def __contains__(self, name):
        return self.resolver.has_key(name)  # noqa: W601

    def is_foreach_generated(self, name: str):
        return (
            name in self.stages_data and FOREACH_KWD in self.stages_data[name]
        )


class SingleStageLoader(Mapping):
    def __init__(self, dvcfile, stage_data, stage_text=None):
        self.dvcfile = dvcfile
        self.stage_data = stage_data or {}
        self.stage_text = stage_text

    def __getitem__(self, item):
        if item:
            logger.warning(
                "Ignoring name '%s' for single stage in '%s'.",
                item,
                self.dvcfile,
            )
        # during `load`, we remove attributes from stage data, so as to
        # not duplicate, therefore, for MappingView, we need to deepcopy.
        return self.load_stage(
            self.dvcfile, deepcopy(self.stage_data), self.stage_text
        )

    @classmethod
    def load_stage(cls, dvcfile, d, stage_text):
        path, wdir = resolve_paths(dvcfile.path, d.get(Stage.PARAM_WDIR))
        stage = loads_from(Stage, dvcfile.repo, path, wdir, d)
        stage._stage_text = stage_text  # noqa, pylint:disable=protected-access
        stage.deps = dependency.loadd_from(
            stage, d.get(Stage.PARAM_DEPS) or []
        )
        stage.outs = output.loadd_from(stage, d.get(Stage.PARAM_OUTS) or [])
        return stage

    def __iter__(self):
        return iter([None])

    def __contains__(self, item):
        return False

    def __len__(self):
        return 1
