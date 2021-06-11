import collections
import contextlib
import logging
import os
from typing import TYPE_CHECKING, Any, Union

from voluptuous import MultipleInvalid

from dvc.exceptions import DvcException
from dvc.parsing.versions import LOCKFILE_VERSION, SCHEMA_KWD
from dvc.stage import serialize
from dvc.stage.exceptions import (
    StageFileBadNameError,
    StageFileDoesNotExistError,
    StageFileFormatError,
    StageFileIsNotDvcFileError,
)
from dvc.types import AnyPath
from dvc.utils import relpath
from dvc.utils.collections import apply_diff
from dvc.utils.serialize import (
    dump_yaml,
    modify_yaml,
    parse_yaml,
    parse_yaml_for_update,
)

if TYPE_CHECKING:
    from dvc.repo import Repo

logger = logging.getLogger(__name__)

DVC_FILE = "Dvcfile"
DVC_FILE_SUFFIX = ".dvc"
PIPELINE_FILE = "dvc.yaml"
PIPELINE_LOCK = "dvc.lock"


class FileIsGitIgnored(DvcException):
    def __init__(self, path, pipeline_file=False):
        super().__init__(
            "{}'{}' is git-ignored.".format(
                "bad DVC file name " if pipeline_file else "", path
            )
        )


class LockfileCorruptedError(DvcException):
    pass


class ParametrizedDumpError(DvcException):
    pass


def is_valid_filename(path):
    return path.endswith(DVC_FILE_SUFFIX) or os.path.basename(path) in [
        DVC_FILE,
        PIPELINE_FILE,
    ]


def is_dvc_file(path):
    return os.path.isfile(path) and (
        is_valid_filename(path) or is_lock_file(path)
    )


def is_lock_file(path):
    return os.path.basename(path) == PIPELINE_LOCK


def is_git_ignored(repo, path):
    from dvc.fs.local import LocalFileSystem
    from dvc.scm.base import NoSCMError

    try:
        return isinstance(repo.fs, LocalFileSystem) and repo.scm.is_ignored(
            path
        )
    except NoSCMError:
        return False


def check_dvcfile_path(repo, path):
    if not is_valid_filename(path):
        raise StageFileBadNameError(
            "bad DVC file name '{}'. DVC files should be named "
            "'{}' or have a '.dvc' suffix (e.g. '{}.dvc').".format(
                relpath(path), PIPELINE_FILE, os.path.basename(path)
            )
        )

    if is_git_ignored(repo, path):
        raise FileIsGitIgnored(relpath(path), True)


class FileMixin:
    SCHEMA = None

    def __init__(self, repo, path, verify=True, **kwargs):
        self.repo = repo
        self.path = path
        self.verify = verify

    def __repr__(self):
        return "{}: {}".format(
            self.__class__.__name__, relpath(self.path, self.repo.root_dir)
        )

    def __hash__(self):
        return hash(self.path)

    def __eq__(self, other):
        return self.repo == other.repo and os.path.abspath(
            self.path
        ) == os.path.abspath(other.path)

    def __str__(self):
        return f"{self.__class__.__name__}: {self.relpath}"

    @property
    def relpath(self):
        return relpath(self.path)

    def exists(self):
        is_ignored = self.repo.dvcignore.is_ignored_file(self.path)
        return self.repo.fs.exists(self.path) and not is_ignored

    def _is_git_ignored(self):
        return is_git_ignored(self.repo, self.path)

    def _verify_filename(self):
        if self.verify:
            check_dvcfile_path(self.repo, self.path)

    def _check_gitignored(self):
        if self._is_git_ignored():
            raise FileIsGitIgnored(self.path)

    def _load(self):
        # it raises the proper exceptions by priority:
        # 1. when the file doesn't exists
        # 2. filename is not a DVC file
        # 3. path doesn't represent a regular file
        # 4. when the file is git ignored
        if not self.exists():
            dvc_ignored = self.repo.dvcignore.is_ignored_file(self.path)
            raise StageFileDoesNotExistError(
                self.path, dvc_ignored=dvc_ignored
            )

        self._verify_filename()
        if not self.repo.fs.isfile(self.path):
            raise StageFileIsNotDvcFileError(self.path)

        self._check_gitignored()
        with self.repo.fs.open(self.path, encoding="utf-8") as fd:
            stage_text = fd.read()
        d = parse_yaml(stage_text, self.path)
        return self.validate(d, self.relpath), stage_text

    @classmethod
    def validate(cls, d, fname=None):
        assert isinstance(cls.SCHEMA, collections.abc.Callable)
        try:
            return cls.SCHEMA(d)  # pylint: disable=not-callable
        except MultipleInvalid as exc:
            raise StageFileFormatError(f"'{fname}' format error: {exc}")

    def remove(self, force=False):  # pylint: disable=unused-argument
        with contextlib.suppress(FileNotFoundError):
            os.unlink(self.path)

    def dump(self, stage, **kwargs):
        raise NotImplementedError

    def merge(self, ancestor, other):
        raise NotImplementedError


class SingleStageFile(FileMixin):
    from dvc.schema import COMPILED_SINGLE_STAGE_SCHEMA as SCHEMA
    from dvc.stage.loader import SingleStageLoader as LOADER

    @property
    def stage(self):
        data, raw = self._load()
        return self.LOADER.load_stage(self, data, raw)

    @property
    def stages(self):
        data, raw = self._load()
        return self.LOADER(self, data, raw)

    def dump(self, stage, **kwargs):
        """Dumps given stage appropriately in the dvcfile."""
        from dvc.stage import PipelineStage

        assert not isinstance(stage, PipelineStage)
        if self.verify:
            check_dvcfile_path(self.repo, self.path)
        logger.debug(
            "Saving information to '{file}'.".format(file=relpath(self.path))
        )
        dump_yaml(self.path, serialize.to_single_stage_file(stage))
        self.repo.scm.track_file(self.relpath)

    def remove_stage(self, stage):  # pylint: disable=unused-argument
        self.remove()

    def merge(self, ancestor, other):
        assert isinstance(ancestor, SingleStageFile)
        assert isinstance(other, SingleStageFile)

        stage = self.stage
        stage.merge(ancestor.stage, other.stage)
        self.dump(stage)


class PipelineFile(FileMixin):
    """Abstraction for pipelines file, .yaml + .lock combined."""

    from dvc.schema import COMPILED_MULTI_STAGE_SCHEMA as SCHEMA
    from dvc.stage.loader import StageLoader as LOADER

    @property
    def _lockfile(self):
        return Lockfile(self.repo, os.path.splitext(self.path)[0] + ".lock")

    def dump(
        self, stage, update_pipeline=True, update_lock=True, **kwargs
    ):  # pylint: disable=arguments-differ
        """Dumps given stage appropriately in the dvcfile."""
        from dvc.stage import PipelineStage

        assert isinstance(stage, PipelineStage)
        if self.verify:
            check_dvcfile_path(self.repo, self.path)

        if update_pipeline and not stage.is_data_source:
            self._dump_pipeline_file(stage)

        if update_lock:
            self._dump_lockfile(stage)

    def _dump_lockfile(self, stage):
        self._lockfile.dump(stage)

    @staticmethod
    def _check_if_parametrized(stage):
        if stage.raw_data.parametrized:
            raise ParametrizedDumpError(f"cannot dump a parametrized {stage}")

    def _dump_pipeline_file(self, stage):
        self._check_if_parametrized(stage)
        stage_data = serialize.to_pipeline_file(stage)

        with modify_yaml(self.path, fs=self.repo.fs) as data:
            if not data:
                logger.info("Creating '%s'", self.relpath)

            data["stages"] = data.get("stages", {})
            existing_entry = stage.name in data["stages"]
            action = "Modifying" if existing_entry else "Adding"
            logger.info(
                "%s stage '%s' in '%s'", action, stage.name, self.relpath
            )

            if existing_entry:
                orig_stage_data = data["stages"][stage.name]
                apply_diff(stage_data[stage.name], orig_stage_data)
            else:
                data["stages"].update(stage_data)

        self.repo.scm.track_file(self.relpath)

    @property
    def stage(self):
        raise DvcException(
            "PipelineFile has multiple stages. Please specify it's name."
        )

    @property
    def stages(self):
        data, _ = self._load()
        lockfile_data = self._lockfile.load()
        return self.LOADER(self, data, lockfile_data)

    def remove(self, force=False):
        if not force:
            logger.warning("Cannot remove pipeline file.")
            return

        super().remove()
        self._lockfile.remove()

    def remove_stage(self, stage):
        self._lockfile.remove_stage(stage)
        if not self.exists():
            return

        with open(self.path, "r", encoding="utf-8") as f:
            d = parse_yaml_for_update(f.read(), self.path)

        self.validate(d, self.path)
        if stage.name not in d.get("stages", {}):
            return

        logger.debug("Removing '%s' from '%s'", stage.name, self.path)
        del d["stages"][stage.name]

        if d["stages"]:
            dump_yaml(self.path, d)
        else:
            super().remove()

    def merge(self, ancestor, other):
        raise NotImplementedError


def get_lockfile_schema(d):
    from dvc.schema import (
        COMPILED_LOCKFILE_V1_SCHEMA,
        COMPILED_LOCKFILE_V2_SCHEMA,
    )

    schema = {
        LOCKFILE_VERSION.V1: COMPILED_LOCKFILE_V1_SCHEMA,
        LOCKFILE_VERSION.V2: COMPILED_LOCKFILE_V2_SCHEMA,
    }

    version = LOCKFILE_VERSION.from_dict(d)
    return schema[version]


def migrate_lock_v1_to_v2(d, version_info):
    stages = {k: v for k, v in d.items()}

    for key in stages:
        d.pop(key)

    # forcing order, meta should always be at the top
    d.update(version_info)
    d["stages"] = stages


class Lockfile(FileMixin):
    @classmethod
    def validate(cls, d, fname=None):
        schema = get_lockfile_schema(d)
        try:
            return schema(d)
        except MultipleInvalid as exc:
            raise StageFileFormatError(f"'{fname}' format error: {exc}")

    def _verify_filename(self):
        pass  # lockfile path is hardcoded, so no need to verify here

    def _load(self):
        try:
            return super()._load()
        except StageFileDoesNotExistError:
            # we still need to account for git-ignored dvc.lock file
            # even though it may not exist or have been .dvcignored
            self._check_gitignored()
            return {}, ""
        except StageFileFormatError as exc:
            raise LockfileCorruptedError(
                f"Lockfile '{self.relpath}' is corrupted."
            ) from exc

    def load(self):
        d, _ = self._load()
        return d

    @property
    def latest_version_info(self):
        version = LOCKFILE_VERSION.V2.value  # pylint:disable=no-member
        return {SCHEMA_KWD: version}

    def dump(self, stage, **kwargs):
        stage_data = serialize.to_lockfile(stage)

        with modify_yaml(self.path, fs=self.repo.fs) as data:
            version = LOCKFILE_VERSION.from_dict(data)
            if version == LOCKFILE_VERSION.V1:
                logger.info(
                    "Migrating lock file '%s' from v1 to v2", self.relpath
                )
                migrate_lock_v1_to_v2(data, self.latest_version_info)
            else:
                if not data:
                    data.update(self.latest_version_info)
                    # order is important, meta should always be at the top
                    logger.info("Generating lock file '%s'", self.relpath)

            data["stages"] = data.get("stages", {})
            modified = data["stages"].get(stage.name, {}) != stage_data.get(
                stage.name, {}
            )
            if modified:
                logger.info("Updating lock file '%s'", self.relpath)

            data["stages"].update(stage_data)

        if modified:
            self.repo.scm.track_file(self.relpath)

    def remove_stage(self, stage):
        if not self.exists():
            return

        with open(self.path, encoding="utf-8") as f:
            d = parse_yaml_for_update(f.read(), self.path)
        self.validate(d, self.path)

        version = LOCKFILE_VERSION.from_dict(d)
        data = d if version == LOCKFILE_VERSION.V1 else d.get("stages", {})
        if stage.name not in data:
            return

        logger.debug("Removing '%s' from '%s'", stage.name, self.path)
        del data[stage.name]

        if data:
            dump_yaml(self.path, d)
        else:
            self.remove()

    def merge(self, ancestor, other):
        raise NotImplementedError


class Dvcfile:
    def __new__(cls, repo: "Repo", path: AnyPath, **kwargs: Any):
        assert path
        assert repo

        return make_dvcfile(repo, path, **kwargs)


DVCFile = Union["PipelineFile", "SingleStageFile"]


def make_dvcfile(repo: "Repo", path: AnyPath, **kwargs: Any) -> DVCFile:
    _, ext = os.path.splitext(str(path))
    if ext in [".yaml", ".yml"]:
        return PipelineFile(repo, path, **kwargs)
    # fallback to single stage file for better error messages
    return SingleStageFile(repo, path, **kwargs)
