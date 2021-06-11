"""DVC config objects."""
import logging
import os
import re
from contextlib import contextmanager
from functools import partial

from funcy import cached_property, compact, memoize, re_find

from dvc.exceptions import DvcException, NotDvcRepoError
from dvc.path_info import PathInfo

logger = logging.getLogger(__name__)


class ConfigError(DvcException):
    """DVC config exception."""

    def __init__(self, msg):
        super().__init__(f"config file error: {msg}")


class NoRemoteError(ConfigError):
    pass


@memoize
def get_compiled_schema():
    from voluptuous import Schema

    from .config_schema import SCHEMA

    return Schema(SCHEMA)


def to_bool(value):
    from .config_schema import Bool

    return Bool(value)


class Config(dict):
    """Class that manages configuration files for a DVC repo.

    Args:
        dvc_dir (str): optional path to `.dvc` directory, that is used to
            access repo-specific configs like .dvc/config and
            .dvc/config.local.
        validate (bool): optional flag to tell dvc if it should validate the
            config or just load it as is. 'True' by default.

    Raises:
        ConfigError: thrown if config has an invalid format.
    """

    APPNAME = "dvc"
    APPAUTHOR = "iterative"

    # In the order they shadow each other
    LEVELS = ("system", "global", "repo", "local")

    CONFIG = "config"
    CONFIG_LOCAL = "config.local"

    def __init__(
        self, dvc_dir=None, validate=True, fs=None, config=None
    ):  # pylint: disable=super-init-not-called
        from dvc.fs.local import LocalFileSystem

        self.dvc_dir = dvc_dir

        if not dvc_dir:
            try:
                from dvc.repo import Repo

                self.dvc_dir = os.path.join(Repo.find_dvc_dir())
            except NotDvcRepoError:
                self.dvc_dir = None
        else:
            self.dvc_dir = os.path.abspath(os.path.realpath(dvc_dir))

        self.wfs = LocalFileSystem(url=self.dvc_dir)
        self.fs = fs or self.wfs

        self.load(validate=validate, config=config)

    @classmethod
    def get_dir(cls, level):
        from appdirs import site_config_dir, user_config_dir

        assert level in ("global", "system")

        if level == "global":
            return user_config_dir(cls.APPNAME, cls.APPAUTHOR)
        if level == "system":
            return site_config_dir(cls.APPNAME, cls.APPAUTHOR)

    @cached_property
    def files(self):
        files = {
            level: os.path.join(self.get_dir(level), self.CONFIG)
            for level in ("system", "global")
        }

        if self.dvc_dir is not None:
            files["repo"] = os.path.join(self.dvc_dir, self.CONFIG)
            files["local"] = os.path.join(self.dvc_dir, self.CONFIG_LOCAL)

        return files

    @staticmethod
    def init(dvc_dir):
        """Initializes dvc config.

        Args:
            dvc_dir (str): path to .dvc directory.

        Returns:
            dvc.config.Config: config object.
        """
        config_file = os.path.join(dvc_dir, Config.CONFIG)
        open(config_file, "w+").close()
        return Config(dvc_dir)

    def load(self, validate=True, config=None):
        """Loads config from all the config files.

        Raises:
            ConfigError: thrown if config has an invalid format.
        """
        conf = self.load_config_to_level()

        if config is not None:
            merge(conf, config)

        if validate:
            conf = self.validate(conf)

        self.clear()
        self.update(conf)

        # Add resolved default cache.dir
        if not self["cache"].get("dir") and self.dvc_dir:
            self["cache"]["dir"] = os.path.join(self.dvc_dir, "cache")

    def _get_fs(self, level):
        # NOTE: this might be a Gitfs, which doesn't see things outside of
        # the repo.
        return self.fs if level == "repo" else self.wfs

    def _load_config(self, level):
        from configobj import ConfigObj

        filename = self.files[level]
        fs = self._get_fs(level)

        if fs.exists(filename):
            with fs.open(filename) as fobj:
                conf_obj = ConfigObj(fobj)
        else:
            conf_obj = ConfigObj()
        return _parse_remotes(_lower_keys(conf_obj.dict()))

    def _save_config(self, level, conf_dict):
        from configobj import ConfigObj

        filename = self.files[level]
        fs = self._get_fs(level)

        logger.debug(f"Writing '{filename}'.")

        fs.makedirs(os.path.dirname(filename))

        config = ConfigObj(_pack_remotes(conf_dict))
        with fs.open(filename, "wb") as fobj:
            config.write(fobj)
        config.filename = filename

    def load_one(self, level):
        conf = self._load_config(level)
        conf = self._load_paths(conf, self.files[level])

        # Auto-verify sections
        for key in get_compiled_schema().schema:
            conf.setdefault(key, {})

        return conf

    @staticmethod
    def _load_paths(conf, filename):
        abs_conf_dir = os.path.abspath(os.path.dirname(filename))

        def resolve(path):
            from .config_schema import RelPath

            if os.path.isabs(path) or re.match(r"\w+://", path):
                return path

            # on windows convert slashes to backslashes
            # to have path compatible with abs_conf_dir
            if os.path.sep == "\\" and "/" in path:
                path = path.replace("/", "\\")

            return RelPath(os.path.join(abs_conf_dir, path))

        return Config._map_dirs(conf, resolve)

    @staticmethod
    def _to_relpath(conf_dir, path):
        from dvc.utils import relpath

        from .config_schema import RelPath

        if re.match(r"\w+://", path):
            return path

        if isinstance(path, RelPath) or not os.path.isabs(path):
            path = relpath(path, conf_dir)

        return PathInfo(path).as_posix()

    @staticmethod
    def _save_paths(conf, filename):
        conf_dir = os.path.dirname(filename)
        rel = partial(Config._to_relpath, conf_dir)

        return Config._map_dirs(conf, rel)

    @staticmethod
    def _map_dirs(conf, func):
        from voluptuous import ALLOW_EXTRA, Schema

        dirs_schema = {
            "cache": {"dir": func},
            "remote": {
                str: {
                    "url": func,
                    "gdrive_user_credentials_file": func,
                    "gdrive_service_account_json_file_path": func,
                    "credentialpath": func,
                    "keyfile": func,
                    "cert_path": func,
                    "key_path": func,
                }
            },
        }
        return Schema(dirs_schema, extra=ALLOW_EXTRA)(conf)

    def load_config_to_level(self, level=None):
        merged_conf = {}
        for merge_level in self.LEVELS:
            if merge_level == level:
                break
            if merge_level in self.files:
                merge(merged_conf, self.load_one(merge_level))
        return merged_conf

    def read(self, level=None):
        # NOTE: we read from a merged config by default, same as git config
        if level is None:
            return self.load_config_to_level()
        return self.load_one(level)

    @contextmanager
    def edit(self, level=None, validate=True):
        # NOTE: we write to repo config by default, same as git config
        level = level or "repo"
        if level in {"repo", "local"} and self.dvc_dir is None:
            raise ConfigError("Not inside a DVC repo")

        conf = self.load_one(level)
        yield conf

        conf = self._save_paths(conf, self.files[level])

        merged_conf = self.load_config_to_level(level)
        merge(merged_conf, conf)

        if validate:
            self.validate(merged_conf)

        self._save_config(level, conf)
        self.load(validate=validate)

    @staticmethod
    def validate(data):
        from voluptuous import Invalid

        try:
            return get_compiled_schema()(data)
        except Invalid as exc:
            raise ConfigError(str(exc)) from None


def _parse_remotes(conf):
    result = {"remote": {}}

    for section, val in conf.items():
        name = re_find(r'^\s*remote\s*"(.*)"\s*$', section)
        if name:
            result["remote"][name] = val
        else:
            result[section] = val

    return result


def _pack_remotes(conf):
    # Drop empty sections
    result = compact(conf)

    # Transform remote.name -> 'remote "name"'
    for name, val in conf["remote"].items():
        result[f'remote "{name}"'] = val
    result.pop("remote", None)

    return result


def merge(into, update):
    """Merges second dict into first recursively"""
    for key, val in update.items():
        if isinstance(into.get(key), dict) and isinstance(val, dict):
            merge(into[key], val)
        else:
            into[key] = val


def _lower_keys(data):
    return {
        k.lower(): _lower_keys(v) if isinstance(v, dict) else v
        for k, v in data.items()
    }
