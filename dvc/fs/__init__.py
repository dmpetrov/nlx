import posixpath
from urllib.parse import urlparse

from ..scheme import Schemes
from .azure import AzureFileSystem
from .gdrive import GDriveFileSystem
from .gs import GSFileSystem
from .hdfs import HDFSFileSystem
from .http import HTTPFileSystem
from .https import HTTPSFileSystem
from .local import LocalFileSystem
from .oss import OSSFileSystem
from .s3 import S3FileSystem
from .ssh import SSHFileSystem
from .webdav import WebDAVFileSystem
from .webdavs import WebDAVSFileSystem
from .webhdfs import WebHDFSFileSystem

FS_MAP = {
    Schemes.AZURE: AzureFileSystem,
    Schemes.GDRIVE: GDriveFileSystem,
    Schemes.GS: GSFileSystem,
    Schemes.HDFS: HDFSFileSystem,
    Schemes.WEBHDFS: WebHDFSFileSystem,
    Schemes.HTTP: HTTPFileSystem,
    Schemes.HTTPS: HTTPSFileSystem,
    Schemes.S3: S3FileSystem,
    Schemes.SSH: SSHFileSystem,
    Schemes.OSS: OSSFileSystem,
    Schemes.WEBDAV: WebDAVFileSystem,
    Schemes.WEBDAVS: WebDAVSFileSystem,
    # NOTE: LocalFileSystem is the default
}


def get_fs_cls(remote_conf):
    scheme = urlparse(remote_conf["url"]).scheme
    return FS_MAP.get(scheme, LocalFileSystem)


def get_fs_config(config, **kwargs):
    name = kwargs.get("name")
    if name:
        remote_conf = config["remote"][name.lower()]
    else:
        remote_conf = kwargs
    return _resolve_remote_refs(config, remote_conf)


def _resolve_remote_refs(config, remote_conf):
    # Support for cross referenced remotes.
    # This will merge the settings, shadowing base ref with remote_conf.
    # For example, having:
    #
    #       dvc remote add server ssh://localhost
    #       dvc remote modify server user root
    #       dvc remote modify server ask_password true
    #
    #       dvc remote add images remote://server/tmp/pictures
    #       dvc remote modify images user alice
    #       dvc remote modify images ask_password false
    #       dvc remote modify images password asdf1234
    #
    # Results on a config dictionary like:
    #
    #       {
    #           "url": "ssh://localhost/tmp/pictures",
    #           "user": "alice",
    #           "password": "asdf1234",
    #           "ask_password": False,
    #       }
    parsed = urlparse(remote_conf["url"])
    if parsed.scheme != "remote":
        return remote_conf

    base = get_fs_config(config, name=parsed.netloc)
    url = posixpath.join(base["url"], parsed.path.lstrip("/"))
    return {**base, **remote_conf, "url": url}


def get_cloud_fs(repo, **kwargs):
    from dvc.config import ConfigError
    from dvc.config_schema import SCHEMA, Invalid

    repo_config = repo.config if repo else {}
    core_config = repo_config.get("core", {})

    remote_conf = get_fs_config(repo_config, **kwargs)
    try:
        remote_conf = SCHEMA["remote"][str](remote_conf)
    except Invalid as exc:
        raise ConfigError(str(exc)) from None

    if "jobs" not in remote_conf:
        jobs = core_config.get("jobs")
        if jobs:
            remote_conf["jobs"] = jobs

    if "checksum_jobs" not in remote_conf:
        checksum_jobs = core_config.get("checksum_jobs")
        if checksum_jobs:
            remote_conf["checksum_jobs"] = checksum_jobs

    cls = get_fs_cls(remote_conf)

    if cls == GDriveFileSystem and repo:
        remote_conf["gdrive_credentials_tmp_dir"] = repo.tmp_dir

    url = remote_conf.pop("url")
    path_info = cls.PATH_CLS(
        cls._strip_protocol(url)  # pylint:disable=protected-access
    )

    extras = cls._get_kwargs_from_urls(url)  # pylint:disable=protected-access

    return cls, dict(**extras, **remote_conf), path_info
