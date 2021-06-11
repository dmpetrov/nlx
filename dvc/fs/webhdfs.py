import logging
import os
import posixpath
import shutil
import threading
from contextlib import contextmanager

from funcy import cached_property, wrap_prop

from dvc.hash_info import HashInfo
from dvc.path_info import CloudURLInfo
from dvc.progress import Tqdm
from dvc.scheme import Schemes

from .base import BaseFileSystem

logger = logging.getLogger(__name__)


def update_pbar(pbar, total):
    """Update pbar to accept the two arguments passed by hdfs"""

    def update(_, bytes_transfered):
        if bytes_transfered == -1:
            pbar.update_to(total)
            return
        pbar.update_to(bytes_transfered)

    return update


class WebHDFSFileSystem(BaseFileSystem):  # pylint:disable=abstract-method
    scheme = Schemes.WEBHDFS
    PATH_CLS = CloudURLInfo
    REQUIRES = {"hdfs": "hdfs"}
    PARAM_CHECKSUM = "checksum"
    TRAVERSE_PREFIX_LEN = 2

    def __init__(self, **config):
        super().__init__(**config)

        self.host = config["host"]
        self.user = config.get("user")
        self.port = config.get("port")

        self.hdfscli_config = config.get("hdfscli_config")
        self.token = config.get("webhdfs_token")
        self.alias = config.get("webhdfs_alias")

    @staticmethod
    def _get_kwargs_from_urls(urlpath):
        from fsspec.implementations.webhdfs import WebHDFS

        return (
            WebHDFS._get_kwargs_from_urls(  # pylint:disable=protected-access
                urlpath
            )
        )

    @wrap_prop(threading.Lock())
    @cached_property
    def hdfs_client(self):
        import hdfs

        logger.debug("HDFSConfig: %s", self.hdfscli_config)

        try:
            return hdfs.config.Config(self.hdfscli_config).get_client(
                self.alias
            )
        except hdfs.util.HdfsError as exc:
            exc_msg = str(exc)
            errors = (
                "No alias specified",
                "Invalid configuration file",
                f"Alias {self.alias} not found",
            )
            if not any(err in exc_msg for err in errors):
                raise

            http_url = f"http://{self.host}:{self.port}"
            logger.debug("URL: %s", http_url)

            if self.token is not None:
                client = hdfs.TokenClient(http_url, token=self.token, root="/")
            else:
                client = hdfs.InsecureClient(
                    http_url, user=self.user, root="/"
                )

        return client

    @contextmanager
    def open(self, path_info, mode="r", encoding=None, **kwargs):
        assert mode in {"r", "rt", "rb"}

        with self.hdfs_client.read(
            path_info.path, encoding=encoding
        ) as reader:
            yield reader

    def walk_files(self, path_info, **kwargs):
        if not self.exists(path_info):
            return

        root = path_info.path
        for path, _, fnames in self.hdfs_client.walk(root):
            for fname in fnames:
                yield path_info.replace(path=posixpath.join(path, fname))

    def remove(self, path_info):
        if path_info.scheme != self.scheme:
            raise NotImplementedError

        self.hdfs_client.delete(path_info.path)

    def exists(self, path_info) -> bool:
        assert not isinstance(path_info, list)
        assert path_info.scheme == "webhdfs"

        status = self.hdfs_client.status(path_info.path, strict=False)
        return status is not None

    def info(self, path_info):
        st = self.hdfs_client.status(path_info.path)
        return {"size": st["length"]}

    def checksum(self, path_info):
        return HashInfo(
            "checksum",
            self.hdfs_client.checksum(path_info.path)["bytes"],
            size=self.hdfs_client.status(path_info.path)["length"],
        )

    def copy(self, from_info, to_info, **_kwargs):
        with self.hdfs_client.read(from_info.path) as reader:
            with self.hdfs_client.write(to_info.path) as writer:
                shutil.copyfileobj(reader, writer)

    def move(self, from_info, to_info):
        self.hdfs_client.makedirs(to_info.parent.path)
        self.hdfs_client.rename(from_info.path, to_info.path)

    def _upload_fobj(self, fobj, to_info):
        with self.hdfs_client.write(to_info.path) as fdest:
            shutil.copyfileobj(fobj, fdest)

    def _upload(
        self, from_file, to_info, name=None, no_progress_bar=False, **_kwargs
    ):
        total = os.path.getsize(from_file)
        with Tqdm(
            desc=name, total=total, disable=no_progress_bar, bytes=True
        ) as pbar:
            self.hdfs_client.upload(
                to_info.path,
                from_file,
                overwrite=True,
                progress=update_pbar(pbar, total),
            )

    def _download(
        self, from_info, to_file, name=None, no_progress_bar=False, **_kwargs
    ):
        total = self.getsize(from_info)
        with Tqdm(
            desc=name, total=total, disable=no_progress_bar, bytes=True
        ) as pbar:
            self.hdfs_client.download(
                from_info.path, to_file, progress=update_pbar(pbar, total)
            )
