import logging
import os
import threading

from funcy import cached_property, wrap_prop

from dvc.path_info import CloudURLInfo
from dvc.progress import Tqdm
from dvc.scheme import Schemes

from .fsspec_wrapper import ObjectFSWrapper

logger = logging.getLogger(__name__)


# pylint:disable=abstract-method
class OSSFileSystem(ObjectFSWrapper):
    scheme = Schemes.OSS
    PATH_CLS = CloudURLInfo
    REQUIRES = {"ossfs": "ossfs"}
    PARAM_CHECKSUM = "etag"
    COPY_POLL_SECONDS = 5
    LIST_OBJECT_PAGE_SIZE = 100
    DETAIL_FIELDS = frozenset(("etag", "size"))

    def _prepare_credentials(self, **config):
        login_info = {}
        login_info["key"] = config.get("oss_key_id") or os.getenv(
            "OSS_ACCESS_KEY_ID"
        )
        login_info["secret"] = config.get("oss_key_secret") or os.getenv(
            "OSS_ACCESS_KEY_SECRET"
        )
        login_info["endpoint"] = config.get("oss_endpoint")
        return login_info

    @wrap_prop(threading.Lock())
    @cached_property
    def fs(self):
        from ossfs import OSSFileSystem as _OSSFileSystem

        return _OSSFileSystem(**self.fs_args)

    def remove(self, path_info):
        self.fs.rm_file(self._with_bucket(path_info))

    def _upload(
        self, from_file, to_info, name=None, no_progress_bar=False, **kwargs
    ):
        total = os.path.getsize(from_file)
        with Tqdm(
            disable=no_progress_bar,
            total=total,
            bytes=True,
            desc=name,
            **kwargs,
        ) as pbar:
            self.fs.put_file(
                from_file,
                self._with_bucket(to_info),
                progress_callback=pbar.update_to,
            )
        self.fs.invalidate_cache(self._with_bucket(to_info.parent))

    def _download(
        self, from_info, to_file, name=None, no_progress_bar=False, **pbar_args
    ):
        total = self.fs.size(self._with_bucket(from_info))
        with Tqdm(
            disable=no_progress_bar,
            total=total,
            bytes=True,
            desc=name,
            **pbar_args,
        ) as pbar:
            self.fs.get_file(
                self._with_bucket(from_info),
                to_file,
                progress_callback=pbar.update_to,
            )
