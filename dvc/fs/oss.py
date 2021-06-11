import logging
import os
import threading

from funcy import cached_property, wrap_prop

from dvc.path_info import CloudURLInfo
from dvc.progress import Tqdm
from dvc.scheme import Schemes

from .base import BaseFileSystem

logger = logging.getLogger(__name__)


class OSSFileSystem(BaseFileSystem):  # pylint:disable=abstract-method
    """
    oss2 document:
    https://www.alibabacloud.com/help/doc-detail/32026.htm


    Examples
    ----------
    $ dvc remote add myremote oss://my-bucket/path
    Set key id, key secret and endpoint using modify command
    $ dvc remote modify myremote oss_key_id my-key-id
    $ dvc remote modify myremote oss_key_secret my-key-secret
    $ dvc remote modify myremote oss_endpoint endpoint
    or environment variables
    $ export OSS_ACCESS_KEY_ID="my-key-id"
    $ export OSS_ACCESS_KEY_SECRET="my-key-secret"
    $ export OSS_ENDPOINT="endpoint"
    """

    scheme = Schemes.OSS
    PATH_CLS = CloudURLInfo
    REQUIRES = {"oss2": "oss2"}
    PARAM_CHECKSUM = "etag"
    COPY_POLL_SECONDS = 5
    LIST_OBJECT_PAGE_SIZE = 100

    def __init__(self, **config):
        super().__init__(**config)

        self.endpoint = config.get("oss_endpoint") or os.getenv("OSS_ENDPOINT")

        self.key_id = (
            config.get("oss_key_id")
            or os.getenv("OSS_ACCESS_KEY_ID")
            or "defaultId"
        )

        self.key_secret = (
            config.get("oss_key_secret")
            or os.getenv("OSS_ACCESS_KEY_SECRET")
            or "defaultSecret"
        )

    @wrap_prop(threading.Lock())
    @cached_property
    def oss_service(self):
        import oss2

        logger.debug(f"key id: {self.key_id}")
        logger.debug(f"key secret: {self.key_secret}")

        return oss2.Auth(self.key_id, self.key_secret)

    def _get_bucket(self, bucket):
        import oss2

        return oss2.Bucket(self.oss_service, self.endpoint, bucket)

    def _generate_download_url(self, path_info, expires=3600):
        return self._get_bucket(path_info.bucket).sign_url(
            "GET", path_info.path, expires
        )

    def exists(self, path_info) -> bool:
        paths = self._list_paths(path_info)
        return any(path_info.path == path for path in paths)

    def _list_paths(self, path_info):
        import oss2

        for blob in oss2.ObjectIterator(
            self._get_bucket(path_info.bucket), prefix=path_info.path
        ):
            yield blob.key

    def walk_files(self, path_info, **kwargs):
        if not kwargs.pop("prefix", False):
            path_info = path_info / ""
        for fname in self._list_paths(path_info):
            if fname.endswith("/"):
                continue

            yield path_info.replace(path=fname)

    def remove(self, path_info):
        if path_info.scheme != self.scheme:
            raise NotImplementedError

        logger.debug(f"Removing oss://{path_info}")
        self._get_bucket(path_info.bucket).delete_object(path_info.path)

    def _upload_fobj(self, fobj, to_info):
        self._get_bucket(to_info.bucket).put_object(to_info.path, fobj)

    def _upload(
        self, from_file, to_info, name=None, no_progress_bar=False, **_kwargs
    ):
        with Tqdm(desc=name, disable=no_progress_bar, bytes=True) as pbar:
            bucket = self._get_bucket(to_info.bucket)
            bucket.put_object_from_file(
                to_info.path, from_file, progress_callback=pbar.update_to
            )

    def _download(
        self, from_info, to_file, name=None, no_progress_bar=False, **_kwargs
    ):
        with Tqdm(desc=name, disable=no_progress_bar, bytes=True) as pbar:
            import oss2

            bucket = self._get_bucket(from_info.bucket)
            oss2.resumable_download(
                bucket,
                from_info.path,
                to_file,
                progress_callback=pbar.update_to,
            )
