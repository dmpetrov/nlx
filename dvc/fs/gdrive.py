import io
import logging
import os
import posixpath
import re
import threading
from collections import defaultdict
from contextlib import contextmanager
from urllib.parse import urlparse

from funcy import cached_property, retry, wrap_prop, wrap_with
from funcy.py3 import cat

from dvc.exceptions import DvcException, FileMissingError
from dvc.path_info import CloudURLInfo
from dvc.progress import Tqdm
from dvc.scheme import Schemes
from dvc.utils import format_link, tmp_fname
from dvc.utils.stream import IterStream

from .base import BaseFileSystem

logger = logging.getLogger(__name__)
FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"


class GDriveAuthError(DvcException):
    def __init__(self, cred_location):

        if cred_location:
            message = (
                "GDrive remote auth failed with credentials in '{}'.\n"
                "Backup first, remove or fix them, and run DVC again.\n"
                "It should do auth again and refresh the credentials.\n\n"
                "Details:".format(cred_location)
            )
        else:
            message = "Failed to authenticate GDrive remote"

        super().__init__(message)


def _gdrive_retry(func):
    def should_retry(exc):
        from pydrive2.files import ApiRequestError

        if not isinstance(exc, ApiRequestError):
            return False

        error_code = exc.error.get("code", 0)
        result = False
        if 500 <= error_code < 600:
            result = True

        if error_code == 403:
            result = exc.GetField("reason") in [
                "userRateLimitExceeded",
                "rateLimitExceeded",
            ]
        if result:
            logger.debug(f"Retrying GDrive API call, error: {exc}.")

        return result

    # 16 tries, start at 0.5s, multiply by golden ratio, cap at 20s
    return retry(
        16,
        timeout=lambda a: min(0.5 * 1.618 ** a, 20),
        filter_errors=should_retry,
    )(func)


class GDriveURLInfo(CloudURLInfo):
    def __init__(self, url):
        super().__init__(url)

        # GDrive URL host part is case sensitive,
        # we are restoring it here.
        p = urlparse(url)
        self.host = p.netloc
        assert self.netloc == self.host

        # Normalize path. Important since we have a cache (path to ID)
        # and don't want to deal with different variations of path in it.
        self._spath = re.sub("/{2,}", "/", self._spath.rstrip("/"))


class GDriveFileSystem(BaseFileSystem):  # pylint:disable=abstract-method
    scheme = Schemes.GDRIVE
    PATH_CLS = GDriveURLInfo
    PARAM_CHECKSUM = "checksum"
    REQUIRES = {"pydrive2": "pydrive2"}
    # Always prefer traverse for GDrive since API usage quotas are a concern.
    TRAVERSE_WEIGHT_MULTIPLIER = 1
    TRAVERSE_PREFIX_LEN = 2

    GDRIVE_CREDENTIALS_DATA = "GDRIVE_CREDENTIALS_DATA"
    DEFAULT_USER_CREDENTIALS_FILE = "gdrive-user-credentials.json"

    DEFAULT_GDRIVE_CLIENT_ID = "710796635688-iivsgbgsb6uv1fap6635dhvuei09o66c.apps.googleusercontent.com"  # noqa: E501
    DEFAULT_GDRIVE_CLIENT_SECRET = "a1Fz59uTpVNeG_VGuSKDLJXv"

    def __init__(self, **config):
        super().__init__(**config)

        self.path_info = self.PATH_CLS(config["url"])

        if not self.path_info.bucket:
            raise DvcException(
                "Empty GDrive URL '{}'. Learn more at {}".format(
                    config["url"],
                    format_link("https://man.dvc.org/remote/add"),
                )
            )

        self._bucket = self.path_info.bucket
        self._path = self.path_info.path
        self._trash_only = config.get("gdrive_trash_only")
        self._use_service_account = config.get("gdrive_use_service_account")
        self._service_account_user_email = config.get(
            "gdrive_service_account_user_email"
        )
        self._service_account_json_file_path = config.get(
            "gdrive_service_account_json_file_path"
        )
        self._client_id = config.get("gdrive_client_id")
        self._client_secret = config.get("gdrive_client_secret")
        self._validate_config()

        tmp_dir = config["gdrive_credentials_tmp_dir"]
        assert tmp_dir

        self._gdrive_service_credentials_path = tmp_fname(
            os.path.join(tmp_dir, "")
        )
        self._gdrive_user_credentials_path = (
            tmp_fname(os.path.join(tmp_dir, ""))
            if os.getenv(GDriveFileSystem.GDRIVE_CREDENTIALS_DATA)
            else config.get(
                "gdrive_user_credentials_file",
                os.path.join(tmp_dir, self.DEFAULT_USER_CREDENTIALS_FILE),
            )
        )

    @staticmethod
    def _get_kwargs_from_urls(urlpath):
        return {"url": urlpath}

    def _validate_config(self):
        # Validate Service Account configuration
        if (
            self._use_service_account
            and not self._service_account_json_file_path
        ):
            raise DvcException(
                "To use service account, set "
                "`gdrive_service_account_json_file_path`, and optionally"
                "`gdrive_service_account_user_email` in DVC config\n"
                "Learn more at {}".format(
                    format_link("https://man.dvc.org/remote/modify")
                )
            )

        # Validate OAuth 2.0 Client ID configuration
        if not self._use_service_account:
            if bool(self._client_id) != bool(self._client_secret):
                raise DvcException(
                    "Please specify GDrive's client ID and secret in "
                    "DVC config or omit both to use the defaults.\n"
                    "Learn more at {}".format(
                        format_link("https://man.dvc.org/remote/modify")
                    )
                )

    @cached_property
    def credentials_location(self):
        """
        Helper to determine where will GDrive remote read credentials from.
        Useful for tests, exception messages, etc. Returns either env variable
        name if it's set or actual path to the credentials file.
        """
        if os.getenv(GDriveFileSystem.GDRIVE_CREDENTIALS_DATA):
            return GDriveFileSystem.GDRIVE_CREDENTIALS_DATA
        if os.path.exists(self._gdrive_user_credentials_path):
            return self._gdrive_user_credentials_path
        return None

    @staticmethod
    def _validate_credentials(auth, settings):
        """
        Detects discrepancy in DVC config and cached credentials file.
        Usually happens when a second remote is added and it is using
        the same credentials default file. Or when someones decides to change
        DVC config client id or secret but forgets to remove the cached
        credentials file.
        """
        if not os.getenv(GDriveFileSystem.GDRIVE_CREDENTIALS_DATA):
            if (
                settings["client_config"]["client_id"]
                != auth.credentials.client_id
                or settings["client_config"]["client_secret"]
                != auth.credentials.client_secret
            ):
                logger.warning(
                    "Client ID and secret configured do not match the "
                    "actual ones used\nto access the remote. Do you "
                    "use multiple GDrive remotes and forgot to\nset "
                    "`gdrive_user_credentials_file` for one or more of them? "
                    "Learn more at\n{}.\n".format(
                        format_link("https://man.dvc.org/remote/modify")
                    )
                )

    @wrap_prop(threading.RLock())
    @cached_property
    def _drive(self):
        from pydrive2.auth import GoogleAuth
        from pydrive2.drive import GoogleDrive

        temporary_save_path = self._gdrive_user_credentials_path
        is_credentials_temp = os.getenv(
            GDriveFileSystem.GDRIVE_CREDENTIALS_DATA
        )
        if self._use_service_account:
            temporary_save_path = self._gdrive_service_credentials_path

        if is_credentials_temp:
            with open(temporary_save_path, "w") as cred_file:
                cred_file.write(
                    os.getenv(GDriveFileSystem.GDRIVE_CREDENTIALS_DATA)
                )

        auth_settings = {
            "client_config_backend": "settings",
            "save_credentials": True,
            "save_credentials_backend": "file",
            "save_credentials_file": self._gdrive_user_credentials_path,
            "get_refresh_token": True,
            "oauth_scope": [
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/drive.appdata",
            ],
        }

        if self._use_service_account:
            auth_settings["service_config"] = {
                "client_user_email": self._service_account_user_email,
                "client_json_file_path": self._service_account_json_file_path,
            }
            if is_credentials_temp:
                auth_settings["service_config"][
                    "client_json_file_path"
                ] = temporary_save_path

        else:
            auth_settings["client_config"] = {
                "client_id": self._client_id or self.DEFAULT_GDRIVE_CLIENT_ID,
                "client_secret": self._client_secret
                or self.DEFAULT_GDRIVE_CLIENT_SECRET,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "revoke_uri": "https://oauth2.googleapis.com/revoke",
                "redirect_uri": "",
            }

        GoogleAuth.DEFAULT_SETTINGS.update(auth_settings)

        # Pass non existent settings path to force DEFAULT_SETTINGS loadings
        gauth = GoogleAuth(settings_file="")

        try:
            logger.debug(
                "GDrive remote auth with config '{}'.".format(
                    GoogleAuth.DEFAULT_SETTINGS
                )
            )
            if self._use_service_account:
                gauth.ServiceAuth()
            else:
                gauth.CommandLineAuth()
                GDriveFileSystem._validate_credentials(gauth, auth_settings)

        # Handle AuthenticationError, RefreshError and other auth failures
        # It's hard to come up with a narrow exception, since PyDrive throws
        # a lot of different errors - broken credentials file, refresh token
        # expired, flow failed, etc.
        except Exception as exc:
            raise GDriveAuthError(self.credentials_location) from exc
        finally:
            if is_credentials_temp:
                os.remove(temporary_save_path)

        return GoogleDrive(gauth)

    @wrap_prop(threading.RLock())
    @cached_property
    def _ids_cache(self):
        cache = {
            "dirs": defaultdict(list),
            "ids": {},
            "root_id": self._get_item_id(
                self.path_info,
                use_cache=False,
                hint="Confirm the directory exists and you can access it.",
            ),
        }

        self._cache_path_id(self._path, cache["root_id"], cache=cache)

        for item in self._gdrive_list(
            "'{}' in parents and trashed=false".format(cache["root_id"])
        ):
            item_path = (self.path_info / item["title"]).path
            self._cache_path_id(item_path, item["id"], cache=cache)

        return cache

    def _cache_path_id(self, path, *item_ids, cache=None):
        cache = cache or self._ids_cache
        for item_id in item_ids:
            cache["dirs"][path].append(item_id)
            cache["ids"][item_id] = path

    @cached_property
    def _list_params(self):
        params = {"corpora": "default"}
        if self._bucket != "root" and self._bucket != "appDataFolder":
            drive_id = self._gdrive_shared_drive_id(self._bucket)
            if drive_id:
                logger.debug(
                    "GDrive remote '{}' is using shared drive id '{}'.".format(
                        self.path_info, drive_id
                    )
                )
                params["driveId"] = drive_id
                params["corpora"] = "drive"
        return params

    @_gdrive_retry
    def _gdrive_shared_drive_id(self, item_id):
        from pydrive2.files import ApiRequestError

        param = {"id": item_id}
        # it does not create a file on the remote
        item = self._drive.CreateFile(param)
        # ID of the shared drive the item resides in.
        # Only populated for items in shared drives.
        try:
            item.FetchMetadata("driveId")
        except ApiRequestError as exc:
            error_code = exc.error.get("code", 0)
            if error_code == 404:
                raise DvcException(
                    "'{}' for '{}':\n\n"
                    "1. Confirm the directory exists and you can access it.\n"
                    "2. Make sure that credentials in '{}'\n"
                    "   are correct for this remote e.g. "
                    "use the `gdrive_user_credentials_file` config\n"
                    "   option if you use multiple GDrive remotes with "
                    "different email accounts.\n\nDetails".format(
                        item_id, self.path_info, self.credentials_location
                    )
                ) from exc
            raise

        return item.get("driveId", None)

    @_gdrive_retry
    def _gdrive_upload_fobj(self, fobj, parent_id, title):
        item = self._drive.CreateFile(
            {"title": title, "parents": [{"id": parent_id}]}
        )
        item.content = fobj
        item.Upload()
        return item

    @_gdrive_retry
    def _gdrive_download_file(
        self, item_id, to_file, progress_desc, no_progress_bar
    ):
        param = {"id": item_id}
        # it does not create a file on the remote
        gdrive_file = self._drive.CreateFile(param)

        with Tqdm(
            desc=progress_desc,
            disable=no_progress_bar,
            bytes=True,
            # explicit `bar_format` as `total` will be set by `update_to`
            bar_format=Tqdm.BAR_FMT_DEFAULT,
        ) as pbar:
            gdrive_file.GetContentFile(to_file, callback=pbar.update_to)

    @contextmanager
    @_gdrive_retry
    def open(self, path_info, mode="r", encoding=None, **kwargs):
        assert mode in {"r", "rt", "rb"}

        item_id = self._get_item_id(path_info)
        param = {"id": item_id}
        # it does not create a file on the remote
        gdrive_file = self._drive.CreateFile(param)
        fd = gdrive_file.GetContentIOBuffer()
        stream = IterStream(iter(fd))

        if mode != "rb":
            stream = io.TextIOWrapper(stream, encoding=encoding)

        yield stream

    @_gdrive_retry
    def gdrive_delete_file(self, item_id):
        from pydrive2.files import ApiRequestError

        param = {"id": item_id}
        # it does not create a file on the remote
        item = self._drive.CreateFile(param)

        try:
            item.Trash() if self._trash_only else item.Delete()
        except ApiRequestError as exc:
            http_error_code = exc.error.get("code", 0)
            if (
                http_error_code == 403
                and self._list_params["corpora"] == "drive"
                and exc.GetField("location") == "file.permissions"
            ):
                raise DvcException(
                    "Insufficient permissions to {}. You should have {} "
                    "access level for the used shared drive. More details "
                    "at {}.".format(
                        "move the file into Trash"
                        if self._trash_only
                        else "permanently delete the file",
                        "Manager or Content Manager"
                        if self._trash_only
                        else "Manager",
                        "https://support.google.com/a/answer/7337554",
                    )
                ) from exc
            raise

    def _gdrive_list(self, query):
        param = {"q": query, "maxResults": 1000}
        param.update(self._list_params)
        file_list = self._drive.ListFile(param)

        # Isolate and decorate fetching of remote drive items in pages.
        get_list = _gdrive_retry(lambda: next(file_list, None))

        # Fetch pages until None is received, lazily flatten the thing.
        return cat(iter(get_list, None))

    @_gdrive_retry
    def _gdrive_create_dir(self, parent_id, title):
        parent = {"id": parent_id}
        item = self._drive.CreateFile(
            {"title": title, "parents": [parent], "mimeType": FOLDER_MIME_TYPE}
        )
        item.Upload()
        return item

    @wrap_with(threading.RLock())
    def _create_dir(self, parent_id, title, remote_path):
        cached = self._ids_cache["dirs"].get(remote_path)
        if cached:
            return cached[0]

        item = self._gdrive_create_dir(parent_id, title)

        if parent_id == self._ids_cache["root_id"]:
            self._cache_path_id(remote_path, item["id"])

        return item["id"]

    def _get_remote_item_ids(self, parent_ids, title):
        if not parent_ids:
            return None
        query = "trashed=false and ({})".format(
            " or ".join(
                f"'{parent_id}' in parents" for parent_id in parent_ids
            )
        )
        query += " and title='{}'".format(title.replace("'", "\\'"))

        # GDrive list API is case insensitive, we need to compare
        # all results and pick the ones with the right title
        return [
            item["id"]
            for item in self._gdrive_list(query)
            if item["title"] == title
        ]

    def _get_cached_item_ids(self, path, use_cache):
        if not path:
            return [self._bucket]
        if use_cache:
            return self._ids_cache["dirs"].get(path, [])
        return []

    def _path_to_item_ids(self, path, create=False, use_cache=True):
        item_ids = self._get_cached_item_ids(path, use_cache)
        if item_ids:
            return item_ids

        parent_path, title = posixpath.split(path)
        parent_ids = self._path_to_item_ids(parent_path, create, use_cache)
        item_ids = self._get_remote_item_ids(parent_ids, title)
        if item_ids:
            return item_ids

        return (
            [self._create_dir(min(parent_ids), title, path)] if create else []
        )

    def _get_item_id(self, path_info, create=False, use_cache=True, hint=None):
        assert path_info.bucket == self._bucket

        item_ids = self._path_to_item_ids(path_info.path, create, use_cache)
        if item_ids:
            return min(item_ids)

        assert not create
        raise FileMissingError(path_info, hint)

    def exists(self, path_info) -> bool:
        try:
            self._get_item_id(path_info)
        except FileMissingError:
            return False
        else:
            return True

    def _gdrive_list_ids(self, query_ids):
        query = " or ".join(
            f"'{query_id}' in parents" for query_id in query_ids
        )
        query = f"({query}) and trashed=false"
        return self._gdrive_list(query)

    def find(self, path_info, detail=False):
        root_path = path_info.path
        seen_paths = set()

        dir_ids = [self._ids_cache["ids"].copy()]
        while dir_ids:
            query_ids = {
                dir_id: dir_name
                for dir_id, dir_name in dir_ids.pop().items()
                if posixpath.commonpath([root_path, dir_name]) == root_path
                if dir_id not in seen_paths
            }
            if not query_ids:
                continue

            seen_paths |= query_ids.keys()

            new_query_ids = {}
            dir_ids.append(new_query_ids)
            for item in self._gdrive_list_ids(query_ids):
                parent_id = item["parents"][0]["id"]
                item_path = posixpath.join(query_ids[parent_id], item["title"])
                if item["mimeType"] == FOLDER_MIME_TYPE:
                    new_query_ids[item["id"]] = item_path
                    self._cache_path_id(item_path, item["id"])
                    continue

                if detail:
                    yield {
                        "type": "file",
                        "name": item_path,
                        "size": item["fileSize"],
                        "checksum": item["md5Checksum"],
                    }
                else:
                    yield item_path

    def ls(self, path_info, detail=False):
        cached = path_info.path in self._ids_cache["dirs"]
        if cached:
            dir_ids = self._ids_cache["dirs"][path_info.path]
        else:
            dir_ids = self._path_to_item_ids(path_info.path)

        if not dir_ids:
            return None

        root_path = path_info.path
        for item in self._gdrive_list_ids(dir_ids):
            item_path = posixpath.join(root_path, item["title"])
            if detail:
                if item["mimeType"] == FOLDER_MIME_TYPE:
                    yield {"type": "directory", "name": item_path}
                else:
                    yield {
                        "type": "file",
                        "name": item_path,
                        "size": item["fileSize"],
                        "checksum": item["md5Checksum"],
                    }
            else:
                yield item_path

        if not cached:
            self._cache_path_id(root_path, *dir_ids)

    def walk_files(self, path_info, **kwargs):
        for file in self.find(path_info):
            yield path_info.replace(path=file)

    def remove(self, path_info):
        item_id = self._get_item_id(path_info)
        self.gdrive_delete_file(item_id)

    def info(self, path_info):
        item_id = self._get_item_id(path_info)
        gdrive_file = self._drive.CreateFile({"id": item_id})
        gdrive_file.FetchMetadata(fields="fileSize")
        return {"size": gdrive_file.get("fileSize")}

    def _upload_fobj(self, fobj, to_info):
        dirname = to_info.parent
        assert dirname
        parent_id = self._get_item_id(dirname, create=True)
        self._gdrive_upload_fobj(fobj, parent_id, to_info.name)

    def _upload(
        self, from_file, to_info, name=None, no_progress_bar=False, **_kwargs
    ):
        with open(from_file, "rb") as fobj:
            self.upload_fobj(
                fobj,
                to_info,
                no_progress_bar=no_progress_bar,
                desc=name or to_info.name,
                total=os.path.getsize(from_file),
            )

    def _download(self, from_info, to_file, name=None, no_progress_bar=False):
        item_id = self._get_item_id(from_info)
        self._gdrive_download_file(item_id, to_file, name, no_progress_bar)
