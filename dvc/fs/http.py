import logging
import os.path
import threading
from typing import Optional
from urllib.parse import urlparse

from funcy import cached_property, memoize, wrap_prop, wrap_with

from dvc import prompt
from dvc.exceptions import DvcException, HTTPError
from dvc.path_info import HTTPURLInfo
from dvc.progress import Tqdm
from dvc.scheme import Schemes

from .base import BaseFileSystem

logger = logging.getLogger(__name__)


@wrap_with(threading.Lock())
@memoize
def ask_password(host, user):
    return prompt.password(
        "Enter a password for "
        "host '{host}' user '{user}'".format(host=host, user=user)
    )


class HTTPFileSystem(BaseFileSystem):  # pylint:disable=abstract-method
    scheme = Schemes.HTTP
    PATH_CLS = HTTPURLInfo
    PARAM_CHECKSUM = "etag"
    CAN_TRAVERSE = False

    SESSION_RETRIES = 5
    SESSION_BACKOFF_FACTOR = 0.1
    REQUEST_TIMEOUT = 60
    CHUNK_SIZE = 2 ** 16

    def __init__(self, **config):
        super().__init__(**config)

        self.user = config.get("user", None)

        self.auth = config.get("auth", None)
        self.custom_auth_header = config.get("custom_auth_header", None)
        self.password = config.get("password", None)
        self.ask_password = config.get("ask_password", False)
        self.headers = {}
        self.ssl_verify = config.get("ssl_verify", True)
        self.method = config.get("method", "POST")
        self.chunked_upload = config.get("chunked_upload", True)

    def _auth_method(self, url):
        from requests.auth import HTTPBasicAuth, HTTPDigestAuth

        if self.auth:
            if self.ask_password and self.password is None:
                self.password = ask_password(urlparse(url).hostname, self.user)
            if self.auth == "basic":
                return HTTPBasicAuth(self.user, self.password)
            if self.auth == "digest":
                return HTTPDigestAuth(self.user, self.password)
            if self.auth == "custom" and self.custom_auth_header:
                self.headers.update({self.custom_auth_header: self.password})
        return None

    def _generate_download_url(self, path_info):
        return path_info.url

    @wrap_prop(threading.Lock())
    @cached_property
    def _session(self):
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        session = requests.Session()

        session.verify = self.ssl_verify

        retries = Retry(
            total=self.SESSION_RETRIES,
            backoff_factor=self.SESSION_BACKOFF_FACTOR,
        )

        session.mount("http://", HTTPAdapter(max_retries=retries))
        session.mount("https://", HTTPAdapter(max_retries=retries))

        return session

    def request(self, method, url, **kwargs):
        import requests

        kwargs.setdefault("allow_redirects", True)
        kwargs.setdefault("timeout", self.REQUEST_TIMEOUT)

        try:
            res = self._session.request(
                method,
                url,
                auth=self._auth_method(url),
                headers=self.headers,
                **kwargs,
            )

            redirect_no_location = (
                kwargs["allow_redirects"]
                and res.status_code in (301, 302)
                and "location" not in res.headers
            )

            if redirect_no_location:
                # AWS s3 doesn't like to add a location header to its redirects
                # from https://s3.amazonaws.com/<bucket name>/* type URLs.
                # This should be treated as an error
                raise requests.exceptions.RequestException

            return res

        except requests.exceptions.RequestException:
            raise DvcException(f"could not perform a {method} request")

    def _head(self, url):
        response = self.request("HEAD", url)
        if response.ok:
            return response

        # Sometimes servers are configured to forbid HEAD requests
        # Context: https://github.com/iterative/dvc/issues/4131
        with self.request("GET", url, stream=True) as r:
            if r.ok or r.status_code == 404:
                return r

        return response

    def exists(self, path_info) -> bool:
        res = self._head(path_info.url)
        if res.status_code == 404:
            return False
        if bool(res):
            return True
        raise HTTPError(res.status_code, res.reason)

    def info(self, path_info):
        resp = self._head(path_info.url)
        etag = resp.headers.get("ETag") or resp.headers.get("Content-MD5")
        size = self._content_length(resp)
        return {"etag": etag, "size": size}

    def _upload_fobj_chunked(self, fobj, to_info):
        def chunks(fobj):
            while True:
                chunk = fobj.read(self.CHUNK_SIZE)
                if not chunk:
                    break
                yield chunk

        response = self.request(self.method, to_info.url, data=chunks(fobj))
        return response

    def _upload_fobj_not_chunked(self, fobj, to_info):
        return self.request(self.method, to_info.url, files={"file": fobj})

    def _upload_fobj(self, fobj, to_info):
        if self.chunked_upload:
            response = self._upload_fobj_chunked(fobj, to_info)
        else:
            response = self._upload_fobj_not_chunked(fobj, to_info)
        if response.status_code not in (200, 201):
            raise HTTPError(response.status_code, response.reason)

    def _download(self, from_info, to_file, name=None, no_progress_bar=False):
        response = self.request("GET", from_info.url, stream=True)
        if response.status_code != 200:
            raise HTTPError(response.status_code, response.reason)
        with open(to_file, "wb") as fd:
            with Tqdm.wrapattr(
                fd,
                "write",
                total=None
                if no_progress_bar
                else self._content_length(response),
                leave=False,
                desc=from_info.url if name is None else name,
                disable=no_progress_bar,
            ) as fd_wrapped:
                for chunk in response.iter_content(chunk_size=self.CHUNK_SIZE):
                    fd_wrapped.write(chunk)

    def _upload(
        self, from_file, to_info, name=None, no_progress_bar=False, **_kwargs
    ):
        with open(from_file, "rb") as fobj:
            self.upload_fobj(
                fobj,
                to_info,
                no_progress_bar=no_progress_bar,
                desc=name or to_info.url,
                total=None if no_progress_bar else os.path.getsize(from_file),
            )

    @staticmethod
    def _content_length(response) -> Optional[int]:
        res = response.headers.get("Content-Length")
        return int(res) if res else None
