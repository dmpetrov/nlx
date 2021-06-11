import hashlib
import os
import sys
import threading
from contextlib import contextmanager
from http import HTTPStatus
from http.server import HTTPServer

from RangeHTTPServer import RangeRequestHandler


class TestRequestHandler(RangeRequestHandler):
    def __init__(self, *args, **kwargs):
        # NOTE: `directory` was introduced in 3.7
        if sys.version_info < (3, 7):
            self.directory = kwargs.pop("directory", None) or os.getcwd()
        super().__init__(*args, **kwargs)

    def translate_path(self, path):
        import posixpath
        import urllib

        # NOTE: `directory` was introduced in 3.7
        if sys.version_info >= (3, 7):
            return super().translate_path(path)

        path = path.split("?", 1)[0]
        path = path.split("#", 1)[0]
        # Don't forget explicit trailing slash when normalizing. Issue17324
        trailing_slash = path.rstrip().endswith("/")
        try:
            path = urllib.parse.unquote(path, errors="surrogatepass")
        except UnicodeDecodeError:
            path = urllib.parse.unquote(path)
        path = posixpath.normpath(path)
        words = path.split("/")
        words = filter(None, words)
        path = self.directory
        for word in words:
            if os.path.dirname(word) or word in (os.curdir, os.pardir):
                # Ignore components that are not a simple file/directory name
                continue
            path = os.path.join(path, word)
        if trailing_slash:
            path += "/"
        return path

    def end_headers(self):
        # RangeRequestHandler only sends Accept-Ranges header if Range header
        # is present, see https://github.com/danvk/RangeHTTPServer/issues/23
        if not self.headers.get("Range"):
            self.send_header("Accept-Ranges", "bytes")

        # Add a checksum header
        file = self.translate_path(self.path)

        if not os.path.isdir(file) and os.path.exists(file):
            with open(file) as fd:
                encoded_text = fd.read().encode("utf8")
                checksum = hashlib.md5(encoded_text).hexdigest()
                self.send_header("Content-MD5", checksum)

        RangeRequestHandler.end_headers(self)

    def _chunks(self):
        while True:
            data = self.rfile.readline(65537)
            chunk_size = int(data[:-2], 16)
            if chunk_size == 0:
                return
            data = self.rfile.read(chunk_size)
            yield data
            self.rfile.read(2)

    def do_POST(self):
        chunked = self.headers.get("Transfer-Encoding", "") == "chunked"
        path = self.translate_path(self.path)
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as fd:
                if chunked:
                    for chunk in self._chunks():
                        fd.write(chunk)
                else:
                    size = int(self.headers.get("Content-Length", 0))
                    fd.write(self.rfile.read(size))
        except OSError as e:
            self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, str(e))
        self.send_response(HTTPStatus.OK)
        self.end_headers()


@contextmanager
def run_server_on_thread(server):
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()

    yield server

    server.socket.close()
    server.shutdown()
    server.server_close()


class StaticFileServer:
    _lock = threading.Lock()

    def __init__(self, directory):
        from functools import partial

        addr = ("localhost", 0)
        req = partial(TestRequestHandler, directory=directory)
        server = HTTPServer(addr, req)
        self.runner = run_server_on_thread(server)

    # pylint: disable=no-member
    def __enter__(self):
        self._lock.acquire()
        return self.runner.__enter__()

    def __exit__(self, *args):
        self.runner.__exit__(*args)
        self._lock.release()
