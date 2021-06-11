import io
from contextlib import contextmanager

from dvc.utils.stream import IterStream


@contextmanager
def open_url(url, mode="r", encoding=None, **iter_opts):
    """Opens an url as a readable stream.

    Resumes on connection error.
    Url could be a string or a callable returning a string.
    """
    assert mode in {"r", "rt", "rb"}

    with iter_url(url, **iter_opts) as (response, it):
        bytes_stream = IterStream(it)

        if mode == "rb":
            yield bytes_stream
        else:
            encoding = encoding or response.encoding
            yield io.TextIOWrapper(bytes_stream, encoding=encoding)


@contextmanager
def iter_url(url, chunk_size=io.DEFAULT_BUFFER_SIZE):
    """Iterate over chunks requested from url."""
    import requests

    def request(headers=None):
        the_url = url() if callable(url) else url
        response = requests.get(the_url, stream=True, headers=headers)
        if response.status_code == 404:
            raise FileNotFoundError(f"Can't open {the_url}")
        response.raise_for_status()
        return response

    def gen(response):
        try:
            pos = 0
            while True:
                try:
                    for chunk in response.iter_content(chunk_size):
                        pos += len(chunk)
                        yield chunk
                    break
                except requests.ConnectionError:
                    response.close()
                    if response.headers.get("Accept-Ranges") != "bytes":
                        raise

                    # Reopen request from where we stopped
                    headers = {"Range": f"bytes={pos}-"}
                    response = request(headers)
        finally:
            response.close()

    response = request()
    it = gen(response)
    try:
        yield response, it
    finally:
        # Ensure connection is closed
        it.close()
