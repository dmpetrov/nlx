import io

import requests

from dvc.utils.http import open_url


def read_nbytes(fd, num, mode="r"):
    data = b"" if mode == "rb" else ""
    while len(data) < num:
        chunk = fd.read(num - len(data))
        if not chunk:
            break
        data += chunk
    return data


def test_open_url(tmp_path, monkeypatch, http):
    # Simulate bad connection
    original_iter_content = requests.Response.iter_content

    def bad_iter_content(self, *args, **kwargs):
        it = original_iter_content(self, *args, **kwargs)
        for i, chunk in enumerate(it):
            # Drop connection error on second chunk if there is one
            if i > 0:
                raise requests.ConnectionError("Simulated connection drop")
            yield chunk

    monkeypatch.setattr(requests.Response, "iter_content", bad_iter_content)

    # Text should be longer than default chunk to test resume,
    # using twice of that plus something tests second resume,
    # this is important because second response is different
    text = "0123456789" * (io.DEFAULT_BUFFER_SIZE // 10 + 1)
    http.gen("sample.txt", text * 2)

    with open_url((http / "sample.txt").url) as fd:
        # Test various .read() variants
        assert read_nbytes(fd, len(text), mode="r") == text
        assert fd.read() == text
        assert fd.read() == ""


def test_open_url_peek_rb(tmp_path, monkeypatch, http):
    # Goes over seek feature in 'rb' mode
    text = "0123456789" * (io.DEFAULT_BUFFER_SIZE // 10 + 1)
    http.gen("sample.txt", text * 2)

    with open_url((http / "sample.txt").url, mode="rb") as fd:
        text = text.encode("utf8")
        assert fd.peek(len(text)) == text
        assert read_nbytes(fd, len(text), mode="rb") == text
        assert fd.peek(len(text)) == text
        assert fd.read() == text
        assert fd.read() == b""
