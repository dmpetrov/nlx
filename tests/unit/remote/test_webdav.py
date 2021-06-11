from dvc.fs.webdav import WebDAVFileSystem

# Test configuration
url = "webdavs://example.com/public.php/webdav"
user = "username"
userurl = f"webdavs://{user}@example.com/public.php/webdav"
password = "password"


def test_user(dvc):
    fs = WebDAVFileSystem(url=url, user=user)
    assert fs.user == user


def test_password(dvc):
    config = {"url": url, "user": user, "password": password}
    fs = WebDAVFileSystem(**config)
    assert fs.password == password


def test_ssl_verify_custom_cert(dvc):
    config = {
        "url": "http://example.com/",
        "path_info": "file.html",
        "ssl_verify": "/path/to/custom/cabundle.pem",
    }

    fs = WebDAVFileSystem(**config)
    assert fs.fs_args["verify"] == "/path/to/custom/cabundle.pem"
