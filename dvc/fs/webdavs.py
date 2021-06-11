from dvc.scheme import Schemes

from .webdav import WebDAVFileSystem


class WebDAVSFileSystem(WebDAVFileSystem):  # pylint:disable=abstract-method
    scheme = Schemes.WEBDAVS
