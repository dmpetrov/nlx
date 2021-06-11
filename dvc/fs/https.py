from dvc.scheme import Schemes

from .http import HTTPFileSystem


class HTTPSFileSystem(HTTPFileSystem):  # pylint:disable=abstract-method
    scheme = Schemes.HTTPS
