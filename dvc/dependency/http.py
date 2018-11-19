try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from dvc.dependency.base import DependencyBase
from dvc.remote.http import RemoteHTTP


class DependencyHTTP(DependencyBase):
    REGEX = RemoteHTTP.REGEX

    def __init__(self, stage, path, info=None, remote=None):
        super(DependencyHTTP, self).__init__(stage, path)

        self.info = info
        self.remote = remote or RemoteHTTP(stage.project, {})
        self.path_info = {
            'scheme': urlparse(path).scheme,
            'url': path,
        }

    def save(self):
        self.info = self.remote.save_info(self.path_info)

    def dumpd(self):
        ret = self.info
        ret[self.PARAM_PATH] = self.path
        return ret

    def changed(self):
        return self.info != self.remote.save_info(self.path_info)
