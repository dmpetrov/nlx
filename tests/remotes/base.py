import locale
import pathlib

from funcy import cached_property

from dvc.path_info import URLInfo


class Base(URLInfo):

    IS_OBJECT_STORAGE = False

    def is_file(self):
        raise NotImplementedError

    def is_dir(self):
        raise NotImplementedError

    def exists(self):
        raise NotImplementedError

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        raise NotImplementedError

    def write_text(self, contents, encoding=None, errors=None):
        if not encoding:
            encoding = locale.getpreferredencoding(False)
        assert errors is None
        self.write_bytes(contents.encode(encoding))

    def write_bytes(self, contents):
        raise NotImplementedError

    def read_text(self, encoding=None, errors=None):
        raise NotImplementedError

    def read_bytes(self):
        raise NotImplementedError

    def _gen(self, struct, prefix=None):
        for name, contents in struct.items():
            path = (prefix or self) / name

            if isinstance(contents, dict):
                if not contents:
                    path.mkdir(parents=True, exist_ok=True)
                else:
                    self._gen(contents, prefix=path)
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                if isinstance(contents, bytes):
                    path.write_bytes(contents)
                else:
                    path.write_text(contents, encoding="utf-8")

    def gen(self, struct, text=""):
        if isinstance(struct, (str, bytes, pathlib.PurePath)):
            struct = {struct: text}

        self._gen(struct)
        return struct.keys()

    def close(self):
        pass

    @staticmethod
    def should_test():
        return True

    @staticmethod
    def get_url():
        raise NotImplementedError

    @cached_property
    def config(self):
        return {"url": self.url}
