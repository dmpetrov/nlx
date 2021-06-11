from .base import BaseFileSystem


class MemoryFileSystem(BaseFileSystem):
    scheme = "local"
    PARAM_CHECKSUM = "md5"

    def __init__(self, **kwargs):
        from fsspec.implementations.memory import MemoryFileSystem as MemFS

        super().__init__(**kwargs)

        self.fs = MemFS()

    def exists(self, path_info) -> bool:
        return self.fs.exists(path_info.fspath)

    def open(self, path_info, mode="r", encoding=None, **kwargs):
        return self.fs.open(
            path_info.fspath, mode=mode, encoding=encoding, **kwargs
        )

    def info(self, path_info):
        return self.fs.info(path_info.fspath)

    def stat(self, path_info):
        import os

        info = self.fs.info(path_info.fspath)

        return os.stat_result((0, 0, 0, 0, 0, 0, info["size"], 0, 0, 0))

    def walk_files(self, path_info, **kwargs):
        raise NotImplementedError
