from .base import Remote
from .index import RemoteIndexNoop


class LocalRemote(Remote):
    INDEX_CLS = RemoteIndexNoop  # type: ignore[assignment]
