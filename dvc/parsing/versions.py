import enum
from collections.abc import Mapping

from voluptuous import validators

SCHEMA_KWD = "schema"
META_KWD = "meta"


def lockfile_version_schema(value):
    expected = [LOCKFILE_VERSION.V2.value]  # pylint: disable=no-member
    msg = "invalid schema version {}, expected one of {}".format(
        value, expected
    )
    return validators.Any(*expected, msg=msg)(value)


class VersionEnum(str, enum.Enum):
    @classmethod
    def all_versions(cls):
        return [v.value for v in cls]


class LOCKFILE_VERSION(VersionEnum):
    V1 = "1.0"
    V2 = "2.0"

    @classmethod
    def from_dict(cls, data):
        # 1) if it's empty or or is not a dict, use the latest one (V2).
        # 2) use the `schema` identifier if it exists and is a supported
        # version
        # 3) if it's not in any of the supported version, use the latest one
        # 4) if there's no identifier, it's a V1
        if not data or not isinstance(data, Mapping):
            return cls(cls.V2)

        version = data.get(SCHEMA_KWD)
        if version:
            return cls(version if version in cls.all_versions() else cls.V2)
        return cls(cls.V1)
