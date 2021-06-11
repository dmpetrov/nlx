from PyInstaller.utils.hooks import (  # pylint:disable=import-error
    copy_metadata,
)

datas = copy_metadata("pydrive2")
datas += copy_metadata("google-api-python-client")
