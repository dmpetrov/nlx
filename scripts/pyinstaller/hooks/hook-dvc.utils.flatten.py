from PyInstaller.utils.hooks import (  # pylint:disable=import-error
    copy_metadata,
)

datas = copy_metadata("flatten-dict")
