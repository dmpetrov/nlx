from PyInstaller.utils.hooks import (  # pylint:disable=import-error
    copy_metadata,
)

datas = copy_metadata("google-compute-engine")
hiddenimports = ["google_cloud_engine"]
