"""Temporary fix for https://github.com/iterative/dvc/issues/5618."""
from PyInstaller.utils.hooks import (  # pylint:disable=import-error
    collect_data_files,
    copy_metadata,
)

datas = collect_data_files("googleapiclient.discovery")
datas += copy_metadata("google_api_python_client")
