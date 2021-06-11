import subprocess
import sys


def test_no_remote_imports():
    remote_modules = {
        "boto3",
        "botocore",
        "google.cloud.storage",
        "azure.storage.blob",
        "oss2",
        "pydrive2",
        "paramiko",
        "pyarrow",
    }

    code = "import dvc.main, sys; print(' '.join(sys.modules))"
    res = subprocess.run(
        [sys.executable, "-c", code], stdout=subprocess.PIPE, check=True
    )
    modules = res.stdout.decode().split()
    assert not set(modules) & remote_modules
