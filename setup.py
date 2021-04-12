import importlib.util
import os
from pathlib import Path

from setuptools import find_packages, setup
from setuptools.command.build_py import build_py as _build_py

# Prevents pkg_resources import in entry point script,
# see https://github.com/ninjaaron/fast-entry_points.
# This saves about 200 ms on startup time for non-wheel installs.
try:
    import fastentrypoints  # noqa: F401, pylint: disable=unused-import
except ImportError:
    pass  # not able to import when installing through pre-commit


# Read package meta-data from version.py
# see https://packaging.python.org/guides/single-sourcing-package-version/
pkg_dir = os.path.dirname(os.path.abspath(__file__))
version_path = os.path.join(pkg_dir, "dvc", "version.py")
spec = importlib.util.spec_from_file_location("dvc.version", version_path)
dvc_version = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dvc_version)
version = dvc_version.__version__  # noqa: F821


# To achieve consistency between the build version and the one provided
# by your package during runtime, you need to **pin** the build version.
#
# This custom class will replace the version.py module with a **static**
# `__version__` that your package can read at runtime, assuring consistency.
#
# References:
#   - https://docs.python.org/3.7/distutils/extending.html
#   - https://github.com/python/mypy
class build_py(_build_py):
    def pin_version(self):
        path = os.path.join(self.build_lib, "dvc")
        self.mkpath(path)
        with open(os.path.join(path, "version.py"), "w") as fobj:
            fobj.write("# AUTOGENERATED at build time by setup.py\n")
            fobj.write('__version__ = "{}"\n'.format(version))

    def run(self):
        self.execute(self.pin_version, ())
        _build_py.run(self)


install_requires = [
    "ply>=3.9",  # See https://github.com/pyinstaller/pyinstaller/issues/1945
    "colorama>=0.3.9",
    "configobj>=5.0.6",
    "gitpython>3",
    "dulwich>=0.20.21",
    "pygit2>=1.5.0",
    "setuptools>=34.0.0",
    "nanotime>=0.5.2",
    "pyasn1>=0.4.1",
    "voluptuous>=0.11.7",
    "jsonpath-ng>=1.5.1",
    "requests>=2.22.0",
    "grandalf==0.6",
    "distro>=1.3.0",
    "appdirs>=1.4.3",
    "ruamel.yaml>=0.16.1",
    "toml>=0.10.1",
    "funcy>=1.14",
    "pathspec>=0.6.0",
    "shortuuid>=0.5.0",
    "tqdm>=4.45.0,<5",
    "packaging>=19.0",
    "zc.lockfile>=1.2.1",
    "flufl.lock>=3.2,<4",
    "win-unicode-console>=0.5; sys_platform == 'win32'",
    "pywin32>=225; sys_platform == 'win32'",
    "networkx>=2.1",
    "psutil>=5.8.0",
    "pydot>=1.2.4",
    "speedcopy>=2.0.1; python_version < '3.8' and sys_platform == 'win32'",
    "dataclasses==0.7; python_version < '3.7'",
    "flatten_dict>=0.3.0,<1",
    "tabulate>=0.8.7",
    "pygtrie==2.3.2",
    "dpath>=2.0.1,<3",
    "shtab>=1.3.4,<2",
    "rich>=10.0.0",
    "dictdiffer>=0.8.1",
    "python-benedict>=0.21.1",
    "pyparsing==2.4.7",
    "typing_extensions>=3.7.4",
    "fsspec==0.9.0",
    "diskcache>=5.2.1",
]


# Extra dependencies for remote integrations

gs = ["gcsfs==0.8.0"]
gdrive = ["pydrive2>=1.8.1", "six >= 1.13.0"]
# temporary dependency to fetch from master
s3 = ["s3fs @ git+https://github.com/dask/s3fs.git", "boto3==1.16.52"]
azure = ["adlfs==0.7.1", "azure-identity>=1.4.0", "knack"]
# https://github.com/Legrandin/pycryptodome/issues/465
oss = ["oss2==2.6.1", "pycryptodome>=3.10"]
ssh = ["paramiko[invoke]>=2.7.0"]

# Remove the env marker if/when pyarrow is available for Python3.9
hdfs = ["pyarrow>=2.0.0"]
webhdfs = ["hdfs==2.5.8"]
webdav = ["webdavclient3>=3.14.5"]
# gssapi should not be included in all_remotes, because it doesn't have wheels
# for linux and mac, so it will fail to compile if user doesn't have all the
# requirements, including kerberos itself. Once all the wheels are available,
# we can start shipping it by default.
ssh_gssapi = ["paramiko[invoke,gssapi]>=2.7.0"]
all_remotes = gs + s3 + azure + ssh + oss + gdrive + hdfs + webhdfs + webdav

tests_requirements = (
    Path("test_requirements.txt").read_text().strip().splitlines()
)

setup(
    name="dvc",
    version=version,
    description="Git for data scientists - manage your code and data together",
    long_description=open("README.rst", "r", encoding="UTF-8").read(),
    author="Dmitry Petrov",
    author_email="dmitry@dvc.org",
    download_url="https://github.com/iterative/dvc",
    license="Apache License 2.0",
    install_requires=install_requires,
    extras_require={
        "all": all_remotes,
        "gs": gs,
        "gdrive": gdrive,
        "s3": s3,
        "azure": azure,
        "oss": oss,
        "ssh": ssh,
        "ssh_gssapi": ssh_gssapi,
        "hdfs": hdfs,
        "webhdfs": webhdfs,
        "webdav": webdav,
        "tests": tests_requirements,
    },
    keywords="data-science data-version-control machine-learning git"
    " developer-tools reproducibility collaboration ai",
    python_requires=">=3.6",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    url="http://dvc.org",
    entry_points={"console_scripts": ["dvc = dvc.main:main"]},
    cmdclass={"build_py": build_py},
    zip_safe=False,
)
