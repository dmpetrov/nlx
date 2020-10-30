import importlib.util
import os

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
    "dulwich>=0.20.11",
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
    "networkx>=2.1,<2.5",
    "pydot>=1.2.4",
    "speedcopy>=2.0.1; python_version < '3.8' and sys_platform == 'win32'",
    "dataclasses; python_version < '3.7'",
    "flatten_dict>=0.3.0,<1",
    "tabulate>=0.8.7",
    "pygtrie==2.3.2",
    "dpath>=2.0.1,<3",
    "shtab>=1.3.2,<2",
    "rich>=3.0.5",
    "dictdiffer>=0.8.1",
]


# Extra dependencies for remote integrations

gs = ["google-cloud-storage==1.19.0"]
gdrive = ["pydrive2>=1.6.3", "six >= 1.13.0"]
s3 = ["boto3>=1.9.201"]
azure = ["azure-storage-blob>=12.0", "knack"]
oss = ["oss2==2.6.1"]
ssh = ["paramiko[invoke]>=2.7.0"]

# Remove the env marker if/when pyarrow is available for Python3.9
hdfs = ["pyarrow>=2.0.0;  python_version < '3.9'"]
webdav = ["webdavclient3>=3.14.5"]
# gssapi should not be included in all_remotes, because it doesn't have wheels
# for linux and mac, so it will fail to compile if user doesn't have all the
# requirements, including kerberos itself. Once all the wheels are available,
# we can start shipping it by default.
ssh_gssapi = ["paramiko[invoke,gssapi]>=2.7.0"]
all_remotes = gs + s3 + azure + ssh + oss + gdrive + hdfs + webdav

# Extra dependecies to run tests
tests_requirements = [
    "wheel>=0.31.1",
    # Test requirements:
    "pytest>=6.0.1",
    "pytest-cov",
    "pytest-docker>=0.7.2",
    "pytest-timeout>=1.3.3",
    "pytest-cov>=2.6.1",
    "pytest-xdist>=1.26.1",
    "pytest-mock==1.11.2",
    "pytest-lazy-fixture",
    "pytest-tap",
    "flaky>=3.5.3",
    "mock>=3.0.0",
    "xmltodict>=0.11.0",
    "awscli>=1.16.297",
    "google-compute-engine==2.8.13",
    "Pygments",  # required by collective.checkdocs,
    "collective.checkdocs",
    "flake8==3.8.3",
    "psutil",
    "flake8-docstrings",
    "pydocstyle<4.0",
    "jaraco.windows==3.9.2",
    "mock-ssh-server>=0.8.2",
    "moto==1.3.14.dev464",
    "rangehttpserver==1.2.0",
    "beautifulsoup4==4.4.0",
    "flake8-bugbear",
    "flake8-comprehensions==3.3.0",
    "flake8-string-format",
    "pylint==2.5.3",
    "pylint-pytest>=0.3.0",
    "pylint-plugin-utils",
    "wget",
    "filelock",
    "black==19.10b0",
]

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
    ],
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    url="http://dvc.org",
    entry_points={"console_scripts": ["dvc = dvc.main:main"]},
    cmdclass={"build_py": build_py},
    zip_safe=False,
)
