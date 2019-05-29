from setuptools import setup, find_packages
from setuptools.command.build_py import build_py as _build_py
import os


# https://packaging.python.org/guides/single-sourcing-package-version/
pkg_dir = os.path.dirname(__file__)

# This will define __version__ implicitly
with open(os.path.join(pkg_dir, "dvc", "version.py")) as fobj:
    exec(fobj.read())

version = __version__  # noqa: F821


# To achieve consistency between the build version and the one provided
# by your package during runtime, you need to **pin** the build version.
#
# This custom class will replace the version.py module with a **static**
# `__version__` that your package can read at runtime, assuring consistancy.
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
    "configparser>=3.5.0",
    "zc.lockfile>=1.2.1",
    "future>=0.16.0",
    "colorama>=0.3.9",
    "configobj>=5.0.6",
    "networkx>=2.1",
    "gitpython>=2.1.8",
    "setuptools>=34.0.0",
    "nanotime>=0.5.2",
    "pyasn1>=0.4.1",
    "schema>=0.6.7",
    "jsonpath-ng>=1.4.3",
    "requests>=2.22.0",
    "grandalf==0.6",
    "asciimatics>=1.10.0",
    "distro>=1.3.0",
    "appdirs>=1.4.3",
    "treelib>=1.5.5",
    "inflect>=2.1.0",
    "humanize>=0.5.1",
    "dulwich>=0.19.11",
    "ruamel.yaml>=0.15.91",
]

# Extra dependencies for remote integrations
gs = ["google-cloud-storage==1.13.0"]
s3 = ["boto3==1.9.115"]
azure = ["azure-storage-blob==1.3.0"]
oss = ["oss2==2.6.1"]
ssh = ["paramiko>=2.4.1"]
all_remotes = gs + s3 + azure + ssh + oss

# Extra dependecies to run tests
tests_requirements = [
    "PyInstaller==3.4",
    "wheel>=0.31.1",
    "pydot>=1.2.4",
    # Test requirements:
    "pytest>=4.4.0",
    "pytest-timeout>=1.3.3",
    "pytest-cov>=2.6.1",
    "pytest-xdist>=1.26.1",
    "pytest-mock>=1.10.4",
    "flaky>=3.5.3",
    "mock>=3.0.0",
    "xmltodict>=0.11.0",
    "awscli>=1.16.125",
    "google-compute-engine",
    "pywin32; sys_platform == 'win32'",
    "Pygments",  # required by collective.checkdocs,
    "collective.checkdocs",
    "black==19.3b0",
    "flake8",
    "flake8-docstrings",
    "jaraco.windows==3.9.2",
    "mock-ssh-server>=0.5.0",
]

setup(
    name="dvc",
    version=version,
    description="Git for data scientists - manage your code and data together",
    long_description=open("README.rst", "r").read(),
    author="Dmitry Petrov",
    author_email="dmitry@dataversioncontrol.com",
    download_url="https://github.com/iterative/dvc",
    license="Apache License 2.0",
    install_requires=install_requires,
    extras_require={
        "all": all_remotes,
        "gs": gs,
        "s3": s3,
        "azure": azure,
        "oss": oss,
        "ssh": ssh,
        # NOTE: https://github.com/inveniosoftware/troubleshooting/issues/1
        ":python_version=='2.7'": ["futures", "pathlib2"],
        "tests": tests_requirements,
    },
    keywords="data science, data version control, machine learning",
    python_requires=">=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    url="http://dataversioncontrol.com",
    entry_points={"console_scripts": ["dvc = dvc.main:main"]},
    cmdclass={"build_py": build_py},
    zip_safe=False,
)
