import argparse
import configparser
import os
import pathlib
import shutil
from subprocess import STDOUT, check_call, check_output

path = pathlib.Path(__file__).parent.absolute()
config = path / "config.ini"

dvc = path.parent.parent / "dvc"
pyinstaller = path.parent / "pyinstaller"

build = path / "build"
install = build / "usr"

parser = argparse.ArgumentParser()
args = parser.parse_args()

try:
    shutil.rmtree(build)
except FileNotFoundError:
    pass

build.mkdir()
shutil.copytree(pyinstaller / "dist" / "dvc", build / "dvc")
shutil.copy(path / "addSymLinkPermissions.ps1", build)

version = check_output(
    [os.fspath(build / "dvc" / "dvc"), "--version"], text=True
).strip()

cfg = configparser.ConfigParser()
cfg.add_section("Version")
cfg.set("Version", "version", version)

with (path / "config.ini").open("w") as fobj:
    cfg.write(fobj)

check_call(
    ["iscc", "setup.iss"],
    cwd=path,
    stderr=STDOUT,
)
