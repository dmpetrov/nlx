import argparse
import json
import os
import pathlib
import sys
from subprocess import STDOUT, check_call

if sys.platform != "darwin":
    raise NotImplementedError

parser = argparse.ArgumentParser()
parser.add_argument(
    "path",
    nargs="?",
    help="Path to the osxpkg to notarize. If not specified - try to find one.",
)
parser.add_argument(
    "--apple-id-username", required=True, help="Apple ID username."
)
parser.add_argument(
    "--apple-id-password",
    required=True,
    help=(
        "Apple ID app-specific password. Note that this is not a regular "
        "Apple ID password, so you need to generate one at "
        "https://appleid.apple.com/account/manage"
    ),
)
args = parser.parse_args()

path = pathlib.Path(__file__).parent.absolute()

if args.path:
    pkg = pathlib.Path(args.path)
else:
    pkgs = list(path.glob("*.pkg"))
    if not pkgs:
        print("No pkgs found")
        exit(1)

    if len(pkgs) > 1:
        print("Too many packages")
        exit(1)

    (pkg,) = pkgs


config = {
    "notarize": {
        "path": os.fspath(pkg),
        "bundle_id": "com.iterative.dvc",
        "staple": True,
    },
    "apple_id": {
        "username": args.apple_id_username,
        "password": args.apple_id_password,
    },
}

(path / "config.json").write_text(json.dumps(config))

check_call(
    ["gon", "config.json"],
    cwd=path,
    stderr=STDOUT,
)
