import argparse
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
    help="Path to the osxpkg to sign. If not specified - try to find one.",
)
parser.add_argument(
    "--installer-id",
    required=True,
    help="Certificate ID (should be added to the keychain).",
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

unsigned = pkg.with_suffix(".unsigned")
os.rename(pkg, unsigned)
check_call(
    [
        "productsign",
        "--sign",
        args.installer_id,
        os.fspath(unsigned),
        os.fspath(pkg),
    ],
    stderr=STDOUT,
)

check_call(
    ["pkgutil", "--check-signature", os.fspath(pkg)],
    stderr=STDOUT,
)
