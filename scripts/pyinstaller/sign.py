import argparse
import os
import pathlib
import sys
from subprocess import STDOUT, check_call

if sys.platform != "darwin":
    raise NotImplementedError

parser = argparse.ArgumentParser()
parser.add_argument(
    "--application-id",
    required=True,
    help="Certificate ID (should be added to the keychain).",
)
args = parser.parse_args()

path = pathlib.Path(__file__).parent.absolute()
dvc = path / "dist" / "dvc"
for root, _, fnames in os.walk(dvc):
    for fname in fnames:
        fpath = os.path.join(root, fname)
        print(f"signing {fpath}")
        check_call(
            [
                "codesign",
                "--force",
                "--verbose",
                "-s",
                args.application_id,
                "-o",
                "runtime",
                "--entitlements",
                "entitlements.plist",
                fpath,
            ],
            stderr=STDOUT,
            timeout=180,
        )
