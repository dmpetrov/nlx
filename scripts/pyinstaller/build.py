import os
import pathlib
from subprocess import STDOUT, check_call

path = pathlib.Path(__file__).parent.absolute()
hooks = path / "hooks"
dvc = path.parent.parent / "dvc"
entry = dvc / "__main__.py"

check_call(
    [
        "pyinstaller",
        "--additional-hooks-dir",
        os.fspath(hooks),
        "--name",
        "dvc",
        "-y",
        os.fspath(entry),
    ],
    cwd=path,
    stderr=STDOUT,
)
