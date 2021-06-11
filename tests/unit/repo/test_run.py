import pytest

from dvc.exceptions import InvalidArgumentError


def test_file(tmp_dir, dvc):
    msg = (
        "`--file` is currently incompatible with `-n|--name` "
        "and requires `--single-stage`"
    )
    with pytest.raises(InvalidArgumentError, match=msg):
        dvc.run(fname="path/dvc.yaml", name="my", cmd="mycmd")
