import logging

import pytest

from dvc.stage import Stage
from dvc.stage.run import run_stage


@pytest.mark.parametrize(
    "cmd, expected",
    [
        ("mycmd arg1 arg2", ["> mycmd arg1 arg2"]),
        (["mycmd1 arg1", "mycmd2 arg2"], ["> mycmd1 arg1", "> mycmd2 arg2"]),
    ],
)
def test_run_stage_dry(caplog, cmd, expected):
    with caplog.at_level(level=logging.INFO, logger="dvc"):
        stage = Stage(None, "stage.dvc", cmd=cmd)
        run_stage(stage, dry=True)

    expected.insert(0, "Running stage 'stage.dvc':")
    assert caplog.messages == expected
