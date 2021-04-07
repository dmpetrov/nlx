import textwrap

import pytest

from dvc.utils.serialize import modify_yaml


@pytest.mark.parametrize(
    "val",
    [
        "1000.0",
        "1_000.000_000",
        "1e4",
        "1e+4",
        "1e04",
        "1.0e4",
        "1.0e+4",
        "1.0e+04",
        "1e-4",
        "1.0e-4",
        "1.0e-04",
        "-1e4",
        "-1e04",
        "-1e+4",
        "-1.0e4",
        "-1.0e+4",
        "-1.0e+04",
        "-1e-4",
        "-1.0e-4",
        "-1.0e-04",
    ],
)
def test_modify_override_floats(tmp_dir, val):
    source = textwrap.dedent(
        f"""
        threshold: {val}
        epochs: 10
    """
    )
    param_file = tmp_dir / "params.py"
    param_file.write_text(source)

    with modify_yaml(param_file) as d:
        d["threshold"] = 1e3
    assert "threshold: 1000.0" in param_file.read_text()

    parsed = float(val)
    with modify_yaml(param_file) as d:
        d["threshold"] = parsed
    assert f"threshold: {parsed}" in param_file.read_text()
