from pytest import CaptureFixture

from dvc.ui import Console


def test_write(capsys: CaptureFixture[str]):
    """Test that ui.write works."""
    console = Console(enable=True)
    message = "hello world"
    console.write(message)
    console.error_write(message)

    captured = capsys.readouterr()
    assert captured.out == f"{message}\n"
    assert captured.err == f"{message}\n"


def test_capsys_works(capsys: CaptureFixture[str]):
    """Sanity check that capsys can capture outputs from a global ui."""
    from dvc.ui import ui

    message = "hello world"
    ui.write(message)
    ui.error_write(message)

    captured = capsys.readouterr()
    assert captured.out == f"{message}\n"
    assert captured.err == f"{message}\n"
