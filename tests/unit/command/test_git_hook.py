import pytest

from dvc.cli import parse_args
from dvc.command.git_hook import CmdPostCheckout, CmdPreCommit, CmdPrePush


@pytest.mark.parametrize(
    "hook, cls",
    [
        ("pre-commit", CmdPreCommit),
        ("post-checkout", CmdPostCheckout),
        ("pre-push", CmdPrePush),
    ],
)
def test_out_of_repo(tmp_dir, hook, cls, mocker):
    cli_args = parse_args(["git-hook", hook])
    assert cli_args.func == cls
    cmd = cli_args.func(cli_args)
    mock_main = mocker.patch("dvc.main.main")
    assert cmd.run() == 0
    assert not mock_main.called
