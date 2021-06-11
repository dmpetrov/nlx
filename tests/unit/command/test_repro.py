from dvc.cli import parse_args
from dvc.command.repro import CmdRepro

default_arguments = {
    "all_pipelines": False,
    "downstream": False,
    "dry": False,
    "force": False,
    "run_cache": True,
    "interactive": False,
    "no_commit": False,
    "pipeline": False,
    "single_item": False,
    "recursive": False,
    "force_downstream": False,
    "pull": False,
    "glob": False,
    "targets": [],
}


def test_default_arguments(dvc, mocker):
    cmd = CmdRepro(parse_args(["repro"]))
    mocker.patch.object(cmd.repo, "reproduce")
    cmd.run()
    # pylint: disable=no-member
    cmd.repo.reproduce.assert_called_with(**default_arguments)


def test_downstream(dvc, mocker):
    cmd = CmdRepro(parse_args(["repro", "--downstream"]))
    mocker.patch.object(cmd.repo, "reproduce")
    cmd.run()
    arguments = default_arguments.copy()
    arguments.update({"downstream": True})
    # pylint: disable=no-member
    cmd.repo.reproduce.assert_called_with(**arguments)
