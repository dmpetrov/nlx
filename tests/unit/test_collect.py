import logging

from dvc.repo.collect import collect


def test_no_file_on_target_rev(tmp_dir, scm, dvc, caplog):
    with caplog.at_level(logging.WARNING, "dvc"):
        collect(dvc, targets=["file.yaml"], rev="current_branch")

    assert "'file.yaml' was not found at: 'current_branch'." in caplog.text
