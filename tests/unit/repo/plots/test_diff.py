import pytest

from dvc.repo.plots.diff import _revisions


@pytest.mark.parametrize(
    "arg_revisions,is_dirty,expected_revisions",
    [
        ([], False, ["workspace"]),
        ([], True, ["HEAD", "workspace"]),
        (["v1", "v2", "workspace"], False, ["v1", "v2", "workspace"]),
        (["v1", "v2", "workspace"], True, ["v1", "v2", "workspace"]),
    ],
)
def test_revisions(mocker, arg_revisions, is_dirty, expected_revisions):
    mock_scm = mocker.Mock()
    mock_scm.configure_mock(
        **{"is_dirty.return_value": is_dirty, "get_ref.return_value": None}
    )
    mock_repo = mocker.Mock(scm=mock_scm)
    assert _revisions(mock_repo, arg_revisions, False) == expected_revisions


@pytest.mark.parametrize(
    "arg_revisions,baseline,expected_revisions",
    [
        (["v1"], "v0", ["v1", "v0"]),
        (["v1"], None, ["v1", "workspace"]),
        (["v1", "v2"], "v0", ["v1", "v2"]),
        (["v1", "v2"], None, ["v1", "v2"]),
    ],
)
def test_revisions_experiment(
    mocker, arg_revisions, baseline, expected_revisions
):
    mock_scm = mocker.Mock()
    mock_scm.configure_mock(
        **{"is_dirty.return_value": False, "get_ref.return_value": None}
    )
    mock_experiments = mocker.Mock()
    mock_experiments.configure_mock(**{"get_baseline.return_value": baseline})
    mock_repo = mocker.Mock(scm=mock_scm, experiments=mock_experiments)
    assert _revisions(mock_repo, arg_revisions, True) == expected_revisions
