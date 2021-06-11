import logging
import os
import posixpath

import pytest

from dvc.cli import parse_args
from dvc.command.plots import CmdPlotsDiff, CmdPlotsShow


def test_plots_diff(dvc, mocker):
    cli_args = parse_args(
        [
            "plots",
            "diff",
            "--out",
            "result.extension",
            "-t",
            "template",
            "--targets",
            "datafile",
            "--show-vega",
            "-x",
            "x_field",
            "-y",
            "y_field",
            "--title",
            "my_title",
            "--x-label",
            "x_title",
            "--y-label",
            "y_title",
            "--experiment",
            "HEAD",
            "tag1",
            "tag2",
        ]
    )
    assert cli_args.func == CmdPlotsDiff

    cmd = cli_args.func(cli_args)
    m = mocker.patch(
        "dvc.repo.plots.diff.diff", return_value={"datafile": "filledtemplate"}
    )

    assert cmd.run() == 0

    m.assert_called_once_with(
        cmd.repo,
        targets=["datafile"],
        revs=["HEAD", "tag1", "tag2"],
        props={
            "template": "template",
            "x": "x_field",
            "y": "y_field",
            "title": "my_title",
            "x_label": "x_title",
            "y_label": "y_title",
        },
        experiment=True,
    )


def test_plots_show_vega(dvc, mocker):
    cli_args = parse_args(
        [
            "plots",
            "show",
            "-o",
            "result.extension",
            "-t",
            "template",
            "--show-vega",
            "--no-header",
            "datafile",
        ]
    )
    assert cli_args.func == CmdPlotsShow

    cmd = cli_args.func(cli_args)

    m = mocker.patch(
        "dvc.repo.plots.Plots.show",
        return_value={"datafile": "filledtemplate"},
    )

    assert cmd.run() == 0

    m.assert_called_once_with(
        targets=["datafile"], props={"template": "template", "header": False}
    )


def test_plots_diff_vega(dvc, mocker, caplog):
    cli_args = parse_args(
        [
            "plots",
            "diff",
            "HEAD~10",
            "HEAD~1",
            "--show-vega",
            "--targets",
            "plots.csv",
        ]
    )
    cmd = cli_args.func(cli_args)
    mocker.patch(
        "dvc.repo.plots.diff.diff", return_value={"plots.csv": "plothtml"}
    )
    assert cmd.run() == 0
    assert "plothtml" in caplog.text


def test_plots_diff_open(tmp_dir, dvc, mocker, caplog):
    mocked_open = mocker.patch("webbrowser.open", return_value=True)
    cli_args = parse_args(["plots", "diff", "--targets", "datafile", "--open"])
    cmd = cli_args.func(cli_args)
    mocker.patch(
        "dvc.repo.plots.diff.diff", return_value={"datafile": "filledtemplate"}
    )

    assert cmd.run() == 0
    mocked_open.assert_called_once_with("plots.html")

    expected_url = posixpath.join(tmp_dir.as_uri(), "plots.html")
    assert expected_url in caplog.text


def test_plots_diff_open_failed(tmp_dir, dvc, mocker, caplog):
    mocked_open = mocker.patch("webbrowser.open", return_value=False)
    cli_args = parse_args(["plots", "diff", "--targets", "datafile", "--open"])
    cmd = cli_args.func(cli_args)
    mocker.patch(
        "dvc.repo.plots.diff.diff", return_value={"datafile": "filledtemplate"}
    )

    assert cmd.run() == 1
    mocked_open.assert_called_once_with("plots.html")

    error_message = "Failed to open. Please try opening it manually."
    expected_url = posixpath.join(tmp_dir.as_uri(), "plots.html")

    assert caplog.record_tuples == [
        ("dvc.command.plots", logging.INFO, expected_url),
        ("dvc.command.plots", logging.ERROR, error_message),
    ]


@pytest.mark.parametrize(
    "output, expected_url_path",
    [
        ("plots file with spaces.html", "plots%20file%20with%20spaces.html"),
        (os.path.join("dir", "..", "plots.html"), "plots.html"),
    ],
    ids=["quote", "resolve"],
)
def test_plots_path_is_quoted_and_resolved_properly(
    tmp_dir, dvc, mocker, caplog, output, expected_url_path
):
    cli_args = parse_args(
        ["plots", "diff", "--targets", "datafile", "--out", output]
    )
    cmd = cli_args.func(cli_args)
    mocker.patch(
        "dvc.repo.plots.diff.diff", return_value={"datafile": "filledtemplate"}
    )

    assert cmd.run() == 0
    expected_url = posixpath.join(tmp_dir.as_uri(), expected_url_path)
    assert expected_url in caplog.text
