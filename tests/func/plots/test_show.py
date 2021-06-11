import json
import logging
import os
import shutil
from collections import OrderedDict

import pytest

from dvc.exceptions import (
    MetricDoesNotExistError,
    NoMetricsFoundError,
    NoMetricsParsedError,
    OverlappingOutputPathsError,
)
from dvc.main import main
from dvc.path_info import PathInfo
from dvc.repo import Repo
from dvc.repo.plots.data import (
    JSONPlotData,
    PlotData,
    PlotMetricTypeError,
    YAMLPlotData,
)
from dvc.repo.plots.template import (
    BadTemplateError,
    NoFieldInDataError,
    TemplateNotFoundError,
)
from dvc.utils.fs import remove
from dvc.utils.serialize import dump_yaml, dumps_yaml, modify_yaml
from tests.func.plots.utils import _write_csv, _write_json


def test_plot_csv_one_column(tmp_dir, scm, dvc, run_copy_metrics):
    # no header
    metric = [{"val": 2}, {"val": 3}]
    _write_csv(metric, "metric_t.csv", header=False)
    run_copy_metrics(
        "metric_t.csv", "metric.csv", plots_no_cache=["metric.csv"]
    )

    props = {
        "header": False,
        "x_label": "x_title",
        "y_label": "y_title",
        "title": "mytitle",
    }
    plot_string = dvc.plots.show(props=props)["metric.csv"]

    plot_content = json.loads(plot_string)
    assert plot_content["title"] == "mytitle"
    assert plot_content["data"]["values"] == [
        {"0": "2", PlotData.INDEX_FIELD: 0, "rev": "workspace"},
        {"0": "3", PlotData.INDEX_FIELD: 1, "rev": "workspace"},
    ]
    assert plot_content["encoding"]["x"]["field"] == PlotData.INDEX_FIELD
    assert plot_content["encoding"]["y"]["field"] == "0"
    assert plot_content["encoding"]["x"]["title"] == "x_title"
    assert plot_content["encoding"]["y"]["title"] == "y_title"


def test_plot_csv_multiple_columns(tmp_dir, scm, dvc, run_copy_metrics):
    metric = [
        OrderedDict([("first_val", 100), ("second_val", 100), ("val", 2)]),
        OrderedDict([("first_val", 200), ("second_val", 300), ("val", 3)]),
    ]
    _write_csv(metric, "metric_t.csv")
    run_copy_metrics(
        "metric_t.csv", "metric.csv", plots_no_cache=["metric.csv"]
    )

    plot_string = dvc.plots.show()["metric.csv"]

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {
            "val": "2",
            PlotData.INDEX_FIELD: 0,
            "rev": "workspace",
            "first_val": "100",
            "second_val": "100",
        },
        {
            "val": "3",
            PlotData.INDEX_FIELD: 1,
            "rev": "workspace",
            "first_val": "200",
            "second_val": "300",
        },
    ]
    assert plot_content["encoding"]["x"]["field"] == PlotData.INDEX_FIELD
    assert plot_content["encoding"]["y"]["field"] == "val"


def test_plot_csv_choose_axes(tmp_dir, scm, dvc, run_copy_metrics):
    metric = [
        OrderedDict([("first_val", 100), ("second_val", 100), ("val", 2)]),
        OrderedDict([("first_val", 200), ("second_val", 300), ("val", 3)]),
    ]
    _write_csv(metric, "metric_t.csv")
    run_copy_metrics(
        "metric_t.csv", "metric.csv", plots_no_cache=["metric.csv"]
    )

    props = {"x": "first_val", "y": "second_val"}
    plot_string = dvc.plots.show(props=props)["metric.csv"]

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {
            "val": "2",
            "rev": "workspace",
            "first_val": "100",
            "second_val": "100",
        },
        {
            "val": "3",
            "rev": "workspace",
            "first_val": "200",
            "second_val": "300",
        },
    ]
    assert plot_content["encoding"]["x"]["field"] == "first_val"
    assert plot_content["encoding"]["y"]["field"] == "second_val"


def test_plot_json_single_val(tmp_dir, scm, dvc, run_copy_metrics):
    metric = [{"val": 2}, {"val": 3}]
    _write_json(tmp_dir, metric, "metric_t.json")
    run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots_no_cache=["metric.json"],
        commit="first run",
    )

    plot_string = dvc.plots.show()["metric.json"]

    plot_json = json.loads(plot_string)
    assert plot_json["data"]["values"] == [
        {"val": 2, PlotData.INDEX_FIELD: 0, "rev": "workspace"},
        {"val": 3, PlotData.INDEX_FIELD: 1, "rev": "workspace"},
    ]
    assert plot_json["encoding"]["x"]["field"] == PlotData.INDEX_FIELD
    assert plot_json["encoding"]["y"]["field"] == "val"


def test_plot_json_multiple_val(tmp_dir, scm, dvc, run_copy_metrics):
    metric = [{"first_val": 100, "val": 2}, {"first_val": 200, "val": 3}]
    _write_json(tmp_dir, metric, "metric_t.json")
    run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots_no_cache=["metric.json"],
        commit="first run",
    )

    plot_string = dvc.plots.show()["metric.json"]

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {
            "val": 2,
            PlotData.INDEX_FIELD: 0,
            "first_val": 100,
            "rev": "workspace",
        },
        {
            "val": 3,
            PlotData.INDEX_FIELD: 1,
            "first_val": 200,
            "rev": "workspace",
        },
    ]
    assert plot_content["encoding"]["x"]["field"] == PlotData.INDEX_FIELD
    assert plot_content["encoding"]["y"]["field"] == "val"


def test_plot_confusion(tmp_dir, dvc, run_copy_metrics):
    confusion_matrix = [
        {"predicted": "B", "actual": "A"},
        {"predicted": "A", "actual": "A"},
    ]
    _write_json(tmp_dir, confusion_matrix, "metric_t.json")
    run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots_no_cache=["metric.json"],
        commit="first run",
    )

    props = {"template": "confusion", "x": "predicted", "y": "actual"}
    plot_string = dvc.plots.show(props=props)["metric.json"]

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {"predicted": "B", "actual": "A", "rev": "workspace"},
        {"predicted": "A", "actual": "A", "rev": "workspace"},
    ]
    assert plot_content["spec"]["transform"][0]["groupby"] == [
        "actual",
        "predicted",
    ]
    assert plot_content["spec"]["encoding"]["x"]["field"] == "predicted"
    assert plot_content["spec"]["encoding"]["y"]["field"] == "actual"


def test_plot_confusion_normalized(tmp_dir, dvc, run_copy_metrics):
    confusion_matrix = [
        {"predicted": "B", "actual": "A"},
        {"predicted": "A", "actual": "A"},
    ]
    _write_json(tmp_dir, confusion_matrix, "metric_t.json")
    run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots_no_cache=["metric.json"],
        commit="first run",
    )

    props = {
        "template": "confusion_normalized",
        "x": "predicted",
        "y": "actual",
    }
    plot_string = dvc.plots.show(props=props)["metric.json"]

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {"predicted": "B", "actual": "A", "rev": "workspace"},
        {"predicted": "A", "actual": "A", "rev": "workspace"},
    ]
    assert plot_content["spec"]["transform"][0]["groupby"] == [
        "actual",
        "predicted",
    ]
    assert plot_content["spec"]["transform"][1]["groupby"] == ["rev", "actual"]
    assert plot_content["spec"]["encoding"]["x"]["field"] == "predicted"
    assert plot_content["spec"]["encoding"]["y"]["field"] == "actual"


def test_plot_multiple_revs_default(tmp_dir, scm, dvc, run_copy_metrics):
    metric_1 = [{"y": 2}, {"y": 3}]
    _write_json(tmp_dir, metric_1, "metric_t.json")
    run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots_no_cache=["metric.json"],
        commit="init",
        tag="v1",
    )

    metric_2 = [{"y": 3}, {"y": 5}]
    _write_json(tmp_dir, metric_2, "metric_t.json")
    run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots_no_cache=["metric.json"],
        commit="second",
        tag="v2",
    )

    metric_3 = [{"y": 5}, {"y": 6}]
    _write_json(tmp_dir, metric_3, "metric_t.json")
    run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots_no_cache=["metric.json"],
        commit="third",
    )
    plot_string = dvc.plots.show(
        revs=["HEAD", "v2", "v1"], props={"fields": {"y"}}
    )["metric.json"]

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {"y": 5, PlotData.INDEX_FIELD: 0, "rev": "HEAD"},
        {"y": 6, PlotData.INDEX_FIELD: 1, "rev": "HEAD"},
        {"y": 3, PlotData.INDEX_FIELD: 0, "rev": "v2"},
        {"y": 5, PlotData.INDEX_FIELD: 1, "rev": "v2"},
        {"y": 2, PlotData.INDEX_FIELD: 0, "rev": "v1"},
        {"y": 3, PlotData.INDEX_FIELD: 1, "rev": "v1"},
    ]
    assert plot_content["encoding"]["x"]["field"] == PlotData.INDEX_FIELD
    assert plot_content["encoding"]["y"]["field"] == "y"


def test_plot_multiple_revs(tmp_dir, scm, dvc, run_copy_metrics):
    templates_dir = dvc.plots.templates.templates_dir
    shutil.copy(
        os.path.join(templates_dir, "default.json"),
        os.path.join(templates_dir, "template.json"),
    )

    metric_1 = [{"y": 2}, {"y": 3}]
    _write_json(tmp_dir, metric_1, "metric_t.json")
    stage = run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots_no_cache=["metric.json"],
        commit="init",
        tag="v1",
    )

    metric_2 = [{"y": 3}, {"y": 5}]
    _write_json(tmp_dir, metric_2, "metric_t.json")
    assert dvc.reproduce(stage.addressing) == [stage]
    scm.add(["metric.json", stage.path])
    scm.commit("second")
    scm.tag("v2")

    metric_3 = [{"y": 5}, {"y": 6}]
    _write_json(tmp_dir, metric_3, "metric_t.json")
    assert dvc.reproduce(stage.addressing) == [stage]
    scm.add(["metric.json", stage.path])
    scm.commit("third")

    props = {"template": "template.json"}
    plot_string = dvc.plots.show(revs=["HEAD", "v2", "v1"], props=props)[
        "metric.json"
    ]

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {"y": 5, PlotData.INDEX_FIELD: 0, "rev": "HEAD"},
        {"y": 6, PlotData.INDEX_FIELD: 1, "rev": "HEAD"},
        {"y": 3, PlotData.INDEX_FIELD: 0, "rev": "v2"},
        {"y": 5, PlotData.INDEX_FIELD: 1, "rev": "v2"},
        {"y": 2, PlotData.INDEX_FIELD: 0, "rev": "v1"},
        {"y": 3, PlotData.INDEX_FIELD: 1, "rev": "v1"},
    ]
    assert plot_content["encoding"]["x"]["field"] == PlotData.INDEX_FIELD
    assert plot_content["encoding"]["y"]["field"] == "y"


def test_plot_even_if_metric_missing(
    tmp_dir, scm, dvc, caplog, run_copy_metrics
):
    tmp_dir.scm_gen("some_file", "content", commit="there is no metric")
    scm.tag("v1")

    metric = [{"y": 2}, {"y": 3}]
    _write_json(tmp_dir, metric, "metric_t.json")
    run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots_no_cache=["metric.json"],
        commit="there is metric",
        tag="v2",
    )

    caplog.clear()
    with caplog.at_level(logging.WARNING, "dvc"):
        plots = dvc.plots.show(revs=["v1", "v2"], targets=["metric.json"])
        assert "'metric.json' was not found at: 'v1'." in caplog.text

    plot_content = json.loads(plots["metric.json"])
    assert plot_content["data"]["values"] == [
        {"y": 2, PlotData.INDEX_FIELD: 0, "rev": "v2"},
        {"y": 3, PlotData.INDEX_FIELD: 1, "rev": "v2"},
    ]
    assert plot_content["encoding"]["x"]["field"] == PlotData.INDEX_FIELD
    assert plot_content["encoding"]["y"]["field"] == "y"


def test_plot_cache_missing(tmp_dir, scm, dvc, caplog, run_copy_metrics):
    metric = [{"y": 2}, {"y": 3}]
    _write_json(tmp_dir, metric, "metric_t.json")
    stage = run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots=["metric.json"],
        commit="there is metric",
    )
    scm.tag("v1")

    # Make a different plot and then remove its datafile
    metric = [{"y": 3}, {"y": 4}]
    _write_json(tmp_dir, metric, "metric_t.json")
    stage = run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots=["metric.json"],
        commit="there is an another metric",
    )
    scm.tag("v2")
    remove(stage.outs[0].fspath)
    remove(stage.outs[0].cache_path)

    plots = dvc.plots.show(revs=["v1", "v2"], targets=["metric.json"])
    plot_content = json.loads(plots["metric.json"])
    assert plot_content["data"]["values"] == [
        {"y": 2, PlotData.INDEX_FIELD: 0, "rev": "v1"},
        {"y": 3, PlotData.INDEX_FIELD: 1, "rev": "v1"},
    ]


def test_throw_on_no_metric_at_all(tmp_dir, scm, dvc, caplog):
    tmp_dir.scm_gen("some_file", "content", commit="there is no metric")
    scm.tag("v1")

    tmp_dir.gen("some_file", "make repo dirty")

    caplog.clear()
    with pytest.raises(MetricDoesNotExistError) as error, caplog.at_level(
        logging.WARNING, "dvc"
    ):
        dvc.plots.show(targets="plot.json", revs=["v1"])

        # do not warn if none found
        assert len(caplog.messages) == 0

    assert str(error.value) == "'plot.json' does not exist."


def test_custom_template(tmp_dir, scm, dvc, custom_template, run_copy_metrics):
    metric = [{"a": 1, "b": 2}, {"a": 2, "b": 3}]
    _write_json(tmp_dir, metric, "metric_t.json")
    run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots_no_cache=["metric.json"],
        commit="init",
        tag="v1",
    )

    props = {"template": os.fspath(custom_template), "x": "a", "y": "b"}
    plot_string = dvc.plots.show(props=props)["metric.json"]

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {"a": 1, "b": 2, "rev": "workspace"},
        {"a": 2, "b": 3, "rev": "workspace"},
    ]
    assert plot_content["encoding"]["x"]["field"] == "a"
    assert plot_content["encoding"]["y"]["field"] == "b"


def _replace(path, src, dst):
    path.write_text(path.read_text().replace(src, dst))


def test_should_raise_on_no_template(tmp_dir, dvc, run_copy_metrics):
    metric = [{"val": 2}, {"val": 3}]
    _write_json(tmp_dir, metric, "metric_t.json")
    run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots_no_cache=["metric.json"],
        commit="first run",
    )

    with pytest.raises(TemplateNotFoundError):
        props = {"template": "non_existing_template.json"}
        dvc.plots.show("metric.json", props=props)


def test_bad_template(tmp_dir, dvc, run_copy_metrics):
    metric = [{"val": 2}, {"val": 3}]
    _write_json(tmp_dir, metric, "metric_t.json")
    run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots_no_cache=["metric.json"],
        commit="first run",
    )

    tmp_dir.gen("template.json", json.dumps({"a": "b", "c": "d"}))

    with pytest.raises(BadTemplateError):
        props = {"template": "template.json"}
        dvc.plots.show("metric.json", props=props)


def test_plot_wrong_metric_type(tmp_dir, scm, dvc, run_copy_metrics):
    tmp_dir.gen("metric_t.txt", "some text")
    run_copy_metrics(
        "metric_t.txt",
        "metric.txt",
        plots_no_cache=["metric.txt"],
        commit="add text metric",
    )

    with pytest.raises(PlotMetricTypeError):
        dvc.plots.show(targets=["metric.txt"])


def test_plot_choose_columns(
    tmp_dir, scm, dvc, custom_template, run_copy_metrics
):
    metric = [{"a": 1, "b": 2, "c": 3}, {"a": 2, "b": 3, "c": 4}]
    _write_json(tmp_dir, metric, "metric_t.json")
    run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots_no_cache=["metric.json"],
        commit="init",
        tag="v1",
    )

    props = {
        "template": os.fspath(custom_template),
        "fields": {"b", "c"},
        "x": "b",
        "y": "c",
    }
    plot_string = dvc.plots.show(props=props)["metric.json"]

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {"b": 2, "c": 3, "rev": "workspace"},
        {"b": 3, "c": 4, "rev": "workspace"},
    ]
    assert plot_content["encoding"]["x"]["field"] == "b"
    assert plot_content["encoding"]["y"]["field"] == "c"


def test_plot_default_choose_column(tmp_dir, scm, dvc, run_copy_metrics):
    metric = [{"a": 1, "b": 2, "c": 3}, {"a": 2, "b": 3, "c": 4}]
    _write_json(tmp_dir, metric, "metric_t.json")
    run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots_no_cache=["metric.json"],
        commit="init",
        tag="v1",
    )

    plot_string = dvc.plots.show(props={"fields": {"b"}})["metric.json"]

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {PlotData.INDEX_FIELD: 0, "b": 2, "rev": "workspace"},
        {PlotData.INDEX_FIELD: 1, "b": 3, "rev": "workspace"},
    ]
    assert plot_content["encoding"]["x"]["field"] == PlotData.INDEX_FIELD
    assert plot_content["encoding"]["y"]["field"] == "b"


def test_plot_yaml(tmp_dir, scm, dvc, run_copy_metrics):
    metric = [{"val": 2}, {"val": 3}]
    dump_yaml("metric_t.yaml", metric)
    run_copy_metrics(
        "metric_t.yaml", "metric.yaml", plots_no_cache=["metric.yaml"]
    )

    plot_string = dvc.plots.show()["metric.yaml"]

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {"val": 2, PlotData.INDEX_FIELD: 0, "rev": "workspace"},
        {"val": 3, PlotData.INDEX_FIELD: 1, "rev": "workspace"},
    ]


def test_raise_on_wrong_field(tmp_dir, scm, dvc, run_copy_metrics):
    metric = [{"val": 2}, {"val": 3}]
    _write_json(tmp_dir, metric, "metric_t.json")
    run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots_no_cache=["metric.json"],
        commit="first run",
    )

    with pytest.raises(NoFieldInDataError):
        dvc.plots.show("metric.json", props={"x": "no_val"})

    with pytest.raises(NoFieldInDataError):
        dvc.plots.show("metric.json", props={"y": "no_val"})


def test_load_metric_from_dict_json(tmp_dir):
    metric = [{"acccuracy": 1, "loss": 2}, {"accuracy": 3, "loss": 4}]
    dmetric = {"train": metric}

    plot_data = JSONPlotData("-", "revision", json.dumps(dmetric))

    expected = metric
    for d in expected:
        d["rev"] = "revision"

    assert list(map(dict, plot_data.to_datapoints())) == expected


def test_load_metric_from_dict_yaml(tmp_dir):
    metric = [{"acccuracy": 1, "loss": 2}, {"accuracy": 3, "loss": 4}]
    dmetric = {"train": metric}

    plot_data = YAMLPlotData("-", "revision", dumps_yaml(dmetric))

    expected = metric
    for d in expected:
        d["rev"] = "revision"

    assert list(map(dict, plot_data.to_datapoints())) == expected


def test_multiple_plots(tmp_dir, scm, dvc, run_copy_metrics):
    metric1 = [
        OrderedDict([("first_val", 100), ("second_val", 100), ("val", 2)]),
        OrderedDict([("first_val", 200), ("second_val", 300), ("val", 3)]),
    ]
    metric2 = [
        OrderedDict([("first_val", 100), ("second_val", 100), ("val", 2)]),
        OrderedDict([("first_val", 200), ("second_val", 300), ("val", 3)]),
    ]
    _write_csv(metric1, "metric_t1.csv")
    _write_json(tmp_dir, metric2, "metric_t2.json")
    run_copy_metrics(
        "metric_t1.csv", "metric1.csv", plots_no_cache=["metric1.csv"]
    )
    run_copy_metrics(
        "metric_t2.json", "metric2.json", plots_no_cache=["metric2.json"]
    )

    assert len(dvc.plots.show().keys()) == 2


@pytest.mark.parametrize("use_dvc", [True, False])
def test_show_non_plot(tmp_dir, scm, use_dvc):
    metric = [{"first_val": 100, "val": 2}, {"first_val": 200, "val": 3}]
    _write_json(tmp_dir, metric, "metric.json")

    if use_dvc:
        dvc = Repo.init()
    else:
        dvc = Repo(uninitialized=True)

    plot_string = dvc.plots.show(targets=["metric.json"])["metric.json"]

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {
            "val": 2,
            PlotData.INDEX_FIELD: 0,
            "first_val": 100,
            "rev": "workspace",
        },
        {
            "val": 3,
            PlotData.INDEX_FIELD: 1,
            "first_val": 200,
            "rev": "workspace",
        },
    ]
    assert plot_content["encoding"]["x"]["field"] == PlotData.INDEX_FIELD
    assert plot_content["encoding"]["y"]["field"] == "val"

    if not use_dvc:
        assert not (tmp_dir / ".dvc").exists()


def test_show_non_plot_and_plot_with_params(
    tmp_dir, scm, dvc, run_copy_metrics
):
    metric = [{"first_val": 100, "val": 2}, {"first_val": 200, "val": 3}]
    _write_json(tmp_dir, metric, "metric.json")
    run_copy_metrics(
        "metric.json", "metric2.json", plots_no_cache=["metric2.json"]
    )

    dvc.plots.modify("metric2.json", props={"title": "TITLE"})
    result = dvc.plots.show(targets=["metric.json", "metric2.json"])

    plot_content = json.loads(result["metric.json"])
    plot2_content = json.loads(result["metric2.json"])

    assert plot2_content["title"] == "TITLE"

    assert plot_content != plot2_content
    plot_content.pop("title")
    plot2_content.pop("title")
    assert plot_content == plot2_content


def test_show_no_repo(tmp_dir):
    metric = [{"first_val": 100, "val": 2}, {"first_val": 200, "val": 3}]
    _write_json(tmp_dir, metric, "metric.json")

    dvc = Repo(uninitialized=True)

    dvc.plots.show(["metric.json"])


def test_show_from_subdir(tmp_dir, dvc, caplog):
    subdir = tmp_dir / "subdir"

    subdir.mkdir()
    metric = [{"first_val": 100, "val": 2}, {"first_val": 200, "val": 3}]
    _write_json(subdir, metric, "metric.json")

    with subdir.chdir(), caplog.at_level(logging.INFO, "dvc"):
        assert main(["plots", "show", "metric.json"]) == 0

    assert subdir.as_uri() in caplog.text
    assert (subdir / "plots.html").exists()


def test_show_malformed_plots(tmp_dir, scm, dvc, caplog):
    tmp_dir.gen("plot.json", '[{"m":1]')

    with pytest.raises(NoMetricsParsedError):
        dvc.plots.show(targets=["plot.json"])


def test_plots_show_no_target(tmp_dir, dvc):
    with pytest.raises(MetricDoesNotExistError):
        dvc.plots.show(targets=["plot.json"])


def test_show_no_plots_files(tmp_dir, dvc, caplog):
    with pytest.raises(NoMetricsFoundError):
        dvc.plots.show()


@pytest.mark.parametrize("clear_before_run", [True, False])
def test_plots_show_overlap(tmp_dir, dvc, run_copy_metrics, clear_before_run):
    data_dir = PathInfo("data")
    (tmp_dir / data_dir).mkdir()

    dump_yaml(data_dir / "m1_temp.yaml", {"a": {"b": {"c": 2, "d": 1}}})
    run_copy_metrics(
        str(data_dir / "m1_temp.yaml"),
        str(data_dir / "m1.yaml"),
        single_stage=False,
        commit="add m1",
        name="cp-m1",
        plots=[str(data_dir / "m1.yaml")],
    )
    with modify_yaml("dvc.yaml") as d:
        # trying to make an output overlaps error
        d["stages"]["corrupted-stage"] = {
            "cmd": "mkdir data",
            "outs": ["data"],
        }

    # running by clearing and not clearing stuffs
    # so as it works even for optimized cases
    if clear_before_run:
        remove(data_dir)
        remove(dvc.odb.local.cache_dir)

    dvc._reset()

    with pytest.raises(OverlappingOutputPathsError):
        dvc.plots.show()


def test_dir_plots(tmp_dir, dvc, run_copy_metrics):
    subdir = tmp_dir / "subdir"
    subdir.mkdir()

    metric = [{"first_val": 100, "val": 2}, {"first_val": 200, "val": 3}]

    fname = "file.json"
    _write_json(tmp_dir, metric, fname)

    p1 = os.path.join("subdir", "p1.json")
    p2 = os.path.join("subdir", "p2.json")
    tmp_dir.dvc.run(
        cmd=(
            f"mkdir subdir && python copy.py {fname} {p1} && "
            f"python copy.py {fname} {p2}"
        ),
        deps=[fname],
        single_stage=False,
        plots=["subdir"],
        name="copy_double",
    )
    dvc.plots.modify("subdir", {"title": "TITLE"})

    result = dvc.plots.show()
    p1_content = json.loads(result[p1])
    p2_content = json.loads(result[p2])

    assert p1_content["title"] == p2_content["title"] == "TITLE"


def test_show_dir_plots(tmp_dir, dvc, run_copy_metrics):
    subdir = tmp_dir / "subdir"
    subdir.mkdir()
    metric = [{"first_val": 100, "val": 2}, {"first_val": 200, "val": 3}]

    fname = "file.json"
    _write_json(tmp_dir, metric, fname)

    p1 = os.path.join("subdir", "p1.json")
    p2 = os.path.join("subdir", "p2.json")
    tmp_dir.dvc.run(
        cmd=(
            f"mkdir subdir && python copy.py {fname} {p1} && "
            f"python copy.py {fname} {p2}"
        ),
        deps=[fname],
        single_stage=False,
        plots=["subdir"],
        name="copy_double",
    )

    result = dvc.plots.show(targets=["subdir"])
    p1_content = json.loads(result[p1])
    p2_content = json.loads(result[p2])

    assert p1_content == p2_content

    result = dvc.plots.show(targets=[p1])
    assert set(result.keys()) == {p1}

    remove(dvc.odb.local.cache_dir)
    remove(subdir)
    with pytest.raises(NoMetricsParsedError):
        dvc.plots.show()


def test_ignore_binary_file(tmp_dir, dvc, run_copy_metrics):
    with open("file", "wb") as fobj:
        fobj.write(b"\xc1")

    run_copy_metrics("file", "plot_file", plots=["plot_file"])

    with pytest.raises(NoMetricsParsedError):
        dvc.plots.show()
