import json

from dvc.repo.plots.data import PlotData
from tests.func.plots.utils import _write_json


def test_diff_dirty(tmp_dir, scm, dvc, run_copy_metrics):
    metric_1 = [{"y": 2}, {"y": 3}]
    _write_json(tmp_dir, metric_1, "metric_t.json")
    run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots_no_cache=["metric.json"],
        commit="init",
    )

    metric_2 = [{"y": 3}, {"y": 5}]
    _write_json(tmp_dir, metric_2, "metric_t.json")
    run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots_no_cache=["metric.json"],
        commit="second",
    )

    metric_3 = [{"y": 5}, {"y": 6}]
    _write_json(tmp_dir, metric_3, "metric_t.json")
    run_copy_metrics(
        "metric_t.json", "metric.json", plots_no_cache=["metric.json"]
    )

    plot_string = dvc.plots.diff(props={"fields": {"y"}})["metric.json"]

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {"y": 3, PlotData.INDEX_FIELD: 0, "rev": "HEAD"},
        {"y": 5, PlotData.INDEX_FIELD: 1, "rev": "HEAD"},
        {"y": 5, PlotData.INDEX_FIELD: 0, "rev": "workspace"},
        {"y": 6, PlotData.INDEX_FIELD: 1, "rev": "workspace"},
    ]
    assert plot_content["encoding"]["x"]["field"] == PlotData.INDEX_FIELD
    assert plot_content["encoding"]["y"]["field"] == "y"

    _write_json(tmp_dir, [{"y": 7}, {"y": 8}], "metric.json")

    plot_string = dvc.plots.diff(props={"fields": {"y"}})["metric.json"]

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {"y": 3, PlotData.INDEX_FIELD: 0, "rev": "HEAD"},
        {"y": 5, PlotData.INDEX_FIELD: 1, "rev": "HEAD"},
        {"y": 7, PlotData.INDEX_FIELD: 0, "rev": "workspace"},
        {"y": 8, PlotData.INDEX_FIELD: 1, "rev": "workspace"},
    ]
    assert plot_content["encoding"]["x"]["field"] == PlotData.INDEX_FIELD
    assert plot_content["encoding"]["y"]["field"] == "y"
