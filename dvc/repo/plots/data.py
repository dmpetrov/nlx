import os
from copy import copy

from funcy import first

from dvc.exceptions import DvcException
from dvc.utils.serialize import ParseError


class PlotMetricTypeError(DvcException):
    def __init__(self, file):
        super().__init__(
            "'{}' - file type error\n"
            "Only JSON, YAML, CSV and TSV formats are supported.".format(file)
        )


class PlotDataStructureError(DvcException):
    def __init__(self):
        super().__init__(
            "Plot data extraction failed. Please see "
            "https://man.dvc.org/plot for supported data formats."
        )


class PlotParsingError(ParseError):
    def __init__(self, path, revision):
        self.path = path
        self.revision = revision

        super().__init__(path, f"revision: '{revision}'")


def plot_data(filename, revision, content):
    _, extension = os.path.splitext(filename.lower())
    if extension == ".json":
        return JSONPlotData(filename, revision, content)
    if extension == ".csv":
        return CSVPlotData(filename, revision, content)
    if extension == ".tsv":
        return CSVPlotData(filename, revision, content, delimiter="\t")
    if extension == ".yaml":
        return YAMLPlotData(filename, revision, content)
    raise PlotMetricTypeError(filename)


def _filter_fields(data_points, filename, revision, fields=None, **kwargs):
    if not fields:
        return data_points
    assert isinstance(fields, set)

    new_data = []
    for data_point in data_points:
        new_dp = copy(data_point)

        keys = set(data_point.keys())
        if keys & fields != fields:
            raise DvcException(
                "Could not find fields: '{}' for '{}' at '{}'.".format(
                    ", ".join(fields), filename, revision
                )
            )

        to_del = keys - fields
        for key in to_del:
            del new_dp[key]
        new_data.append(new_dp)
    return new_data


def _apply_path(data, path=None, **kwargs):
    if not path or not isinstance(data, dict):
        return data

    import jsonpath_ng

    found = jsonpath_ng.parse(path).find(data)
    first_datum = first(found)
    if (
        len(found) == 1
        and isinstance(first_datum.value, list)
        and isinstance(first(first_datum.value), dict)
    ):
        data_points = first_datum.value
    elif len(first_datum.path.fields) == 1:
        field_name = first(first_datum.path.fields)
        data_points = [{field_name: datum.value} for datum in found]
    else:
        raise PlotDataStructureError()

    if not isinstance(data_points, list) or not (
        isinstance(first(data_points), dict)
    ):
        raise PlotDataStructureError()

    return data_points


def _lists(dictionary):
    for _, value in dictionary.items():
        if isinstance(value, dict):
            yield from _lists(value)
        elif isinstance(value, list):
            yield value


def _find_data(data, fields=None, **kwargs):
    if not isinstance(data, dict):
        return data

    if not fields:
        # just look for first list of dicts
        fields = set()

    for lst in _lists(data):
        if (
            all(isinstance(dp, dict) for dp in lst)
            and set(first(lst).keys()) & fields == fields
        ):
            return lst
    raise PlotDataStructureError()


def _append_index(data_points, append_index=False, **kwargs):
    if not append_index or PlotData.INDEX_FIELD in first(data_points).keys():
        return data_points

    for index, data_point in enumerate(data_points):
        data_point[PlotData.INDEX_FIELD] = index
    return data_points


def _append_revision(data_points, revision, **kwargs):
    for data_point in data_points:
        data_point[PlotData.REVISION_FIELD] = revision
    return data_points


class PlotData:
    REVISION_FIELD = "rev"
    INDEX_FIELD = "step"

    def __init__(self, filename, revision, content, **kwargs):
        self.filename = filename
        self.revision = revision
        self.content = content

    def _processors(self):
        return [_filter_fields, _append_index, _append_revision]

    def to_datapoints(self, **kwargs):
        data = self.content

        for data_proc in self._processors():
            data = data_proc(
                data, filename=self.filename, revision=self.revision, **kwargs
            )
        return data


class JSONPlotData(PlotData):
    def _processors(self):
        parent_processors = super()._processors()
        return [_apply_path, _find_data] + parent_processors


class CSVPlotData(PlotData):
    pass


class YAMLPlotData(PlotData):
    def _processors(self):
        parent_processors = super()._processors()
        return [_find_data] + parent_processors
