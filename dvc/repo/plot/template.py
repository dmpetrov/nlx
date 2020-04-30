import json
import logging
import os
import re

from funcy import cached_property

from dvc.exceptions import DvcException
from dvc.utils.fs import makedirs


logger = logging.getLogger(__name__)


class TemplateNotFound(DvcException):
    def __init__(self, path):
        super().__init__("Template '{}' not found.".format(path))


class NoDataForTemplateError(DvcException):
    def __init__(self, template_path):
        super().__init__(
            "No data provided for '{}'.".format(os.path.relpath(template_path))
        )


class Template:
    INDENT = 4
    SEPARATORS = (",", ": ")
    EXTENSION = ".json"
    METRIC_DATA_ANCHOR = "<DVC_METRIC_DATA>"
    X_AXIS_ANCHOR = "<DVC_METRIC_X>"
    Y_AXIS_ANCHOR = "<DVC_METRIC_Y>"
    TITLE_ANCHOR = "<DVC_METRIC_TITLE>"

    def __init__(self, templates_dir):
        self.plot_templates_dir = templates_dir

    def dump(self):
        makedirs(self.plot_templates_dir, exist_ok=True)

        if not os.path.exists(self.plot_templates_dir):
            makedirs(self.plot_templates_dir)

        with open(
            os.path.join(
                self.plot_templates_dir, self.TEMPLATE_NAME + self.EXTENSION
            ),
            "w",
        ) as fobj:
            json.dump(
                self.DEFAULT_CONTENT,
                fobj,
                indent=self.INDENT,
                separators=self.SEPARATORS,
            )

    def load_template(self, path):
        try:
            with open(path, "r") as fd:
                return json.load(fd)
        except FileNotFoundError:
            try:
                with open(
                    os.path.join(self.plot_templates_dir, path), "r"
                ) as fd:
                    return json.load(fd)
            except FileNotFoundError:
                raise DvcException("Not in repo nor in defaults")

    @staticmethod
    def get_data_placeholders(template_content):
        regex = re.compile('"<DVC_METRIC_DATA[^>"]*>"')
        return regex.findall(template_content)

    @staticmethod
    def parse_data_placeholders(template_content):
        data_files = {
            Template.get_datafile(m)
            for m in Template.get_data_placeholders(template_content)
        }
        return {df for df in data_files if df}

    @staticmethod
    def get_datafile(placeholder_string):
        return (
            placeholder_string.replace("<", "")
            .replace(">", "")
            .replace('"', "")
            .replace("DVC_METRIC_DATA", "")
            .replace(",", "")
        )

    @staticmethod
    def fill(
        template_path, data, priority_datafile=None, x_field=None, y_field=None
    ):
        with open(template_path, "r") as fobj:
            result_content = fobj.read()

        for placeholder in Template.get_data_placeholders(result_content):
            file = Template.get_datafile(placeholder)

            if not file or priority_datafile:
                key = priority_datafile
            else:
                key = file

            if Template.TITLE_ANCHOR in result_content:
                result_content = result_content.replace(
                    Template.TITLE_ANCHOR, key
                )

            result_content = result_content.replace(
                placeholder,
                json.dumps(
                    data[key],
                    indent=Template.INDENT,
                    separators=Template.SEPARATORS,
                    sort_keys=True,
                ),
            )

        if Template.X_AXIS_ANCHOR in result_content and x_field:
            result_content = result_content.replace(
                Template.X_AXIS_ANCHOR, x_field
            )
        if Template.Y_AXIS_ANCHOR in result_content and y_field:
            result_content = result_content.replace(
                Template.Y_AXIS_ANCHOR, y_field
            )

        return result_content


class DefaultLinearTemplate(Template):
    TEMPLATE_NAME = "default"

    DEFAULT_CONTENT = {
        "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
        "data": {"values": Template.METRIC_DATA_ANCHOR},
        "title": Template.TITLE_ANCHOR,
        "mark": {"type": "line"},
        "encoding": {
            "x": {"field": Template.X_AXIS_ANCHOR, "type": "quantitative"},
            "y": {"field": Template.Y_AXIS_ANCHOR, "type": "quantitative"},
            "color": {"field": "rev", "type": "nominal"},
        },
    }


class DefaultConfusionTemplate(Template):
    TEMPLATE_NAME = "confusion"
    DEFAULT_CONTENT = {
        "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
        "data": {"values": Template.METRIC_DATA_ANCHOR},
        "title": Template.TITLE_ANCHOR,
        "mark": "rect",
        "encoding": {
            "x": {
                "field": Template.X_AXIS_ANCHOR,
                "type": "nominal",
                "sort": "ascending",
            },
            "y": {
                "field": Template.Y_AXIS_ANCHOR,
                "type": "nominal",
                "sort": "ascending",
            },
            "color": {"aggregate": "count", "type": "quantitative"},
            "facet": {"field": "rev", "type": "nominal"},
        },
    }


class DefaultScatterTemplate(Template):
    TEMPLATE_NAME = "scatter"
    DEFAULT_CONTENT = {
        "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
        "data": {"values": Template.METRIC_DATA_ANCHOR},
        "title": Template.TITLE_ANCHOR,
        "mark": "point",
        "encoding": {
            "x": {"field": Template.X_AXIS_ANCHOR, "type": "quantitative"},
            "y": {"field": Template.Y_AXIS_ANCHOR, "type": "quantitative"},
            "color": {"field": "rev", "type": "nominal"},
        },
    }


class PlotTemplates:
    TEMPLATES_DIR = "plot"
    TEMPLATES = [
        DefaultLinearTemplate,
        DefaultConfusionTemplate,
        DefaultScatterTemplate,
    ]

    @cached_property
    def templates_dir(self):
        return os.path.join(self.dvc_dir, self.TEMPLATES_DIR)

    @cached_property
    def default_template(self):
        default_plot_path = os.path.join(self.templates_dir, "default.json")
        if not os.path.exists(default_plot_path):
            raise TemplateNotFound(os.path.relpath(default_plot_path))
        return default_plot_path

    def get_template(self, path):
        t_path = os.path.join(self.templates_dir, path)
        if os.path.exists(t_path):
            return t_path

        all_templates = [
            os.path.join(root, file)
            for root, _, files in os.walk(self.templates_dir)
            for file in files
        ]
        matches = [
            template
            for template in all_templates
            if os.path.splitext(template)[0] == t_path
        ]
        if matches:
            assert len(matches) == 1
            return matches[0]

        raise TemplateNotFound(path)

    def __init__(self, dvc_dir):
        self.dvc_dir = dvc_dir

        if not os.path.exists(self.templates_dir):
            makedirs(self.templates_dir, exist_ok=True)
            for t in self.TEMPLATES:
                t(self.templates_dir).dump()
