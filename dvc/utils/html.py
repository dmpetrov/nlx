from typing import Dict, List, Optional

from dvc.exceptions import DvcException
from dvc.types import StrPath

PAGE_HTML = """<!DOCTYPE html>
<html>
<head>
    <title>DVC Plot</title>
    <script src="https://cdn.jsdelivr.net/npm/vega@5.10.0"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-lite@4.8.1"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-embed@6.5.1"></script>
</head>
<body>
    {plot_divs}
</body>
</html>"""

VEGA_DIV_HTML = """<div id = "{id}"></div>
<script type = "text/javascript">
    var spec = {vega_json};
    vegaEmbed('#{id}', spec);
</script>"""


class MissingPlaceholderError(DvcException):
    def __init__(self, placeholder):
        super().__init__(f"HTML template has to contain '{placeholder}'.")


class HTML:
    PLACEHOLDER = "plot_divs"
    PLACEHOLDER_FORMAT_STR = f"{{{PLACEHOLDER}}}"

    def __init__(self, template: str = None):
        template = template or PAGE_HTML
        if self.PLACEHOLDER_FORMAT_STR not in template:
            raise MissingPlaceholderError(self.PLACEHOLDER_FORMAT_STR)

        self.template = template
        self.elements: List[str] = []

    def with_metrics(self, metrics: Dict[str, Dict]) -> "HTML":
        import tabulate

        header: List[str] = []
        rows: List[List[str]] = []

        for _, rev_data in metrics.items():
            for _, data in rev_data.items():
                if not header:
                    header.extend(sorted(data.keys()))

                rows.append([data[key] for key in header])

        self.elements.append(tabulate.tabulate(rows, header, tablefmt="html"))
        return self

    def with_plots(self, plots: Dict[str, Dict]) -> "HTML":
        self.elements.extend(
            [
                VEGA_DIV_HTML.format(id=f"plot{i}", vega_json=plot)
                for i, plot in enumerate(plots.values())
            ]
        )
        return self

    def with_element(self, html: str) -> "HTML":
        self.elements.append(html)
        return self

    def embed(self) -> str:
        kwargs = {self.PLACEHOLDER: "\n".join(self.elements)}
        return self.template.format(**kwargs)


def write(
    path: StrPath,
    plots: Dict[str, Dict],
    metrics: Optional[Dict[str, Dict]] = None,
    template_path: Optional[StrPath] = None,
):
    page_html = None
    if template_path:
        with open(template_path, "r") as fobj:
            page_html = fobj.read()

    document = HTML(page_html)
    if metrics:
        document.with_metrics(metrics)
        document.with_element("<br>")

    document.with_plots(plots)

    with open(path, "w") as fd:
        fd.write(document.embed())
