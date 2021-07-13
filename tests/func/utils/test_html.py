import pytest

from dvc.utils.html import HTML, PAGE_HTML, MissingPlaceholderError

CUSTOM_PAGE_HTML = """<!DOCTYPE html>
<html>
<head>
    <title>TITLE</title>
    <script type="text/javascript" src="vega"></script>
    <script type="text/javascript" src="vega-lite"></script>
    <script type="text/javascript" src="vega-embed"></script>
</head>
<body>
    {dvc_plots}
</body>
</html>"""


@pytest.mark.parametrize(
    "template,page_elements,expected_page",
    [
        (None, ["content"], PAGE_HTML.format(dvc_plots="content")),
        (
            CUSTOM_PAGE_HTML,
            ["content"],
            CUSTOM_PAGE_HTML.format(dvc_plots="content"),
        ),
    ],
)
def test_html(tmp_dir, template, page_elements, expected_page):
    page = HTML(template)
    page.elements = page_elements

    result = page.embed()

    assert result == expected_page


def test_no_placeholder(tmp_dir):
    template = "<head></head><body></body>"

    with pytest.raises(MissingPlaceholderError):
        HTML(template)
