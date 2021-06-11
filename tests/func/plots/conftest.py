import shutil

import pytest


@pytest.fixture()
def custom_template(tmp_dir, dvc):
    template = tmp_dir / "custom_template.json"
    shutil.copy(tmp_dir / ".dvc" / "plots" / "default.json", template)
    return template
