import os

import pytest


@pytest.fixture
def run_copy_metrics(tmp_dir, run_copy):
    def run(file1, file2, commit=None, tag=None, single_stage=True, **kwargs):
        stage = tmp_dir.dvc.run(
            cmd=f"python copy.py {file1} {file2}",
            deps=[file1],
            single_stage=single_stage,
            **kwargs,
        )

        if hasattr(tmp_dir.dvc, "scm"):
            files = [stage.path]
            files += [
                os.fspath(out.path_info)
                for out in stage.outs
                if not out.use_cache
            ]
            tmp_dir.dvc.scm.add(files)
            if commit:
                tmp_dir.dvc.scm.commit(commit)
            if tag:
                tmp_dir.dvc.scm.tag(tag)
        return stage

    return run
