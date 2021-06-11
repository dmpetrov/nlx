import os

import pytest

from dvc.exceptions import DvcException
from dvc.main import main
from dvc.stage.exceptions import StageFileDoesNotExistError
from dvc.system import System
from dvc.utils.fs import remove
from tests.utils import get_gitignore_content


@pytest.mark.parametrize("remove_outs", [True, False])
def test_remove(tmp_dir, scm, dvc, run_copy, remove_outs):
    (stage1,) = tmp_dir.dvc_gen("foo", "foo")
    stage2 = run_copy("foo", "bar", single_stage=True)
    stage3 = run_copy("bar", "foobar", name="copy-bar-foobar")

    assert "/foo" in get_gitignore_content()
    assert "/bar" in get_gitignore_content()
    assert "/foobar" in get_gitignore_content()

    for stage in [stage1, stage2, stage3]:
        dvc.remove(stage.addressing, outs=remove_outs)
        out_exists = (out.exists for out in stage.outs)
        assert stage not in dvc.stage.collect_repo()
        if remove_outs:
            assert not any(out_exists)
        else:
            assert all(out_exists)

    assert not (tmp_dir / ".gitignore").exists()


def test_remove_non_existent_file(tmp_dir, dvc):
    with pytest.raises(StageFileDoesNotExistError):
        dvc.remove("non_existent_dvc_file.dvc")
    with pytest.raises(StageFileDoesNotExistError):
        dvc.remove("non_existent_stage_name")


def test_remove_broken_symlink(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    dvc.odb.local.cache_types = ["symlink"]

    (stage,) = dvc.add("foo")
    remove(dvc.odb.local.cache_dir)
    assert System.is_symlink("foo")

    with pytest.raises(DvcException):
        dvc.remove(stage.addressing)
    assert os.path.lexists("foo")
    assert (tmp_dir / stage.relpath).exists()

    dvc.remove(stage.addressing, outs=True)
    assert not os.path.lexists("foo")
    assert not (tmp_dir / stage.relpath).exists()


def test_cmd_remove(tmp_dir, dvc):
    assert main(["remove", "non-existing-dvc-file"]) == 1

    (stage,) = tmp_dir.dvc_gen("foo", "foo")
    assert main(["remove", stage.addressing]) == 0
    assert not (tmp_dir / stage.relpath).exists()
    assert (tmp_dir / "foo").exists()

    (stage,) = tmp_dir.dvc_gen("foo", "foo")
    assert main(["remove", stage.addressing, "--outs"]) == 0
    assert not (tmp_dir / stage.relpath).exists()
    assert not (tmp_dir / "foo").exists()


def test_cmd_remove_gitignore_single_stage(tmp_dir, scm, dvc, run_copy):
    stage = dvc.run(name="my", cmd='echo "hello" > out', deps=[], outs=["out"])

    assert (tmp_dir / ".gitignore").exists()

    assert main(["remove", stage.addressing]) == 0
    assert not (tmp_dir / stage.relpath).exists()
    assert not (stage.dvcfile._lockfile).exists()
    assert not (tmp_dir / ".gitignore").exists()


def test_cmd_remove_gitignore_multistage(tmp_dir, scm, dvc, run_copy):
    (stage,) = tmp_dir.dvc_gen("foo", "foo")
    stage1 = run_copy("foo", "foo1", single_stage=True)
    stage2 = run_copy("foo1", "foo2", name="copy-foo1-foo2")

    assert (tmp_dir / ".gitignore").exists()

    assert main(["remove", stage2.addressing]) == 0
    assert main(["remove", stage1.addressing]) == 0
    assert main(["remove", stage.addressing]) == 0
    assert not (tmp_dir / ".gitignore").exists()
