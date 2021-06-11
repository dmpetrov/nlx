from dvc.dvcfile import PIPELINE_FILE, PIPELINE_LOCK
from dvc.objects.db import ODBManager
from dvc.system import System


def test_destroy(tmp_dir, dvc, run_copy):
    dvc.config["cache"]["type"] = ["symlink"]
    dvc.odb = ODBManager(dvc)

    tmp_dir.dvc_gen("file", "text")
    tmp_dir.dvc_gen({"dir": {"file": "lorem", "subdir/file": "ipsum"}})

    run_copy("file", "file2", single_stage=True)
    run_copy("file2", "file3", name="copy-file2-file3")
    run_copy("file3", "file4", name="copy-file3-file4")

    dvc.destroy()

    # Remove all the files related to DVC
    for path in [
        ".dvc",
        "file.dvc",
        "file2.dvc",
        "dir.dvc",
        PIPELINE_FILE,
        PIPELINE_LOCK,
    ]:
        assert not (tmp_dir / path).exists()

    # Leave the rest of the files
    for path in [
        "file",
        "file2",
        "file3",
        "file4",
        "dir/file",
        "dir/subdir/file",
    ]:
        assert (tmp_dir / path).is_file()

    # Make sure that data was unprotected after `destroy`
    for path in [
        "file",
        "file2",
        "file3",
        "file4",
        "dir",
        "dir/file",
        "dir/subdir",
        "dir/subdir/file",
    ]:
        assert not System.is_symlink(tmp_dir / path)
