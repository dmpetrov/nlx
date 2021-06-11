import os
import textwrap

from dvc.main import main


def test_cache_dir_local(tmp_dir, dvc, capsys, caplog):
    (tmp_dir / ".dvc" / "config.local").write_text(
        textwrap.dedent(
            """\
            [cache]
                dir = some/path
            """
        )
    )
    path = os.path.join(dvc.dvc_dir, "some", "path")

    assert main(["cache", "dir", "--local"]) == 0

    out, _ = capsys.readouterr()
    assert path in out

    assert main(["cache", "dir"]) == 0
    out, _ = capsys.readouterr()
    assert path in out

    assert main(["cache", "dir", "--project"]) == 251
    assert "option 'dir' doesn't exist in section 'cache'" in caplog.text
