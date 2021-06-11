from dvc.cli import parse_args
from dvc.command.checkout import CmdCheckout, log_changes


def test_checkout(tmp_dir, dvc, mocker):
    cli_args = parse_args(
        ["checkout", "foo.dvc", "bar.dvc", "--relink", "--with-deps"]
    )
    assert cli_args.func == CmdCheckout

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.Repo.checkout")

    assert cmd.run() == 0
    m.assert_called_once_with(
        targets=["foo.dvc", "bar.dvc"],
        force=False,
        recursive=False,
        relink=True,
        with_deps=True,
    )


def test_log_changes(capsys):
    stats = {
        "added": ["file1", "dir1/"],
        "deleted": ["dir2/"],
        "modified": ["file2"],
    }

    from itertools import zip_longest

    def _assert_output(stats, expected_outs):
        log_changes(stats)
        out, _ = capsys.readouterr()
        actual_output = out.splitlines()
        for out, line in zip_longest(expected_outs, actual_output):
            assert out.expandtabs() in line

    _assert_output(stats, ["M\tfile2", "A\tfile1", "A\tdir1/", "D\tdir2/"])

    del stats["deleted"][0]
    _assert_output(stats, ["M\tfile2", "A\tfile1", "A\tdir1/"])

    del stats["modified"]
    _assert_output(stats, ["A\tfile1", "A\tdir1/"])
