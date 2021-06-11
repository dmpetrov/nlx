import os

from dvc.main import main


def test_quiet(tmp_dir, dvc, capsys):
    tmp_dir.dvc_gen("foo", "foo")

    # clear
    capsys.readouterr()

    assert main(["status", "--quiet"]) == 0
    out_err = capsys.readouterr()
    assert not out_err.out
    assert not out_err.err

    tmp_dir.gen("foo", "barr")

    assert main(["status", "--quiet"]) == 1
    out_err = capsys.readouterr()
    assert not out_err.out
    assert not out_err.err


def test_implied_cloud(dvc, mocker):
    mock_status = mocker.patch(
        "dvc.repo.status._cloud_status", return_value=True
    )

    main(["status", "--remote", "something"])
    assert mock_status.called


def test_status_non_dvc_repo_import(tmp_dir, dvc, git_dir):
    with git_dir.branch("branch", new=True):
        git_dir.scm_gen("file", "first version", commit="first version")

    dvc.imp(os.fspath(git_dir), "file", "file", rev="branch")

    assert dvc.status(["file.dvc"]) == {}

    with git_dir.branch("branch", new=False):
        git_dir.scm_gen("file", "second version", commit="update file")

    (status,) = dvc.status(["file.dvc"])["file.dvc"]
    assert status == {
        "changed deps": {f"file ({git_dir})": "update available"}
    }


def test_status_before_and_after_dvc_init(tmp_dir, dvc, git_dir):
    git_dir.scm_gen("file", "first version", commit="first verison")
    old_rev = git_dir.scm.get_rev()

    dvc.imp(os.fspath(git_dir), "file", "file")

    assert dvc.status(["file.dvc"]) == {}

    with git_dir.chdir():
        git_dir.init(dvc=True)
        git_dir.scm.gitpython.repo.index.remove(["file"])
        os.remove("file")
        git_dir.dvc_gen("file", "second version", commit="with dvc")
        new_rev = git_dir.scm.get_rev()

    assert old_rev != new_rev

    (status,) = dvc.status(["file.dvc"])["file.dvc"]
    assert status == {
        "changed deps": {
            "file ({})".format(os.fspath(git_dir)): "update available"
        }
    }


def test_status_on_pipeline_stages(tmp_dir, dvc, run_copy):
    tmp_dir.dvc_gen("foo", "foo")
    stage = run_copy("foo", "bar", name="copy-foo-bar")

    stage.cmd = "  ".join(stage.cmd.split())
    stage.dvcfile._dump_pipeline_file(stage)
    assert dvc.status("copy-foo-bar") == {"copy-foo-bar": ["changed command"]}

    # delete outputs
    (tmp_dir / "bar").unlink()
    assert dvc.status() == {
        "copy-foo-bar": [
            {"changed outs": {"bar": "deleted"}},
            "changed command",
        ]
    }
    (tmp_dir / "foo").unlink()
    assert dvc.status() == {
        "foo.dvc": [{"changed outs": {"foo": "deleted"}}],
        "copy-foo-bar": [
            {"changed deps": {"foo": "deleted"}},
            {"changed outs": {"bar": "deleted"}},
            "changed command",
        ],
    }


def test_status_recursive(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"file": "text1", "subdir": {"file2": "text2"}}})
    stages = dvc.add("dir", recursive=True, no_commit=True)

    assert len(stages) == 2

    assert dvc.status(targets=["dir"], recursive=True) == {
        os.path.join("dir", "file.dvc"): [
            {"changed outs": {os.path.join("dir", "file"): "not in cache"}}
        ],
        os.path.join("dir", "subdir", "file2.dvc"): [
            {
                "changed outs": {
                    os.path.join("dir", "subdir", "file2"): "not in cache"
                }
            }
        ],
    }


def test_status_outputs(tmp_dir, dvc):
    tmp_dir.dvc_gen({"foo": "foo", "bar": "bar"})
    dvc.run(
        outs=["alice", "bob"],
        deps=["foo", "bar"],
        cmd="echo alice>alice && echo bob>bob",
        name="alice_bob",
    )
    tmp_dir.gen({"alice": "new alice", "bob": "new bob"})

    assert dvc.status(targets=["alice_bob"]) == {
        "alice_bob": [
            {"changed outs": {"alice": "modified", "bob": "modified"}}
        ]
    }

    assert dvc.status(targets=["alice"]) == {
        "alice_bob": [{"changed outs": {"alice": "modified"}}]
    }
