import os

import pytest

from dvc.dvcfile import Dvcfile
from dvc.exceptions import InvalidArgumentError
from tests.unit.fs.test_repo import make_subrepo


@pytest.mark.parametrize("cached", [True, False])
def test_update_import(tmp_dir, dvc, erepo_dir, cached):
    gen = erepo_dir.dvc_gen if cached else erepo_dir.scm_gen

    with erepo_dir.branch("branch", new=True), erepo_dir.chdir():
        gen(
            {
                "version": "branch",
                "dir": {"version": "branch", "subdir": {"file": "file"}},
            },
            commit="add version file",
        )
        old_rev = erepo_dir.scm.get_rev()

    stage = dvc.imp(os.fspath(erepo_dir), "version", "version", rev="branch")
    dir_stage = dvc.imp(os.fspath(erepo_dir), "dir", "dir", rev="branch")
    assert dvc.status() == {}

    assert (tmp_dir / "version").read_text() == "branch"
    assert (tmp_dir / "dir").read_text() == {
        "version": "branch",
        "subdir": {"file": "file"},
    }
    assert stage.deps[0].def_repo["rev_lock"] == old_rev
    assert dir_stage.deps[0].def_repo["rev_lock"] == old_rev

    # Update version file
    with erepo_dir.branch("branch", new=False), erepo_dir.chdir():
        gen(
            {
                "version": "updated",
                "dir": {"version": "updated", "subdir": {"file": "file"}},
            },
            commit="update version content",
        )
        new_rev = erepo_dir.scm.get_rev()

    assert old_rev != new_rev

    assert dvc.status() == {
        "dir.dvc": [
            {
                "changed deps": {
                    f"dir ({os.fspath(erepo_dir)})": "update available"
                }
            }
        ],
        "version.dvc": [
            {
                "changed deps": {
                    f"version ({os.fspath(erepo_dir)})": "update available"
                }
            }
        ],
    }

    (stage,) = dvc.update(stage.path)
    (dir_stage,) = dvc.update(dir_stage.path)
    assert dvc.status() == {}

    assert (tmp_dir / "version").read_text() == "updated"
    assert (tmp_dir / "dir").read_text() == {
        "version": "updated",
        "subdir": {"file": "file"},
    }

    assert stage.deps[0].def_repo["rev_lock"] == new_rev
    assert dir_stage.deps[0].def_repo["rev_lock"] == new_rev


def test_update_import_after_remote_updates_to_dvc(tmp_dir, dvc, erepo_dir):
    old_rev = None
    with erepo_dir.branch("branch", new=True), erepo_dir.chdir():
        erepo_dir.scm_gen("version", "branch", commit="add version file")
        old_rev = erepo_dir.scm.get_rev()

    stage = dvc.imp(os.fspath(erepo_dir), "version", "version", rev="branch")

    imported = tmp_dir / "version"
    assert imported.is_file()
    assert imported.read_text() == "branch"
    assert stage.deps[0].def_repo == {
        "url": os.fspath(erepo_dir),
        "rev": "branch",
        "rev_lock": old_rev,
    }

    new_rev = None
    with erepo_dir.branch("branch", new=False), erepo_dir.chdir():
        erepo_dir.scm.gitpython.repo.index.remove(["version"])
        erepo_dir.dvc_gen(
            "version", "updated", commit="upgrade to DVC tracking"
        )
        new_rev = erepo_dir.scm.get_rev()

    assert old_rev != new_rev

    (status,) = dvc.status([stage.path])["version.dvc"]
    (changed_dep,) = list(status["changed deps"].items())
    assert changed_dep[0].startswith("version ")
    assert changed_dep[1] == "update available"

    dvc.update([stage.path])

    assert dvc.status([stage.path]) == {}

    assert imported.is_file()
    assert imported.read_text() == "updated"

    stage = Dvcfile(dvc, stage.path).stage
    assert stage.deps[0].def_repo == {
        "url": os.fspath(erepo_dir),
        "rev": "branch",
        "rev_lock": new_rev,
    }


def test_update_before_and_after_dvc_init(tmp_dir, dvc, git_dir):
    with git_dir.chdir():
        git_dir.scm_gen("file", "first version", commit="first version")
        old_rev = git_dir.scm.get_rev()

    stage = dvc.imp(os.fspath(git_dir), "file", "file")

    with git_dir.chdir():
        git_dir.init(dvc=True)
        git_dir.scm.gitpython.repo.index.remove(["file"])
        os.remove("file")
        git_dir.dvc_gen("file", "second version", commit="with dvc")
        new_rev = git_dir.scm.get_rev()

    assert old_rev != new_rev

    assert dvc.status([stage.path]) == {
        "file.dvc": [
            {
                "changed deps": {
                    "file ({})".format(os.fspath(git_dir)): "update available"
                }
            }
        ]
    }

    dvc.update([stage.path])

    assert (tmp_dir / "file").read_text() == "second version"
    assert dvc.status([stage.path]) == {}


@pytest.mark.parametrize(
    "workspace",
    [
        pytest.lazy_fixture("local_cloud"),
        pytest.lazy_fixture("s3"),
        pytest.param(
            pytest.lazy_fixture("gs"), marks=pytest.mark.needs_internet
        ),
        pytest.lazy_fixture("hdfs"),
        pytest.lazy_fixture("webhdfs"),
        pytest.param(
            pytest.lazy_fixture("ssh"),
            marks=pytest.mark.skipif(
                os.name == "nt", reason="disabled on windows"
            ),
        ),
    ],
    indirect=True,
)
def test_update_import_url(tmp_dir, dvc, workspace):
    workspace.gen("file", "file content")

    dst = tmp_dir / "imported_file"
    stage = dvc.imp_url("remote://workspace/file", os.fspath(dst))

    assert dst.is_file()
    assert dst.read_text() == "file content"

    # update data
    workspace.gen("file", "updated file content")

    assert dvc.status([stage.path]) == {}
    dvc.update([stage.path])
    assert dvc.status([stage.path]) == {}

    assert dst.is_file()
    assert dst.read_text() == "updated file content"


def test_update_rev(tmp_dir, dvc, scm, git_dir):
    with git_dir.chdir():
        git_dir.scm_gen({"foo": "foo"}, commit="first")

    dvc.imp(os.fspath(git_dir), "foo")
    assert (tmp_dir / "foo.dvc").exists()

    with git_dir.chdir(), git_dir.branch("branch1", new=True):
        git_dir.scm_gen({"foo": "foobar"}, commit="branch1 commit")
        branch1_head = git_dir.scm.get_rev()

    with git_dir.chdir(), git_dir.branch("branch2", new=True):
        git_dir.scm_gen({"foo": "foobar foo"}, commit="branch2 commit")
        branch2_head = git_dir.scm.get_rev()

    stage = dvc.update(["foo.dvc"], rev="branch1")[0]
    assert stage.deps[0].def_repo == {
        "url": os.fspath(git_dir),
        "rev": "branch1",
        "rev_lock": branch1_head,
    }
    with open(tmp_dir / "foo") as f:
        assert "foobar" == f.read()

    stage = dvc.update(["foo.dvc"], rev="branch2")[0]
    assert stage.deps[0].def_repo == {
        "url": os.fspath(git_dir),
        "rev": "branch2",
        "rev_lock": branch2_head,
    }
    with open(tmp_dir / "foo") as f:
        assert "foobar foo" == f.read()


def test_update_recursive(tmp_dir, dvc, erepo_dir):
    with erepo_dir.branch("branch", new=True), erepo_dir.chdir():
        erepo_dir.scm_gen(
            {"foo1": "text1", "foo2": "text2", "foo3": "text3"},
            commit="add foo files",
        )
        old_rev = erepo_dir.scm.get_rev()

    tmp_dir.gen({"dir": {"subdir": {}}})
    stage1 = dvc.imp(
        os.fspath(erepo_dir), "foo1", os.path.join("dir", "foo1"), rev="branch"
    )
    stage2 = dvc.imp(
        os.fspath(erepo_dir),
        "foo2",
        os.path.join("dir", "subdir", "foo2"),
        rev="branch",
    )
    stage3 = dvc.imp(
        os.fspath(erepo_dir),
        "foo3",
        os.path.join("dir", "subdir", "foo3"),
        rev="branch",
    )

    assert (tmp_dir / os.path.join("dir", "foo1")).read_text() == "text1"
    assert (
        tmp_dir / os.path.join("dir", "subdir", "foo2")
    ).read_text() == "text2"
    assert (
        tmp_dir / os.path.join("dir", "subdir", "foo3")
    ).read_text() == "text3"

    assert stage1.deps[0].def_repo["rev_lock"] == old_rev
    assert stage2.deps[0].def_repo["rev_lock"] == old_rev
    assert stage3.deps[0].def_repo["rev_lock"] == old_rev

    with erepo_dir.branch("branch", new=False), erepo_dir.chdir():
        erepo_dir.scm_gen(
            {"foo1": "updated1", "foo2": "updated2", "foo3": "updated3"},
            "",
            "update foo content",
        )
        new_rev = erepo_dir.scm.get_rev()

    assert old_rev != new_rev

    dvc.update(["dir"], recursive=True)

    stage1 = Dvcfile(dvc, stage1.path).stage
    stage2 = Dvcfile(dvc, stage2.path).stage
    stage3 = Dvcfile(dvc, stage3.path).stage
    assert stage1.deps[0].def_repo["rev_lock"] == new_rev
    assert stage2.deps[0].def_repo["rev_lock"] == new_rev
    assert stage3.deps[0].def_repo["rev_lock"] == new_rev


@pytest.mark.parametrize("is_dvc", [True, False])
def test_update_from_subrepos(tmp_dir, dvc, erepo_dir, is_dvc):
    subrepo = erepo_dir / "subrepo"
    make_subrepo(subrepo, erepo_dir.scm)
    gen = subrepo.dvc_gen if is_dvc else subrepo.scm_gen
    with subrepo.chdir():
        gen("foo", "foo", commit="subrepo initial")

    path = os.path.join("subrepo", "foo")
    repo_path = os.fspath(erepo_dir)
    dvc.imp(repo_path, path, out="out")
    assert dvc.status() == {}

    with subrepo.chdir():
        gen("foo", "foobar", commit="subrepo second commit")

    assert dvc.status()["out.dvc"][0]["changed deps"] == {
        f"{path} ({repo_path})": "update available"
    }
    (stage,) = dvc.update(["out.dvc"])

    assert (tmp_dir / "out").read_text() == "foobar"
    assert stage.deps[0].def_path == os.path.join("subrepo", "foo")
    assert stage.deps[0].def_repo == {
        "url": repo_path,
        "rev_lock": erepo_dir.scm.get_rev(),
    }


def test_update_import_to_remote(tmp_dir, dvc, erepo_dir, local_remote):
    erepo_dir.scm_gen({"foo": "foo"}, commit="add foo")
    stage = dvc.imp(os.fspath(erepo_dir), "foo")
    erepo_dir.scm_gen({"foo": "bar"}, commit="update foo")
    with pytest.raises(InvalidArgumentError):
        dvc.update(stage.path, to_remote=True)


@pytest.mark.parametrize(
    "workspace",
    [
        pytest.lazy_fixture("local_cloud"),
        pytest.lazy_fixture("s3"),
        pytest.param(
            pytest.lazy_fixture("gs"), marks=pytest.mark.needs_internet
        ),
        pytest.lazy_fixture("hdfs"),
    ],
    indirect=True,
)
def test_update_import_url_to_remote(tmp_dir, dvc, workspace, local_remote):
    workspace.gen("foo", "foo")
    stage = dvc.imp_url("remote://workspace/foo", to_remote=True)

    workspace.gen("foo", "bar")
    stage = dvc.update(stage.path, to_remote=True)

    dvc.pull("foo")
    assert (tmp_dir / "foo").read_text() == "bar"


@pytest.mark.parametrize(
    "workspace",
    [
        pytest.lazy_fixture("local_cloud"),
        pytest.lazy_fixture("s3"),
        pytest.param(
            pytest.lazy_fixture("gs"), marks=pytest.mark.needs_internet
        ),
        pytest.lazy_fixture("hdfs"),
    ],
    indirect=True,
)
def test_update_import_url_to_remote_directory(
    mocker, tmp_dir, dvc, workspace, local_remote
):
    workspace.gen({"data": {"foo": "foo", "bar": {"baz": "baz"}}})
    stage = dvc.imp_url("remote://workspace/data", to_remote=True)

    workspace.gen(
        {
            "data": {
                "foo2": "foo2",
                "bar": {"baz2": "baz2"},
                "repeated_hashes": {
                    "foo": "foo",
                    "baz": "baz",
                    "foo_with_different_name": "foo",
                },
            }
        }
    )

    upload_file_mock = mocker.spy(type(dvc.odb.local.fs), "upload_fobj")
    stage = dvc.update(stage.path, to_remote=True)

    dvc.pull("data")
    assert (tmp_dir / "data").read_text() == {
        "foo": "foo",
        "foo2": "foo2",
        "bar": {"baz": "baz", "baz2": "baz2"},
        "repeated_hashes": {
            "foo": "foo",
            "baz": "baz",
            "foo_with_different_name": "foo",
        },
    }
    # 2 new hashes (foo2, baz2) + 1 .dir hash
    assert upload_file_mock.mock.call_count == 3


def test_update_import_url_to_remote_directory_changed_contents(
    tmp_dir, dvc, local_cloud, local_remote
):
    local_cloud.gen({"data": {"foo": "foo", "bar": {"baz": "baz"}}})
    stage = dvc.imp_url("remote://upstream/data", to_remote=True)

    local_cloud.gen(
        {"data": {"foo": "not_foo", "foo2": "foo", "bar": {"baz2": "baz2"}}}
    )
    stage = dvc.update(stage.path, to_remote=True)

    dvc.pull("data")
    assert (tmp_dir / "data").read_text() == {
        "foo": "not_foo",
        "foo2": "foo",
        "bar": {"baz": "baz", "baz2": "baz2"},
    }


def test_update_import_url_to_remote_directory_same_hash(
    tmp_dir, dvc, local_cloud, local_remote
):
    local_cloud.gen(
        {"data": {"foo": "foo", "bar": {"baz": "baz"}, "same": "same"}}
    )
    stage = dvc.imp_url("remote://upstream/data", to_remote=True)

    local_cloud.gen(
        {"data": {"foo": "baz", "bar": {"baz": "foo"}, "same": "same"}}
    )
    stage = dvc.update(stage.path, to_remote=True)

    dvc.pull("data")
    assert (tmp_dir / "data").read_text() == {
        "foo": "baz",
        "bar": {"baz": "foo"},
        "same": "same",
    }
