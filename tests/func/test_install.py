import os
import pathlib
import sys

import pytest
from git import GitCommandError

from dvc.exceptions import GitHookAlreadyExistsError
from dvc.utils import file_md5


@pytest.mark.skipif(
    sys.platform == "win32", reason="Git hooks aren't supported on Windows"
)
class TestInstall:
    def _hook(self, name):
        return pathlib.Path(".git") / "hooks" / name

    def test_create_hooks(self, scm, dvc):
        scm.install()

        hooks_with_commands = [
            ("post-checkout", "exec dvc git-hook post-checkout"),
            ("pre-commit", "exec dvc git-hook pre-commit"),
            ("pre-push", "exec dvc git-hook pre-push"),
        ]

        for fname, command in hooks_with_commands:
            hook_path = self._hook(fname)
            assert hook_path.is_file()
            assert command in hook_path.read_text()

    def test_fail_if_hook_exists(self, scm):
        self._hook("post-checkout").write_text("hook content")

        with pytest.raises(GitHookAlreadyExistsError):
            scm.install()

    def test_pre_commit_hook(self, tmp_dir, scm, dvc, caplog):
        tmp_dir.dvc_gen("file", "file content", commit="create foo")
        tmp_dir.gen("file", "file modified")
        scm.install()

        # scm.commit bypasses hooks
        with pytest.raises(GitCommandError, match=r"modified:\s*file"):
            scm.gitpython.repo.git.commit(m="file modified")

    def test_post_checkout(self, tmp_dir, scm, dvc):
        tmp_dir.dvc_gen({"file": "file content"}, commit="add")
        os.unlink("file")
        scm.install()

        scm.gitpython.git.checkout("-b", "new_branch")

        assert os.path.isfile("file")

    def test_pre_push_hook(self, tmp_dir, scm, dvc, tmp_path_factory):
        temp = tmp_path_factory.mktemp("external")
        git_remote = temp / "project.git"
        storage_path = temp / "dvc_storage"

        with dvc.config.edit() as conf:
            conf["remote"]["store"] = {"url": os.fspath(storage_path)}
            conf["core"]["remote"] = "store"
        tmp_dir.dvc_gen("file", "file_content", "commit message")

        file_checksum = file_md5("file", dvc.fs)
        expected_storage_path = (
            storage_path / file_checksum[:2] / file_checksum[2:]
        )

        scm.gitpython.repo.clone(os.fspath(git_remote))
        scm.gitpython.repo.create_remote("origin", os.fspath(git_remote))

        scm.install()

        assert not expected_storage_path.is_file()
        scm.gitpython.repo.git.push("origin", "master")
        assert expected_storage_path.is_file()
        assert expected_storage_path.read_text() == "file_content"


@pytest.mark.skipif(
    sys.platform == "win32", reason="Git hooks aren't supported on Windows"
)
def test_merge_driver_no_ancestor(tmp_dir, scm, dvc):
    with tmp_dir.branch("one", new=True):
        tmp_dir.dvc_gen({"data": {"foo": "foo"}}, commit="one: add data")

    scm.checkout("two", create_new=True)
    dvc.checkout()  # keep things in sync

    tmp_dir.dvc_gen({"data": {"bar": "bar"}}, commit="two: add data")

    # installing hook only before merge, as it runs `dvc` commands which makes
    # `checkouts` and `commits` above slower
    scm.install()
    (tmp_dir / ".gitattributes").write_text("*.dvc merge=dvc")

    scm.gitpython.repo.git.merge(
        "one", m="merged", no_gpg_sign=True, no_signoff=True
    )

    # NOTE: dvc shouldn't checkout automatically as it might take a long time
    assert (tmp_dir / "data").read_text() == {"bar": "bar"}
    assert (tmp_dir / "data.dvc").read_text() == (
        "outs:\n"
        "- md5: 5ea40360f5b4ec688df672a4db9c17d1.dir\n"
        "  size: 6\n"
        "  nfiles: 2\n"
        "  path: data\n"
    )

    dvc.checkout("data.dvc")
    assert (tmp_dir / "data").read_text() == {"foo": "foo", "bar": "bar"}


@pytest.mark.skipif(
    sys.platform == "win32", reason="Git hooks aren't supported on Windows"
)
def test_merge_driver(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen({"data": {"master": "master"}}, commit="master: add data")

    with tmp_dir.branch("one", new=True):
        tmp_dir.dvc_gen({"data": {"one": "one"}}, commit="one: add data")

    scm.checkout("two", create_new=True)
    dvc.checkout()  # keep things in sync

    tmp_dir.dvc_gen({"data": {"two": "two"}}, commit="two: add data")

    # installing hook only before merge, as it runs `dvc` commands on
    # `checkouts` and `commits` which slows tests down
    scm.install()
    (tmp_dir / ".gitattributes").write_text("*.dvc merge=dvc")

    scm.gitpython.repo.git.merge(
        "one", m="merged", no_gpg_sign=True, no_signoff=True
    )

    # NOTE: dvc shouldn't checkout automatically as it might take a long time
    assert (tmp_dir / "data").read_text() == {"master": "master", "two": "two"}
    assert (tmp_dir / "data.dvc").read_text() == (
        "outs:\n"
        "- md5: 839ef9371606817569c1ee0e5f4ed233.dir\n"
        "  size: 12\n"
        "  nfiles: 3\n"
        "  path: data\n"
    )

    dvc.checkout("data.dvc")
    assert (tmp_dir / "data").read_text() == {
        "master": "master",
        "one": "one",
        "two": "two",
    }
