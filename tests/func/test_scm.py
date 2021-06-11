import os
import sys

import pytest
from git import Repo

from dvc.scm import SCM, Git, NoSCM
from dvc.scm.base import SCMError
from dvc.system import System
from tests.basic_env import TestGit, TestGitSubmodule
from tests.utils import get_gitignore_content


def test_init_none(tmp_dir):
    assert isinstance(SCM(os.fspath(tmp_dir), no_scm=True), NoSCM)


def test_init_git(tmp_dir):
    Repo.init(os.fspath(tmp_dir))
    assert isinstance(SCM(os.fspath(tmp_dir)), Git)


def test_init_no_git(tmp_dir):
    with pytest.raises(SCMError):
        SCM(os.fspath(tmp_dir))


def test_init_sub_dir(tmp_dir):
    Repo.init(os.fspath(tmp_dir))
    subdir = tmp_dir / "dir"
    subdir.mkdir()

    scm = SCM(os.fspath(subdir))
    assert scm.root_dir == os.fspath(tmp_dir)


class TestSCMGit(TestGit):
    def test_commit(self):
        G = Git(self._root_dir)
        G.add(["foo"])
        G.commit("add")
        self.assertIn("foo", self.git.git.ls_files())

    def test_is_tracked(self):
        foo = os.path.abspath(self.FOO)
        G = Git(self._root_dir)
        G.add([self.FOO, self.UNICODE])
        self.assertTrue(G.is_tracked(foo))
        self.assertTrue(G.is_tracked(self.FOO))
        self.assertTrue(G.is_tracked(self.UNICODE))
        G.commit("add")
        self.assertTrue(G.is_tracked(foo))
        self.assertTrue(G.is_tracked(self.FOO))
        G.gitpython.repo.index.remove([self.FOO], working_tree=True)
        self.assertFalse(G.is_tracked(foo))
        self.assertFalse(G.is_tracked(self.FOO))
        self.assertFalse(G.is_tracked("not-existing-file"))


class TestSCMGitSubmodule(TestGitSubmodule):
    def test_git_submodule(self):
        self.assertIsInstance(SCM(os.curdir), Git)

    def test_commit_in_submodule(self):
        G = Git(self._root_dir)
        G.add(["foo"])
        G.commit("add")
        self.assertTrue("foo" in self.git.git.ls_files())


def _count_gitignore_entries(line):
    lines = get_gitignore_content()
    return lines.count(line)


def test_ignore(tmp_dir, scm):
    foo = os.fspath(tmp_dir / "foo")
    target = "/foo"

    scm.ignore(foo)
    assert (tmp_dir / ".gitignore").is_file()
    scm._reset()
    assert _count_gitignore_entries(target) == 1

    scm.ignore(foo)
    assert (tmp_dir / ".gitignore").is_file()
    scm._reset()
    assert _count_gitignore_entries(target) == 1

    scm.ignore_remove(foo)
    assert not (tmp_dir / ".gitignore").exists()


def test_ignored(tmp_dir, scm):
    tmp_dir.gen({"dir1": {"file1.jpg": "cont", "file2.txt": "cont"}})
    tmp_dir.gen({".gitignore": "dir1/*.jpg"})

    assert scm.is_ignored(tmp_dir / "dir1" / "file1.jpg")
    assert not scm.is_ignored(tmp_dir / "dir1" / "file2.txt")


def test_get_gitignore(tmp_dir, scm):
    tmp_dir.gen({"file1": "contents", "dir": {}})

    data_dir = os.fspath(tmp_dir / "file1")
    entry, gitignore = scm._get_gitignore(data_dir)
    assert entry == "/file1"
    assert gitignore == os.fspath(tmp_dir / ".gitignore")

    data_dir = os.fspath(tmp_dir / "dir")
    entry, gitignore = scm._get_gitignore(data_dir)

    assert entry == "/dir"
    assert gitignore == os.fspath(tmp_dir / ".gitignore")


def test_get_gitignore_symlink(tmp_dir, scm):
    tmp_dir.gen({"dir": {"subdir": {"data": "contents"}}})
    link = os.fspath(tmp_dir / "link")
    target = os.fspath(tmp_dir / "dir" / "subdir" / "data")
    System.symlink(target, link)
    entry, gitignore = scm._get_gitignore(link)
    assert entry == "/link"
    assert gitignore == os.fspath(tmp_dir / ".gitignore")


def test_get_gitignore_subdir(tmp_dir, scm):
    tmp_dir.gen({"dir1": {"file1": "cont", "dir2": {}}})

    data_dir = os.fspath(tmp_dir / "dir1" / "file1")
    entry, gitignore = scm._get_gitignore(data_dir)
    assert entry == "/file1"
    assert gitignore == os.fspath(tmp_dir / "dir1" / ".gitignore")

    data_dir = os.fspath(tmp_dir / "dir1" / "dir2")
    entry, gitignore = scm._get_gitignore(data_dir)
    assert entry == "/dir2"
    assert gitignore == os.fspath(tmp_dir / "dir1" / ".gitignore")


def test_gitignore_should_end_with_newline(tmp_dir, scm):
    tmp_dir.gen({"foo": "foo", "bar": "bar"})

    foo = os.fspath(tmp_dir / "foo")
    bar = os.fspath(tmp_dir / "bar")
    gitignore = tmp_dir / ".gitignore"

    scm.ignore(foo)
    assert gitignore.read_text().endswith("\n")

    scm.ignore(bar)
    assert gitignore.read_text().endswith("\n")


def test_gitignore_should_append_newline_to_gitignore(tmp_dir, scm):
    tmp_dir.gen({"foo": "foo", "bar": "bar"})

    bar_path = os.fspath(tmp_dir / "bar")
    gitignore = tmp_dir / ".gitignore"

    gitignore.write_text("/foo")
    assert not gitignore.read_text().endswith("\n")

    scm.ignore(bar_path)
    contents = gitignore.read_text()
    assert gitignore.read_text().endswith("\n")

    assert contents.splitlines() == ["/foo", "/bar"]


def test_git_detach_head(tmp_dir, scm):
    tmp_dir.scm_gen({"file": "0"}, commit="init")
    init_rev = scm.get_rev()

    with scm.detach_head() as rev:
        assert init_rev == rev
        assert init_rev == (tmp_dir / ".git" / "HEAD").read_text().strip()
    assert (
        "ref: refs/heads/master"
        == (tmp_dir / ".git" / "HEAD").read_text().strip()
    )


def test_git_stash_workspace(tmp_dir, scm):
    tmp_dir.scm_gen({"file": "0"}, commit="init")
    tmp_dir.gen("file", "1")

    with scm.stash_workspace():
        assert not scm.is_dirty()
        assert "0" == (tmp_dir / "file").read_text()
    assert scm.is_dirty()
    assert "1" == (tmp_dir / "file").read_text()


@pytest.mark.parametrize(
    "ref, include_untracked",
    [
        (None, True),
        (None, False),
        ("refs/foo/stash", True),
        ("refs/foo/stash", False),
    ],
)
def test_git_stash_push(tmp_dir, scm, ref, include_untracked):
    from dvc.scm.git import Stash

    tmp_dir.scm_gen({"file": "0"}, commit="init")
    tmp_dir.gen({"file": "1", "untracked": "0"})

    stash = Stash(scm, ref=ref)
    rev = stash.push(include_untracked=include_untracked)
    assert rev == scm.get_ref(stash.ref)
    assert "0" == (tmp_dir / "file").read_text()
    assert include_untracked != (tmp_dir / "untracked").exists()
    assert len(stash) == 1

    stash.apply(rev)
    assert "1" == (tmp_dir / "file").read_text()
    assert "0" == (tmp_dir / "untracked").read_text()

    parts = list(stash.ref.split("/"))
    assert os.path.exists(os.path.join(os.fspath(tmp_dir), ".git", *parts))
    assert os.path.exists(
        os.path.join(os.fspath(tmp_dir), ".git", "logs", *parts)
    )


@pytest.mark.parametrize("ref", [None, "refs/foo/stash"])
def test_git_stash_drop(tmp_dir, scm, ref):
    from dvc.scm.git import Stash

    tmp_dir.scm_gen({"file": "0"}, commit="init")
    tmp_dir.gen("file", "1")

    stash = Stash(scm, ref=ref)
    stash.push()

    tmp_dir.gen("file", "2")
    expected = stash.push()

    stash.drop(1)
    assert expected == scm.get_ref(stash.ref)
    assert len(stash) == 1


reason = """libgit2 stash_save() is flaky on linux when run inside pytest
    https://github.com/iterative/dvc/pull/5286#issuecomment-792574294"""


@pytest.mark.parametrize(
    "ref",
    [
        pytest.param(
            None,
            marks=pytest.mark.xfail(
                sys.platform == "linux", raises=AssertionError, reason=reason
            ),
        ),
        "refs/foo/stash",
    ],
)
def test_git_stash_pop(tmp_dir, scm, ref):
    from dvc.scm.git import Stash

    tmp_dir.scm_gen({"file": "0"}, commit="init")
    tmp_dir.gen("file", "1")

    stash = Stash(scm, ref=ref)
    first = stash.push()

    tmp_dir.gen("file", "2")
    second = stash.push()

    assert second == stash.pop()
    assert len(stash) == 1
    assert first == scm.get_ref(stash.ref)
    assert "2" == (tmp_dir / "file").read_text()


@pytest.mark.parametrize("ref", [None, "refs/foo/stash"])
def test_git_stash_clear(tmp_dir, scm, ref):
    from dvc.scm.git import Stash

    tmp_dir.scm_gen({"file": "0"}, commit="init")
    tmp_dir.gen("file", "1")

    stash = Stash(scm, ref=ref)
    stash.push()

    tmp_dir.gen("file", "2")
    stash.push()

    stash.clear()
    assert len(stash) == 0

    parts = list(stash.ref.split("/"))
    assert not os.path.exists(os.path.join(os.fspath(tmp_dir), ".git", *parts))
    # NOTE: some backends will completely remove reflog file on clear, some
    # will only truncate it, either case means an empty stash
    log_path = os.path.join(os.fspath(tmp_dir), ".git", "logs", *parts)
    assert not os.path.exists(log_path) or not open(log_path).read()
