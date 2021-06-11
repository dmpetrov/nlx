import os

import pytest

from dvc.scm.base import SCMError


# Behaves the same as SCM but will test against all supported Git backends.
# tmp_dir.scm will still contain a default SCM instance.
@pytest.fixture(params=["gitpython", "dulwich", "pygit2"])
def git(tmp_dir, scm, request):
    from dvc.scm.git import Git

    git_ = Git(os.fspath(tmp_dir), backends=[request.param])
    git_.test_backend = request.param
    yield git_
    git_.close()


@pytest.mark.parametrize(
    "path, expected",
    [
        (os.path.join("path", "to", ".gitignore"), True),
        (os.path.join("path", "to", ".git", "internal", "file"), True),
        (os.path.join("some", "non-.git", "file"), False),
    ],
    ids=["gitignore_file", "git_internal_file", "non_git_file"],
)
def test_belongs_to_scm(scm, path, expected):
    assert scm.belongs_to_scm(path) == expected


def test_walk_with_submodules(tmp_dir, scm, git_dir):
    git_dir.scm_gen(
        {"foo": "foo", "bar": "bar", "dir": {"data": "data"}},
        commit="add dir and files",
    )
    scm.gitpython.repo.create_submodule(
        "submodule", "submodule", url=os.fspath(git_dir)
    )
    scm.commit("added submodule")

    files = []
    dirs = []
    fs = scm.get_fs("HEAD")
    for _, dnames, fnames in fs.walk("."):
        dirs.extend(dnames)
        files.extend(fnames)

    # currently we don't walk through submodules
    assert not dirs
    assert set(files) == {".gitmodules", "submodule"}


def test_walk_onerror(tmp_dir, scm):
    def onerror(exc):
        raise exc

    tmp_dir.scm_gen({"foo": "foo"}, commit="init")
    fs = scm.get_fs("HEAD")

    # path does not exist
    for _ in fs.walk("dir"):
        pass
    with pytest.raises(OSError):
        for _ in fs.walk("dir", onerror=onerror):
            pass

    # path is not a directory
    for _ in fs.walk("foo"):
        pass
    with pytest.raises(OSError):
        for _ in fs.walk("foo", onerror=onerror):
            pass


def test_is_tracked(tmp_dir, scm):
    tmp_dir.scm_gen(
        {
            "tracked": "tracked",
            "dir": {"data": "data", "subdir": {"subdata": "subdata"}},
        },
        commit="add dirs and files",
    )
    tmp_dir.gen({"untracked": "untracked", "dir": {"untracked": "untracked"}})

    # sanity check
    assert (tmp_dir / "untracked").exists()
    assert (tmp_dir / "tracked").exists()
    assert (tmp_dir / "dir" / "untracked").exists()
    assert (tmp_dir / "dir" / "data").exists()
    assert (tmp_dir / "dir" / "subdir" / "subdata").exists()

    assert not scm.is_tracked("untracked")
    assert not scm.is_tracked(os.path.join("dir", "untracked"))

    assert scm.is_tracked("tracked")
    assert scm.is_tracked("dir")
    assert scm.is_tracked(os.path.join("dir", "data"))
    assert scm.is_tracked(os.path.join("dir", "subdir"))
    assert scm.is_tracked(os.path.join("dir", "subdir", "subdata"))


def test_is_tracked_unicode(tmp_dir, scm):
    tmp_dir.scm_gen("ṭṝḁḉḵḗḋ", "tracked", commit="add unicode")
    tmp_dir.gen("ṳṋṭṝḁḉḵḗḋ", "untracked")
    assert scm.is_tracked("ṭṝḁḉḵḗḋ")
    assert not scm.is_tracked("ṳṋṭṝḁḉḵḗḋ")


def test_no_commits(tmp_dir):
    from dvc.scm.git import Git
    from tests.dir_helpers import git_init

    git_init(".")
    assert Git().no_commits

    tmp_dir.gen("foo", "foo")
    Git().add(["foo"])
    Git().commit("foo")

    assert not Git().no_commits


def test_branch_revs(tmp_dir, scm):
    tmp_dir.scm_gen({"file": "0"}, commit="init")
    init_rev = scm.get_rev()

    expected = []
    for i in range(1, 5):
        tmp_dir.scm_gen({"file": f"{i}"}, commit=f"{i}")
        expected.append(scm.get_rev())

    for rev in scm.branch_revs("master", init_rev):
        assert rev == expected.pop()
    assert len(expected) == 0


def test_set_ref(tmp_dir, git):
    tmp_dir.scm_gen({"file": "0"}, commit="init")
    init_rev = tmp_dir.scm.get_rev()
    tmp_dir.scm_gen({"file": "1"}, commit="commit")
    commit_rev = tmp_dir.scm.get_rev()

    git.set_ref("refs/foo/bar", init_rev)
    assert (
        init_rev
        == (tmp_dir / ".git" / "refs" / "foo" / "bar").read_text().strip()
    )

    with pytest.raises(SCMError):
        git.set_ref("refs/foo/bar", commit_rev, old_ref=commit_rev)
    git.set_ref("refs/foo/bar", commit_rev, old_ref=init_rev)
    assert (
        commit_rev
        == (tmp_dir / ".git" / "refs" / "foo" / "bar").read_text().strip()
    )

    git.set_ref("refs/foo/baz", "refs/heads/master", symbolic=True)
    assert (
        "ref: refs/heads/master"
        == (tmp_dir / ".git" / "refs" / "foo" / "baz").read_text().strip()
    )


def test_set_ref_with_message(tmp_dir, git):
    tmp_dir.scm_gen({"file": "0"}, commit="init")
    init_rev = tmp_dir.scm.get_rev()
    tmp_dir.scm_gen({"file": "1"}, commit="commit")
    commit_rev = tmp_dir.scm.get_rev()

    git.set_ref("refs/foo/bar", init_rev, message="init message")
    assert (
        "init message"
        in (tmp_dir / ".git" / "logs" / "refs" / "foo" / "bar").read_text()
    )

    git.set_ref("refs/foo/bar", commit_rev, message="modify message")
    assert (
        "modify message"
        in (tmp_dir / ".git" / "logs" / "refs" / "foo" / "bar").read_text()
    )


def test_get_ref(tmp_dir, git):
    tmp_dir.scm_gen({"file": "0"}, commit="init")
    init_rev = tmp_dir.scm.get_rev()
    tmp_dir.gen(
        {
            os.path.join(".git", "refs", "foo", "bar"): init_rev,
            os.path.join(
                ".git", "refs", "foo", "baz"
            ): "ref: refs/heads/master",
        }
    )

    assert init_rev == git.get_ref("refs/foo/bar")
    assert init_rev == git.get_ref("refs/foo/baz")
    assert "refs/heads/master" == git.get_ref("refs/foo/baz", follow=False)
    assert git.get_ref("refs/foo/qux") is None


def test_remove_ref(tmp_dir, git):
    tmp_dir.scm_gen({"file": "0"}, commit="init")
    init_rev = tmp_dir.scm.get_rev()
    tmp_dir.gen(os.path.join(".git", "refs", "foo", "bar"), init_rev)
    tmp_dir.scm_gen({"file": "1"}, commit="commit")
    commit_rev = tmp_dir.scm.get_rev()

    with pytest.raises(SCMError):
        git.remove_ref("refs/foo/bar", old_ref=commit_rev)
    git.remove_ref("refs/foo/bar", old_ref=init_rev)
    assert not (tmp_dir / ".git" / "refs" / "foo" / "bar").exists()


def test_refs_containing(tmp_dir, scm):
    tmp_dir.scm_gen({"file": "0"}, commit="init")
    init_rev = scm.get_rev()
    tmp_dir.gen(
        {
            os.path.join(".git", "refs", "foo", "bar"): init_rev,
            os.path.join(".git", "refs", "foo", "baz"): init_rev,
        }
    )

    expected = {"refs/foo/bar", "refs/foo/baz", "refs/heads/master"}
    assert expected == set(scm.get_refs_containing(init_rev))


@pytest.mark.parametrize("use_url", [True, False])
def test_push_refspec(tmp_dir, scm, make_tmp_dir, use_url):
    tmp_dir.scm_gen({"file": "0"}, commit="init")
    init_rev = scm.get_rev()
    tmp_dir.gen(
        {
            os.path.join(".git", "refs", "foo", "bar"): init_rev,
            os.path.join(".git", "refs", "foo", "baz"): init_rev,
        }
    )
    remote_dir = make_tmp_dir("git-remote", scm=True)
    url = "file://{}".format(remote_dir.resolve().as_posix())
    scm.gitpython.repo.create_remote("origin", url)

    with pytest.raises(SCMError):
        scm.push_refspec("bad-remote", "refs/foo/bar", "refs/foo/bar")

    remote = url if use_url else "origin"
    scm.push_refspec(remote, "refs/foo/bar", "refs/foo/bar")
    assert init_rev == remote_dir.scm.get_ref("refs/foo/bar")

    remote_dir.scm.checkout("refs/foo/bar")
    assert init_rev == remote_dir.scm.get_rev()
    assert "0" == (remote_dir / "file").read_text()

    scm.push_refspec(remote, "refs/foo/", "refs/foo/")
    assert init_rev == remote_dir.scm.get_ref("refs/foo/baz")

    scm.push_refspec(remote, None, "refs/foo/baz")
    assert remote_dir.scm.get_ref("refs/foo/baz") is None


def test_fetch_refspecs(tmp_dir, scm, make_tmp_dir):
    remote_dir = make_tmp_dir("git-remote", scm=True)
    url = "file://{}".format(remote_dir.resolve().as_posix())

    remote_dir.scm_gen({"file": "0"}, commit="init")
    init_rev = remote_dir.scm.get_rev()
    remote_dir.gen(
        {
            os.path.join(".git", "refs", "foo", "bar"): init_rev,
            os.path.join(".git", "refs", "foo", "baz"): init_rev,
        }
    )

    scm.fetch_refspecs(
        url, ["refs/foo/bar:refs/foo/bar", "refs/foo/baz:refs/foo/baz"]
    )
    assert init_rev == scm.get_ref("refs/foo/bar")
    assert init_rev == scm.get_ref("refs/foo/baz")

    remote_dir.scm.checkout("refs/foo/bar")
    assert init_rev == remote_dir.scm.get_rev()
    assert "0" == (remote_dir / "file").read_text()


def test_list_all_commits(tmp_dir, scm):
    tmp_dir.scm_gen("1", "1", commit="1")
    rev_a = scm.get_rev()
    tmp_dir.scm_gen("2", "2", commit="2")
    rev_b = scm.get_rev()
    scm.tag("tag")
    tmp_dir.scm_gen("3", "3", commit="3")
    rev_c = scm.get_rev()
    scm.gitpython.git.reset(rev_a, hard=True)
    scm.set_ref("refs/foo/bar", rev_c)

    assert {rev_a, rev_b} == set(scm.list_all_commits())


def test_ignore_remove_empty(tmp_dir, scm, git):
    if git.test_backend == "pygit2":
        pytest.skip()

    test_entries = [
        {"entry": "/foo1", "path": f"{tmp_dir}/foo1"},
        {"entry": "/foo2", "path": f"{tmp_dir}/foo2"},
    ]

    path_to_gitignore = tmp_dir / ".gitignore"

    with open(path_to_gitignore, "a") as f:
        for entry in test_entries:
            f.write(entry["entry"] + "\n")

    assert path_to_gitignore.exists()

    git.ignore_remove(test_entries[0]["path"])
    assert path_to_gitignore.exists()

    git.ignore_remove(test_entries[1]["path"])
    assert not path_to_gitignore.exists()


@pytest.mark.skipif(
    os.name == "nt", reason="Git hooks not supported on Windows"
)
@pytest.mark.parametrize("hook", ["pre-commit", "commit-msg"])
def test_commit_no_verify(tmp_dir, scm, git, hook):
    import stat

    if git.test_backend == "pygit2":
        pytest.skip()

    hook_file = os.path.join(".git", "hooks", hook)
    tmp_dir.gen(hook_file, "#!/usr/bin/env python\nimport sys\nsys.exit(1)")
    os.chmod(hook_file, stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC)

    tmp_dir.gen("foo", "foo")
    git.add(["foo"])
    with pytest.raises(SCMError):
        git.commit("commit foo")
    git.commit("commit foo", no_verify=True)


@pytest.mark.parametrize("squash", [True, False])
def test_merge(tmp_dir, scm, git, squash):
    from dvc.scm.base import MergeConflictError

    if git.test_backend == "dulwich":
        pytest.skip()

    tmp_dir.scm_gen("foo", "foo", commit="init")
    init_rev = scm.get_rev()

    scm.checkout("branch", create_new=True)
    tmp_dir.scm_gen("foo", "bar", commit="bar")
    branch = scm.resolve_rev("branch")

    scm.checkout("master")

    with pytest.raises(MergeConflictError):
        tmp_dir.scm_gen("foo", "baz", commit="baz")
        git.merge(branch, commit=not squash, squash=squash, msg="merge")

    scm.gitpython.git.reset(init_rev, hard=True)
    merge_rev = git.merge(
        branch, commit=not squash, squash=squash, msg="merge"
    )
    assert (tmp_dir / "foo").read_text() == "bar"
    if squash:
        assert merge_rev is None
        assert scm.get_rev() == init_rev
    else:
        assert scm.get_rev() == merge_rev


def test_checkout_index(tmp_dir, scm, git):
    if git.test_backend == "dulwich":
        pytest.skip()

    tmp_dir.scm_gen(
        {"foo": "foo", "bar": "bar", "dir": {"baz": "baz"}}, commit="init"
    )
    tmp_dir.gen({"foo": "baz", "dir": {"baz": "foo"}})

    with (tmp_dir / "dir").chdir():
        git.checkout_index([os.path.join("..", "foo"), "baz"], force=True)
    assert (tmp_dir / "foo").read_text() == "foo"
    assert (tmp_dir / "dir" / "baz").read_text() == "baz"

    tmp_dir.gen({"foo": "baz", "bar": "baz", "dir": {"baz": "foo"}})
    git.checkout_index(force=True)
    assert (tmp_dir / "foo").read_text() == "foo"
    assert (tmp_dir / "bar").read_text() == "bar"
    assert (tmp_dir / "dir" / "baz").read_text() == "baz"


@pytest.mark.parametrize(
    "strategy, expected", [("ours", "baz"), ("theirs", "bar")]
)
def test_checkout_index_conflicts(tmp_dir, scm, git, strategy, expected):
    from dvc.scm.base import MergeConflictError

    if git.test_backend == "dulwich":
        pytest.skip()

    tmp_dir.scm_gen({"file": "foo"}, commit="init")
    scm.checkout("branch", create_new=True)
    tmp_dir.scm_gen({"file": "bar"}, commit="bar")
    bar = scm.get_rev()
    scm.checkout("master")
    tmp_dir.scm_gen({"file": "baz"}, commit="baz")

    try:
        git.merge(bar, commit=False, squash=True)
    except MergeConflictError:
        if strategy == "ours":
            git.checkout_index(ours=True)
        else:
            git.checkout_index(theirs=True)
    assert (tmp_dir / "file").read_text() == expected


def test_resolve_rev(tmp_dir, scm, make_tmp_dir, git):
    from dvc.scm.base import RevError

    if git.test_backend == "dulwich":
        pytest.skip()

    remote_dir = make_tmp_dir("git-remote", scm=True)
    url = "file://{}".format(remote_dir.resolve().as_posix())
    scm.gitpython.repo.create_remote("origin", url)
    scm.gitpython.repo.create_remote("upstream", url)

    tmp_dir.scm_gen({"file": "0"}, commit="init")
    init_rev = scm.get_rev()
    tmp_dir.scm_gen({"file": "1"}, commit="1")
    rev = scm.get_rev()
    scm.checkout("branch", create_new=True)
    tmp_dir.gen(
        {
            os.path.join(".git", "refs", "foo"): rev,
            os.path.join(".git", "refs", "remotes", "origin", "bar"): rev,
            os.path.join(".git", "refs", "remotes", "origin", "baz"): rev,
            os.path.join(
                ".git", "refs", "remotes", "upstream", "baz"
            ): init_rev,
        }
    )

    assert git.resolve_rev(rev) == rev
    assert git.resolve_rev(rev[:7]) == rev
    assert git.resolve_rev("HEAD") == rev
    assert git.resolve_rev("branch") == rev
    assert git.resolve_rev("refs/foo") == rev
    assert git.resolve_rev("bar") == rev
    assert git.resolve_rev("origin/baz") == rev

    with pytest.raises(RevError):
        git.resolve_rev("qux")

    with pytest.raises(RevError):
        git.resolve_rev("baz")


def test_checkout(tmp_dir, scm, git):
    if git.test_backend == "dulwich":
        pytest.skip()

    tmp_dir.scm_gen({"foo": "foo"}, commit="foo")
    foo_rev = scm.get_rev()
    tmp_dir.scm_gen("foo", "bar", commit="bar")
    bar_rev = scm.get_rev()

    git.checkout("branch", create_new=True)
    assert git.get_ref("HEAD", follow=False) == "refs/heads/branch"
    assert (tmp_dir / "foo").read_text() == "bar"

    git.checkout("master", detach=True)
    assert git.get_ref("HEAD", follow=False) == bar_rev

    git.checkout("master")
    assert git.get_ref("HEAD", follow=False) == "refs/heads/master"

    git.checkout(foo_rev[:7])
    assert git.get_ref("HEAD", follow=False) == foo_rev
    assert (tmp_dir / "foo").read_text() == "foo"


def test_reset(tmp_dir, scm, git):
    if git.test_backend == "dulwich":
        pytest.skip()

    tmp_dir.scm_gen({"foo": "foo", "dir": {"baz": "baz"}}, commit="init")

    tmp_dir.gen({"foo": "bar", "dir": {"baz": "bar"}})
    scm.add(["foo", os.path.join("dir", "baz")])
    git.reset()
    assert (tmp_dir / "foo").read_text() == "bar"
    assert (tmp_dir / "dir" / "baz").read_text() == "bar"
    staged, unstaged, _ = scm.status()
    assert len(staged) == 0
    assert set(unstaged) == {"foo", "dir/baz"}

    scm.add(["foo", os.path.join("dir", "baz")])
    git.reset(hard=True)
    assert (tmp_dir / "foo").read_text() == "foo"
    assert (tmp_dir / "dir" / "baz").read_text() == "baz"
    staged, unstaged, _ = scm.status()
    assert len(staged) == 0
    assert len(unstaged) == 0

    tmp_dir.gen({"foo": "bar", "bar": "bar", "dir": {"baz": "bar"}})
    scm.add(["foo", "bar", os.path.join("dir", "baz")])
    with (tmp_dir / "dir").chdir():
        git.reset(paths=[os.path.join("..", "foo"), os.path.join("baz")])
    assert (tmp_dir / "foo").read_text() == "bar"
    assert (tmp_dir / "bar").read_text() == "bar"
    assert (tmp_dir / "dir" / "baz").read_text() == "bar"
    staged, unstaged, _ = scm.status()
    assert len(staged) == 1
    assert len(unstaged) == 2


def test_remind_to_track(scm, caplog):
    scm.files_to_track = ["fname with spaces.txt", "тест", "foo"]
    scm.remind_to_track()
    assert "git add 'fname with spaces.txt' 'тест' foo" in caplog.text


def test_add(tmp_dir, scm, git):
    if git.test_backend == "pygit2":
        pytest.skip()

    tmp_dir.gen({"foo": "foo", "bar": "bar", "dir": {"baz": "baz"}})
    git.add(["foo", "dir"])
    staged, unstaged, untracked = scm.status()
    assert set(staged["add"]) == {"foo", "dir/baz"}
    assert len(unstaged) == 0
    assert len(untracked) == 1

    scm.commit("commit")
    tmp_dir.gen({"foo": "bar", "dir": {"baz": "bar"}})
    git.add([], update=True)
    staged, unstaged, _ = scm.status()
    assert set(staged["modify"]) == {"foo", "dir/baz"}
    assert len(unstaged) == 0
    assert len(untracked) == 1

    scm.reset()
    git.add(["dir"], update=True)
    staged, unstaged, _ = scm.status()
    assert set(staged["modify"]) == {"dir/baz"}
    assert len(unstaged) == 1
    assert len(untracked) == 1
