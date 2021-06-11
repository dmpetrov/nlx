import os
import shutil
import textwrap
from operator import itemgetter

import pytest

from dvc.exceptions import PathMissingError
from dvc.repo import Repo
from dvc.scm.base import CloneError

FS_STRUCTURE = {
    "README.md": "content",
    "model/script.py": "content",
    "model/train.py": "content",
    ".gitignore": "content",
}

DVC_STRUCTURE = {
    "structure.xml": "content",
    "data/subcontent/data.xml": "content",
    "data/subcontent/statistics/data.csv": "content",
    "model/people.csv": "content",
}


def match_files(files, expected_files):
    left = {(f["path"], f["isout"]) for f in files}
    right = {(os.path.join(*args), isout) for (args, isout) in expected_files}
    assert left == right


def create_dvc_pipeline(tmp_dir, dvc):
    script = textwrap.dedent(
        """\
        import os, sys
        f = sys.argv[1]
        os.makedirs(os.path.dirname(f))
        open(f, "w+").close()
    """
    )
    tmp_dir.scm_gen({"script.py": script}, commit="init")
    tmp_dir.dvc_gen({"dep": "content"}, commit="init dvc")
    dvc.run(
        **{
            "cmd": "python script.py {}".format(os.path.join("out", "file")),
            "outs": [os.path.join("out", "file")],
            "deps": ["dep"],
            "fname": "out.dvc",
            "single_stage": True,
        }
    )
    tmp_dir.scm_add(["out.dvc"], commit="run")
    shutil.rmtree("out")


def test_ls_repo(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    files = Repo.ls(os.fspath(tmp_dir))
    match_files(
        files,
        (
            ((".dvcignore",), False),
            ((".gitignore",), False),
            (("README.md",), False),
            (("structure.xml.dvc",), False),
            (("model",), False),
            (("data",), False),
            (("structure.xml",), True),
        ),
    )


def test_ls_repo_recursive(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    files = Repo.ls(os.fspath(tmp_dir), recursive=True)
    match_files(
        files,
        (
            ((".dvcignore",), False),
            ((".gitignore",), False),
            (("README.md",), False),
            (("structure.xml.dvc",), False),
            (("model", "script.py"), False),
            (("model", "train.py"), False),
            (("model", "people.csv.dvc"), False),
            (("data", "subcontent", "data.xml.dvc"), False),
            (("data", "subcontent", "statistics", "data.csv.dvc"), False),
            (("data", "subcontent", "statistics", "data.csv"), True),
            (("data", "subcontent", "statistics", ".gitignore"), False),
            (("data", "subcontent", "data.xml"), True),
            (("data", "subcontent", ".gitignore"), False),
            (("model", "people.csv"), True),
            (("model", ".gitignore"), False),
            (("structure.xml",), True),
        ),
    )


def test_ls_repo_dvc_only_recursive(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    files = Repo.ls(os.fspath(tmp_dir), recursive=True, dvc_only=True)
    match_files(
        files,
        (
            (("data", "subcontent", "statistics", "data.csv"), True),
            (("data", "subcontent", "data.xml"), True),
            (("model", "people.csv"), True),
            (("structure.xml",), True),
        ),
    )


def test_ls_repo_with_path_dir(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    files = Repo.ls(os.fspath(tmp_dir), path="model")
    match_files(
        files,
        (
            (("script.py",), False),
            (("train.py",), False),
            (("people.csv",), True),
            (("people.csv.dvc",), False),
            ((".gitignore",), False),
        ),
    )


def test_ls_repo_with_path_dir_dvc_only_empty(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")
    tmp_dir.scm_gen({"folder/.keep": "content"}, commit="add .keep")

    with pytest.raises(PathMissingError):
        Repo.ls(os.fspath(tmp_dir), path="folder", dvc_only=True)


def test_ls_repo_with_path_subdir(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    path = os.path.join("data", "subcontent")
    files = Repo.ls(os.fspath(tmp_dir), path)
    match_files(
        files,
        (
            (("data.xml",), True),
            (("data.xml.dvc",), False),
            (("statistics",), False),
            ((".gitignore",), False),
        ),
    )


def test_ls_repo_with_path_subdir_dvc_only(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    path = os.path.join("data", "subcontent")
    files = Repo.ls(os.fspath(tmp_dir), path, dvc_only=True)
    match_files(files, ((("data.xml",), True), (("statistics",), False)))


def test_ls_repo_with_path_subdir_dvc_only_recursive(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    path = os.path.join("data", "subcontent")
    files = Repo.ls(os.fspath(tmp_dir), path, dvc_only=True, recursive=True)
    match_files(
        files, ((("data.xml",), True), (("statistics", "data.csv"), True))
    )


def test_ls_repo_with_path_file_out(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    path = os.path.join("data", "subcontent", "data.xml")
    files = Repo.ls(os.fspath(tmp_dir), path)
    match_files(files, ((("data.xml",), True),))


def test_ls_repo_with_file_path_fs(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    path = "README.md"
    files = Repo.ls(os.fspath(tmp_dir), path, recursive=True)
    match_files(files, ((("README.md",), False),))


def test_ls_repo_with_missed_path(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    with pytest.raises(PathMissingError) as exc_info:
        Repo.ls(os.fspath(tmp_dir), path="missed_path")
    assert not exc_info.value.dvc_only


def test_ls_repo_with_missed_path_dvc_only(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(FS_STRUCTURE, commit="init")
    tmp_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    with pytest.raises(PathMissingError) as exc_info:
        Repo.ls(
            os.fspath(tmp_dir),
            path="missed_path",
            recursive=True,
            dvc_only=True,
        )
    assert exc_info.value.dvc_only


def test_ls_repo_with_removed_dvc_dir(tmp_dir, dvc, scm):
    create_dvc_pipeline(tmp_dir, dvc)

    files = Repo.ls(os.fspath(tmp_dir))
    match_files(
        files,
        (
            (("script.py",), False),
            (("dep.dvc",), False),
            (("out.dvc",), False),
            (("dep",), True),
            (("out",), False),
            ((".dvcignore",), False),
            ((".gitignore",), False),
        ),
    )


def test_ls_repo_with_removed_dvc_dir_recursive(tmp_dir, dvc, scm):
    create_dvc_pipeline(tmp_dir, dvc)

    files = Repo.ls(os.fspath(tmp_dir), recursive=True)
    match_files(
        files,
        (
            (("script.py",), False),
            (("dep.dvc",), False),
            (("out.dvc",), False),
            (("dep",), True),
            (("out", "file"), True),
            ((".dvcignore",), False),
            ((".gitignore",), False),
        ),
    )


def test_ls_repo_with_removed_dvc_dir_with_path_dir(tmp_dir, dvc, scm):
    create_dvc_pipeline(tmp_dir, dvc)

    path = "out"
    files = Repo.ls(os.fspath(tmp_dir), path)
    match_files(files, ((("file",), True),))


def test_ls_repo_with_removed_dvc_dir_with_path_file(tmp_dir, dvc, scm):
    create_dvc_pipeline(tmp_dir, dvc)

    path = os.path.join("out", "file")
    files = Repo.ls(os.fspath(tmp_dir), path)
    match_files(files, ((("file",), True),))


def test_ls_repo_with_rev(erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.scm_gen(FS_STRUCTURE, commit="init")
        erepo_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    rev = erepo_dir.scm.list_all_commits()[1]
    files = Repo.ls(os.fspath(erepo_dir), rev=rev)
    match_files(
        files,
        (
            ((".dvcignore",), False),
            ((".gitignore",), False),
            (("README.md",), False),
            (("model",), False),
        ),
    )


def test_ls_remote_repo(erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.scm_gen(FS_STRUCTURE, commit="init")
        erepo_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    url = f"file://{erepo_dir}"
    files = Repo.ls(url)
    match_files(
        files,
        (
            ((".dvcignore",), False),
            ((".gitignore",), False),
            (("README.md",), False),
            (("structure.xml.dvc",), False),
            (("model",), False),
            (("data",), False),
            (("structure.xml",), True),
        ),
    )


def test_ls_remote_repo_recursive(erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.scm_gen(FS_STRUCTURE, commit="init")
        erepo_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    url = f"file://{erepo_dir}"
    files = Repo.ls(url, recursive=True)
    match_files(
        files,
        (
            ((".dvcignore",), False),
            ((".gitignore",), False),
            (("README.md",), False),
            (("structure.xml.dvc",), False),
            (("model", "script.py"), False),
            (("model", "train.py"), False),
            (("model", "people.csv.dvc"), False),
            (("data", "subcontent", "data.xml.dvc"), False),
            (("data", "subcontent", "statistics", "data.csv.dvc"), False),
            (("data", "subcontent", "statistics", "data.csv"), True),
            (("data", "subcontent", "statistics", ".gitignore"), False),
            (("data", "subcontent", "data.xml"), True),
            (("data", "subcontent", ".gitignore"), False),
            (("model", "people.csv"), True),
            (("model", ".gitignore"), False),
            (("structure.xml",), True),
        ),
    )


def test_ls_remote_git_only_repo_recursive(git_dir):
    with git_dir.chdir():
        git_dir.scm_gen(FS_STRUCTURE, commit="init")

    url = f"file://{git_dir}"
    files = Repo.ls(url, recursive=True)
    match_files(
        files,
        (
            ((".gitignore",), False),
            (("README.md",), False),
            (("model", "script.py"), False),
            (("model", "train.py"), False),
        ),
    )


def test_ls_remote_repo_with_path_dir(erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.scm_gen(FS_STRUCTURE, commit="init")
        erepo_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    url = f"file://{erepo_dir}"
    path = "model"
    files = Repo.ls(url, path)
    match_files(
        files,
        (
            (("script.py",), False),
            (("train.py",), False),
            (("people.csv",), True),
            (("people.csv.dvc",), False),
            ((".gitignore",), False),
        ),
    )


def test_ls_remote_repo_with_rev(erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.scm_gen(FS_STRUCTURE, commit="init")
        erepo_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")

    rev = erepo_dir.scm.list_all_commits()[1]
    url = f"file://{erepo_dir}"
    files = Repo.ls(url, rev=rev)
    match_files(
        files,
        (
            ((".dvcignore",), False),
            ((".gitignore",), False),
            (("README.md",), False),
            (("model",), False),
        ),
    )


def test_ls_remote_repo_with_rev_recursive(erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen(DVC_STRUCTURE, commit="dvc")
        erepo_dir.scm_gen(FS_STRUCTURE, commit="init")

    rev = erepo_dir.scm.list_all_commits()[1]
    url = f"file://{erepo_dir}"
    files = Repo.ls(url, rev=rev, recursive=True)
    match_files(
        files,
        (
            (("structure.xml.dvc",), False),
            (("model", "people.csv.dvc"), False),
            (("data", "subcontent", "data.xml.dvc"), False),
            (("data", "subcontent", "statistics", "data.csv.dvc"), False),
            (("data", "subcontent", "statistics", "data.csv"), True),
            (("data", "subcontent", "statistics", ".gitignore"), False),
            (("data", "subcontent", "data.xml"), True),
            (("data", "subcontent", ".gitignore"), False),
            (("model", "people.csv"), True),
            (("model", ".gitignore"), False),
            (("structure.xml",), True),
            ((".dvcignore",), False),
            ((".gitignore",), False),
        ),
    )


def test_ls_not_existed_url():
    from time import time

    dirname = "__{}_{}".format("not_existed", time())
    with pytest.raises(CloneError):
        Repo.ls(dirname, recursive=True)


def test_ls_shows_pipeline_tracked_outs(tmp_dir, dvc, scm, run_copy):
    from dvc.dvcfile import PIPELINE_FILE, PIPELINE_LOCK

    tmp_dir.gen("foo", "foo")
    run_copy("foo", "bar", name="copy-foo-bar")
    dvc.scm.add([PIPELINE_FILE, PIPELINE_LOCK])
    dvc.scm.commit("add pipeline stage")

    files = Repo.ls(os.curdir, dvc_only=True)
    match_files(files, ((("bar",), True),))


def test_ls_granular(erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen(
            {
                "dir": {
                    "1": "1",
                    "2": "2",
                    "subdir": {"foo": "foo", "bar": "bar"},
                }
            },
            commit="create dir",
        )

    entries = Repo.ls(os.fspath(erepo_dir), os.path.join("dir", "subdir"))
    assert entries == [
        {"isout": False, "isdir": False, "isexec": False, "path": "bar"},
        {"isout": False, "isdir": False, "isexec": False, "path": "foo"},
    ]

    entries = Repo.ls(os.fspath(erepo_dir), "dir")
    assert entries == [
        {"isout": False, "isdir": False, "isexec": False, "path": "1"},
        {"isout": False, "isdir": False, "isexec": False, "path": "2"},
        {"isout": False, "isdir": True, "isexec": False, "path": "subdir"},
    ]


@pytest.mark.parametrize("use_scm", [True, False])
def test_ls_target(erepo_dir, use_scm):
    with erepo_dir.chdir():
        gen = erepo_dir.scm_gen if use_scm else erepo_dir.dvc_gen
        gen(
            {
                "dir": {
                    "1": "1",
                    "2": "2",
                    "subdir": {"foo": "foo", "bar": "bar"},
                }
            },
            commit="create dir",
        )

    def _ls(path):
        return Repo.ls(os.fspath(erepo_dir), path)

    assert _ls(os.path.join("dir", "1")) == [
        {"isout": False, "isdir": False, "isexec": False, "path": "1"}
    ]
    assert _ls(os.path.join("dir", "subdir", "foo")) == [
        {"isout": False, "isdir": False, "isexec": False, "path": "foo"}
    ]
    assert _ls(os.path.join("dir", "subdir")) == [
        {"isdir": False, "isexec": 0, "isout": False, "path": "bar"},
        {"isdir": False, "isexec": 0, "isout": False, "path": "foo"},
    ]


@pytest.mark.parametrize(
    "dvc_top_level, erepo",
    [
        (True, pytest.lazy_fixture("erepo_dir")),
        (False, pytest.lazy_fixture("git_dir")),
    ],
)
def test_subrepo(dvc_top_level, erepo):
    from tests.func.test_get import make_subrepo

    dvc_files = {"foo.txt": "foo.txt", "dvc_dir": {"lorem": "lorem"}}
    scm_files = {"bar.txt": "bar.txt", "scm_dir": {"ipsum": "ipsum"}}
    subrepo = erepo / "subrepo"
    make_subrepo(subrepo, erepo.scm)

    for repo in [erepo, subrepo]:
        with repo.chdir():
            repo.scm_gen(scm_files, commit=f"scm track for top {repo}")
            if hasattr(repo, "dvc"):
                repo.dvc_gen(dvc_files, commit=f"dvc track for {repo}")

    def _list_files(repo, path=None):
        return set(map(itemgetter("path"), Repo.ls(os.fspath(repo), path)))

    extras = {".dvcignore", ".gitignore"}
    git_tracked_outputs = {"bar.txt", "scm_dir"}
    dvc_files = {"dvc_dir", "foo.txt", "foo.txt.dvc", "dvc_dir.dvc"}
    common_outputs = git_tracked_outputs | extras | dvc_files

    top_level_outputs = (
        common_outputs if dvc_top_level else git_tracked_outputs
    )
    assert _list_files(erepo) == top_level_outputs
    assert _list_files(erepo, "scm_dir") == {"ipsum"}
    if dvc_top_level:
        assert _list_files(erepo, "dvc_dir") == {"lorem"}

    assert _list_files(subrepo, ".") == common_outputs
    assert _list_files(subrepo, "scm_dir") == {"ipsum"}
    assert _list_files(subrepo, "dvc_dir") == {"lorem"}
