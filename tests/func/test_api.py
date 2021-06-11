import os

import pytest
from funcy import first, get_in

from dvc import api
from dvc.exceptions import FileMissingError, OutputNotFoundError
from dvc.path_info import URLInfo
from dvc.utils.fs import remove
from tests.unit.fs.test_repo import make_subrepo

cloud_names = [
    "s3",
    "gs",
    "azure",
    "gdrive",
    "oss",
    "ssh",
    "http",
    "hdfs",
    "webdav",
    "webhdfs",
]
clouds = [pytest.lazy_fixture(cloud) for cloud in cloud_names]
all_clouds = [pytest.lazy_fixture("local_cloud")] + clouds

# `lazy_fixture` is confusing pylint, pylint: disable=unused-argument


@pytest.mark.needs_internet
@pytest.mark.parametrize("remote", clouds, indirect=True)
def test_get_url(tmp_dir, dvc, remote):
    tmp_dir.dvc_gen("foo", "foo")

    expected_url = URLInfo(remote.url) / "ac/bd18db4cc2f85cedef654fccc4a4d8"
    assert api.get_url("foo") == expected_url


@pytest.mark.needs_internet
@pytest.mark.parametrize("cloud", clouds)
def test_get_url_external(tmp_dir, erepo_dir, cloud):
    erepo_dir.add_remote(config=cloud.config)
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("foo", "foo", commit="add foo")

    # Using file url to force clone to tmp repo
    repo_url = f"file://{erepo_dir}"
    expected_url = URLInfo(cloud.url) / "ac/bd18db4cc2f85cedef654fccc4a4d8"
    assert api.get_url("foo", repo=repo_url) == expected_url


def test_get_url_requires_dvc(tmp_dir, scm):
    tmp_dir.scm_gen({"foo": "foo"}, commit="initial")

    with pytest.raises(OutputNotFoundError, match="output 'foo'"):
        api.get_url("foo", repo=os.fspath(tmp_dir))

    with pytest.raises(OutputNotFoundError, match="output 'foo'"):
        api.get_url("foo", repo=f"file://{tmp_dir}")


@pytest.mark.needs_internet
@pytest.mark.parametrize("remote", all_clouds, indirect=True)
def test_open(tmp_dir, dvc, remote):
    tmp_dir.dvc_gen("foo", "foo-text")
    dvc.push()

    # Remove cache to force download
    remove(dvc.odb.local.cache_dir)

    with api.open("foo") as fd:
        assert fd.read() == "foo-text"


@pytest.mark.needs_internet
@pytest.mark.parametrize(
    "cloud",
    [
        pytest.lazy_fixture(cloud)
        for cloud in [
            "s3",
            "gs",
            "azure",
            "gdrive",
            "oss",
            "http",
            "hdfs",
            "ssh",
            "webdav",
        ]
    ],
)
def test_open_external(tmp_dir, erepo_dir, cloud):
    erepo_dir.add_remote(config=cloud.config)

    with erepo_dir.chdir():
        erepo_dir.dvc_gen("version", "master", commit="add version")

        with erepo_dir.branch("branch", new="True"):
            # NOTE: need file to be other size for Mac
            erepo_dir.dvc_gen("version", "branchver", commit="add version")

    erepo_dir.dvc.push(all_branches=True)

    # Remove cache to force download
    remove(erepo_dir.dvc.odb.local.cache_dir)

    # Using file url to force clone to tmp repo
    repo_url = f"file://{erepo_dir}"
    with api.open("version", repo=repo_url) as fd:
        assert fd.read() == "master"

    assert api.read("version", repo=repo_url, rev="branch") == "branchver"


@pytest.mark.needs_internet
@pytest.mark.parametrize("remote", all_clouds, indirect=True)
def test_open_granular(tmp_dir, dvc, remote):
    tmp_dir.dvc_gen({"dir": {"foo": "foo-text"}})
    dvc.push()

    # Remove cache to force download
    remove(dvc.odb.local.cache_dir)

    with api.open("dir/foo") as fd:
        assert fd.read() == "foo-text"


@pytest.mark.needs_internet
@pytest.mark.parametrize(
    "remote",
    [
        pytest.lazy_fixture(cloud)
        for cloud in [
            "s3",
            "gs",
            "azure",
            "gdrive",
            "oss",
            "ssh",
            "http",
            "hdfs",
            "webdav",
        ]
    ],
    indirect=True,
)
def test_missing(tmp_dir, dvc, remote):
    tmp_dir.dvc_gen("foo", "foo")

    # Remove cache to make foo missing
    remove(dvc.odb.local.cache_dir)

    api.read("foo")

    remove("foo")

    with pytest.raises(FileMissingError):
        api.read("foo")


def test_open_scm_controlled(tmp_dir, erepo_dir):
    erepo_dir.scm_gen({"scm_controlled": "file content"}, commit="create file")

    with api.open("scm_controlled", repo=os.fspath(erepo_dir)) as fd:
        assert fd.read() == "file content"


def test_open_not_cached(dvc):
    metric_file = "metric.txt"
    metric_content = "0.6"
    metric_code = "open('{}', 'w').write('{}')".format(
        metric_file, metric_content
    )
    dvc.run(
        single_stage=True,
        metrics_no_cache=[metric_file],
        cmd=(f'python -c "{metric_code}"'),
    )

    with api.open(metric_file) as fd:
        assert fd.read() == metric_content

    os.remove(metric_file)
    with pytest.raises(FileMissingError):
        api.read(metric_file)


def test_open_rev(tmp_dir, scm, dvc):
    tmp_dir.scm_gen("foo", "foo", commit="foo")

    (tmp_dir / "foo").write_text("bar")

    with api.open("foo", rev="master") as fobj:
        assert fobj.read() == "foo"


@pytest.mark.parametrize("as_external", [True, False])
@pytest.mark.parametrize("remote", [pytest.lazy_fixture("ssh")], indirect=True)
@pytest.mark.parametrize(
    "files, to_read",
    [
        ({"foo": "foo"}, "foo"),
        ({"dir": {"foo": "foo", "bar": "bar"}}, os.path.join("dir", "foo")),
    ],
    ids=["file", "inside-dir"],
)
def test_api_missing_local_cache_exists_on_remote(
    tmp_dir, scm, dvc, as_external, remote, files, to_read
):
    tmp_dir.dvc_gen(files, commit="DVC track files")
    dvc.push()

    # Remove cache to make foo missing
    remove(dvc.odb.local.cache_dir)
    remove(first(files))

    repo_url = f"file://{tmp_dir}" if as_external else None
    file_content = get_in(files, to_read.split(os.sep))
    assert api.read(to_read, repo=repo_url) == file_content


@pytest.mark.parametrize("local_repo", [False, True])
def test_read_with_subrepos(tmp_dir, scm, local_cloud, local_repo):
    tmp_dir.scm_gen("foo.txt", "foo.txt", commit="add foo.txt")
    subrepo = tmp_dir / "dir" / "subrepo"
    make_subrepo(subrepo, scm, config=local_cloud.config)
    with subrepo.chdir():
        subrepo.scm_gen({"lorem": "lorem"}, commit="add lorem")
        subrepo.dvc_gen({"dir": {"file.txt": "file.txt"}}, commit="add dir")
        subrepo.dvc_gen("dvc-file", "dvc-file", commit="add dir")
        subrepo.dvc.push()

    repo_path = None if local_repo else f"file:///{tmp_dir}"
    subrepo_path = os.path.join("dir", "subrepo")

    assert api.read("foo.txt", repo=repo_path) == "foo.txt"
    assert (
        api.read(os.path.join(subrepo_path, "lorem"), repo=repo_path)
        == "lorem"
    )
    assert (
        api.read(os.path.join(subrepo_path, "dvc-file"), repo=repo_path)
        == "dvc-file"
    )
    assert (
        api.read(os.path.join(subrepo_path, "dir", "file.txt"), repo=repo_path)
        == "file.txt"
    )


def test_get_url_granular(tmp_dir, dvc, s3):
    tmp_dir.add_remote(config=s3.config)
    tmp_dir.dvc_gen(
        {"dir": {"foo": "foo", "bar": "bar", "nested": {"file": "file"}}}
    )

    expected_url = URLInfo(s3.url) / "5f/c28ea78987408341668eba6525ebd1.dir"
    assert api.get_url("dir") == expected_url

    expected_url = URLInfo(s3.url) / "ac/bd18db4cc2f85cedef654fccc4a4d8"
    assert api.get_url("dir/foo") == expected_url

    expected_url = URLInfo(s3.url) / "37/b51d194a7513e45b56f6524f2d51f2"
    assert api.get_url("dir/bar") == expected_url

    expected_url = URLInfo(s3.url) / "8c/7dd922ad47494fc02c388e12c00eac"
    assert api.get_url(os.path.join("dir", "nested", "file")) == expected_url


def test_get_url_subrepos(tmp_dir, scm, local_cloud):
    subrepo = tmp_dir / "subrepo"
    make_subrepo(subrepo, scm, config=local_cloud.config)
    with subrepo.chdir():
        subrepo.dvc_gen(
            {"dir": {"foo": "foo"}, "bar": "bar"}, commit="add files"
        )
        subrepo.dvc.push()

    path = os.path.relpath(local_cloud.config["url"])

    expected_url = os.path.join(path, "ac", "bd18db4cc2f85cedef654fccc4a4d8")
    assert api.get_url(os.path.join("subrepo", "dir", "foo")) == expected_url

    expected_url = os.path.join(path, "37", "b51d194a7513e45b56f6524f2d51f2")
    assert api.get_url("subrepo/bar") == expected_url
