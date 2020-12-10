import logging
import os
import shutil

import pytest
from flaky.flaky_decorator import flaky

from dvc.cache import NamedCache
from dvc.exceptions import DvcException
from dvc.external_repo import clean_repos
from dvc.main import main
from dvc.remote.base import (
    STATUS_DELETED,
    STATUS_MISSING,
    STATUS_NEW,
    STATUS_OK,
)
from dvc.stage.exceptions import StageNotFound
from dvc.tree.local import LocalTree
from dvc.utils.fs import move, remove
from dvc.utils.serialize import dump_yaml, load_yaml

all_clouds = [
    pytest.lazy_fixture(cloud)
    for cloud in [
        "s3",
        "gs",
        "azure",
        "gdrive",
        "ssh",
        "http",
        "hdfs",
        "webdav",
        "webhdfs",
    ]
] + [
    pytest.param(
        pytest.lazy_fixture("oss"),
        marks=pytest.mark.xfail(
            reason="https://github.com/iterative/dvc/issues/4633",
        ),
    )
]


@pytest.mark.parametrize("remote", all_clouds, indirect=True)
def test_cloud(tmp_dir, dvc, remote):  # pylint:disable=unused-argument
    (stage,) = tmp_dir.dvc_gen("foo", "foo")
    out = stage.outs[0]
    cache = out.cache_path
    md5 = out.hash_info.value
    info = out.get_used_cache()

    (stage_dir,) = tmp_dir.dvc_gen(
        {
            "data_dir": {
                "data_sub_dir": {"data_sub": "data_sub"},
                "data": "data",
                "empty": "",
            }
        }
    )
    out_dir = stage_dir.outs[0]
    cache_dir = out_dir.cache_path
    name_dir = str(out_dir)
    md5_dir = out_dir.hash_info.value
    info_dir = NamedCache.make(out_dir.scheme, md5_dir, name_dir)

    # Check status
    status = dvc.cloud.status(info, show_checksums=True)
    expected = {md5: {"name": md5, "status": STATUS_NEW}}
    assert status == expected

    status_dir = dvc.cloud.status(info_dir, show_checksums=True)
    expected = {md5_dir: {"name": md5_dir, "status": STATUS_NEW}}
    assert status_dir == expected

    # Move cache and check status
    # See issue https://github.com/iterative/dvc/issues/4383 for details
    backup_dir = dvc.cache.local.cache_dir + ".backup"
    move(dvc.cache.local.cache_dir, backup_dir)
    status = dvc.cloud.status(info, show_checksums=True)
    expected = {md5: {"name": md5, "status": STATUS_MISSING}}
    assert status == expected

    status_dir = dvc.cloud.status(info_dir, show_checksums=True)
    expected = {md5_dir: {"name": md5_dir, "status": STATUS_MISSING}}
    assert status_dir == expected

    # Restore original cache:
    remove(dvc.cache.local.cache_dir)
    move(backup_dir, dvc.cache.local.cache_dir)

    # Push and check status
    dvc.cloud.push(info)
    assert os.path.exists(cache)
    assert os.path.isfile(cache)

    dvc.cloud.push(info_dir)
    assert os.path.isfile(cache_dir)

    status = dvc.cloud.status(info, show_checksums=True)
    expected = {md5: {"name": md5, "status": STATUS_OK}}
    assert status == expected

    status_dir = dvc.cloud.status(info_dir, show_checksums=True)
    expected = {md5_dir: {"name": md5_dir, "status": STATUS_OK}}
    assert status_dir == expected

    # Remove and check status
    remove(dvc.cache.local.cache_dir)

    status = dvc.cloud.status(info, show_checksums=True)
    expected = {md5: {"name": md5, "status": STATUS_DELETED}}
    assert status == expected

    status_dir = dvc.cloud.status(info_dir, show_checksums=True)
    expected = {md5_dir: {"name": md5_dir, "status": STATUS_DELETED}}
    assert status_dir == expected

    # Pull and check status
    dvc.cloud.pull(info)
    assert os.path.exists(cache)
    assert os.path.isfile(cache)
    with open(cache) as fd:
        assert fd.read() == "foo"

    dvc.cloud.pull(info_dir)
    assert os.path.isfile(cache_dir)

    status = dvc.cloud.status(info, show_checksums=True)
    expected = {md5: {"name": md5, "status": STATUS_OK}}
    assert status == expected

    status_dir = dvc.cloud.status(info_dir, show_checksums=True)
    expected = {md5_dir: {"name": md5_dir, "status": STATUS_OK}}
    assert status_dir == expected


@pytest.mark.parametrize("remote", all_clouds, indirect=True)
def test_cloud_cli(tmp_dir, dvc, remote):
    args = ["-v", "-j", "2"]

    (stage,) = tmp_dir.dvc_gen("foo", "foo")
    cache = stage.outs[0].cache_path

    (stage_dir,) = tmp_dir.dvc_gen(
        {
            "data_dir": {
                "data_sub_dir": {"data_sub": "data_sub"},
                "data": "data",
                "empty": "",
            }
        }
    )
    assert stage_dir is not None
    cache_dir = stage_dir.outs[0].cache_path

    # FIXME check status output

    assert main(["push"] + args) == 0
    assert os.path.exists(cache)
    assert os.path.isfile(cache)
    assert os.path.isfile(cache_dir)

    remove(dvc.cache.local.cache_dir)

    assert main(["fetch"] + args) == 0
    assert os.path.exists(cache)
    assert os.path.isfile(cache)
    assert os.path.isfile(cache_dir)

    assert main(["pull"] + args) == 0
    assert os.path.exists(cache)
    assert os.path.isfile(cache)
    assert os.path.isfile(cache_dir)
    assert os.path.isfile("foo")
    assert os.path.isdir("data_dir")

    with open(cache) as fd:
        assert fd.read() == "foo"
    assert os.path.isfile(cache_dir)

    # NOTE: http doesn't support gc yet
    if remote.url.startswith("http"):
        return

    # NOTE: check if remote gc works correctly on directories
    assert main(["gc", "-cw", "-f"] + args) == 0
    shutil.move(
        dvc.cache.local.cache_dir, dvc.cache.local.cache_dir + ".back",
    )

    assert main(["fetch"] + args) == 0

    assert main(["pull", "-f"] + args) == 0
    assert os.path.exists(cache)
    assert os.path.isfile(cache)
    assert os.path.isfile(cache_dir)
    assert os.path.isfile("foo")
    assert os.path.isdir("data_dir")


def test_data_cloud_error_cli(dvc):
    f = "non-existing-file"
    assert main(["status", "-c", f])
    assert main(["push", f])
    assert main(["pull", f])
    assert main(["fetch", f])


def test_warn_on_outdated_stage(tmp_dir, dvc, local_remote, caplog):
    stage = dvc.run(outs=["bar"], cmd="echo bar > bar", single_stage=True)
    assert main(["push"]) == 0

    stage_file_path = stage.relpath
    content = load_yaml(stage_file_path)
    del content["outs"][0]["md5"]
    dump_yaml(stage_file_path, content)

    with caplog.at_level(logging.WARNING, logger="dvc"):
        caplog.clear()
        assert main(["status", "-c"]) == 0
        expected_warning = (
            "Output 'bar'(stage: 'bar.dvc') is missing version info. "
            "Cache for it will not be collected. "
            "Use `dvc repro` to get your pipeline up to date."
        )

        assert expected_warning in caplog.text


def test_hash_recalculation(mocker, dvc, tmp_dir, local_remote):
    tmp_dir.gen({"foo": "foo"})
    test_get_file_hash = mocker.spy(LocalTree, "get_file_hash")
    ret = main(["config", "cache.type", "hardlink"])
    assert ret == 0
    ret = main(["add", "foo"])
    assert ret == 0
    ret = main(["push"])
    assert ret == 0
    ret = main(["run", "--single-stage", "-d", "foo", "echo foo"])
    assert ret == 0
    assert test_get_file_hash.mock.call_count == 1


def test_missing_cache(tmp_dir, dvc, local_remote, caplog):
    tmp_dir.dvc_gen({"foo": "foo", "bar": "bar"})

    # purge cache
    remove(dvc.cache.local.cache_dir)

    header = (
        "Some of the cache files do not exist "
        "neither locally nor on remote. Missing cache files:\n"
    )
    foo = "name: bar, md5: 37b51d194a7513e45b56f6524f2d51f2\n"
    bar = "name: foo, md5: acbd18db4cc2f85cedef654fccc4a4d8\n"

    caplog.clear()
    dvc.push()
    assert header in caplog.text
    assert foo in caplog.text
    assert bar in caplog.text

    caplog.clear()
    dvc.fetch()
    assert header in caplog.text
    assert foo in caplog.text
    assert bar in caplog.text

    caplog.clear()
    assert dvc.status(cloud=True) == {"bar": "missing", "foo": "missing"}
    assert header not in caplog.text
    assert foo not in caplog.text
    assert bar not in caplog.text


def test_verify_hashes(
    tmp_dir, scm, dvc, mocker, tmp_path_factory, local_remote
):
    tmp_dir.dvc_gen({"file": "file1 content"}, commit="add file")
    tmp_dir.dvc_gen({"dir": {"subfile": "file2 content"}}, commit="add dir")
    dvc.push()

    # remove artifacts and cache to trigger fetching
    remove("file")
    remove("dir")
    remove(dvc.cache.local.cache_dir)

    hash_spy = mocker.spy(dvc.cache.local.tree, "get_file_hash")

    dvc.pull()
    assert hash_spy.call_count == 0

    # Removing cache will invalidate existing state entries
    remove(dvc.cache.local.cache_dir)

    dvc.config["remote"]["upstream"]["verify"] = True

    dvc.pull()
    assert hash_spy.call_count == 3


@flaky(max_runs=3, min_passes=1)
@pytest.mark.parametrize(
    "erepo", [pytest.lazy_fixture("git_dir"), pytest.lazy_fixture("erepo_dir")]
)
def test_pull_git_imports(tmp_dir, dvc, scm, erepo, local_remote):
    with erepo.chdir():
        erepo.scm_gen({"dir": {"bar": "bar"}}, commit="second")
        erepo.scm_gen("foo", "foo", commit="first")

    dvc.imp(os.fspath(erepo), "foo")
    dvc.imp(os.fspath(erepo), "dir", out="new_dir", rev="HEAD~")

    assert dvc.pull()["fetched"] == 0
    assert dvc.push() == 0

    for item in ["foo", "new_dir", dvc.cache.local.cache_dir]:
        remove(item)
    os.makedirs(dvc.cache.local.cache_dir, exist_ok=True)
    clean_repos()

    assert dvc.pull(force=True)["fetched"] == 2

    assert (tmp_dir / "foo").exists()
    assert (tmp_dir / "foo").read_text() == "foo"

    assert (tmp_dir / "new_dir").exists()
    assert (tmp_dir / "new_dir" / "bar").read_text() == "bar"


@pytest.mark.parametrize(
    "erepo", [pytest.lazy_fixture("git_dir"), pytest.lazy_fixture("erepo_dir")]
)
def test_pull_git_imports_with_store(tmp_dir, dvc, scm, erepo, local_remote):
    with erepo.chdir():
        erepo.scm_gen({"dir": {"bar": "bar"}}, commit="second")
        erepo.scm_gen("foo", "foo", commit="first")

    dvc.imp(os.fspath(erepo), "foo", store=True)
    dvc.imp(os.fspath(erepo), "dir", out="new_dir", store=True, rev="HEAD~")

    assert dvc.pull()["fetched"] == 0
    assert dvc.push() == 3

    for item in ["foo", "new_dir", dvc.cache.local.cache_dir]:
        remove(item)
    os.makedirs(dvc.cache.local.cache_dir, exist_ok=True)
    clean_repos()

    # Close the repo since otherwise Windows holds references open which
    # prevent deletion
    erepo.close()
    remove(erepo)

    assert dvc.pull(force=True)["fetched"] == 2

    assert (tmp_dir / "foo").exists()
    assert (tmp_dir / "foo").read_text() == "foo"

    assert (tmp_dir / "new_dir").exists()
    assert (tmp_dir / "new_dir" / "bar").read_text() == "bar"


def test_store_false_pull_error(tmp_dir, dvc, scm, local_remote, caplog):
    tmp_dir.dvc_gen("foo", "Locally added", commit="add no-store file")
    tmp_dir.dvc_gen("bar", "Another local file", commit="add normal file")

    dvc.push()
    clean(["foo", "bar"], dvc)
    dvc.pull()

    config = load_yaml(tmp_dir / "foo.dvc")
    config["outs"][0]["store"] = False
    dump_yaml(tmp_dir / "foo.dvc", config)

    dvc.push()
    clean(["foo", "bar"], dvc)

    missing_error = (
        "Cache 'md5: d089d68656330fd712dad63400c8710f' not found. "
        "File 'foo' won't be created"
    )
    missing_error_hints = (
        "foo (marked with 'store: false' in its dvc file)",
        (
            "Note that 'store: false' outputs are not searched for in remote "
            "storage, only in the cache. See"
        ),
    )
    caplog.clear()
    with pytest.raises(DvcException) as excinfo:
        dvc.pull()
    assert missing_error in caplog.text
    assert all(h in str(excinfo.value) for h in missing_error_hints)


def test_pull_external_dvc_imports(tmp_dir, dvc, scm, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen({"dir": {"bar": "bar"}}, commit="second")
        erepo_dir.dvc_gen("foo", "foo", commit="first")

        os.remove("foo")
        shutil.rmtree("dir")

    dvc.imp(os.fspath(erepo_dir), "foo")
    dvc.imp(os.fspath(erepo_dir), "dir", out="new_dir", rev="HEAD~")

    assert dvc.pull()["fetched"] == 0

    clean(["foo", "new_dir"], dvc)

    assert dvc.pull(force=True)["fetched"] == 2

    assert (tmp_dir / "foo").exists()
    assert (tmp_dir / "foo").read_text() == "foo"

    assert (tmp_dir / "new_dir").exists()
    assert (tmp_dir / "new_dir" / "bar").read_text() == "bar"


def clean(outs, dvc=None):
    if dvc:
        outs = outs + [dvc.cache.local.cache_dir]
    for path in outs:
        print(path)
        remove(path)
    if dvc:
        os.makedirs(dvc.cache.local.cache_dir, exist_ok=True)
        clean_repos()


def recurse_list_dir(d):
    return [
        os.path.join(d, f) for _, _, filenames in os.walk(d) for f in filenames
    ]


def test_dvc_pull_pipeline_stages(tmp_dir, dvc, run_copy, local_remote):
    (stage0,) = tmp_dir.dvc_gen("foo", "foo")
    stage1 = run_copy("foo", "bar", single_stage=True)
    stage2 = run_copy("bar", "foobar", name="copy-bar-foobar")
    dvc.push()

    outs = ["foo", "bar", "foobar"]

    clean(outs, dvc)
    dvc.pull()
    assert all((tmp_dir / file).exists() for file in outs)

    for out, stage in zip(outs, [stage0, stage1, stage2]):
        for target in [stage.addressing, out]:
            clean(outs, dvc)
            stats = dvc.pull([target])
            assert stats["fetched"] == 1
            assert stats["added"] == [out]
            assert os.path.exists(out)
            assert not any(os.path.exists(out) for out in set(outs) - {out})

    clean(outs, dvc)
    stats = dvc.pull([stage2.addressing], with_deps=True)
    assert len(stats["added"]) == 3
    assert set(stats["added"]) == set(outs)

    clean(outs, dvc)
    stats = dvc.pull([os.curdir], recursive=True)
    assert set(stats["added"]) == set(outs)


def test_pipeline_file_target_ops(tmp_dir, dvc, run_copy, local_remote):
    path = local_remote.url
    tmp_dir.dvc_gen("foo", "foo")
    run_copy("foo", "bar", single_stage=True)

    tmp_dir.dvc_gen("lorem", "lorem")
    run_copy("lorem", "lorem2", name="copy-lorem-lorem2")

    tmp_dir.dvc_gen("ipsum", "ipsum")
    run_copy("ipsum", "baz", name="copy-ipsum-baz")

    outs = ["foo", "bar", "lorem", "ipsum", "baz", "lorem2"]

    remove(dvc.stage_cache.cache_dir)

    dvc.push()

    outs = ["foo", "bar", "lorem", "ipsum", "baz", "lorem2"]

    # each one's a copy of other, hence 3
    assert len(recurse_list_dir(path)) == 3

    clean(outs, dvc)
    assert set(dvc.pull(["dvc.yaml"])["added"]) == {"lorem2", "baz"}

    clean(outs, dvc)
    assert set(dvc.pull()["added"]) == set(outs)

    # clean everything in remote and push
    from tests.dir_helpers import TmpDir

    clean(TmpDir(path).iterdir())
    dvc.push(["dvc.yaml:copy-ipsum-baz"])
    assert len(recurse_list_dir(path)) == 1

    clean(TmpDir(path).iterdir())
    dvc.push(["dvc.yaml"])
    assert len(recurse_list_dir(path)) == 2

    with pytest.raises(StageNotFound):
        dvc.push(["dvc.yaml:StageThatDoesNotExist"])

    with pytest.raises(StageNotFound):
        dvc.pull(["dvc.yaml:StageThatDoesNotExist"])


@pytest.mark.parametrize(
    "fs, msg",
    [
        ({"foo": "foo", "bar": "bar"}, "2 files pushed"),
        ({"foo": "foo"}, "1 file pushed"),
        ({}, "Everything is up to date"),
    ],
)
def test_push_stats(tmp_dir, dvc, fs, msg, caplog, local_remote):
    tmp_dir.dvc_gen(fs)

    caplog.clear()
    with caplog.at_level(level=logging.INFO, logger="dvc"):
        main(["push"])
    assert msg in caplog.text


@pytest.mark.parametrize(
    "fs, msg",
    [
        ({"foo": "foo", "bar": "bar"}, "2 files fetched"),
        ({"foo": "foo"}, "1 file fetched"),
        ({}, "Everything is up to date."),
    ],
)
def test_fetch_stats(tmp_dir, dvc, fs, msg, caplog, local_remote):
    tmp_dir.dvc_gen(fs)
    dvc.push()
    clean(list(fs.keys()), dvc)
    caplog.clear()
    with caplog.at_level(level=logging.INFO, logger="dvc"):
        main(["fetch"])
    assert msg in caplog.text


def test_pull_stats(tmp_dir, dvc, caplog, local_remote):
    tmp_dir.dvc_gen({"foo": "foo", "bar": "bar"})
    dvc.push()
    clean(["foo", "bar"], dvc)
    (tmp_dir / "bar").write_text("foobar")
    caplog.clear()
    with caplog.at_level(level=logging.INFO, logger="dvc"):
        main(["pull", "--force"])
    assert "M\tbar" in caplog.text
    assert "A\tfoo" in caplog.text
    assert "2 files fetched" in caplog.text
    assert "1 file added" in caplog.text
    assert "1 file modified" in caplog.text
    with caplog.at_level(level=logging.INFO, logger="dvc"):
        main(["pull"])
    assert "Everything is up to date." in caplog.text


@pytest.mark.parametrize(
    "key,expected", [("all_tags", 2), ("all_branches", 3), ("all_commits", 3)]
)
def test_push_pull_all(tmp_dir, scm, dvc, local_remote, key, expected):
    tmp_dir.dvc_gen({"foo": "foo"}, commit="first")
    scm.tag("v1")
    dvc.remove("foo.dvc")
    tmp_dir.dvc_gen({"bar": "bar"}, commit="second")
    scm.tag("v2")
    with tmp_dir.branch("branch", new=True):
        dvc.remove("bar.dvc")
        tmp_dir.dvc_gen({"baz": "baz"}, commit="branch")

    assert dvc.push(**{key: True}) == expected

    clean(["foo", "bar", "baz"], dvc)
    assert dvc.pull(**{key: True})["fetched"] == expected


def test_push_pull_fetch_pipeline_stages(tmp_dir, dvc, run_copy, local_remote):
    tmp_dir.dvc_gen("foo", "foo")
    run_copy("foo", "bar", no_commit=True, name="copy-foo-bar")

    dvc.push("copy-foo-bar")
    assert len(recurse_list_dir(local_remote.url)) == 1
    # pushing everything so as we can check pull/fetch only downloads
    # from specified targets
    dvc.push()
    clean(["foo", "bar"], dvc)

    dvc.pull("copy-foo-bar")
    assert (tmp_dir / "bar").exists()
    assert len(recurse_list_dir(dvc.cache.local.cache_dir)) == 1
    clean(["bar"], dvc)

    dvc.fetch("copy-foo-bar")
    assert len(recurse_list_dir(dvc.cache.local.cache_dir)) == 1
