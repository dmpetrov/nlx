import errno
import os
import stat

import configobj
import pytest
from mock import patch

from dvc.config import Config
from dvc.exceptions import DownloadError, RemoteCacheRequiredError, UploadError
from dvc.fs.local import LocalFileSystem
from dvc.main import main
from dvc.path_info import PathInfo
from dvc.utils.fs import remove
from tests.basic_env import TestDvc
from tests.remotes import Local


class TestRemote(TestDvc):
    def test(self):
        remotes = ["a", "b", "c"]

        self.assertEqual(main(["remote", "list"]), 0)
        self.assertNotEqual(main(["remote", "remove", remotes[0]]), 0)

        for r in remotes:
            self.assertEqual(
                main(["remote", "add", "--default", r, "s3://bucket/name"]), 0
            )

        self.assertEqual(main(["remote", "list"]), 0)

        self.assertEqual(
            main(["remote", "modify", remotes[0], "checksum_jobs", "1"]), 0
        )
        self.assertEqual(main(["remote", "remove", remotes[0]]), 0)

        self.assertEqual(main(["remote", "list"]), 0)

    def test_relative_path(self):
        dname = os.path.join("..", "path", "to", "dir")
        ret = main(["remote", "add", "mylocal", dname])
        self.assertEqual(ret, 0)

        # NOTE: we are in the repo's root and config is in .dvc/, so
        # dir path written to config should be just one level above.
        rel = os.path.join("..", dname)
        config = configobj.ConfigObj(self.dvc.config.files["repo"])
        self.assertEqual(
            config['remote "mylocal"']["url"], rel.replace("\\", "/")
        )

    def test_overwrite(self):
        remote_name = "a"
        remote_url = "s3://bucket/name"
        self.assertEqual(main(["remote", "add", remote_name, remote_url]), 0)
        self.assertEqual(main(["remote", "add", remote_name, remote_url]), 251)
        self.assertEqual(
            main(["remote", "add", "-f", remote_name, remote_url]), 0
        )

    def test_referencing_other_remotes(self):
        assert main(["remote", "add", "foo", "ssh://localhost/"]) == 0
        assert main(["remote", "add", "bar", "remote://foo/dvc-storage"]) == 0

        config = configobj.ConfigObj(self.dvc.config.files["repo"])
        assert config['remote "bar"']["url"] == "remote://foo/dvc-storage"


def test_remove_default(tmp_dir, dvc):
    remote = "mys3"
    assert (
        main(["remote", "add", "--default", remote, "s3://bucket/name"]) == 0
    )
    assert main(["remote", "modify", remote, "profile", "default"]) == 0
    assert main(["config", "--local", "core.remote", remote]) == 0

    config = configobj.ConfigObj(dvc.config.files["repo"])
    local_config = configobj.ConfigObj(dvc.config.files["local"])
    assert config["core"]["remote"] == remote
    assert local_config["core"]["remote"] == remote

    assert main(["remote", "remove", remote]) == 0

    config = configobj.ConfigObj(dvc.config.files["repo"])
    local_config = configobj.ConfigObj(dvc.config.files["local"])
    assert config.get("core", {}).get("remote") is None
    assert local_config.get("core", {}).get("remote") is None


class TestRemoteRemove(TestDvc):
    def test(self):
        ret = main(["config", "core.checksum_jobs", "1"])
        self.assertEqual(ret, 0)

        remote = "mys3"
        ret = main(["remote", "add", remote, "s3://bucket/name"])
        self.assertEqual(ret, 0)

        ret = main(["remote", "remove", remote])
        self.assertEqual(ret, 0)


class TestRemoteDefault(TestDvc):
    def test(self):
        remote = "mys3"
        ret = main(["remote", "add", "mys3", "s3://bucket/path"])
        self.assertEqual(ret, 0)

        ret = main(["remote", "default", "mys3"])
        self.assertEqual(ret, 0)
        config_file = os.path.join(self.dvc.dvc_dir, Config.CONFIG)
        config = configobj.ConfigObj(config_file)
        default = config["core"]["remote"]
        self.assertEqual(default, remote)

        ret = main(["remote", "default", "--unset"])
        self.assertEqual(ret, 0)
        config = configobj.ConfigObj(config_file)
        default = config.get("core", {}).get("remote")
        self.assertEqual(default, None)


def test_show_default(dvc, capsys):
    assert main(["remote", "add", "foo", "s3://bucket/name"]) == 0
    assert main(["remote", "default", "foo"]) == 0
    assert main(["remote", "default"]) == 0
    out, _ = capsys.readouterr()
    assert out == "foo\n"


class TestRemoteShouldHandleUppercaseRemoteName(TestDvc):
    upper_case_remote_name = "UPPERCASEREMOTE"

    def test(self):
        remote_url = Local.get_storagepath()
        ret = main(["remote", "add", self.upper_case_remote_name, remote_url])
        self.assertEqual(ret, 0)

        ret = main(["add", self.FOO])
        self.assertEqual(ret, 0)

        ret = main(["push", "-r", self.upper_case_remote_name])
        self.assertEqual(ret, 0)


def test_dir_hash_should_be_key_order_agnostic(tmp_dir, dvc):
    from dvc.objects.stage import stage
    from dvc.objects.tree import Tree

    tmp_dir.gen({"data": {"1": "1 content", "2": "2 content"}})

    path_info = PathInfo("data")

    tree = Tree.from_list(
        [{"relpath": "1", "md5": "1"}, {"relpath": "2", "md5": "2"}]
    )
    with patch("dvc.objects.stage._get_tree_obj", return_value=tree):
        hash1 = stage(
            dvc.odb.local, path_info, dvc.odb.local.fs, "md5"
        ).hash_info

    tree = Tree.from_list(
        [{"md5": "1", "relpath": "1"}, {"md5": "2", "relpath": "2"}]
    )
    with patch("dvc.objects.stage._get_tree_obj", return_value=tree):
        hash2 = stage(
            dvc.odb.local, path_info, dvc.odb.local.fs, "md5"
        ).hash_info

    assert hash1 == hash2


def test_partial_push_n_pull(tmp_dir, dvc, tmp_path_factory, local_remote):
    foo = tmp_dir.dvc_gen({"foo": "foo content"})[0].outs[0]
    bar = tmp_dir.dvc_gen({"bar": "bar content"})[0].outs[0]
    baz = tmp_dir.dvc_gen({"baz": {"foo": "baz content"}})[0].outs[0]

    # Faulty upload version, failing on foo
    original = LocalFileSystem._upload

    def unreliable_upload(self, from_file, to_info, name=None, **kwargs):
        if "foo" in name:
            raise Exception("stop foo")
        return original(self, from_file, to_info, name, **kwargs)

    with patch.object(LocalFileSystem, "_upload", unreliable_upload):
        with pytest.raises(UploadError) as upload_error_info:
            dvc.push()
        assert upload_error_info.value.amount == 3

        remote = dvc.cloud.get_remote("upstream")
        assert not remote.fs.exists(
            remote.odb.hash_to_path_info(foo.hash_info.value)
        )
        assert remote.fs.exists(
            remote.odb.hash_to_path_info(bar.hash_info.value)
        )
        assert not remote.fs.exists(
            remote.odb.hash_to_path_info(baz.hash_info.value)
        )

    # Push everything and delete local cache
    dvc.push()
    remove(dvc.odb.local.cache_dir)

    baz.collect_used_dir_cache()
    with patch.object(LocalFileSystem, "_download", side_effect=Exception):
        with pytest.raises(DownloadError) as download_error_info:
            dvc.pull()
        # error count should be len(.dir + standalone file checksums)
        # since files inside dir are ignored if dir cache entry is missing
        assert download_error_info.value.amount == 3


def test_raise_on_too_many_open_files(
    tmp_dir, dvc, tmp_path_factory, mocker, local_remote
):
    tmp_dir.dvc_gen({"file": "file content"})

    mocker.patch.object(
        LocalFileSystem,
        "_upload",
        side_effect=OSError(errno.EMFILE, "Too many open files"),
    )

    with pytest.raises(OSError) as e:
        dvc.push()
        assert e.errno == errno.EMFILE


def test_modify_missing_remote(tmp_dir, dvc):
    assert main(["remote", "modify", "myremote", "user", "xxx"]) == 251


def test_remote_modify_local_on_repo_config(tmp_dir, dvc):
    assert main(["remote", "add", "myremote", "http://example.com/path"]) == 0
    assert (
        main(["remote", "modify", "myremote", "user", "xxx", "--local"]) == 0
    )
    assert dvc.config.load_one("local")["remote"]["myremote"] == {
        "user": "xxx"
    }
    assert dvc.config.load_one("repo")["remote"]["myremote"] == {
        "url": "http://example.com/path"
    }
    dvc.config.load()
    assert dvc.config["remote"]["myremote"] == {
        "url": "http://example.com/path",
        "user": "xxx",
    }


def test_external_dir_resource_on_no_cache(tmp_dir, dvc, tmp_path_factory):
    # https://github.com/iterative/dvc/issues/2647, is some situations
    # (external dir dependency) cache is required to calculate dir md5
    external_dir = tmp_path_factory.mktemp("external_dir")
    (external_dir / "file").write_text("content")

    dvc.odb.local = None
    with pytest.raises(RemoteCacheRequiredError):
        dvc.run(
            cmd="echo hello world",
            outs=[os.fspath(external_dir)],
            single_stage=True,
        )


def test_push_order(tmp_dir, dvc, tmp_path_factory, mocker, local_remote):
    foo = tmp_dir.dvc_gen({"foo": {"bar": "bar content"}})[0].outs[0]
    tmp_dir.dvc_gen({"baz": "baz content"})

    mocked_upload = mocker.patch.object(
        LocalFileSystem, "_upload", return_value=0
    )
    dvc.push()

    # foo .dir file should be uploaded after bar
    remote = dvc.cloud.get_remote("upstream")
    foo_path = remote.odb.hash_to_path_info(foo.hash_info.value)
    bar_path = remote.odb.hash_to_path_info(
        foo.obj.trie[("bar",)].hash_info.value
    )
    paths = [args[1] for args, _ in mocked_upload.call_args_list]
    assert paths.index(foo_path) > paths.index(bar_path)


def test_remote_modify_validation(dvc):
    remote_name = "drive"
    unsupported_config = "unsupported_config"
    assert (
        main(["remote", "add", "-d", remote_name, "gdrive://test/test"]) == 0
    )
    assert (
        main(
            ["remote", "modify", remote_name, unsupported_config, "something"]
        )
        == 251
    )
    config = configobj.ConfigObj(dvc.config.files["repo"])
    assert unsupported_config not in config[f'remote "{remote_name}"']


def test_remote_modify_unset(dvc):
    assert main(["remote", "add", "-d", "myremote", "gdrive://test/test"]) == 0
    config = configobj.ConfigObj(dvc.config.files["repo"])
    assert config['remote "myremote"'] == {"url": "gdrive://test/test"}

    assert (
        main(["remote", "modify", "myremote", "gdrive_client_id", "something"])
        == 0
    )
    config = configobj.ConfigObj(dvc.config.files["repo"])
    assert config['remote "myremote"'] == {
        "url": "gdrive://test/test",
        "gdrive_client_id": "something",
    }

    assert (
        main(["remote", "modify", "myremote", "gdrive_client_id", "--unset"])
        == 0
    )
    config = configobj.ConfigObj(dvc.config.files["repo"])
    assert config['remote "myremote"'] == {"url": "gdrive://test/test"}


def test_remote_modify_default(dvc):
    remote_repo = "repo_level"
    remote_local = "local_level"
    wrong_name = "anything"
    assert main(["remote", "add", remote_repo, "s3://bucket/repo"]) == 0
    assert main(["remote", "add", remote_local, "s3://bucket/local"]) == 0

    assert main(["remote", "default", wrong_name]) == 251
    assert main(["remote", "default", remote_repo]) == 0
    assert main(["remote", "default", "--local", remote_local]) == 0

    repo_config = configobj.ConfigObj(dvc.config.files["repo"])
    local_config = configobj.ConfigObj(dvc.config.files["local"])

    assert repo_config["core"]["remote"] == remote_repo
    assert local_config["core"]["remote"] == remote_local


def test_remote_rename(dvc):
    remote_name = "drive"
    remote_url = "gdrive://test/test"
    new_name = "new"
    other_name = "other"
    # prepare
    assert main(["remote", "add", remote_name, remote_url]) == 0
    config = dvc.config.load_one("repo")
    assert config["remote"][remote_name]["url"] == remote_url
    assert new_name not in config.get("remote", {})

    # rename failed
    assert main(["remote", "rename", remote_name]) == 254
    assert main(["remote", "rename", new_name, other_name]) == 251
    config = dvc.config.load_one("repo")
    assert config["remote"][remote_name]["url"] == remote_url
    assert new_name not in config.get("remote", {})

    # rename success
    assert main(["remote", "rename", remote_name, new_name]) == 0
    config = dvc.config.load_one("repo")
    assert remote_name not in config.get("remote", {})
    assert config["remote"][new_name]["url"] == remote_url


def test_remote_duplicated(dvc):
    remote_name = "drive"
    remote_url = "gdrive://test/test"
    used_name = "overlap"
    another_url = "gdrive://test/test1"
    # prepare
    assert main(["remote", "add", remote_name, remote_url]) == 0
    assert main(["remote", "add", "--local", used_name, another_url]) == 0
    config = dvc.config.load_one("repo")
    assert config["remote"][remote_name]["url"] == remote_url
    local_config = dvc.config.load_one("local")
    assert local_config["remote"][used_name]["url"] == another_url

    # rename duplicated
    assert main(["remote", "rename", remote_name, used_name]) == 251
    config = dvc.config.load_one("repo")
    assert config["remote"][remote_name]["url"] == remote_url
    local_config = dvc.config.load_one("local")
    assert local_config["remote"][used_name]["url"] == another_url


def test_remote_default(dvc):
    remote_name = "drive"
    remote_url = "gdrive://test/test"
    new_name = "new"
    # prepare
    assert main(["remote", "add", "-d", remote_name, remote_url]) == 0
    assert main(["remote", "default", "--local", remote_name]) == 0
    config = dvc.config.load_one("repo")
    assert config["core"]["remote"] == remote_name
    assert config["remote"][remote_name]["url"] == remote_url
    assert new_name not in config.get("remote", {})
    local_config = dvc.config.load_one("local")
    assert local_config["core"]["remote"] == remote_name

    # rename success
    assert main(["remote", "rename", remote_name, new_name]) == 0
    config = dvc.config.load_one("repo")
    assert remote_name not in config.get("remote", {})
    assert config["core"]["remote"] == new_name
    assert config["remote"][new_name]["url"] == remote_url
    assert remote_name not in config.get("remote", {})
    local_config = dvc.config.load_one("local")
    assert local_config["core"]["remote"] == new_name


def test_protect_local_remote(tmp_dir, dvc, local_remote):
    (stage,) = tmp_dir.dvc_gen("file", "file content")

    dvc.push()
    remote = dvc.cloud.get_remote("upstream")
    remote_cache_file = remote.odb.hash_to_path_info(
        stage.outs[0].hash_info.value
    )

    assert os.path.exists(remote_cache_file)
    assert stat.S_IMODE(os.stat(remote_cache_file).st_mode) == 0o444


def test_push_incomplete_dir(tmp_dir, dvc, mocker, local_remote):
    (stage,) = tmp_dir.dvc_gen({"dir": {"foo": "foo", "bar": "bar"}})
    remote = dvc.cloud.get_remote("upstream")

    odb = dvc.odb.local
    out = stage.outs[0]
    file_objs = [entry_obj for _, entry_obj in out.obj]

    # remove one of the cache files for directory
    remove(odb.hash_to_path_info(file_objs[0].hash_info.value))

    dvc.push()
    assert not remote.fs.exists(
        remote.odb.hash_to_path_info(out.hash_info.value)
    )
    assert not remote.fs.exists(
        remote.odb.hash_to_path_info(file_objs[0].hash_info.value)
    )
    assert remote.fs.exists(
        remote.odb.hash_to_path_info(file_objs[1].hash_info.value)
    )


def test_upload_exists(tmp_dir, dvc, local_remote):
    tmp_dir.gen("foo", "foo")
    remote = dvc.cloud.get_remote("upstream")
    # allow uploaded files to be writable for this test,
    # normally they are set to read-only for DVC remotes
    remote.fs.CACHE_MODE = 0o644

    from_info = PathInfo(tmp_dir / "foo")
    to_info = remote.odb.path_info / "foo"
    remote.fs.upload(from_info, to_info)
    assert remote.fs.exists(to_info)

    tmp_dir.gen("foo", "bar")
    remote.fs.upload(from_info, to_info)
    with remote.fs.open(to_info) as fobj:
        assert fobj.read() == "bar"
