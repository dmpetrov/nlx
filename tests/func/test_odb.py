import os
import stat

import configobj
import pytest

from dvc.hash_info import HashInfo
from dvc.main import main
from dvc.objects.db import ODBManager
from dvc.objects.errors import ObjectFormatError
from dvc.utils import relpath
from tests.basic_env import TestDir, TestDvc


class TestCache(TestDvc):
    def setUp(self):
        super().setUp()
        self.cache1_md5 = "123"
        self.cache2_md5 = "234"
        self.cache1 = os.path.join(
            self.dvc.odb.local.cache_dir,
            self.cache1_md5[0:2],
            self.cache1_md5[2:],
        )
        self.cache2 = os.path.join(
            self.dvc.odb.local.cache_dir,
            self.cache2_md5[0:2],
            self.cache2_md5[2:],
        )
        self.create(self.cache1, "1")
        self.create(self.cache2, "2")

    def test_all(self):
        md5_list = list(ODBManager(self.dvc).local.all())
        self.assertEqual(len(md5_list), 2)
        self.assertIn(self.cache1_md5, md5_list)
        self.assertIn(self.cache2_md5, md5_list)

    def test_get(self):
        cache = ODBManager(self.dvc).local.hash_to_path_info(self.cache1_md5)
        self.assertEqual(os.fspath(cache), self.cache1)


class TestCacheLoadBadDirCache(TestDvc):
    def _do_test(self, ret):
        self.assertTrue(isinstance(ret, list))
        self.assertEqual(len(ret), 0)

    def test(self):
        from dvc.objects import load

        dir_hash = "123.dir"
        fname = os.fspath(self.dvc.odb.local.hash_to_path_info(dir_hash))
        self.create(fname, "<clearly>not,json")
        with pytest.raises(ObjectFormatError):
            load(self.dvc.odb.local, HashInfo("md5", dir_hash))

        dir_hash = "234.dir"
        fname = os.fspath(self.dvc.odb.local.hash_to_path_info(dir_hash))
        self.create(fname, '{"a": "b"}')
        with pytest.raises(ObjectFormatError):
            load(self.dvc.odb.local, HashInfo("md5", dir_hash))


class TestExternalCacheDir(TestDvc):
    def test(self):
        cache_dir = TestDvc.mkdtemp()

        ret = main(["config", "cache.dir", cache_dir])
        self.assertEqual(ret, 0)

        self.assertFalse(os.path.exists(self.dvc.odb.local.cache_dir))

        ret = main(["add", self.FOO])
        self.assertEqual(ret, 0)

        ret = main(["add", self.DATA_DIR])
        self.assertEqual(ret, 0)

        self.assertFalse(os.path.exists(".dvc/cache"))
        self.assertNotEqual(len(os.listdir(cache_dir)), 0)

    def test_remote_references(self):
        ssh_url = "ssh://user@localhost:23"
        assert main(["remote", "add", "storage", ssh_url]) == 0
        assert main(["remote", "add", "cache", "remote://storage/tmp"]) == 0
        assert main(["config", "cache.ssh", "cache"]) == 0

        self.dvc.__init__()

        assert self.dvc.odb.ssh.path_info == ssh_url + "/tmp"


class TestSharedCacheDir(TestDir):
    def test(self):
        cache_dir = os.path.abspath(os.path.join(os.curdir, "cache"))
        for d in ["dir1", "dir2"]:
            os.mkdir(d)
            os.chdir(d)

            ret = main(["init", "--no-scm"])
            self.assertEqual(ret, 0)

            ret = main(["config", "cache.dir", cache_dir])
            self.assertEqual(ret, 0)

            self.assertFalse(os.path.exists(os.path.join(".dvc", "cache")))

            with open("common", "w+") as fd:
                fd.write("common")

            with open("unique", "w+") as fd:
                fd.write(d)

            ret = main(["add", "common", "unique"])
            self.assertEqual(ret, 0)

            os.chdir("..")

        self.assertFalse(os.path.exists(os.path.join("dir1", ".dvc", "cache")))
        self.assertFalse(os.path.exists(os.path.join("dir2", ".dvc", "cache")))

        subdirs = list(
            filter(
                lambda x: os.path.isdir(os.path.join(cache_dir, x)),
                os.listdir(cache_dir),
            )
        )
        self.assertEqual(len(subdirs), 3)
        self.assertEqual(
            len(os.listdir(os.path.join(cache_dir, subdirs[0]))), 1
        )
        self.assertEqual(
            len(os.listdir(os.path.join(cache_dir, subdirs[1]))), 1
        )
        self.assertEqual(
            len(os.listdir(os.path.join(cache_dir, subdirs[2]))), 1
        )


class TestCacheLinkType(TestDvc):
    def test(self):
        ret = main(["config", "cache.type", "reflink,copy"])
        self.assertEqual(ret, 0)

        ret = main(["add", self.FOO])
        self.assertEqual(ret, 0)


class TestCmdCacheDir(TestDvc):
    def test(self):
        ret = main(["cache", "dir"])
        self.assertEqual(ret, 0)

    def test_abs_path(self):
        dname = os.path.join(os.path.dirname(self._root_dir), "dir")
        ret = main(["cache", "dir", dname])
        self.assertEqual(ret, 0)

        config = configobj.ConfigObj(self.dvc.config.files["repo"])
        self.assertEqual(config["cache"]["dir"], dname.replace("\\", "/"))

    def test_relative_path(self):
        tmpdir = os.path.realpath(self.mkdtemp())
        dname = relpath(tmpdir)
        ret = main(["cache", "dir", dname])
        self.assertEqual(ret, 0)

        # NOTE: we are in the repo's root and config is in .dvc/, so
        # dir path written to config should be just one level above.
        rel = os.path.join("..", dname)
        config = configobj.ConfigObj(self.dvc.config.files["repo"])
        self.assertEqual(config["cache"]["dir"], rel.replace("\\", "/"))

        ret = main(["add", self.FOO])
        self.assertEqual(ret, 0)

        subdirs = os.listdir(tmpdir)
        self.assertEqual(len(subdirs), 1)
        files = os.listdir(os.path.join(tmpdir, subdirs[0]))
        self.assertEqual(len(files), 1)


def test_default_cache_type(dvc):
    assert dvc.odb.local.cache_types == ["reflink", "copy"]


@pytest.mark.skipif(os.name == "nt", reason="Not supported for Windows.")
@pytest.mark.parametrize("group", [False, True])
def test_shared_cache(tmp_dir, dvc, group):
    from dvc.utils.fs import umask

    if group:
        with dvc.config.edit() as conf:
            conf["cache"].update({"shared": "group"})
    dvc.odb = ODBManager(dvc)
    cache_dir = dvc.odb.local.cache_dir

    assert not os.path.exists(cache_dir)

    tmp_dir.dvc_gen(
        {"file": "file content", "dir": {"file2": "file 2 " "content"}}
    )

    actual = {}
    for root, dnames, fnames in os.walk(cache_dir):
        for name in dnames + fnames:
            path = os.path.join(root, name)
            actual[path] = oct(stat.S_IMODE(os.stat(path).st_mode))

    file_mode = oct(0o444)
    dir_mode = oct(0o2775 if group else (0o777 & ~umask))

    expected = {
        os.path.join(cache_dir, "17"): dir_mode,
        os.path.join(
            cache_dir, "17", "4eaa1dd94050255b7b98a7e1924b31.dir"
        ): file_mode,
        os.path.join(cache_dir, "97"): dir_mode,
        os.path.join(
            cache_dir, "97", "e17781c198500e2766ea56bd697c03"
        ): file_mode,
        os.path.join(cache_dir, "d1"): dir_mode,
        os.path.join(
            cache_dir, "d1", "0b4c3ff123b26dc068d43a8bef2d23"
        ): file_mode,
    }

    assert expected == actual
