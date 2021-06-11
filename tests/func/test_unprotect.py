import os

from dvc.main import main
from tests.basic_env import TestDvc


class TestUnprotect(TestDvc):
    def test(self):
        ret = main(["config", "cache.type", "hardlink"])
        self.assertEqual(ret, 0)

        ret = main(["add", self.FOO])
        self.assertEqual(ret, 0)

        cache = os.path.join(
            ".dvc", "cache", "ac", "bd18db4cc2f85cedef654fccc4a4d8"
        )

        self.assertFalse(os.access(self.FOO, os.W_OK))
        self.assertFalse(os.access(cache, os.W_OK))

        ret = main(["unprotect", self.FOO])
        self.assertEqual(ret, 0)

        self.assertTrue(os.access(self.FOO, os.W_OK))

        if os.name == "nt":
            # NOTE: cache is now unprotected, because NTFS doesn't allow
            # deleting read-only files, so we have to try to set write perms
            # on files that we try to delete, which propagates to the cache
            # file. But it should be restored after the next cache check, hence
            # why we call `dvc status` here.
            self.assertTrue(os.access(cache, os.W_OK))
            ret = main(["status"])
            self.assertEqual(ret, 0)

        self.assertFalse(os.access(cache, os.W_OK))
