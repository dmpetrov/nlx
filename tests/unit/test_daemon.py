import inspect
import os
from unittest import TestCase

import mock

from dvc import daemon


class TestDaemon(TestCase):
    @mock.patch("dvc.daemon._spawn_posix")
    @mock.patch("dvc.daemon._spawn_windows")
    def test(self, mock_windows, mock_posix):
        daemon.daemon(["updater"])

        if os.name == "nt":
            mock_posix.assert_not_called()
            mock_windows.assert_called()
            args = mock_windows.call_args[0]
        else:
            mock_windows.assert_not_called()
            mock_posix.assert_called()
            args = mock_posix.call_args[0]

        env = args[1]
        self.assertTrue("PYTHONPATH" in env.keys())

        file_path = os.path.abspath(inspect.stack()[0][1])
        file_dir = os.path.dirname(file_path)
        test_dir = os.path.dirname(file_dir)
        dvc_dir = os.path.dirname(test_dir)
        self.assertEqual(env["PYTHONPATH"], dvc_dir)
        self.assertEqual(env[daemon.DVC_DAEMON], "1")


def test_no_recursive_spawn():
    with mock.patch.dict(os.environ, {daemon.DVC_DAEMON: "1"}):
        with mock.patch("dvc.daemon._spawn") as mock_spawn:
            daemon.daemon(["updater"])
            mock_spawn.assert_not_called()
