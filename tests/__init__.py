import os
import sys

# Increasing fd ulimit for tests
if os.name == "nt":
    import subprocess

    import win32file  # pylint: disable=import-error

    win32file._setmaxstdio(4096)

    # Workaround for two bugs:
    #
    # 1) gitpython-developers/GitPython#546
    # GitPython leaves git cat-file --batch/--batch-check processes that are
    # not cleaned up correctly, so Popen._active list has their defunct
    # process handles, that it is not able to cleanup because of bug 2)
    #
    # 2) https://bugs.python.org/issue37380
    # subprocess.Popen._internal_poll on windows is getting
    #
    # 	OSError: [WinError 6] The handle is invalid
    #
    # exception, which it doesn't ignore and so Popen is not able to cleanup
    # old processes and that prevents it from creating any new processes at
    # all, which results in our tests failing whenever they try to use Popen.
    # This patch was released in 3.9.0 and backported to some earlier
    # versions.
    if sys.version_info < (3, 9, 0):

        def noop():
            pass

        subprocess._cleanup = noop
        subprocess._active = None
else:
    import resource

    resource.setrlimit(resource.RLIMIT_NOFILE, (4096, 4096))

    nproc_soft, nproc_hard = resource.getrlimit(resource.RLIMIT_NPROC)
    resource.setrlimit(resource.RLIMIT_NPROC, (nproc_hard, nproc_hard))
