import logging
import os
import signal
import subprocess
import threading

from funcy import first

from dvc.stage.monitor import Monitor
from dvc.utils import fix_env

from .decorators import unlocked_repo
from .exceptions import StageCmdFailedError

logger = logging.getLogger(__name__)


def _make_cmd(executable, cmd):
    if executable is None:
        return cmd
    opts = {"zsh": ["--no-rcs"], "bash": ["--noprofile", "--norc"]}
    name = os.path.basename(executable).lower()
    return [executable] + opts.get(name, []) + ["-c", cmd]


def warn_if_fish(executable):
    if (
        executable is None
        or os.path.basename(os.path.realpath(executable)) != "fish"
    ):
        return

    logger.warning(
        "DVC detected that you are using fish as your default "
        "shell. Be aware that it might cause problems by overwriting "
        "your current environment variables with values defined "
        "in '.fishrc', which might affect your command. See "
        "https://github.com/iterative/dvc/issues/1307. "
    )


def _enforce_cmd_list(cmd):
    assert cmd
    return cmd if isinstance(cmd, list) else cmd.splitlines()


def prepare_kwargs(stage, checkpoint_func=None, run_env=None):
    kwargs = {"cwd": stage.wdir, "env": fix_env(None), "close_fds": True}

    kwargs["env"].update(stage.env(checkpoint_func=checkpoint_func))
    if run_env:
        kwargs["env"].update(run_env)

    # NOTE: when you specify `shell=True`, `Popen` [1] will default to
    # `/bin/sh` on *nix and will add ["/bin/sh", "-c"] to your command.
    # But we actually want to run the same shell that we are running
    # from right now, which is usually determined by the `SHELL` env
    # var. So instead, we compose our command on our own, making sure
    # to include special flags to prevent shell from reading any
    # configs and modifying env, which may change the behavior or the
    # command we are running. See [2] for more info.
    #
    # [1] https://github.com/python/cpython/blob/3.7/Lib/subprocess.py
    #                                                            #L1426
    # [2] https://github.com/iterative/dvc/issues/2506
    #                                           #issuecomment-535396799
    kwargs["shell"] = True if os.name == "nt" else False
    return kwargs


def display_command(cmd):
    logger.info("%s %s", ">", cmd)


def get_executable():
    return (os.getenv("SHELL") or "/bin/sh") if os.name != "nt" else None


def _run(stage, executable, cmd, checkpoint_func, **kwargs):
    main_thread = isinstance(
        threading.current_thread(),
        threading._MainThread,  # pylint: disable=protected-access
    )

    exec_cmd = _make_cmd(executable, cmd)
    old_handler = None

    try:
        p = subprocess.Popen(exec_cmd, **kwargs)
        if main_thread:
            old_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)

        tasks = _get_monitor_tasks(stage, checkpoint_func, p)

        if tasks:
            with Monitor(tasks):
                p.communicate()
        else:
            p.communicate()

        if p.returncode != 0:
            for t in tasks:
                if t.killed.is_set():
                    raise t.error_cls(cmd, p.returncode)
            raise StageCmdFailedError(cmd, p.returncode)
    finally:
        if old_handler:
            signal.signal(signal.SIGINT, old_handler)


def _get_monitor_tasks(stage, checkpoint_func, proc):

    result = []
    if checkpoint_func:
        from .monitor import CheckpointTask

        result.append(CheckpointTask(stage, checkpoint_func, proc))

    live = first(o for o in stage.outs if (o.live and o.live["html"]))
    if live:
        from .monitor import LiveTask

        result.append(LiveTask(stage, live, proc))

    return result


def cmd_run(stage, dry=False, checkpoint_func=None, run_env=None):
    logger.info("Running stage '%s':", stage.addressing)
    commands = _enforce_cmd_list(stage.cmd)
    kwargs = prepare_kwargs(
        stage, checkpoint_func=checkpoint_func, run_env=run_env
    )
    executable = get_executable()

    if not dry:
        warn_if_fish(executable)

    for cmd in commands:
        display_command(cmd)
        if dry:
            continue

        _run(stage, executable, cmd, checkpoint_func=checkpoint_func, **kwargs)


def run_stage(
    stage, dry=False, force=False, checkpoint_func=None, run_env=None, **kwargs
):
    if not (dry or force or checkpoint_func):
        from .cache import RunCacheNotFoundError

        try:
            stage.repo.stage_cache.restore(stage, **kwargs)
            return
        except RunCacheNotFoundError:
            stage.save_deps()

    run = cmd_run if dry else unlocked_repo(cmd_run)
    run(stage, dry=dry, checkpoint_func=checkpoint_func, run_env=run_env)
