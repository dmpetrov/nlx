import json
import logging
import os

logger = logging.getLogger(__name__)


def collect_and_send_report(args=None, return_code=None):
    """
    Collect information from the runtime/environment and the command
    being executed into a report and send it over the network.

    To prevent analytics from blocking the execution of the main thread,
    sending the report is done in a separate process.

    The inter-process communication happens through a file containing the
    report as a JSON, where the _collector_ generates it and the _sender_
    removes it after sending it.
    """
    import tempfile

    from dvc.daemon import daemon

    report = {}

    # Include command execution information on the report only when available.
    if args and hasattr(args, "func"):
        report.update({"cmd_class": args.func.__name__})

    if return_code is not None:
        report.update({"cmd_return_code": return_code})

    with tempfile.NamedTemporaryFile(delete=False, mode="w") as fobj:
        json.dump(report, fobj)
        daemon(["analytics", fobj.name])


def is_enabled():
    from dvc.config import Config, to_bool
    from dvc.utils import env2bool

    if env2bool("DVC_TEST"):
        return False

    enabled = to_bool(
        Config(validate=False).get("core", {}).get("analytics", "true")
    )
    logger.debug("Analytics is {}abled.".format("en" if enabled else "dis"))

    return enabled


def send(path):
    """
    Side effect: Removes the report after sending it.

    The report is generated and stored in a temporary file, see:
    `collect_and_send_report`. Sending happens on another process,
    thus, the need of removing such file afterwards.
    """
    import requests

    url = "https://analytics.dvc.org"
    headers = {"content-type": "application/json"}

    with open(path) as fobj:
        report = json.load(fobj)

    report.update(_runtime_info())

    try:
        requests.post(url, json=report, headers=headers, timeout=5)
    except requests.exceptions.RequestException:
        logger.debug("failed to send analytics report", exc_info=True)

    os.remove(path)


def _scm_in_use():
    from dvc.exceptions import NotDvcRepoError
    from dvc.repo import Repo
    from dvc.scm import SCM, NoSCM
    from dvc.scm.base import SCMError

    try:
        scm = SCM(root_dir=Repo.find_root())
        return type(scm).__name__
    except SCMError:
        return NoSCM.__name__
    except NotDvcRepoError:
        pass


def _runtime_info():
    """
    Gather information from the environment where DVC runs to fill a report.
    """
    from dvc import __version__
    from dvc.utils import is_binary

    return {
        "dvc_version": __version__,
        "is_binary": is_binary(),
        "scm_class": _scm_in_use(),
        "system_info": _system_info(),
        "user_id": _find_or_create_user_id(),
    }


def _system_info():
    import platform
    import sys

    import distro

    system = platform.system()

    if system == "Windows":
        version = sys.getwindowsversion()

        return {
            "os": "windows",
            "windows_version_build": version.build,
            "windows_version_major": version.major,
            "windows_version_minor": version.minor,
            "windows_version_service_pack": version.service_pack,
        }

    if system == "Darwin":
        return {"os": "mac", "mac_version": platform.mac_ver()[0]}

    if system == "Linux":
        return {
            "os": "linux",
            "linux_distro": distro.id(),
            "linux_distro_like": distro.like(),
            "linux_distro_version": distro.version(),
        }

    # We don't collect data for any other system.
    raise NotImplementedError


def _find_or_create_user_id():
    """
    The user's ID is stored on a file under the global config directory.

    The file should contain a JSON with a "user_id" key:

        {"user_id": "16fd2706-8baf-433b-82eb-8c7fada847da"}

    IDs are generated randomly with UUID.
    """
    import uuid

    from dvc.config import Config
    from dvc.lock import Lock, LockError
    from dvc.utils.fs import makedirs

    config_dir = Config.get_dir("global")
    fname = os.path.join(config_dir, "user_id")
    lockfile = os.path.join(config_dir, "user_id.lock")

    # Since the `fname` and `lockfile` are under the global config,
    # we need to make sure such directory exist already.
    makedirs(config_dir, exist_ok=True)

    try:
        with Lock(lockfile):
            try:
                with open(fname) as fobj:
                    user_id = json.load(fobj)["user_id"]

            except (FileNotFoundError, ValueError, KeyError):
                user_id = str(uuid.uuid4())

                with open(fname, "w") as fobj:
                    json.dump({"user_id": user_id}, fobj)

            return user_id

    except LockError:
        logger.debug(f"Failed to acquire '{lockfile}'")
