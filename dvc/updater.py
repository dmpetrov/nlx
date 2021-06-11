import logging
import os
import sys
import time

import colorama
from packaging import version

from dvc import __version__
from dvc.utils.pkg import PKG

logger = logging.getLogger(__name__)


class Updater:  # pragma: no cover
    URL = "https://updater.dvc.org"
    UPDATER_FILE = "updater"
    TIMEOUT = 24 * 60 * 60  # every day
    TIMEOUT_GET = 10

    def __init__(self, tmp_dir, friendly=False, hardlink_lock=False):
        from dvc.lock import make_lock

        self.updater_file = os.path.join(tmp_dir, self.UPDATER_FILE)
        self.lock = make_lock(
            self.updater_file + ".lock",
            tmp_dir=tmp_dir,
            friendly=friendly,
            hardlink_lock=hardlink_lock,
        )
        self.current = version.parse(__version__).base_version

    def _is_outdated_file(self):
        ctime = os.path.getmtime(self.updater_file)
        outdated = time.time() - ctime >= self.TIMEOUT
        if outdated:
            logger.debug(f"'{self.updater_file}' is outdated")
        return outdated

    def _with_lock(self, func, action):
        from dvc.lock import LockError

        try:
            with self.lock:
                func()
        except LockError:
            msg = "Failed to acquire '{}' before {} updates"
            logger.debug(msg.format(self.lock.lockfile, action))

    def check(self):
        from dvc.utils import env2bool

        if (
            os.getenv("CI")
            or env2bool("DVC_TEST")
            or PKG == "snap"
            or not self.is_enabled()
        ):
            return

        self._with_lock(self._check, "checking")

    def _check(self):
        if not os.path.exists(self.updater_file) or self._is_outdated_file():
            self.fetch()
            return

        with open(self.updater_file) as fobj:
            import json

            try:
                info = json.load(fobj)
                latest = info["version"]
            except Exception as exc:  # pylint: disable=broad-except
                msg = "'{}' is not a valid json: {}"
                logger.debug(msg.format(self.updater_file, exc))
                self.fetch()
                return

        if version.parse(self.current) < version.parse(latest):
            self._notify(latest)

    def fetch(self, detach=True):
        from dvc.daemon import daemon

        if detach:
            daemon(["updater"])
            return

        self._with_lock(self._get_latest_version, "fetching")

    def _get_latest_version(self):
        import json

        import requests

        try:
            resp = requests.get(self.URL, timeout=self.TIMEOUT_GET)
            info = resp.json()
        except requests.exceptions.RequestException as exc:
            msg = "Failed to retrieve latest version: {}"
            logger.debug(msg.format(exc))
            return

        with open(self.updater_file, "w+") as fobj:
            json.dump(info, fobj)

    def _notify(self, latest):
        if not sys.stdout.isatty():
            return

        message = (
            "Update available {red}{current}{reset} -> {green}{latest}{reset}"
            + "\n"
            + self._get_update_instructions()
        ).format(
            red=colorama.Fore.RED,
            reset=colorama.Fore.RESET,
            green=colorama.Fore.GREEN,
            yellow=colorama.Fore.YELLOW,
            blue=colorama.Fore.BLUE,
            current=self.current,
            latest=latest,
        )

        from dvc.utils import boxify

        logger.info(boxify(message, border_color="yellow"))

    def _get_update_instructions(self):
        instructions = {
            "pip": "Run `{yellow}pip{reset} install dvc "
            "{blue}--upgrade{reset}`",
            "rpm": "Run `{yellow}yum{reset} update dvc`",
            "brew": "Run `{yellow}brew{reset} upgrade dvc`",
            "deb": (
                "Run `{yellow}apt-get{reset} install"
                " {blue}--only-upgrade{reset} dvc`"
            ),
            "binary": (
                "To upgrade follow these steps:\n"
                "1. Uninstall dvc binary\n"
                "2. Go to {blue}https://dvc.org{reset}\n"
                "3. Download and install new binary"
            ),
            "conda": "Run `{yellow}conda{reset} update dvc`",
            "choco": "Run `{yellow}choco{reset} upgrade dvc`",
            None: (
                "Find the latest release at\n"
                "{blue}https://github.com/iterative/dvc/releases/latest{reset}"
            ),
        }

        package_manager = PKG
        if package_manager in ("osxpkg", "exe"):
            package_manager = "binary"

        return instructions[package_manager]

    def is_enabled(self):
        from dvc.config import Config, to_bool

        enabled = to_bool(
            Config(validate=False).get("core", {}).get("check_update", "true")
        )
        logger.debug(
            "Check for update is {}abled.".format("en" if enabled else "dis")
        )
        return enabled
