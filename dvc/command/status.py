import logging

from dvc.command.data_sync import CmdDataBase
from dvc.exceptions import DvcException
from dvc.utils import format_link

logger = logging.getLogger(__name__)


class CmdDataStatus(CmdDataBase):
    STATUS_LEN = 20
    STATUS_INDENT = "\t"
    UP_TO_DATE_MSG = "Data and pipelines are up to date."
    IN_SYNC_MSG = "Cache and remote '{remote}' are in sync."
    EMPTY_PROJECT_MSG = (
        "There are no data or pipelines tracked in this project yet.\n"
        "See {link} to get started!"
    ).format(link=format_link("https://dvc.org/doc/start"))

    def _normalize(self, s):
        s += ":"
        assert len(s) < self.STATUS_LEN
        return s + (self.STATUS_LEN - len(s)) * " "

    def _show(self, status, indent=0):
        ind = indent * self.STATUS_INDENT

        if isinstance(status, str):
            logger.info(f"{ind}{status}")
            return

        if isinstance(status, list):
            for entry in status:
                self._show(entry, indent)
            return

        assert isinstance(status, dict)

        for key, value in status.items():
            if isinstance(value, str):
                logger.info("{}{}{}".format(ind, self._normalize(value), key))
            elif value:
                logger.info(f"{ind}{key}:")
                self._show(value, indent + 1)

    def run(self):
        from dvc.repo import lock_repo
        from dvc.ui import ui

        indent = 1 if self.args.cloud else 0

        with lock_repo(self.repo):
            try:
                st = self.repo.status(
                    targets=self.args.targets,
                    jobs=self.args.jobs,
                    cloud=self.args.cloud,
                    remote=self.args.remote,
                    all_branches=self.args.all_branches,
                    all_tags=self.args.all_tags,
                    all_commits=self.args.all_commits,
                    with_deps=self.args.with_deps,
                    recursive=self.args.recursive,
                )
            except DvcException:
                logger.exception("")
                return 1

            if self.args.quiet:
                return bool(st)

            if self.args.show_json:
                import json

                logger.info(json.dumps(st))
                return 0

            if st:
                self._show(st, indent)
                return 0

            # additional hints for the user
            if not self.repo.stages:
                ui.write(self.EMPTY_PROJECT_MSG)
            elif self.args.cloud or self.args.remote:
                remote = self.args.remote or self.repo.config["core"].get(
                    "remote"
                )
                ui.write(self.IN_SYNC_MSG.format(remote=remote))
            else:
                ui.write(self.UP_TO_DATE_MSG)

        return 0
