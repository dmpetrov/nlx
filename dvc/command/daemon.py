from dvc.command import completion
from dvc.command.base import CmdBaseNoRepo, fix_subparsers


class CmdDaemonBase(CmdBaseNoRepo):
    pass


class CmdDaemonUpdater(CmdDaemonBase):
    def run(self):
        import os

        from dvc.config import Config
        from dvc.repo import Repo
        from dvc.updater import Updater

        root_dir = Repo.find_root()
        dvc_dir = os.path.join(root_dir, Repo.DVC_DIR)
        tmp_dir = os.path.join(dvc_dir, "tmp")
        config = Config(dvc_dir, validate=False)
        hardlink_lock = config.get("core", {}).get("hardlink_lock", False)
        updater = Updater(tmp_dir, hardlink_lock=hardlink_lock)
        updater.fetch(detach=False)

        return 0


class CmdDaemonAnalytics(CmdDaemonBase):
    def run(self):
        from dvc import analytics

        analytics.send(self.args.target)

        return 0


def add_parser(subparsers, parent_parser):
    DAEMON_HELP = "Service daemon."
    daemon_parser = subparsers.add_parser(
        "daemon",
        parents=[parent_parser],
        description=DAEMON_HELP,
        add_help=False,
    )

    daemon_subparsers = daemon_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc daemon CMD --help` for command-specific " "help.",
    )

    fix_subparsers(daemon_subparsers)

    DAEMON_UPDATER_HELP = "Fetch latest available version."
    daemon_updater_parser = daemon_subparsers.add_parser(
        "updater",
        parents=[parent_parser],
        description=DAEMON_UPDATER_HELP,
        help=DAEMON_UPDATER_HELP,
    )
    daemon_updater_parser.set_defaults(func=CmdDaemonUpdater)

    DAEMON_ANALYTICS_HELP = "Send dvc usage analytics."
    daemon_analytics_parser = daemon_subparsers.add_parser(
        "analytics",
        parents=[parent_parser],
        description=DAEMON_ANALYTICS_HELP,
        help=DAEMON_ANALYTICS_HELP,
    )
    daemon_analytics_parser.add_argument(
        "target", help="Analytics file."
    ).complete = completion.FILE
    daemon_analytics_parser.set_defaults(func=CmdDaemonAnalytics)
