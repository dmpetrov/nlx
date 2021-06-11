import argparse
import logging

from dvc.command.base import append_doc_link, fix_subparsers
from dvc.command.config import CmdConfig
from dvc.utils import format_link

logger = logging.getLogger(__name__)


class CmdRemote(CmdConfig):
    def __init__(self, args):
        super().__init__(args)

        if getattr(self.args, "name", None):
            self.args.name = self.args.name.lower()

    def _check_exists(self, conf):
        from dvc.config import ConfigError

        if self.args.name not in conf["remote"]:
            raise ConfigError(f"remote '{self.args.name}' doesn't exist.")


class CmdRemoteAdd(CmdRemote):
    def run(self):
        from dvc.config import ConfigError

        if self.args.default:
            logger.info(f"Setting '{self.args.name}' as a default remote.")

        with self.config.edit(self.args.level) as conf:
            if self.args.name in conf["remote"] and not self.args.force:
                raise ConfigError(
                    "remote '{}' already exists. Use `-f|--force` to "
                    "overwrite it.".format(self.args.name)
                )

            conf["remote"][self.args.name] = {"url": self.args.url}
            if self.args.default:
                conf["core"]["remote"] = self.args.name

        return 0


class CmdRemoteRemove(CmdRemote):
    def run(self):
        with self.config.edit(self.args.level) as conf:
            self._check_exists(conf)
            del conf["remote"][self.args.name]

        up_to_level = self.args.level or "repo"
        # Remove core.remote refs to this remote in any shadowing configs
        for level in reversed(self.config.LEVELS):
            with self.config.edit(level) as conf:
                if conf["core"].get("remote") == self.args.name:
                    del conf["core"]["remote"]

            if level == up_to_level:
                break

        return 0


class CmdRemoteModify(CmdRemote):
    def run(self):
        from dvc.config import merge

        with self.config.edit(self.args.level) as conf:
            merged = self.config.load_config_to_level(self.args.level)
            merge(merged, conf)
            self._check_exists(merged)

            if self.args.name not in conf["remote"]:
                conf["remote"][self.args.name] = {}
            section = conf["remote"][self.args.name]
            if self.args.unset:
                section.pop(self.args.option, None)
            else:
                section[self.args.option] = self.args.value
        return 0


class CmdRemoteDefault(CmdRemote):
    def run(self):
        from dvc.config import ConfigError

        if self.args.name is None and not self.args.unset:
            conf = self.config.read(self.args.level)
            try:
                print(conf["core"]["remote"])
            except KeyError:
                logger.info("No default remote set")
                return 1
        else:
            with self.config.edit(self.args.level) as conf:
                if self.args.unset:
                    conf["core"].pop("remote", None)
                else:
                    merged_conf = self.config.load_config_to_level(
                        self.args.level
                    )
                    if (
                        self.args.name in conf["remote"]
                        or self.args.name in merged_conf["remote"]
                    ):
                        conf["core"]["remote"] = self.args.name
                    else:
                        raise ConfigError(
                            "default remote must be present in remote list."
                        )
        return 0


class CmdRemoteList(CmdRemote):
    def run(self):
        conf = self.config.read(self.args.level)
        for name, conf in conf["remote"].items():
            logger.info("{}\t{}".format(name, conf["url"]))
        return 0


class CmdRemoteRename(CmdRemote):
    def _rename_default(self, conf):
        if conf["core"].get("remote") == self.args.name:
            conf["core"]["remote"] = self.args.new

    def run(self):
        from dvc.config import ConfigError

        all_config = self.config.load_config_to_level(None)
        if self.args.new in all_config.get("remote", {}):
            raise ConfigError(
                "Rename failed. Remote name '{}' already exists.".format(
                    {self.args.new}
                )
            )

        with self.config.edit(self.args.level) as conf:
            self._check_exists(conf)
            conf["remote"][self.args.new] = conf["remote"][self.args.name]
            del conf["remote"][self.args.name]
            self._rename_default(conf)

        up_to_level = self.args.level or "repo"
        for level in reversed(self.config.LEVELS):
            if level == up_to_level:
                break
            with self.config.edit(level) as level_conf:
                self._rename_default(level_conf)

        return 0


def add_parser(subparsers, parent_parser):
    from dvc.command.config import parent_config_parser

    REMOTE_HELP = "Set up and manage data remotes."
    remote_parser = subparsers.add_parser(
        "remote",
        parents=[parent_parser],
        description=append_doc_link(REMOTE_HELP, "remote"),
        help=REMOTE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    remote_subparsers = remote_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc remote CMD --help` for " "command-specific help.",
    )

    fix_subparsers(remote_subparsers)

    REMOTE_ADD_HELP = "Add a new data remote."
    remote_add_parser = remote_subparsers.add_parser(
        "add",
        parents=[parent_config_parser, parent_parser],
        description=append_doc_link(REMOTE_ADD_HELP, "remote/add"),
        help=REMOTE_ADD_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    remote_add_parser.add_argument("name", help="Name of the remote")
    remote_add_parser.add_argument(
        "url",
        help="Remote location. See full list of supported URLs at {}".format(
            format_link("https://man.dvc.org/remote")
        ),
    )
    remote_add_parser.add_argument(
        "-d",
        "--default",
        action="store_true",
        default=False,
        help="Set as default remote.",
    )
    remote_add_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Force overwriting existing configs",
    )
    remote_add_parser.set_defaults(func=CmdRemoteAdd)

    REMOTE_DEFAULT_HELP = "Set/unset the default data remote."
    remote_default_parser = remote_subparsers.add_parser(
        "default",
        parents=[parent_config_parser, parent_parser],
        description=append_doc_link(REMOTE_DEFAULT_HELP, "remote/default"),
        help=REMOTE_DEFAULT_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    remote_default_parser.add_argument(
        "name", nargs="?", help="Name of the remote"
    )
    remote_default_parser.add_argument(
        "-u",
        "--unset",
        action="store_true",
        default=False,
        help="Unset default remote.",
    )
    remote_default_parser.set_defaults(func=CmdRemoteDefault)

    REMOTE_MODIFY_HELP = "Modify the configuration of a data remote."
    remote_modify_parser = remote_subparsers.add_parser(
        "modify",
        parents=[parent_config_parser, parent_parser],
        description=append_doc_link(REMOTE_MODIFY_HELP, "remote/modify"),
        help=REMOTE_MODIFY_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    remote_modify_parser.add_argument("name", help="Name of the remote")
    remote_modify_parser.add_argument(
        "option", help="Name of the option to modify."
    )
    remote_modify_parser.add_argument(
        "value", nargs="?", help="(optional) Value of the option."
    )
    remote_modify_parser.add_argument(
        "-u",
        "--unset",
        default=False,
        action="store_true",
        help="Unset option.",
    )
    remote_modify_parser.set_defaults(func=CmdRemoteModify)

    REMOTE_LIST_HELP = "List all available data remotes."
    remote_list_parser = remote_subparsers.add_parser(
        "list",
        parents=[parent_config_parser, parent_parser],
        description=append_doc_link(REMOTE_LIST_HELP, "remote/list"),
        help=REMOTE_LIST_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    remote_list_parser.set_defaults(func=CmdRemoteList)

    REMOTE_REMOVE_HELP = "Remove a data remote."
    remote_remove_parser = remote_subparsers.add_parser(
        "remove",
        parents=[parent_config_parser, parent_parser],
        description=append_doc_link(REMOTE_REMOVE_HELP, "remote/remove"),
        help=REMOTE_REMOVE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    remote_remove_parser.add_argument(
        "name", help="Name of the remote to remove."
    )
    remote_remove_parser.set_defaults(func=CmdRemoteRemove)
    REMOTE_RENAME_HELP = "Rename a DVC remote"
    remote_rename_parser = remote_subparsers.add_parser(
        "rename",
        parents=[parent_config_parser, parent_parser],
        description=append_doc_link(REMOTE_RENAME_HELP, "remote/rename"),
        help=REMOTE_RENAME_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    remote_rename_parser.add_argument("name", help="Remote to be renamed")
    remote_rename_parser.add_argument("new", help="New name of the remote")
    remote_rename_parser.set_defaults(func=CmdRemoteRename)
