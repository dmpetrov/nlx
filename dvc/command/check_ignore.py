import argparse

from dvc.command import completion
from dvc.command.base import CmdBase, append_doc_link
from dvc.ui import ui


class CmdCheckIgnore(CmdBase):
    def __init__(self, args):
        super().__init__(args)
        self.ignore_filter = self.repo.dvcignore

    def _show_results(self, result):
        if not result.match and not self.args.non_matching:
            return

        if self.args.details:
            patterns = result.patterns
            if not self.args.all:
                patterns = patterns[-1:]

            for pattern in patterns:
                ui.write(pattern, result.file, sep="\t")
        else:
            ui.write(result.file)

    def _check_one_file(self, target):
        result = self.ignore_filter.check_ignore(target)
        self._show_results(result)
        if result.match:
            return 0
        return 1

    def _interactive_mode(self):
        ret = 1
        while True:
            try:
                target = input()
            except (KeyboardInterrupt, EOFError):
                break
            if target == "":
                break
            if not self._check_one_file(target):
                ret = 0
        return ret

    def _normal_mode(self):
        ret = 1
        for target in self.args.targets:
            if not self._check_one_file(target):
                ret = 0
        return ret

    def _check_args(self):
        from dvc.exceptions import DvcException

        if not self.args.stdin and not self.args.targets:
            raise DvcException("`targets` or `--stdin` needed")

        if self.args.stdin and self.args.targets:
            raise DvcException("cannot have both `targets` and `--stdin`")

        if self.args.non_matching and not self.args.details:
            raise DvcException(
                "`--non-matching` is only valid with `--details`"
            )

        if self.args.all and not self.args.details:
            raise DvcException("`--all` is only valid with `--details`")

        if self.args.quiet and self.args.details:
            raise DvcException("cannot use both `--details` and `--quiet`")

    def run(self):
        self._check_args()
        if self.args.stdin:
            return self._interactive_mode()
        return self._normal_mode()


def add_parser(subparsers, parent_parser):
    ADD_HELP = (
        "Check whether files or directories are excluded due to `.dvcignore`."
    )

    parser = subparsers.add_parser(
        "check-ignore",
        parents=[parent_parser],
        description=append_doc_link(ADD_HELP, "check-ignore"),
        help=ADD_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-d",
        "--details",
        action="store_true",
        default=False,
        help="Show the exclude patterns along with each target path.",
    )
    parser.add_argument(
        "-a",
        "--all",
        action="store_true",
        default=False,
        help="Include the target paths which don’t match any pattern "
        "in the `--details` list.",
    )
    parser.add_argument(
        "-n",
        "--non-matching",
        action="store_true",
        default=False,
        help="Include the target paths which don’t match any pattern "
        "in the `--details` list.",
    )
    parser.add_argument(
        "--stdin",
        action="store_true",
        default=False,
        help="Read paths from standard input instead of providing `targets`.",
    )
    parser.add_argument(
        "targets", nargs="*", help="File or directory paths to check"
    ).complete = completion.FILE
    parser.set_defaults(func=CmdCheckIgnore)
