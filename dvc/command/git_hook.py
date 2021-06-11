import logging
import os

from dvc.command.base import CmdBaseNoRepo, fix_subparsers
from dvc.exceptions import NotDvcRepoError

logger = logging.getLogger(__name__)


class CmdHookBase(CmdBaseNoRepo):
    def run(self):
        from dvc.repo import Repo

        try:
            repo = Repo()
            repo.close()
        except NotDvcRepoError:
            return 0

        return self._run()

    def _run(self):
        raise NotImplementedError


class CmdPreCommit(CmdHookBase):
    def _run(self):
        from dvc.main import main

        return main(["status"])


class CmdPostCheckout(CmdHookBase):
    def _run(self):
        # when we are running from pre-commit tool, it doesn't provide CLI
        # flags, but instead provides respective env vars that we could use.
        flag = os.environ.get("PRE_COMMIT_CHECKOUT_TYPE")
        if flag is None and len(self.args.args) >= 3:
            # see https://git-scm.com/docs/githooks#_post_checkout
            flag = self.args.args[2]

        # checking out some reference and not specific file.
        if flag != "1":
            return 0

        # make sure we are not in the middle of a rebase/merge, so we
        # don't accidentally break it with an unsuccessful checkout.
        # Note that git hooks are always running in repo root.
        if os.path.isdir(os.path.join(".git", "rebase-merge")):
            return 0

        from dvc.main import main

        return main(["checkout"])


class CmdPrePush(CmdHookBase):
    def _run(self):
        from dvc.main import main

        return main(["push"])


class CmdMergeDriver(CmdHookBase):
    def _run(self):
        from dvc.dvcfile import Dvcfile
        from dvc.repo import Repo

        dvc = Repo()

        try:
            ancestor = Dvcfile(dvc, self.args.ancestor, verify=False)
            our = Dvcfile(dvc, self.args.our, verify=False)
            their = Dvcfile(dvc, self.args.their, verify=False)

            our.merge(ancestor, their)

            return 0
        finally:
            dvc.close()


def add_parser(subparsers, parent_parser):
    GIT_HOOK_HELP = "Run GIT hook."

    git_hook_parser = subparsers.add_parser(
        "git-hook",
        parents=[parent_parser],
        description=GIT_HOOK_HELP,
        add_help=False,
    )

    git_hook_subparsers = git_hook_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc daemon CMD --help` for command-specific help.",
    )

    fix_subparsers(git_hook_subparsers)

    PRE_COMMIT_HELP = "Run pre-commit GIT hook."
    pre_commit_parser = git_hook_subparsers.add_parser(
        "pre-commit",
        parents=[parent_parser],
        description=PRE_COMMIT_HELP,
        help=PRE_COMMIT_HELP,
    )
    pre_commit_parser.add_argument(
        "args", nargs="*", help="Arguments passed by GIT or pre-commit tool."
    )
    pre_commit_parser.set_defaults(func=CmdPreCommit)

    POST_CHECKOUT_HELP = "Run post-checkout GIT hook."
    post_checkout_parser = git_hook_subparsers.add_parser(
        "post-checkout",
        parents=[parent_parser],
        description=POST_CHECKOUT_HELP,
        help=POST_CHECKOUT_HELP,
    )
    post_checkout_parser.add_argument(
        "args", nargs="*", help="Arguments passed by GIT or pre-commit tool."
    )
    post_checkout_parser.set_defaults(func=CmdPostCheckout)

    PRE_PUSH_HELP = "Run pre-push GIT hook."
    pre_push_parser = git_hook_subparsers.add_parser(
        "pre-push",
        parents=[parent_parser],
        description=PRE_PUSH_HELP,
        help=PRE_PUSH_HELP,
    )
    pre_push_parser.add_argument(
        "args", nargs="*", help="Arguments passed by GIT or pre-commit tool."
    )
    pre_push_parser.set_defaults(func=CmdPrePush)

    MERGE_DRIVER_HELP = "Run GIT merge driver."
    merge_driver_parser = git_hook_subparsers.add_parser(
        "merge-driver",
        parents=[parent_parser],
        description=MERGE_DRIVER_HELP,
        help=MERGE_DRIVER_HELP,
    )
    merge_driver_parser.add_argument(
        "--ancestor",
        required=True,
        help="Ancestor's version of the conflicting file.",
    )
    merge_driver_parser.add_argument(
        "--our", required=True, help="Current version of the conflicting file."
    )
    merge_driver_parser.add_argument(
        "--their",
        required=True,
        help="Other branch's version of the conflicting file.",
    )
    merge_driver_parser.set_defaults(func=CmdMergeDriver)
