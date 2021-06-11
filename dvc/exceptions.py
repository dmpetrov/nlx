"""Exceptions raised by the dvc."""
from typing import List


class DvcException(Exception):
    """Base class for all dvc exceptions."""

    def __init__(self, msg, *args):
        assert msg
        self.msg = msg
        super().__init__(msg, *args)


class InvalidArgumentError(ValueError, DvcException):
    """Thrown if arguments are invalid."""


class OutputDuplicationError(DvcException):
    """Thrown if a file/directory is specified as an output in more than one
    stage.

    Args:
        output (unicode): path to the file/directory.
        stages (list): list of paths to stages.
    """

    def __init__(self, output, stages):
        from funcy import first

        assert isinstance(output, str)
        assert all(hasattr(stage, "relpath") for stage in stages)
        if len(stages) == 1:
            msg = "output '{}' is already specified in {}.".format(
                output, first(stages)
            )
        else:
            msg = "output '{}' is already specified in stages:\n{}".format(
                output, "\n".join(f"\t- {s.addressing}" for s in stages)
            )
        super().__init__(msg)
        self.stages = stages
        self.output = output


class OutputNotFoundError(DvcException):
    """Thrown if a file/directory is not found as an output in any pipeline.

    Args:
        output (unicode): path to the file/directory.
    """

    def __init__(self, output, repo=None):
        from dvc.utils import relpath

        self.output = output
        self.repo = repo
        super().__init__(
            "Unable to find DVC file with output '{path}'".format(
                path=relpath(self.output)
            )
        )


class StagePathAsOutputError(DvcException):
    """Thrown if directory that stage is going to be saved in is specified as
    an output of another stage.

    Args:
        stage (Stage): a stage that is in some other stages output
        output (str): an output covering the stage above
    """

    def __init__(self, stage, output):
        assert isinstance(output, str)
        super().__init__(
            "{stage} is within an output '{output}' of another stage".format(
                stage=stage, output=output
            )
        )


class CircularDependencyError(DvcException):
    """Thrown if a file/directory specified both as an output and as a
    dependency.

    Args:
        dependency (str): path to the dependency.
    """

    def __init__(self, dependency):
        assert isinstance(dependency, str)

        msg = "'{}' is specified as an output and as a dependency."
        super().__init__(msg.format(dependency))


class ArgumentDuplicationError(DvcException):
    """Thrown if a file/directory is specified as a dependency/output more
    than once.

    Args:
        path (str): path to the file/directory.
    """

    def __init__(self, path):
        assert isinstance(path, str)
        super().__init__(f"file '{path}' is specified more than once.")


class MoveNotDataSourceError(DvcException):
    """Thrown when trying to move a file/directory that is not an output
    in a data source stage.

    Args:
        path (str): path to the file/directory.
    """

    def __init__(self, path):
        msg = (
            "move is not permitted for stages that are not data sources. "
            "You need to either move '{path}' to a new location and edit "
            "it by hand, or remove '{path}' and create a new one at the "
            "desired location."
        )
        super().__init__(msg.format(path=path))


class NotDvcRepoError(DvcException):
    """Thrown if a directory is not a DVC repo"""


class DvcParserError(DvcException):
    """Base class for CLI parser errors."""

    def __init__(self):
        super().__init__("parser error")


class CyclicGraphError(DvcException):
    def __init__(self, stages):
        assert isinstance(stages, list)
        msg = "Pipeline has a cycle involving: {}.".format(
            ", ".join(s.addressing for s in stages)
        )
        super().__init__(msg)


class ConfirmRemoveError(DvcException):
    def __init__(self, path):
        super().__init__(
            "unable to remove '{}' without a confirmation. Use "
            "`-f` to force.".format(path)
        )


class InitError(DvcException):
    pass


class ReproductionError(DvcException):
    def __init__(self, dvc_file_name):
        self.path = dvc_file_name
        super().__init__(f"failed to reproduce '{dvc_file_name}'")


class BadMetricError(DvcException):
    def __init__(self, paths):
        super().__init__(
            "the following metrics do not exist, "
            "are not metrics files or are malformed: {paths}".format(
                paths=", ".join(f"'{path}'" for path in paths)
            )
        )


class MetricsError(DvcException):
    pass


class NoMetricsParsedError(MetricsError):
    def __init__(self, command):
        super().__init__(
            f"Could not parse {command} files. Use `-v` option to see more "
            "details."
        )


class NoMetricsFoundError(MetricsError):
    def __init__(self, command, run_options):
        super().__init__(
            f"No {command} files in this repository. "
            f"Use `{run_options}` options for "
            f"`dvc run` to mark stage outputs as {command}."
        )


class MetricDoesNotExistError(MetricsError):
    def __init__(self, targets: List[str]):
        if len(targets) == 1:
            msg = "'{}' does not exist."
        else:
            msg = "'{}' do not exist."
        super().__init__(msg.format(", ".join(targets)))


class RecursiveAddingWhileUsingFilename(DvcException):
    def __init__(self):
        super().__init__(
            "cannot use `fname` with multiple targets or `-R|--recursive`"
        )


class OverlappingOutputPathsError(DvcException):
    def __init__(self, parent, overlapping_out, message):
        self.parent = parent
        self.overlapping_out = overlapping_out
        super().__init__(message)


class CheckoutErrorSuggestGit(DvcException):
    def __init__(self, target):
        super().__init__(f"Did you mean `git checkout {target}`?")


class ETagMismatchError(DvcException):
    def __init__(self, etag, cached_etag):
        super().__init__(
            "ETag mismatch detected when copying file to cache! "
            "(expected: '{}', actual: '{}')".format(etag, cached_etag)
        )


class FileMissingError(DvcException):
    def __init__(self, path, hint=None):
        self.path = path
        hint = "" if hint is None else f". {hint}"
        super().__init__(
            f"Can't find '{path}' neither locally nor on remote{hint}"
        )


class DvcIgnoreInCollectedDirError(DvcException):
    def __init__(self, ignore_dirname):
        super().__init__(
            ".dvcignore file should not be in collected dir path: "
            "'{}'".format(ignore_dirname)
        )


class GitHookAlreadyExistsError(DvcException):
    def __init__(self, hook_name):
        from dvc.utils import format_link

        super().__init__(
            "Hook '{}' already exists. Please refer to {} for more "
            "info.".format(
                hook_name, format_link("https://man.dvc.org/install")
            )
        )


class DownloadError(DvcException):
    def __init__(self, amount):
        self.amount = amount

        super().__init__(f"{amount} files failed to download")


class UploadError(DvcException):
    def __init__(self, amount):
        self.amount = amount

        super().__init__(f"{amount} files failed to upload")


class CheckoutError(DvcException):
    def __init__(self, target_infos, stats=None):
        from dvc.utils import error_link

        self.target_infos = target_infos
        self.stats = stats
        targets = [str(t) for t in target_infos]
        m = (
            "Checkout failed for following targets:\n{}\nIs your "
            "cache up to date?\n{}".format(
                "\n".join(targets), error_link("missing-files")
            )
        )
        super().__init__(m)


class CollectCacheError(DvcException):
    pass


class NoRemoteInExternalRepoError(DvcException):
    def __init__(self, url):
        super().__init__(
            f"No DVC remote is specified in target repository '{url}'."
        )


class NoOutputInExternalRepoError(DvcException):
    def __init__(self, path, external_repo_path, external_repo_url):
        from dvc.utils import relpath

        super().__init__(
            "Output '{}' not found in target repository '{}'".format(
                relpath(path, external_repo_path), external_repo_url
            )
        )


class HTTPError(DvcException):
    def __init__(self, code, reason):
        super().__init__(f"'{code} {reason}'")


class PathMissingError(DvcException):
    default_msg = (
        "The path '{}' does not exist in the target repository '{}'"
        " neither as a DVC output nor as a Git-tracked file."
    )
    default_msg_dvc_only = (
        "The path '{}' does not exist in the target repository '{}'"
        " as an DVC output."
    )

    def __init__(self, path, repo, dvc_only=False):
        msg = self.default_msg if not dvc_only else self.default_msg_dvc_only
        super().__init__(msg.format(path, repo))
        self.dvc_only = dvc_only


class RemoteCacheRequiredError(DvcException):
    def __init__(self, path_info):
        from dvc.utils import format_link

        super().__init__(
            (
                "Current operation was unsuccessful because '{}' requires "
                "existing cache on '{}' remote. See {} for information on how "
                "to set up remote cache."
            ).format(
                path_info,
                path_info.scheme,
                format_link("https://man.dvc.org/config#cache"),
            )
        )


class IsADirectoryError(DvcException):  # noqa,pylint:disable=redefined-builtin
    """Raised when a file operation is requested on a directory."""


class NoOutputOrStageError(DvcException):
    """
    Raised when the target is neither an output nor a stage name in dvc.yaml
    """

    def __init__(self, target, file):
        super().__init__(
            f"'{target}' "
            f"does not exist as an output or a stage name in '{file}'"
        )


class MergeError(DvcException):
    pass


class CacheLinkError(DvcException):
    from dvc.utils import format_link

    SUPPORT_LINK = "See {} for more information.".format(
        format_link(
            "https://dvc.org/doc/user-guide/troubleshooting#cache-types"
        )
    )

    def __init__(self, path_infos):
        msg = "No possible cache link types for '{}'. {}".format(
            ", ".join([str(path) for path in path_infos]), self.SUPPORT_LINK
        )
        super().__init__(msg)
        self.path_infos = path_infos
