import logging
import os
from collections import defaultdict
from typing import Dict, List

from dvc.exceptions import PathMissingError
from dvc.objects.stage import get_file_hash
from dvc.objects.stage import stage as ostage
from dvc.repo import locked
from dvc.repo.experiments.utils import fix_exp_head

logger = logging.getLogger(__name__)


@locked
def diff(self, a_rev="HEAD", b_rev=None, targets=None):
    """
    By default, it compares the workspace with the last commit's fs.

    This implementation differs from `git diff` since DVC doesn't have
    the concept of `index`, but it keeps the same interface, thus,
    `dvc diff` would be the same as `dvc diff HEAD`.
    """

    if self.scm.no_commits:
        return {}

    from dvc.fs.repo import RepoFileSystem

    repo_fs = RepoFileSystem(self)

    a_rev = fix_exp_head(self.scm, a_rev)
    b_rev = fix_exp_head(self.scm, b_rev) if b_rev else "workspace"
    results = {}
    missing_targets = {}
    for rev in self.brancher(revs=[a_rev, b_rev]):
        if rev == "workspace" and rev != b_rev:
            # brancher always returns workspace, but we only need to compute
            # workspace paths/checksums if b_rev was None
            continue

        targets_path_infos = None
        if targets is not None:
            # convert targets to path_infos, and capture any missing targets
            targets_path_infos, missing_targets[rev] = _targets_to_path_infos(
                repo_fs, targets
            )

        results[rev] = _paths_checksums(self, repo_fs, targets_path_infos)

    if targets is not None:
        # check for overlapping missing targets between a_rev and b_rev
        for target in set(missing_targets[a_rev]) & set(
            missing_targets[b_rev]
        ):
            raise PathMissingError(target, self)

    old = results[a_rev]
    new = results[b_rev]

    # Compare paths between the old and new fs.
    # set() efficiently converts dict keys to a set
    added = sorted(set(new) - set(old))
    deleted_or_missing = set(old) - set(new)
    if b_rev == "workspace":
        # missing status is only applicable when diffing local workspace
        # against a commit
        missing = sorted(_filter_missing(repo_fs, deleted_or_missing))
    else:
        missing = []
    deleted = sorted(deleted_or_missing - set(missing))
    modified = sorted(set(old) & set(new))

    # Cases when file was changed and renamed are resulted
    # in having deleted and added record
    # To cover such cases we need to change hashing function
    # to produce rolling/chunking hash

    renamed = _calculate_renamed(new, old, added, deleted)

    for renamed_item in renamed:
        added.remove(renamed_item["path"]["new"])
        deleted.remove(renamed_item["path"]["old"])

    ret = {
        "added": [{"path": path, "hash": new[path]} for path in added],
        "deleted": [{"path": path, "hash": old[path]} for path in deleted],
        "modified": [
            {"path": path, "hash": {"old": old[path], "new": new[path]}}
            for path in modified
            if old[path] != new[path]
        ],
        "renamed": renamed,
        "not in cache": [
            {"path": path, "hash": old[path]} for path in missing
        ],
    }

    return ret if any(ret.values()) else {}


def _paths_checksums(repo, repo_fs, targets):
    """
    A dictionary of checksums addressed by relpaths collected from
    the current fs outputs.

    To help distinguish between a directory and a file output,
    the former one will come with a trailing slash in the path:

        directory: "data/"
        file:      "data"
    """

    return dict(_output_paths(repo, repo_fs, targets))


def _output_paths(repo, repo_fs, targets):
    from dvc.fs.local import LocalFileSystem

    on_working_fs = isinstance(repo.fs, LocalFileSystem)

    def _exists(output):
        if on_working_fs:
            return output.exists
        return True

    def _to_path(output):
        return (
            str(output)
            if not output.is_dir_checksum
            else os.path.join(str(output), "")
        )

    def _to_checksum(output):
        if on_working_fs:
            return ostage(
                repo.odb.local, output.path_info, repo.odb.local.fs, "md5"
            ).hash_info.value
        return output.hash_info.value

    for stage in repo.stages:
        for output in stage.outs:
            if _exists(output):
                yield_output = targets is None or any(
                    output.path_info.isin_or_eq(target) for target in targets
                )

                if yield_output:
                    yield _to_path(output), _to_checksum(output)

                if output.is_dir_checksum and (
                    yield_output
                    or any(target.isin(output.path_info) for target in targets)
                ):
                    yield from _dir_output_paths(repo_fs, output, targets)


def _dir_output_paths(repo_fs, output, targets=None):
    from dvc.config import NoRemoteError

    try:
        for fname in repo_fs.walk_files(output.path_info):
            if targets is None or any(
                fname.isin_or_eq(target) for target in targets
            ):
                # pylint: disable=no-member
                yield str(fname), get_file_hash(fname, repo_fs, "md5").value
    except NoRemoteError:
        logger.warning("dir cache entry for '%s' is missing", output)


def _filter_missing(repo_fs, paths):
    for path in paths:
        try:
            metadata = repo_fs.metadata(path)
            if metadata.is_dvc:
                out = metadata.outs[0]
                if out.status().get(str(out)) == "not in cache":
                    yield path
        except FileNotFoundError:
            pass


def _targets_to_path_infos(repo_fs, targets):
    path_infos = []
    missing = []

    for target in targets:
        if repo_fs.exists(target):
            path_infos.append(repo_fs.metadata(target).path_info)
        else:
            missing.append(target)

    return path_infos, missing


def _calculate_renamed(new, old, added, deleted):
    old_inverted: Dict[str, List[str]] = defaultdict(list)
    # It is needed to be dict of lists to cover cases
    # when repo has paths with same hash
    for path, path_hash in old.items():
        old_inverted[path_hash].append(path)

    renamed = []
    for path in added:
        path_hash = new[path]
        old_paths = old_inverted[path_hash]
        try:
            iterator = enumerate(old_paths)
            index = next(idx for idx, path in iterator if path in deleted)
        except StopIteration:
            continue

        old_path = old_paths.pop(index)
        renamed.append(
            {"path": {"old": old_path, "new": path}, "hash": path_hash}
        )

    return renamed
