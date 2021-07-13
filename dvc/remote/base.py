import errno
import hashlib
import itertools
import logging
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
from functools import partial, wraps

from dvc.exceptions import DownloadError, UploadError
from dvc.hash_info import HashInfo
from dvc.objects.tree import Tree

from ..progress import Tqdm
from .index import RemoteIndex

logger = logging.getLogger(__name__)

STATUS_OK = 1
STATUS_MISSING = 2
STATUS_NEW = 3
STATUS_DELETED = 4

STATUS_MAP = {
    # (local_exists, remote_exists)
    (True, True): STATUS_OK,
    (False, False): STATUS_MISSING,
    (True, False): STATUS_NEW,
    (False, True): STATUS_DELETED,
}


def _log_exceptions(func, operation):
    @wraps(func)
    def wrapper(from_info, to_info, *args, **kwargs):
        try:
            func(from_info, to_info, *args, **kwargs)
            return 0
        except Exception as exc:  # pylint: disable=broad-except
            # NOTE: this means we ran out of file descriptors and there is no
            # reason to try to proceed, as we will hit this error anyways.
            # pylint: disable=no-member
            if isinstance(exc, OSError) and exc.errno == errno.EMFILE:
                raise

            logger.exception(
                "failed to %s '%s' to '%s'", operation, from_info, to_info
            )
            return 1

    return wrapper


def index_locked(f):
    @wraps(f)
    def wrapper(obj, *args, **kwargs):
        with obj.index:
            return f(obj, *args, **kwargs)

    return wrapper


class Remote:
    """Cloud remote class.

    Provides methods for indexing and garbage collecting fss which contain
    DVC remotes.
    """

    INDEX_CLS = RemoteIndex

    def __init__(self, fs, path_info, tmp_dir, **config):
        from dvc.objects.db import get_odb

        self.fs = fs
        config["tmp_dir"] = tmp_dir
        self.odb = get_odb(self.fs, path_info, **config)

        url = getattr(path_info, "fspath", path_info.url)
        index_name = hashlib.sha256(url.encode("utf-8")).hexdigest()
        self.index = self.INDEX_CLS(
            tmp_dir, index_name, dir_suffix=self.fs.CHECKSUM_DIR_SUFFIX
        )

    def __repr__(self):
        return "{class_name}: '{path_info}'".format(
            class_name=type(self).__name__,
            path_info=self.odb.path_info or "No path",
        )

    def __eq__(self, other):
        return self.odb == other.odb

    def __hash__(self):
        return hash(self.odb)

    @classmethod
    def from_odb(cls, odb):
        config = odb.config
        return cls(
            odb.fs,
            odb.path_info,
            config.pop("tmp_dir"),
            **config,
        )

    @index_locked
    def gc(self, *args, **kwargs):
        removed = self.odb.gc(*args, **kwargs)

        if removed:
            self.index.clear()

        return removed

    @index_locked
    def status(
        self,
        cache,
        objs,
        jobs=None,
        show_checksums=False,
        download=False,
        log_missing=True,
    ):
        # Return flattened dict containing all status info
        dir_status, file_status, _ = self._status(
            cache,
            objs,
            jobs=jobs,
            show_checksums=show_checksums,
            download=download,
            log_missing=log_missing,
        )
        return dict(dir_status, **file_status)

    def hashes_exist(self, hashes, **kwargs):
        hashes = set(hashes)
        indexed_hashes = set(self.index.intersection(hashes))
        hashes -= indexed_hashes
        indexed_hashes = list(indexed_hashes)
        logger.debug(f"Matched '{len(indexed_hashes)}' indexed hashes")
        if not hashes:
            return indexed_hashes

        return indexed_hashes + self.odb.hashes_exist(list(hashes), **kwargs)

    def _status(  # pylint: disable=unused-argument
        self,
        cache,
        objs,
        jobs=None,
        show_checksums=False,
        download=False,
        log_missing=True,
    ):
        """Return a tuple of (dir_status_info, file_status_info, dir_contents).

        dir_status_info contains status for .dir files, file_status_info
        contains status for all other files, and dir_contents is a dict of
        {dir_hash: set(file_hash, ...)} which can be used to map
        a .dir file to its file contents.
        """

        logger.debug(f"Preparing to collect status from {self.odb.path_info}")
        md5s = set()
        dir_objs = {}
        for obj in objs:
            md5s.add(obj.hash_info.value)
            if isinstance(obj, Tree):
                dir_objs[obj.hash_info.value] = obj
                for _, entry_obj in obj:
                    md5s.add(entry_obj.hash_info.value)

        logger.debug("Collecting information from local cache...")
        local_exists = frozenset(
            cache.hashes_exist(md5s, jobs=jobs, name=cache.cache_dir)
        )

        # This is a performance optimization. We can safely assume that,
        # if the resources that we want to fetch are already cached,
        # there's no need to check the remote storage for the existence of
        # those files.
        if download and local_exists == md5s:
            remote_exists = local_exists
        else:
            logger.debug("Collecting information from remote cache...")
            remote_exists = set()
            if dir_objs:
                remote_exists.update(self._indexed_dir_hashes(dir_objs))
                md5s.difference_update(remote_exists)
            if md5s:
                remote_exists.update(
                    self.hashes_exist(
                        md5s, jobs=jobs, name=str(self.odb.path_info)
                    )
                )
        return self._make_status(
            objs,
            show_checksums,
            local_exists,
            remote_exists,
            log_missing,
        )

    def _make_status(
        self,
        objs,
        show_checksums,
        local_exists,
        remote_exists,
        log_missing,
    ):
        def make_names(obj):
            if show_checksums or obj.name is None:
                name = obj.hash_info.value
            else:
                name = obj.name
            return {"name": name}

        dir_status = {}
        file_status = {}
        dir_contents = {}
        for obj in objs:

            hash_ = obj.hash_info.value
            if isinstance(obj, Tree):
                dir_status[hash_] = make_names(obj)
                dir_contents[hash_] = set()
                for _, child_obj in obj:
                    child_hash = child_obj.hash_info.value
                    file_status[child_hash] = make_names(child_obj)
                    dir_contents[hash_].add(child_hash)
            else:
                file_status[hash_] = make_names(obj)

        self._fill_statuses(dir_status, local_exists, remote_exists)
        self._fill_statuses(file_status, local_exists, remote_exists)

        if log_missing:
            self._log_missing_caches(dict(dir_status, **file_status))

        return dir_status, file_status, dir_contents

    def _indexed_dir_hashes(self, dir_objs):
        # Validate our index by verifying all indexed .dir hashes
        # still exist on the remote
        dir_md5s = set(dir_objs.keys())
        indexed_dirs = set(self.index.dir_hashes())
        indexed_dir_exists = set()
        if indexed_dirs:
            indexed_dir_exists.update(
                self.odb.list_hashes_exists(indexed_dirs)
            )
            missing_dirs = indexed_dirs.difference(indexed_dir_exists)
            if missing_dirs:
                logger.debug(
                    "Remote cache missing indexed .dir hashes '{}', "
                    "clearing remote index".format(", ".join(missing_dirs))
                )
                self.index.clear()

        # Check if non-indexed (new) dir hashes exist on remote
        dir_exists = dir_md5s.intersection(indexed_dir_exists)
        dir_exists.update(self.odb.list_hashes_exists(dir_md5s - dir_exists))

        # If .dir hash exists on the remote, assume directory contents
        # still exists on the remote
        for dir_hash in dir_exists:
            file_hashes = [
                obj.hash_info.value for _, obj in dir_objs[dir_hash]
            ]
            if dir_hash not in self.index:
                logger.debug(
                    "Indexing new .dir '{}' with '{}' nested files".format(
                        dir_hash, len(file_hashes)
                    )
                )
                self.index.update([dir_hash], file_hashes)
            yield dir_hash
            yield from file_hashes

    @staticmethod
    def _fill_statuses(hash_info_dir, local_exists, remote_exists):
        # Using sets because they are way faster for lookups
        local = set(local_exists)
        remote = set(remote_exists)

        for md5, info in hash_info_dir.items():
            status = STATUS_MAP[(md5 in local, md5 in remote)]
            info["status"] = status

    def _get_plans(self, cache_obj, download, status_info, status):
        cache = []
        path_infos = []
        names = []
        hashes = []
        missing = []
        for md5, info in Tqdm(
            status_info.items(), desc="Analysing status", unit="file"
        ):
            if info["status"] == status:
                cache.append(cache_obj.hash_to_path_info(md5))
                path_infos.append(self.odb.hash_to_path_info(md5))
                names.append(info["name"])
                hashes.append(md5)
            elif info["status"] == STATUS_MISSING:
                missing.append(md5)

        if download:
            to_infos = cache
            from_infos = path_infos
        else:
            to_infos = path_infos
            from_infos = cache

        return (from_infos, to_infos, names, hashes), missing

    def _process(
        self,
        cache,
        objs,
        jobs=None,
        show_checksums=False,
        download=False,
    ):
        logger.debug(
            "Preparing to {} '{}'".format(
                "download data from" if download else "upload data to",
                self.odb.path_info,
            )
        )

        if download:
            func = _log_exceptions(self.fs.download_file, "download")
            status = STATUS_DELETED
            desc = "Downloading"
        else:
            func = _log_exceptions(self.fs.upload, "upload")
            status = STATUS_NEW
            desc = "Uploading"

        if jobs is None:
            jobs = self.fs.jobs

        dir_status, file_status, dir_contents = self._status(
            cache,
            objs,
            jobs=jobs,
            show_checksums=show_checksums,
            download=download,
        )

        dir_plans, _ = self._get_plans(cache, download, dir_status, status)
        file_plans, missing_files = self._get_plans(
            cache, download, file_status, status
        )

        total = len(dir_plans[0]) + len(file_plans[0])
        if total == 0:
            return 0

        with Tqdm(total=total, unit="file", desc=desc) as pbar:
            func = pbar.wrap_fn(func)
            self._process_plans(
                download,
                dir_plans,
                file_plans,
                dir_contents,
                missing_files,
                jobs,
                func,
            )

        return total

    def _process_plans(
        self,
        download,
        dir_plans,
        file_plans,
        dir_contents,
        missing_files,
        jobs,
        func,
    ):
        with ThreadPoolExecutor(max_workers=jobs) as executor:
            processor = partial(self._create_tasks, executor, jobs, func)
            processor.transfer_func = func
            if download:
                self._download_plans(dir_plans, file_plans, processor)
            else:
                self._upload_plans(
                    dir_plans,
                    file_plans,
                    dir_contents,
                    missing_files,
                    processor,
                )

    def _create_tasks(self, executor, jobs, func, file_plans):
        fails = 0
        file_plan_iterator = iter(file_plans)

        def create_taskset(amount):
            return {
                executor.submit(func, from_info, to_info, name)
                for from_info, to_info, name, _ in itertools.islice(
                    file_plan_iterator, amount
                )
            }

        tasks = create_taskset(jobs * 5)
        while tasks:
            done, tasks = futures.wait(
                tasks, return_when=futures.FIRST_COMPLETED
            )
            fails += sum(task.result() for task in done)
            tasks.update(create_taskset(len(done)))
        return fails

    def _download_plans(self, dir_plans, file_plans, processor):
        plans = [*zip(*dir_plans), *zip(*file_plans)]
        fails = processor(plans)
        if fails:
            self.index.clear()
            raise DownloadError(fails)

    def _upload_plans(
        self, dir_plans, file_plans, dir_contents, missing_files, processor
    ):
        total_fails = 0
        succeeded_dir_hashes = []
        all_file_plans = list(zip(*file_plans))
        for dir_from_info, dir_to_info, dir_name, dir_hash in zip(*dir_plans):
            bound_file_plans = []
            directory_hashes = dir_contents[dir_hash]

            for file_plan in all_file_plans.copy():
                if file_plan[-1] in directory_hashes:
                    bound_file_plans.append(file_plan)
                    all_file_plans.remove(file_plan)

            dir_fails = processor(bound_file_plans)
            if dir_fails:
                logger.debug(
                    "failed to upload full contents of '{}', "
                    "aborting .dir file upload".format(dir_name)
                )
                logger.error(
                    f"failed to upload '{dir_from_info}'"
                    f" to '{dir_to_info}'"
                )
                total_fails += dir_fails + 1
            elif directory_hashes.intersection(missing_files):
                # if for some reason a file contained in this dir is
                # missing both locally and in the remote, we want to
                # push whatever file content we have, but should not
                # push .dir file
                logger.debug(
                    "directory '%s' contains missing files,"
                    "skipping .dir file upload",
                    dir_name,
                )
            else:
                is_dir_failed = processor.transfer_func(
                    dir_from_info, dir_to_info, dir_name
                )
                total_fails += is_dir_failed
                if not is_dir_failed:
                    succeeded_dir_hashes.append(dir_hash)

        # insert the rest
        total_fails += processor(all_file_plans)
        if total_fails:
            raise UploadError(total_fails)

        # index successfully pushed dirs
        for dir_hash in succeeded_dir_hashes:
            file_hashes = dir_contents[dir_hash]
            logger.debug(
                "Indexing pushed dir '{}' with "
                "'{}' nested files".format(dir_hash, len(file_hashes))
            )
            self.index.update([dir_hash], file_hashes)

    @index_locked
    def push(self, cache, objs, jobs=None, show_checksums=False):
        ret = self._process(
            cache,
            objs,
            jobs=jobs,
            show_checksums=show_checksums,
            download=False,
        )

        if self.fs.scheme == "local":
            for obj in objs:
                checksum = obj.hash_info.value
                cache_file = self.odb.hash_to_path_info(checksum)
                if self.fs.exists(cache_file):
                    hash_info = HashInfo(self.fs.PARAM_CHECKSUM, checksum)
                    self.odb.state.save(cache_file, self.fs, hash_info)
                    self.odb.protect(cache_file)

        return ret

    @index_locked
    def pull(self, cache, objs, jobs=None, show_checksums=False):
        ret = self._process(
            cache,
            objs,
            jobs=jobs,
            show_checksums=show_checksums,
            download=True,
        )

        if not self.odb.verify:

            def save_state(obj):
                checksum = obj.hash_info.value
                cache_file = cache.hash_to_path_info(checksum)
                if cache.fs.exists(cache_file):
                    # We can safely save here, as existing corrupted files
                    # will be removed upon status, while files corrupted
                    # during download will not be moved from tmp_file
                    # (see `BaseFileSystem.download()`)
                    hash_info = HashInfo(cache.fs.PARAM_CHECKSUM, checksum)
                    cache.state.save(cache_file, cache.fs, hash_info)
                    cache.protect(cache_file)

            for obj in objs:
                save_state(obj)
                if isinstance(obj, Tree):
                    for _, entry_obj in obj:
                        save_state(entry_obj)

        return ret

    @staticmethod
    def _log_missing_caches(hash_info_dict):
        missing_caches = [
            (md5, info)
            for md5, info in hash_info_dict.items()
            if info["status"] == STATUS_MISSING
        ]
        if missing_caches:
            missing_desc = "\n".join(
                "name: {}, md5: {}".format(info["name"], md5)
                for md5, info in missing_caches
            )
            msg = (
                "Some of the cache files do not exist neither locally "
                "nor on remote. Missing cache files:\n{}".format(missing_desc)
            )
            logger.warning(msg)
