import errno
import itertools
import logging
import posixpath
from concurrent.futures import ThreadPoolExecutor

from dvc.progress import Tqdm
from dvc.utils import to_chunks

from .base import ObjectDB

logger = logging.getLogger(__name__)


class SSHObjectDB(ObjectDB):
    def batch_exists(self, path_infos, callback):
        def _exists(chunk_and_channel):
            chunk, channel = chunk_and_channel
            ret = []
            for path in chunk:
                try:
                    channel.stat(path)
                    ret.append(True)
                except OSError as exc:
                    if exc.errno != errno.ENOENT:
                        raise
                    ret.append(False)
                callback(path)
            return ret

        with self.fs.ssh(path_infos[0]) as ssh:
            channels = ssh.open_max_sftp_channels()
            max_workers = len(channels)

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                paths = [path_info.path for path_info in path_infos]
                chunks = to_chunks(paths, num_chunks=max_workers)
                chunks_and_channels = zip(chunks, channels)
                outcome = executor.map(_exists, chunks_and_channels)
                results = list(itertools.chain.from_iterable(outcome))

            return results

    def hashes_exist(self, hashes, jobs=None, name=None):
        """This is older implementation used in remote/base.py
        We are reusing it in RemoteSSH, because SSH's batch_exists proved to be
        faster than current approach (relying on exists(path_info)) applied in
        remote/base.
        """
        if not self.fs.CAN_TRAVERSE:
            return list(set(hashes) & set(self.all()))

        # possibly prompt for credentials before "Querying" progress output
        self.fs.ensure_credentials()

        with Tqdm(
            desc="Querying "
            + ("cache in " + name if name else "remote cache"),
            total=len(hashes),
            unit="file",
        ) as pbar:

            def exists_with_progress(chunks):
                return self.batch_exists(chunks, callback=pbar.update_msg)

            with ThreadPoolExecutor(
                max_workers=jobs or self.fs.jobs
            ) as executor:
                path_infos = [self.hash_to_path_info(x) for x in hashes]
                chunks = to_chunks(path_infos, num_chunks=self.fs.jobs)
                results = executor.map(exists_with_progress, chunks)
                in_remote = itertools.chain.from_iterable(results)
                ret = list(itertools.compress(hashes, in_remote))
                return ret

    def _list_paths(self, prefix=None, progress_callback=None):
        if prefix:
            root = posixpath.join(self.path_info.path, prefix[:2])
        else:
            root = self.path_info.path
        with self.fs.ssh(self.path_info) as ssh:
            if prefix and not ssh.exists(root):
                return
            # If we simply return an iterator then with above closes instantly
            if progress_callback:
                for path in ssh.walk_files(root):
                    progress_callback()
                    yield path
            else:
                yield from ssh.walk_files(root)
