import errno
import os
from concurrent.futures import ThreadPoolExecutor
from functools import partial

from dvc.exceptions import DvcIgnoreInCollectedDirError
from dvc.hash_info import HashInfo
from dvc.ignore import DVCIGNORE_FILE
from dvc.objects.file import HashFile
from dvc.progress import Tqdm
from dvc.utils import file_md5


def _upload_file(path_info, fs, odb):
    from dvc.utils import tmp_fname
    from dvc.utils.stream import HashedStreamReader

    tmp_info = odb.fs.path_info / tmp_fname()
    with fs.open(path_info, mode="rb", chunk_size=fs.CHUNK_SIZE) as stream:
        stream = HashedStreamReader(stream)
        odb.fs.upload_fobj(
            stream, tmp_info, desc=path_info.name, total=fs.getsize(path_info)
        )

    obj = HashFile(tmp_info, odb.fs, stream.hash_info)
    return path_info, obj


def _get_file_hash(path_info, fs, name):
    info = fs.info(path_info)
    if name in info:
        assert not info[name].endswith(".dir")
        return HashInfo(name, info[name], size=info["size"])

    func = getattr(fs, name, None)
    if func:
        return func(path_info)

    if name == "md5":
        return HashInfo(
            name, file_md5(path_info, fs), size=fs.getsize(path_info)
        )

    raise NotImplementedError


def get_file_hash(path_info, fs, name, state=None):
    if state:
        hash_info = state.get(  # pylint: disable=assignment-from-none
            path_info, fs
        )
        if hash_info:
            return hash_info

    if not fs.exists(path_info):
        raise FileNotFoundError(
            errno.ENOENT, os.strerror(errno.ENOENT), path_info
        )

    hash_info = _get_file_hash(path_info, fs, name)

    if state:
        assert ".dir" not in hash_info.value
        state.save(path_info, fs, hash_info)

    return hash_info


def _get_file_obj(path_info, fs, name, odb=None, state=None, upload=False):
    if upload:
        assert odb and name == "md5"
        return _upload_file(path_info, fs, odb)

    obj = HashFile(
        path_info, fs, get_file_hash(path_info, fs, name, state=state)
    )
    return path_info, obj


def _build_objects(
    path_info, fs, name, odb, state, upload, dvcignore=None, **kwargs
):
    if dvcignore:
        walk_iterator = dvcignore.walk_files(fs, path_info)
    else:
        walk_iterator = fs.walk_files(path_info)
    with Tqdm(
        unit="md5",
        desc="Computing file/dir hashes (only done once)",
        disable=kwargs.pop("no_progress_bar", False),
    ) as pbar:
        worker = pbar.wrap_fn(
            partial(
                _get_file_obj,
                fs=fs,
                name=name,
                odb=odb,
                state=state,
                upload=upload,
            )
        )
        with ThreadPoolExecutor(
            max_workers=kwargs.pop("jobs", fs.hash_jobs)
        ) as executor:
            yield from executor.map(worker, walk_iterator)


def _iter_objects(path_info, fs, name, odb, state, upload, **kwargs):
    if not upload and name in fs.DETAIL_FIELDS:
        for details in fs.find(path_info, detail=True):
            file_info = path_info.replace(path=details["name"])
            hash_info = HashInfo(name, details[name], size=details.get("size"))
            yield file_info, HashFile(file_info, fs, hash_info)

        return None

    yield from _build_objects(
        path_info, fs, name, odb, state, upload, **kwargs
    )


def _build_tree(path_info, fs, name, odb, state, upload, **kwargs):
    from .tree import Tree

    tree = Tree(None, None, None)
    for file_info, obj in _iter_objects(
        path_info, fs, name, odb, state, upload, **kwargs
    ):
        if DVCIGNORE_FILE == file_info.name:
            raise DvcIgnoreInCollectedDirError(file_info.parent)

        # NOTE: this is lossy transformation:
        #   "hey\there" -> "hey/there"
        #   "hey/there" -> "hey/there"
        # The latter is fine filename on Windows, which
        # will transform to dir/file on back transform.
        #
        # Yes, this is a BUG, as long as we permit "/" in
        # filenames on Windows and "\" on Unix
        tree.add(file_info.relative_to(path_info).parts, obj)

    tree.digest()

    return tree


def _get_tree_obj(path_info, fs, name, odb, state, upload, **kwargs):
    from .tree import Tree

    value = fs.info(path_info).get(name)
    if value:
        hash_info = HashInfo(name, value)
        try:
            return Tree.load(odb, hash_info)
        except FileNotFoundError:
            pass

    tree = _build_tree(path_info, fs, name, odb, state, upload, **kwargs)

    odb.add(tree.path_info, tree.fs, tree.hash_info)
    if name != "md5":
        # NOTE: used only for external outputs. Initial reasoning was to be
        # able to validate .dir files right in the workspace (e.g. check s3
        # etag), but could be dropped for manual validation with regular md5,
        # that would be universal for all clouds.
        raw = odb.get(tree.hash_info)
        hash_info = get_file_hash(raw.path_info, raw.fs, name, state)
        tree.hash_info.name = hash_info.name
        tree.hash_info.value = hash_info.value
        if not tree.hash_info.value.endswith(".dir"):
            tree.hash_info.value += ".dir"
        odb.add(tree.path_info, tree.fs, tree.hash_info)

    return tree


def stage(odb, path_info, fs, name, upload=False, **kwargs):
    assert path_info and (
        isinstance(path_info, str) or path_info.scheme == fs.scheme
    )

    if not fs.exists(path_info):
        raise FileNotFoundError(
            errno.ENOENT, os.strerror(errno.ENOENT), path_info
        )

    state = odb.state
    # pylint: disable=assignment-from-none
    hash_info = state.get(path_info, fs)

    # If we have dir hash in state db, but dir cache file is lost,
    # then we need to recollect the dir via .get_dir_hash() call below,
    # see https://github.com/iterative/dvc/issues/2219 for context
    if (
        hash_info
        and hash_info.isdir
        and not odb.fs.exists(odb.hash_to_path_info(hash_info.value))
    ):
        hash_info = None

    if hash_info:
        from . import load
        from .tree import Tree

        obj = load(odb, hash_info)
        if isinstance(obj, Tree):
            obj.hash_info.nfiles = len(obj)
            for key, entry in obj:
                entry.fs = fs
                entry.path_info = path_info.joinpath(*key)
        else:
            obj.fs = fs
            obj.path_info = path_info
        assert obj.hash_info.name == name
        obj.hash_info.size = hash_info.size
        return obj

    if fs.isdir(path_info):
        obj = _get_tree_obj(path_info, fs, name, odb, state, upload, **kwargs)
    else:
        _, obj = _get_file_obj(path_info, fs, name, odb, state, upload)

    if obj.hash_info and fs.exists(path_info):
        state.save(path_info, fs, obj.hash_info)

    return obj
