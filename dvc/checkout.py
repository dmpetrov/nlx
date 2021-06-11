import logging

from shortuuid import uuid

from dvc import prompt
from dvc.exceptions import (
    CacheLinkError,
    CheckoutError,
    ConfirmRemoveError,
    DvcException,
)
from dvc.ignore import DvcIgnoreFilter
from dvc.objects import check, load
from dvc.objects.errors import ObjectFormatError
from dvc.objects.stage import stage
from dvc.remote.slow_link_detection import (  # type: ignore[attr-defined]
    slow_link_guard,
)
from dvc.types import Optional

logger = logging.getLogger(__name__)


def _changed(path_info, fs, obj, cache):
    logger.trace("checking if '%s'('%s') has changed.", path_info, obj)

    try:
        check(cache, obj)
    except (FileNotFoundError, ObjectFormatError):
        logger.debug(
            "cache for '%s'('%s') has changed.", path_info, obj.hash_info
        )
        return True

    try:
        actual = stage(cache, path_info, fs, obj.hash_info.name).hash_info
    except FileNotFoundError:
        logger.debug("'%s' doesn't exist.", path_info)
        return True

    if obj.hash_info != actual:
        logger.debug(
            "hash value '%s' for '%s' has changed (actual '%s').",
            obj.hash_info,
            actual,
            path_info,
        )
        return True

    logger.trace("'%s' hasn't changed.", path_info)
    return False


def _remove(path_info, fs, cache, force=False):
    if not fs.exists(path_info):
        return

    if force:
        fs.remove(path_info)
        return

    current = stage(cache, path_info, fs, fs.PARAM_CHECKSUM).hash_info
    try:
        obj = load(cache, current)
        check(cache, obj)
    except (FileNotFoundError, ObjectFormatError):
        msg = (
            f"file/directory '{path_info}' is going to be removed. "
            "Are you sure you want to proceed?"
        )

        if not prompt.confirm(msg):
            raise ConfirmRemoveError(str(path_info))

    fs.remove(path_info)


def _verify_link(cache, path_info, link_type):
    if link_type == "hardlink" and cache.fs.getsize(path_info) == 0:
        return

    if cache.cache_type_confirmed:
        return

    is_link = getattr(cache.fs, f"is_{link_type}", None)
    if is_link and not is_link(path_info):
        cache.fs.remove(path_info)
        raise DvcException(f"failed to verify {link_type}")

    cache.cache_type_confirmed = True


def _do_link(cache, from_info, to_info, link_method):
    if cache.fs.exists(to_info):
        raise DvcException(f"Link '{to_info}' already exists!")

    link_method(from_info, to_info)

    logger.debug(
        "Created '%s': %s -> %s", cache.cache_types[0], from_info, to_info
    )


@slow_link_guard
def _try_links(cache, from_info, to_info, link_types):
    while link_types:
        link_method = getattr(cache.fs, link_types[0])
        try:
            _do_link(cache, from_info, to_info, link_method)
            _verify_link(cache, to_info, link_types[0])
            return

        except DvcException as exc:
            logger.debug(
                "Cache type '%s' is not supported: %s", link_types[0], exc
            )
            del link_types[0]

    raise CacheLinkError([to_info])


def _link(cache, from_info, to_info):
    assert cache.fs.isfile(from_info)
    cache.makedirs(to_info.parent)
    _try_links(cache, from_info, to_info, cache.cache_types)


def _cache_is_copy(cache, path_info):
    """Checks whether cache uses copies."""
    if cache.cache_type_confirmed:
        return cache.cache_types[0] == "copy"

    if set(cache.cache_types) <= {"copy"}:
        return True

    workspace_file = path_info.with_name("." + uuid())
    test_cache_file = cache.path_info / ".cache_type_test_file"
    if not cache.fs.exists(test_cache_file):
        cache.makedirs(test_cache_file.parent)
        with cache.fs.open(test_cache_file, "wb") as fobj:
            fobj.write(bytes(1))
    try:
        _link(cache, test_cache_file, workspace_file)
    finally:
        cache.fs.remove(workspace_file)
        cache.fs.remove(test_cache_file)

    cache.cache_type_confirmed = True
    return cache.cache_types[0] == "copy"


def _checkout_file(
    path_info,
    fs,
    obj,
    cache,
    force,
    progress_callback=None,
    relink=False,
    state=None,
):
    """The file is changed we need to checkout a new copy"""
    modified = False
    cache_info = cache.hash_to_path_info(obj.hash_info.value)
    if fs.exists(path_info):
        if not relink and _changed(path_info, fs, obj, cache):
            modified = True
            _remove(path_info, fs, cache, force=force)
            _link(cache, cache_info, path_info)
        else:
            if fs.iscopy(path_info) and _cache_is_copy(cache, path_info):
                cache.unprotect(path_info)
            else:
                _remove(path_info, fs, cache, force=force)
                _link(cache, cache_info, path_info)
    else:
        _link(cache, cache_info, path_info)
        modified = True

    if state:
        state.save(path_info, fs, obj.hash_info)

    if progress_callback:
        progress_callback(str(path_info))

    return modified


def _remove_redundant_files(
    path_info, fs, obj, cache, force, dvcignore: Optional[DvcIgnoreFilter]
):
    if dvcignore:
        existing_files = set(dvcignore.walk_files(fs, path_info))
    else:
        existing_files = set(fs.walk_files(path_info))

    needed_files = {path_info.joinpath(*key) for key, _ in obj}
    redundant_files = existing_files - needed_files
    for path in redundant_files:
        _remove(path, fs, cache, force)

    return bool(redundant_files)


def _checkout_dir(
    path_info,
    fs,
    obj,
    cache,
    force,
    progress_callback=None,
    relink=False,
    dvcignore: Optional[DvcIgnoreFilter] = None,
    state=None,
):
    modified = False
    # Create dir separately so that dir is created
    # even if there are no files in it
    if not fs.exists(path_info):
        modified = True
        fs.makedirs(path_info)

    logger.debug("Linking directory '%s'.", path_info)

    for entry_key, entry_obj in obj:
        entry_modified = _checkout_file(
            path_info.joinpath(*entry_key),
            fs,
            entry_obj,
            cache,
            force,
            progress_callback,
            relink,
            state=None,
        )
        if entry_modified:
            modified = True

    modified = (
        _remove_redundant_files(
            path_info, fs, obj, cache, force, dvcignore=dvcignore
        )
        or modified
    )

    if state:
        state.save(path_info, fs, obj.hash_info)

    # relink is not modified, assume it as nochange
    return modified and not relink


def _checkout(
    path_info,
    fs,
    obj,
    cache,
    force=False,
    progress_callback=None,
    relink=False,
    dvcignore: Optional[DvcIgnoreFilter] = None,
    state=None,
):
    if not obj.hash_info.isdir:
        ret = _checkout_file(
            path_info, fs, obj, cache, force, progress_callback, relink, state
        )
    else:
        ret = _checkout_dir(
            path_info,
            fs,
            obj,
            cache,
            force,
            progress_callback,
            relink,
            dvcignore=dvcignore,
            state=state,
        )

    if state:
        state.save_link(path_info, fs)

    return ret


def checkout(
    path_info,
    fs,
    obj,
    cache,
    force=False,
    progress_callback=None,
    relink=False,
    quiet=False,
    dvcignore: Optional[DvcIgnoreFilter] = None,
    state=None,
):
    if path_info.scheme not in ["local", cache.fs.scheme]:
        raise NotImplementedError

    failed = None
    skip = False
    if not obj:
        if not quiet:
            logger.warning(
                "No file hash info found for '%s'. It won't be created.",
                path_info,
            )
        _remove(path_info, fs, cache, force=force)
        failed = path_info

    elif not relink and not _changed(path_info, fs, obj, cache):
        logger.trace("Data '%s' didn't change.", path_info)  # type: ignore
        skip = True
    else:
        try:
            check(cache, obj)
        except (FileNotFoundError, ObjectFormatError):
            if not quiet:
                logger.warning(
                    "Cache '%s' not found. File '%s' won't be created.",
                    obj.hash_info,
                    path_info,
                )
            _remove(path_info, fs, cache, force=force)
            failed = path_info

    if failed or skip:
        if progress_callback and obj:
            progress_callback(str(path_info), len(obj))
        if failed:
            raise CheckoutError([failed])
        return

    logger.debug("Checking out '%s' with cache '%s'.", path_info, obj)

    return _checkout(
        path_info,
        fs,
        obj,
        cache,
        force,
        progress_callback,
        relink,
        dvcignore=dvcignore,
        state=state,
    )
