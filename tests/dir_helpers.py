"""
The goal of this module is making dvc functional tests setup a breeze. This
includes a temporary dir, initializing git and DVC repos and bootstrapping some
file structure.

The cornerstone of these fixtures is `tmp_dir`, which creates a temporary dir
and changes path to it, it might be combined with `scm` and `dvc` to initialize
empty git and DVC repos. `tmp_dir` returns a Path instance, which should save
you from using `open()`, `os` and `os.path` utils many times:

    (tmp_dir / "some_file").write_text("some text")
    # ...
    assert "some text" == (tmp_dir / "some_file").read_text()
    assert (tmp_dir / "some_file").exists()

Additionally it provides `.gen()`, `.scm_gen()` and `.dvc_gen()` methods to
bootstrap a required file structure in a single call:

    # Generate a dir with files
    tmp_dir.gen({"dir": {"file": "file text", "second_file": "..."}})

    # Generate a single file, dirs will be created along the way
    tmp_dir.gen("dir/file", "file text")

    # Generate + git add
    tmp_dir.scm_gen({"file1": "...", ...})

    # Generate + git add + git commit
    tmp_dir.scm_gen({"file1": "...", ...}, commit="add files")

    # Generate + dvc add
    tmp_dir.dvc_gen({"file1": "...", ...})

    # Generate + dvc add + git commit -am "..."
    # This commits stages to git not the generated files.
    tmp_dir.dvc_gen({"file1": "...", ...}, commit="add files")

Making it easier to bootstrap things has a supergoal of incentivizing a move
from global repo template to creating everything inplace, which:

    - makes all path references local to test, enhancing readability
    - allows using telling filenames, e.g. "git_tracked_file" instead of "foo"
    - does not create unnecessary files
"""

# pylint: disable=redefined-outer-name, attribute-defined-outside-init

import os
import pathlib
from contextlib import contextmanager
from textwrap import dedent

import pytest
from funcy import lmap, retry

from dvc.logger import disable_other_loggers
from dvc.utils.fs import makedirs

__all__ = [
    "make_tmp_dir",
    "tmp_dir",
    "scm",
    "dvc",
    "run_copy",
    "run_head",
    "erepo_dir",
    "git_dir",
    "git_init",
]


# see https://github.com/iterative/dvc/issues/3167
disable_other_loggers()


class TmpDir(pathlib.Path):
    scheme = "local"

    def __new__(cls, *args, **kwargs):
        if cls is TmpDir:
            cls = (  # pylint: disable=self-cls-assignment
                WindowsTmpDir if os.name == "nt" else PosixTmpDir
            )
        self = cls._from_parts(args, init=False)
        if not self._flavour.is_supported:
            raise NotImplementedError(
                f"cannot instantiate {cls.__name__!r} on your system"
            )
        self._init()
        return self

    def init(self, *, scm=False, dvc=False, subdir=False):
        from dvc.repo import Repo
        from dvc.scm.git import Git

        assert not scm or not hasattr(self, "scm")
        assert not dvc or not hasattr(self, "dvc")

        str_path = os.fspath(self)

        if scm:
            git_init(str_path)
        if dvc:
            self.dvc = Repo.init(
                str_path,
                no_scm=not scm and not hasattr(self, "scm"),
                subdir=subdir,
            )
        if scm:
            self.scm = self.dvc.scm if hasattr(self, "dvc") else Git(str_path)
        if dvc and hasattr(self, "scm"):
            self.scm.commit("init dvc")

    def close(self):
        if hasattr(self, "scm"):
            self.scm.close()
        if hasattr(self, "dvc"):
            self.dvc.close()

    def _require(self, name):
        if not hasattr(self, name):
            raise TypeError(
                "Can't use {name} for this temporary dir. "
                'Did you forget to use "{name}" fixture?'.format(name=name)
            )

    # Bootstrapping methods
    def gen(self, struct, text=""):
        if isinstance(struct, (str, bytes, pathlib.PurePath)):
            struct = {struct: text}

        return self._gen(struct)

    def _gen(self, struct, prefix=None):
        paths = []
        for name, contents in struct.items():
            path = (prefix or self) / name

            if isinstance(contents, dict):
                if not contents:
                    makedirs(path, exist_ok=True)
                else:
                    self._gen(contents, prefix=path)
            else:
                makedirs(path.parent, exist_ok=True)
                if isinstance(contents, bytes):
                    path.write_bytes(contents)
                else:
                    path.write_text(contents, encoding="utf-8")
            paths.append(path)
        return paths

    def dvc_gen(self, struct, text="", commit=None):
        paths = self.gen(struct, text)
        return self.dvc_add(paths, commit=commit)

    def scm_gen(self, struct, text="", commit=None):
        paths = self.gen(struct, text)
        return self.scm_add(paths, commit=commit)

    def dvc_add(self, filenames, commit=None):
        self._require("dvc")
        filenames = _coerce_filenames(filenames)

        stages = self.dvc.add(filenames)
        if commit:
            stage_paths = [s.path for s in stages]

            def to_gitignore(stage_path):
                from dvc.scm import Git

                return os.path.join(os.path.dirname(stage_path), Git.GITIGNORE)

            gitignores = [
                to_gitignore(s)
                for s in stage_paths
                if os.path.exists(to_gitignore(s))
            ]
            self.scm_add(stage_paths + gitignores, commit=commit)

        return stages

    def scm_add(self, filenames, commit=None):
        self._require("scm")
        filenames = _coerce_filenames(filenames)

        self.scm.add(filenames)
        if commit:
            self.scm.commit(commit)

    def add_remote(
        self, *, url=None, config=None, name="upstream", default=True
    ):
        self._require("dvc")

        assert bool(url) ^ bool(config)

        if url:
            config = {"url": url}

        with self.dvc.config.edit() as conf:
            conf["remote"][name] = config
            if default:
                conf["core"]["remote"] = name

        if hasattr(self, "scm"):
            self.scm.add(self.dvc.config.files["repo"])
            self.scm.commit(f"add '{name}' remote")

        return url or config["url"]

    # contexts
    @contextmanager
    def chdir(self):
        old = os.getcwd()
        try:
            os.chdir(self)
            yield
        finally:
            os.chdir(old)

    @contextmanager
    def branch(self, name, new=False):
        self._require("scm")
        old = self.scm.active_branch()
        try:
            self.scm.checkout(name, create_new=new)
            yield
        finally:
            self.scm.checkout(old)

    def read_text(self, *args, **kwargs):  # pylint: disable=signature-differs
        # NOTE: on windows we'll get PermissionError instead of
        # IsADirectoryError when we try to `open` a directory, so we can't
        # rely on exception flow control
        if self.is_dir():
            return {
                path.name: path.read_text(*args, **kwargs)
                for path in self.iterdir()
            }
        return super().read_text(*args, **kwargs)

    def hash_to_path_info(self, hash_):
        return self / hash_[0:2] / hash_[2:]


def _coerce_filenames(filenames):
    if isinstance(filenames, (str, bytes, pathlib.PurePath)):
        filenames = [filenames]
    return lmap(os.fspath, filenames)


class WindowsTmpDir(TmpDir, pathlib.PureWindowsPath):
    pass


class PosixTmpDir(TmpDir, pathlib.PurePosixPath):
    pass


CACHE = {}


@pytest.fixture(scope="session")
def make_tmp_dir(tmp_path_factory, request, worker_id):
    def make(name, *, scm=False, dvc=False, subdir=False):
        from shutil import ignore_patterns

        from dvc.repo import Repo
        from dvc.scm.git import Git
        from dvc.utils.fs import fs_copy

        cache = CACHE.get((scm, dvc, subdir))
        if not cache:
            cache = tmp_path_factory.mktemp("dvc-test-cache" + worker_id)
            TmpDir(cache).init(scm=scm, dvc=dvc, subdir=subdir)
            CACHE[(scm, dvc, subdir)] = os.fspath(cache)
        path = tmp_path_factory.mktemp(name) if isinstance(name, str) else name

        # ignore sqlite files from .dvc/tmp. We might not be closing the cache
        # connection resulting in PermissionErrors in Windows.
        ignore = ignore_patterns("cache.db*")
        for entry in os.listdir(cache):
            # shutil.copytree's dirs_exist_ok is only available in >=3.8
            fs_copy(
                os.path.join(cache, entry),
                os.path.join(path, entry),
                ignore=ignore,
            )
        new_dir = TmpDir(path)
        str_path = os.fspath(new_dir)
        if dvc:
            new_dir.dvc = Repo(str_path)
        if scm:
            new_dir.scm = (
                new_dir.dvc.scm if hasattr(new_dir, "dvc") else Git(str_path)
            )
        request.addfinalizer(new_dir.close)
        return new_dir

    return make


@pytest.fixture
def tmp_dir(tmp_path, make_tmp_dir, request, monkeypatch):
    monkeypatch.chdir(tmp_path)
    fixtures = request.fixturenames
    return make_tmp_dir(tmp_path, scm="scm" in fixtures, dvc="dvc" in fixtures)


@pytest.fixture
def scm(tmp_dir):
    return tmp_dir.scm


@pytest.fixture
def dvc(tmp_dir):
    with tmp_dir.dvc as _dvc:
        yield _dvc


def git_init(path):
    from git import Repo
    from git.exc import GitCommandNotFound

    # NOTE: handles EAGAIN error on BSD systems (osx in our case).
    # Otherwise when running tests you might get this exception:
    #
    #    GitCommandNotFound: Cmd('git') not found due to:
    #        OSError('[Errno 35] Resource temporarily unavailable')
    git = retry(5, GitCommandNotFound)(Repo.init)(path)
    git.close()


@pytest.fixture
def run_copy(tmp_dir, dvc):
    tmp_dir.gen(
        "copy.py",
        (
            "import sys, shutil, os\n"
            "shutil.copyfile(sys.argv[1], sys.argv[2]) "
            "if os.path.isfile(sys.argv[1]) "
            "else shutil.copytree(sys.argv[1], sys.argv[2])"
        ),
    )

    def run_copy(src, dst, **run_kwargs):
        wdir = pathlib.Path(run_kwargs.get("wdir", "."))
        wdir = pathlib.Path("../" * len(wdir.parts))
        script_path = wdir / "copy.py"

        return dvc.run(
            cmd=f"python {script_path} {src} {dst}",
            outs=[dst],
            deps=[src, f"{script_path}"],
            **run_kwargs,
        )

    return run_copy


@pytest.fixture
def run_head(tmp_dir, dvc):
    """Output first line of each file to different file with '-1' appended.
    Useful for tracking multiple outputs/dependencies which are not a copy
    of each others.
    """
    tmp_dir.gen(
        {
            "head.py": dedent(
                """
        import sys
        for file in sys.argv[1:]:
            with open(file) as f, open(file +"-1","w+") as w:
                w.write(f.readline())
        """
            )
        }
    )
    script = os.path.abspath(tmp_dir / "head.py")

    def run(*args, **run_kwargs):
        return dvc.run(
            **{
                "cmd": "python {} {}".format(script, " ".join(args)),
                "outs": [dep + "-1" for dep in args],
                "deps": list(args),
                **run_kwargs,
            }
        )

    return run


@pytest.fixture
def erepo_dir(make_tmp_dir):
    return make_tmp_dir("erepo", scm=True, dvc=True)


@pytest.fixture
def git_dir(make_tmp_dir):
    path = make_tmp_dir("git-erepo", scm=True)
    path.scm.commit("init repo")
    return path
