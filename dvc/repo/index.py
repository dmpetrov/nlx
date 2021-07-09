from functools import partialmethod
from typing import TYPE_CHECKING, Iterator, List

from funcy import cached_property

if TYPE_CHECKING:
    from dvc.dependency import ParamsDependency
    from dvc.fs.base import BaseFileSystem
    from dvc.output import Output
    from dvc.stage import Stage


class Index:
    def __init__(
        self,
        repo: "Repo",  # pylint: disable=redefined-outer-name
        fs: "BaseFileSystem",
        stages: List["Stage"] = None,
    ) -> None:
        """Index is an immutable collection of stages.

        Generally, Index is a complete collection of stages at a point in time.
        With "a point in time", it means it is collected from the user's
        workspace or a git revision.
        And, since Index is immutable, the collection is frozen in time.

        Index provides multiple ways to view this collection:

            stages - provides direct access to this collection
            outputs - provides direct access to the outputs
            objects - provides direct access to the objects
            graph -
            ... and many more.

        Index also provides ways to slice and dice this collection.
        Some `views` might not make sense when sliced (eg: pipelines/graph).
        """

        self.fs = fs
        self.repo: "Repo" = repo
        self.stage_collector = repo.stage
        if stages is not None:
            self._stages = stages

    @cached_property
    def _stages(self):
        """Index without stages cannot exist.
        So, stages being lazy, should not be depended upon.
        """
        from pyrsistent import pset

        onerror = self.repo.stage_collection_error_handler
        # pylint: disable=protected-access
        return pset(self.stage_collector._collect_repo(onerror=onerror))

    @property
    def stages(self):
        return self._stages

    def __hash__(self):
        return hash(self.stages)

    def __contains__(self, stage: "Stage") -> bool:
        return stage in self.stages

    def __iter__(self) -> Iterator["Stage"]:
        yield from self.stages

    def slice(self, target_path):
        from pyrsistent import pset

        from dvc.utils.fs import path_isin

        stages = pset(
            stage
            for stage in self
            if path_isin(stage.path_in_repo, target_path)
        )
        return Index(self.repo, self.fs, stages=stages)

    @property
    def outputs(self) -> Iterator["Output"]:
        for stage in self:
            yield from stage.outs

    @property
    def decorated_outputs(self) -> Iterator["Output"]:
        for output in self.outputs:
            if output.is_decorated:
                yield output

    @property
    def metrics(self) -> Iterator["Output"]:
        for output in self.outputs:
            if output.is_metric:
                yield output

    @property
    def plots(self) -> Iterator["Output"]:
        for output in self.outputs:
            if output.is_plot:
                yield output

    @property
    def dependencies(self) -> Iterator["Output"]:
        for stage in self:
            yield from stage.deps

    @property
    def params(self) -> Iterator["ParamsDependency"]:
        from dvc.dependency import ParamsDependency

        for dep in self.dependencies:
            if isinstance(dep, ParamsDependency):
                yield dep

    @cached_property
    def outs_trie(self):
        from .trie import build_outs_trie

        return build_outs_trie(self.stages)

    @cached_property
    def graph(self):
        return self.build_graph()

    @cached_property
    def outs_graph(self):
        from .graph import build_outs_graph

        return build_outs_graph(self.graph, self.outs_trie)

    @cached_property
    def pipelines(self):
        from .graph import get_pipelines

        return get_pipelines(self.graph)

    def used_objs(
        self,
        targets=None,
        rev="",
        with_deps=False,
        remote=None,
        force=False,
        jobs=None,
        recursive=False,
    ):
        from collections import defaultdict

        from funcy import cat

        used = defaultdict(set)
        pairs = cat(
            self.stage_collector.collect_granular(
                target, recursive=recursive, with_deps=with_deps
            )
            for target in targets
        )

        for stage, filter_info in pairs:
            for odb, objs in stage.get_used_objs(
                remote=remote,
                force=force,
                jobs=jobs,
                filter_info=filter_info,
            ).items():
                if rev:
                    _add_suffix(objs, f" ({rev})")
                used[odb].update(objs)
        return used

    def _mutate(self, method, *args, check_graphs: bool = False):
        stages = getattr(self.stages, method)(*args)
        idx = Index(self.repo, self.fs, stages=stages)
        # TODO: Figure out a way to optimize graph and trie build from partial
        #  collections.
        if check_graphs:
            idx.check_graph()
        return idx

    update = partialmethod(_mutate, "update")
    add = partialmethod(_mutate, "add")
    difference = partialmethod(_mutate, "difference")
    discard = partialmethod(_mutate, "discard")
    remove = partialmethod(_mutate, "remove")

    def issubset(self, other):
        return self.stages.issubset(other.stages)

    def issuperset(self, other):
        return self.stages.issuperset(other.stages)

    def build_graph(self):
        from .graph import build_graph

        return build_graph(self.stages, self.outs_trie)

    def check_graph(self):
        if not getattr(self.repo, "_skip_graph_checks", False):
            # remove self.graph later
            self.graph  # pylint: disable=pointless-statement

    @cached_property
    def deterministic_hash(self):
        # we can use this to optimize and skip opening some indices
        # eg. on push/pull/fetch/gc --all-commits
        import hashlib
        import json

        def stage_kv(stage):
            key = "::".join([stage.path_in_repo, getattr(stage, "name", "")])
            return key, stage.md5 or stage.compute_md5()

        d = dict(map(stage_kv, self))
        d_str = json.dumps(d, sort_keys=True).encode("utf-8")
        return hashlib.md5(d_str).hexdigest()

    def __enter__(self):
        return self

    def ___exit__(self, *args):
        self.reset()

    def reset(self):
        # we don't need to reset these for the indexes that are not
        # currently checked out.
        self.__dict__.pop("outs_trie", None)
        self.__dict__.pop("outs_graph", None)
        self.__dict__.pop("graph", None)
        self.__dict__.pop("stages", None)
        self.__dict__.pop("pipelines", None)


def _add_suffix(objs, suffix):
    from itertools import chain

    from dvc.objects import iterobjs

    for obj in chain.from_iterable(map(iterobjs, objs)):
        if obj.name is not None:
            obj.name += suffix


if __name__ == "__main__":
    from funcy import log_durations

    from dvc.repo import Repo

    repo = Repo()
    index = Index(repo, repo.fs)
    with log_durations(print):
        # pylint: disable=pointless-statement
        index.stages
    with log_durations(print):
        index.build_graph()
    with log_durations(print):
        print(index.deterministic_hash)
    with log_durations(print):
        index2 = index.update([])
    with log_durations(print):
        print(index2.deterministic_hash)
