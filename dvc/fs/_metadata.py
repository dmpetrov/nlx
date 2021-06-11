from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List

from dvc.output import Output
from dvc.path_info import PathInfo

if TYPE_CHECKING:
    from dvc.repo import Repo


@dataclass
class Metadata:
    """
    Container for storing metadata for a given path, similar in spirit to
    `os.stat_result`.
    """

    # required field
    path_info: PathInfo
    repo: "Repo"

    # computed fields
    is_output: bool = field(init=False, default=False)  # is it an output?
    # is the path part of an output?
    part_of_output: bool = field(init=False, default=False)
    # does the path contain outputs?
    contains_outputs: bool = field(init=False, default=False)
    # is the path tracked by dvc? equivalent to (is_output or part_of_output)
    is_dvc: bool = field(init=False, default=False)
    # does dvc has any output in that path or inside it? equiv. to bool(outs)
    output_exists: bool = field(init=False)

    # optional fields
    isdir: bool = False  # is it a directory?
    is_exec: bool = False  # is it an executable?
    outs: List[Output] = field(default_factory=list)  # list of outputs

    def __post_init__(self):
        self.output_exists = bool(self.outs)

        if not self.output_exists:
            return

        # it can contain multiple outputs
        if len(self.outs) > 1:
            self.contains_outputs = True
        else:
            out = self.outs[0]
            # or, the path itself could be an output
            self.is_output = self.path_info == out.path_info

            # or, a directory must have an output somewhere deep inside it
            if not self.is_output:
                self.contains_outputs = out.path_info.isin(self.path_info)

        # or, the path could be a part of an output, i.e. inside of an output
        self.part_of_output = not self.is_output and not self.contains_outputs

        self.is_dvc = self.is_output or self.part_of_output
        # if it contains outputs, it must be a dir
        self.isdir = self.contains_outputs

    @property
    def isfile(self):
        # self.isdir might require additional computation, especially if it's
        # a dir that's inside a dvc-tracked dir
        # since, `Metadata()` does not support being used as null (i.e. when
        # path does not exist), `isfile` becomes `isdir`'s complement.
        return not self.isdir

    def __str__(self):
        return f"Metadata: {self.path_info}"
