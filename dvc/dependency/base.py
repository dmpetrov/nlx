from typing import Type

from dvc.exceptions import DvcException
from dvc.output import Output


class DependencyDoesNotExistError(DvcException):
    def __init__(self, path):
        msg = f"dependency '{path}' does not exist"
        super().__init__(msg)


class DependencyIsNotFileOrDirError(DvcException):
    def __init__(self, path):
        msg = f"dependency '{path}' is not a file or directory"
        super().__init__(msg)


class DependencyIsStageFileError(DvcException):
    def __init__(self, path):
        super().__init__(f"DVC file '{path}' cannot be a dependency.")


class Dependency(Output):
    IS_DEPENDENCY = True

    DoesNotExistError = DependencyDoesNotExistError  # type: Type[DvcException]
    IsNotFileOrDirError = (
        DependencyIsNotFileOrDirError
    )  # type: Type[DvcException]
    IsStageFileError = DependencyIsStageFileError  # type: Type[DvcException]

    def update(self, rev=None):
        pass
