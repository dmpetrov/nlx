from dvc.exceptions import DvcException


class ObjectError(DvcException):
    pass


class ObjectFormatError(ObjectError):
    pass
