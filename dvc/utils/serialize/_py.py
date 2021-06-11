import ast
from contextlib import contextmanager

from funcy import reraise

from ._common import ParseError, _dump_data, _load_data, _modify_data

_PARAMS_KEY = "__params_old_key_for_update__"
_PARAMS_TEXT_KEY = "__params_text_key_for_update__"


class PythonFileCorruptedError(ParseError):
    def __init__(self, path, message="Python file structure is corrupted"):
        super().__init__(path, message)


def load_py(path, fs=None):
    return _load_data(path, parser=parse_py, fs=fs)


def parse_py(text, path):
    """Parses text from .py file into Python structure."""
    with reraise(SyntaxError, PythonFileCorruptedError(path)):
        tree = ast.parse(text, filename=path)

    result = _ast_tree_to_dict(tree)
    return result


def parse_py_for_update(text, path):
    """Parses text into dict for update params."""
    with reraise(SyntaxError, PythonFileCorruptedError(path)):
        tree = ast.parse(text, filename=path)

    result = _ast_tree_to_dict(tree)
    result.update({_PARAMS_KEY: _ast_tree_to_dict(tree, lineno=True)})
    result.update({_PARAMS_TEXT_KEY: text})
    return result


def _dump(data, stream):

    old_params = data[_PARAMS_KEY]
    new_params = {
        key: value
        for key, value in data.items()
        if key not in [_PARAMS_KEY, _PARAMS_TEXT_KEY]
    }
    old_lines = data[_PARAMS_TEXT_KEY].splitlines(True)

    def _update_lines(lines, old_dct, new_dct):
        for key, value in new_dct.items():
            if isinstance(value, dict):
                lines = _update_lines(lines, old_dct[key], value)
            elif value != old_dct[key]["value"]:
                lineno = old_dct[key]["lineno"]
                lines[lineno] = lines[lineno].replace(
                    f" = {old_dct[key]['value']}", f" = {value}"
                )
            else:
                continue
        return lines

    new_lines = _update_lines(old_lines, old_params, new_params)
    new_text = "".join(new_lines)

    try:
        ast.parse(new_text)
    except SyntaxError:
        raise PythonFileCorruptedError(
            stream.name,
            "Python file structure is corrupted after update params",
        )

    stream.write(new_text)
    stream.close()


def dump_py(path, data, fs=None):
    return _dump_data(path, data, dumper=_dump, fs=fs)


@contextmanager
def modify_py(path, fs=None):
    with _modify_data(path, parse_py_for_update, dump_py, fs=fs) as d:
        yield d


def _ast_tree_to_dict(tree, only_self_params=False, lineno=False):
    """Parses ast trees to dict.

    :param tree: ast.Tree
    :param only_self_params: get only self params from class __init__ function
    :param lineno: add params line number (needed for update)
    :return:
    """
    result = {}
    for _body in tree.body:
        try:
            if isinstance(_body, (ast.Assign, ast.AnnAssign)):
                result.update(
                    _ast_assign_to_dict(_body, only_self_params, lineno)
                )
            elif isinstance(_body, ast.ClassDef):
                result.update(
                    {_body.name: _ast_tree_to_dict(_body, lineno=lineno)}
                )
            elif (
                isinstance(_body, ast.FunctionDef) and _body.name == "__init__"
            ):
                result.update(
                    _ast_tree_to_dict(
                        _body, only_self_params=True, lineno=lineno
                    )
                )
        except ValueError:
            continue
        except AttributeError:
            continue
    return result


def _ast_assign_to_dict(assign, only_self_params=False, lineno=False):
    result = {}

    if isinstance(assign, ast.AnnAssign):
        name = _get_ast_name(assign.target, only_self_params)
    elif len(assign.targets) == 1:
        name = _get_ast_name(assign.targets[0], only_self_params)
    else:
        raise AttributeError

    if isinstance(assign.value, ast.Dict):
        value = {}
        for key, val in zip(assign.value.keys, assign.value.values):
            if lineno:
                value[_get_ast_value(key)] = {
                    "lineno": assign.lineno - 1,
                    "value": _get_ast_value(val),
                }
            else:
                value[_get_ast_value(key)] = _get_ast_value(val)
    elif isinstance(assign.value, ast.List):
        value = [_get_ast_value(val) for val in assign.value.elts]
    elif isinstance(assign.value, ast.Set):
        values = [_get_ast_value(val) for val in assign.value.elts]
        value = set(values)
    elif isinstance(assign.value, ast.Tuple):
        values = [_get_ast_value(val) for val in assign.value.elts]
        value = tuple(values)
    else:
        value = _get_ast_value(assign.value)

    if lineno and not isinstance(assign.value, ast.Dict):
        result[name] = {"lineno": assign.lineno - 1, "value": value}
    else:
        result[name] = value

    return result


def _get_ast_name(target, only_self_params=False):
    if hasattr(target, "id") and not only_self_params:
        result = target.id
    elif hasattr(target, "attr") and target.value.id == "self":
        result = target.attr
    else:
        raise AttributeError
    return result


def _get_ast_value(value):
    if isinstance(value, ast.Num):
        result = value.n
    elif isinstance(value, ast.Str):
        result = value.s
    elif isinstance(value, ast.NameConstant):
        result = value.value
    else:
        raise ValueError
    return result
