from collections import defaultdict
from typing import Any, Dict, Iterable, List


def parse_params(path_params: Iterable[str]) -> List[Dict[str, List[str]]]:
    """Normalizes the shape of params from the CLI to dict."""
    from dvc.dependency.param import ParamsDependency

    ret: Dict[str, List[str]] = defaultdict(list)
    for path_param in path_params:
        path, _, params_str = path_param.rpartition(":")
        # remove empty strings from params, on condition such as `-p "file1:"`
        params = filter(bool, params_str.split(","))
        if not path:
            path = ParamsDependency.DEFAULT_PARAMS_FILE
        ret[path].extend(params)
    return [{path: params} for path, params in ret.items()]


def loads_param_overrides(
    path_params: Iterable[str],
) -> Dict[str, Dict[str, Any]]:
    """Loads the content of params from the cli as Python object."""
    from ruamel.yaml import YAMLError

    from dvc.dependency.param import ParamsDependency
    from dvc.exceptions import InvalidArgumentError

    from .serialize import loads_yaml

    ret: Dict[str, Dict[str, Any]] = defaultdict(dict)

    for path_param in path_params:
        param_name, _, param_value = path_param.partition("=")
        if not param_value:
            raise InvalidArgumentError(
                f"Must provide a value for parameter '{param_name}'"
            )
        path, _, param_name = param_name.partition(":")
        if not param_name:
            param_name = path
            path = ParamsDependency.DEFAULT_PARAMS_FILE

        try:
            ret[path][param_name] = loads_yaml(param_value)
        except (ValueError, YAMLError):
            raise InvalidArgumentError(
                f"Invalid parameter value for '{param_name}': '{param_value}"
            )

    return ret
