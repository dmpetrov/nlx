import json
from collections import defaultdict

from .flatten import flatten


def _parse(raw):
    if raw is None or isinstance(raw, (dict, list, int, float)):
        return raw

    assert isinstance(raw, str)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def _diff_vals(old, new, with_unchanged):
    if (
        isinstance(new, list)
        and isinstance(old, list)
        and len(old) == len(new) == 1
    ):
        return _diff_vals(old[0], new[0], with_unchanged)

    if not with_unchanged and old == new:
        return {}

    res = {"old": old, "new": new}
    if isinstance(new, (int, float)) and isinstance(old, (int, float)):
        res["diff"] = new - old
    return res


def _flatten(d):
    if not d:
        return defaultdict(lambda: None)

    if isinstance(d, dict):
        return defaultdict(lambda: None, flatten(d))

    return defaultdict(lambda: "unable to parse")


def _diff_dicts(old_dict, new_dict, with_unchanged):
    new = _flatten(new_dict)
    old = _flatten(old_dict)

    res = defaultdict(dict)

    xpaths = set(old.keys())
    xpaths.update(set(new.keys()))
    for xpath in xpaths:
        old_val = old[xpath]
        new_val = new[xpath]
        val_diff = _diff_vals(old_val, new_val, with_unchanged)
        if val_diff:
            res[xpath] = val_diff
    return dict(res)


def _diff(old_raw, new_raw, with_unchanged):
    old = _parse(old_raw)
    new = _parse(new_raw)

    if isinstance(new, dict) or isinstance(old, dict):
        return _diff_dicts(old, new, with_unchanged)

    val_diff = _diff_vals(old, new, with_unchanged)
    if val_diff:
        return {"": val_diff}

    return {}


def diff(old, new, with_unchanged=False):
    paths = set(old.keys())
    paths.update(set(new.keys()))

    res = defaultdict(dict)
    for path in paths:
        path_diff = _diff(
            old.get(path, {}).get("data", {}),
            new.get(path, {}).get("data", {}),
            with_unchanged,
        )
        if path_diff:
            res[path] = path_diff
    return dict(res)


def format_dict(d):
    ret = {}
    for key, val in d.items():
        if isinstance(val, dict):
            new_val = format_dict(val)
        elif isinstance(val, list):
            new_val = str(val)
        else:
            new_val = val
        ret[key] = new_val
    return ret
