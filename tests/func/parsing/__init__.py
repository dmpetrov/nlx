from dvc.parsing import DataResolver, EntryDefinition, ForeachDefinition
from dvc.parsing.context import Context

TEMPLATED_DVC_YAML_DATA = {
    "stages": {
        "stage1": {
            "cmd": "python script.py ${dict.foo} --out ${dict.bar}",
            "outs": ["${dict.bar}"],
            "deps": ["${dict.foo}"],
            "frozen": "${freeze}",
        },
        "stage2": {"cmd": "echo ${dict.foo} ${dict.bar}"},
    }
}

CONTEXT_DATA = {
    "dict": {"foo": "foo", "bar": "bar"},
    "list": ["param1", "param2"],
    "freeze": True,
}

RESOLVED_DVC_YAML_DATA = {
    "stages": {
        "stage1": {
            "cmd": "python script.py foo --out bar",
            "outs": ["bar"],
            "deps": ["foo"],
            "frozen": True,
        },
        "stage2": {"cmd": "echo foo bar"},
    }
}

USED_VARS = {
    "stage1": {"dict.foo": "foo", "dict.bar": "bar", "freeze": True},
    "stage2": {"dict.foo": "foo", "dict.bar": "bar"},
}


def make_entry_definition(wdir, name, data, context=None) -> EntryDefinition:
    return EntryDefinition(
        DataResolver(wdir.dvc, wdir, {}), context or Context(), name, data
    )


def make_foreach_def(
    wdir, name, foreach_data, do_data=None, context=None
) -> ForeachDefinition:
    return ForeachDefinition(
        DataResolver(wdir.dvc, wdir, {}),
        context or Context(),
        name,
        {"foreach": foreach_data, "do": do_data or {}},
    )
