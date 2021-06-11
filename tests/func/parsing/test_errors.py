"""Negative tests for the parametrization."""


import logging
import re

import pytest

from dvc.parsing import ResolveError
from dvc.parsing.context import Context
from dvc.parsing.interpolate import embrace
from dvc.utils.humanize import join
from dvc.utils.serialize import dump_yaml

from . import make_entry_definition, make_foreach_def


def escape_ansi(line):
    ansi_escape = re.compile(r"(\x9B|\x1B\[)[0-?]*[ -\/]*[@-~]")
    return ansi_escape.sub("", line)


# Tests for the interpolated entries


@pytest.mark.parametrize("vars_", ["${file}_params.yaml", {"foo": "${foo}"}])
def test_vars_interpolation_errors(tmp_dir, dvc, vars_):
    definition = make_entry_definition(tmp_dir, "build", {"vars": [vars_]})
    with pytest.raises(ResolveError) as exc_info:
        definition.resolve()

    assert (
        str(exc_info.value)
        == "failed to parse 'stages.build.vars' in 'dvc.yaml': "
        "interpolating is not allowed"
    )


def test_failed_to_interpolate(tmp_dir, dvc):
    context = Context(models={"foo": "bar"})
    definition = make_entry_definition(
        tmp_dir, "build", {"cmd": "echo ${models.foo.}"}, context
    )

    with pytest.raises(ResolveError) as exc_info:
        definition.resolve()

    assert escape_ansi(str(exc_info.value)) == (
        "failed to parse 'stages.build.cmd' in 'dvc.yaml':\n"
        "${models.foo.}\n"
        "            ^\n"
        "ParseException: Expected end of text, found '.'"
        "  (at char 12), (line:1, col:13)"
    )
    assert definition.context == {"models": {"foo": "bar"}}


def test_local_vars_params_file_not_exist(tmp_dir, dvc):
    definition = make_entry_definition(
        tmp_dir,
        "build",
        {"vars": ["not_existing_params.yaml"], "cmd": "echo ${models.foo}"},
    )

    with pytest.raises(ResolveError) as exc_info:
        definition.resolve()

    assert str(exc_info.value) == (
        "failed to parse stage 'build' in 'dvc.yaml': "
        "'not_existing_params.yaml' does not exist"
    )
    assert not definition.context


def test_specified_key_does_not_exist(tmp_dir, dvc):
    definition = make_entry_definition(
        tmp_dir,
        "build",
        {"cmd": "echo ${models.foobar}"},
        Context(models={"foo": "foo"}),
    )
    with pytest.raises(ResolveError) as exc_info:
        definition.resolve()

    assert str(exc_info.value) == (
        "failed to parse 'stages.build.cmd' in 'dvc.yaml': "
        "Could not find 'models.foobar'"
    )
    assert definition.context == {"models": {"foo": "foo"}}


@pytest.mark.parametrize(
    "wdir, expected_msg",
    [
        ("${models[foobar]}", " Could not find 'models.foobar'"),
        (
            "${models.foo]}",
            "\n${models.foo]}\n"
            "            ^\n"
            "ParseException: Expected end of text, found ']'"
            "  (at char 12), (line:1, col:13)",
        ),
    ],
)
def test_wdir_failed_to_interpolate(tmp_dir, dvc, wdir, expected_msg):
    definition = make_entry_definition(
        tmp_dir,
        "build",
        {"wdir": wdir, "cmd": "echo ${models.bar}"},
        Context(models={"bar": "bar"}),
    )
    with pytest.raises(ResolveError) as exc_info:
        definition.resolve()

    assert escape_ansi(str(exc_info.value)) == (
        "failed to parse 'stages.build.wdir' in 'dvc.yaml':" + expected_msg
    )
    assert definition.context == {"models": {"bar": "bar"}}


def test_interpolate_non_string(tmp_dir, dvc):
    definition = make_entry_definition(
        tmp_dir, "build", {"cmd": "echo ${models}"}, Context(models={})
    )
    with pytest.raises(ResolveError) as exc_info:
        definition.resolve()

    assert str(exc_info.value) == (
        "failed to parse 'stages.build.cmd' in 'dvc.yaml':\n"
        "Cannot interpolate data of type 'dict'"
    )
    assert definition.context == {"models": {}}


def test_partial_vars_doesnot_exist(tmp_dir, dvc):
    dump_yaml("test_params.yaml", {"sub1": "sub1", "sub2": "sub2"})

    definition = make_entry_definition(
        tmp_dir,
        "build",
        {"vars": ["test_params.yaml:sub3"], "cmd": "echo ${sub1} ${sub2}"},
    )

    with pytest.raises(ResolveError) as exc_info:
        definition.resolve()

    assert str(exc_info.value) == (
        "failed to parse stage 'build' in 'dvc.yaml': "
        "could not find 'sub3' in 'test_params.yaml'"
    )
    assert not definition.context


# Tests foreach generated stages and their error messages


def test_foreach_data_syntax_error(tmp_dir, dvc):
    definition = make_foreach_def(tmp_dir, "build", "${syntax.[error}", {})
    with pytest.raises(ResolveError) as exc_info:
        definition.resolve_all()

    assert escape_ansi(str(exc_info.value)) == (
        "failed to parse 'stages.build.foreach' in 'dvc.yaml':\n"
        "${syntax.[error}\n"
        "        ^\n"
        "ParseException: Expected end of text, found '.'"
        "  (at char 8), (line:1, col:9)"
    )


@pytest.mark.parametrize("key", ["modelss", "modelss.123"])
def test_foreach_data_key_does_not_exists(tmp_dir, dvc, key):
    definition = make_foreach_def(tmp_dir, "build", embrace(key), {})
    with pytest.raises(ResolveError) as exc_info:
        definition.resolve_all()
    assert str(exc_info.value) == (
        "failed to parse 'stages.build.foreach' in 'dvc.yaml': "
        f"Could not find '{key}'"
    )


@pytest.mark.parametrize(
    "foreach_data", ["${foo}", "${dct.model1}", "${lst.0}", "foobar"]
)
def test_foreach_data_expects_list_or_dict(tmp_dir, dvc, foreach_data):
    context = Context(
        {"foo": "bar", "dct": {"model1": "a-out"}, "lst": ["foo", "bar"]}
    )
    definition = make_foreach_def(tmp_dir, "build", foreach_data, {}, context)
    with pytest.raises(ResolveError) as exc_info:
        definition.resolve_all()
    assert str(exc_info.value) == (
        "failed to resolve 'stages.build.foreach' in 'dvc.yaml': "
        "expected list/dictionary, got str"
    )


@pytest.mark.parametrize(
    "global_data, where",
    [
        ({"item": 10, "key": 10}, "item and key are"),
        ({"item": 10}, "item is"),
        ({"key": 5}, "key is"),
    ],
)
def test_foreach_overwriting_item_in_list(
    tmp_dir, dvc, caplog, global_data, where
):
    context = Context(global_data)
    definition = make_foreach_def(
        tmp_dir, "build", {"model1": 10, "model2": 5}, {}, context
    )
    with caplog.at_level(logging.WARNING, logger="dvc.parsing"):
        definition.resolve_all()

    assert caplog.messages == [
        f"{where} already specified, "
        "will be overwritten for stages generated from 'build'"
    ]


def test_foreach_do_syntax_errors(tmp_dir, dvc):
    definition = make_foreach_def(
        tmp_dir, "build", ["foo", "bar"], {"cmd": "echo ${syntax.[error}"}
    )

    with pytest.raises(ResolveError) as exc_info:
        definition.resolve_all()

    assert escape_ansi(str(exc_info.value)) == (
        "failed to parse 'stages.build.cmd' in 'dvc.yaml':\n"
        "${syntax.[error}\n"
        "        ^\n"
        "ParseException: Expected end of text, found '.'"
        "  (at char 8), (line:1, col:9)"
    )


@pytest.mark.parametrize(
    "key, loc",
    [
        (
            "item.thresh",  # the `thresh` in not available on model2`
            "stages.build@1.cmd",
        ),
        ("foo.bar", "stages.build@0.cmd"),  # not available on any stages
    ],
)
def test_foreach_do_definition_item_does_not_exist(tmp_dir, dvc, key, loc):
    context = Context(foo="bar")
    definition = make_foreach_def(
        tmp_dir,
        "build",
        [{"thresh": "10"}, {}],
        {"cmd": embrace(key)},
        context,
    )

    with pytest.raises(ResolveError) as exc_info:
        definition.resolve_all()

    assert str(exc_info.value) == (
        f"failed to parse '{loc}' in 'dvc.yaml': Could not find '{key}'"
    )

    # should have no `item` and `key` even though it failed to resolve.
    assert context == {"foo": "bar"}


@pytest.mark.parametrize(
    "redefine",
    [
        {"item": 5},
        {"key": 5},
        {"item": 5, "key": 10},
        {"item": {"epochs": 10}},
    ],
)
@pytest.mark.parametrize("from_file", [True, False])
def test_item_key_in_generated_stage_vars(tmp_dir, dvc, redefine, from_file):
    context = Context(foo="bar")
    vars_ = [redefine]
    if from_file:
        dump_yaml("test_params.yaml", redefine)
        vars_ = ["test_params.yaml"]

    definition = make_foreach_def(
        tmp_dir,
        "build",
        {"model1": {"thresh": "10"}, "model2": {"thresh": 5}},
        {"vars": vars_, "cmd": "${item}"},
        context,
    )

    with pytest.raises(ResolveError) as exc_info:
        definition.resolve_all()

    message = str(exc_info.value)
    assert (
        "failed to parse stage 'build@model1' in 'dvc.yaml': "
        "attempted to modify reserved"
    ) in message

    key_or_keys = "keys" if len(redefine) > 1 else "key"
    assert f"{key_or_keys} {join(redefine)}" in message
    if from_file:
        assert "in 'test_params.yaml'" in message
    assert context == {"foo": "bar"}


def test_foreach_wdir_key_does_not_exist(tmp_dir, dvc):
    definition = make_foreach_def(
        tmp_dir,
        "build",
        "${models}",
        {"wdir": "${ite}", "cmd": "echo ${item}"},
        Context(models=["foo", "bar"]),
    )
    with pytest.raises(ResolveError) as exc_info:
        definition.resolve_all()
    assert (
        str(exc_info.value)
        == "failed to parse 'stages.build@foo.wdir' in 'dvc.yaml': "
        "Could not find 'ite'"
    )
    assert definition.context == {"models": ["foo", "bar"]}
