"""Testing happy paths for the foreach."""
import os

import pytest

from dvc.parsing import DEFAULT_PARAMS_FILE, DataResolver, ForeachDefinition
from dvc.parsing.context import Context
from dvc.utils.serialize import dump_yaml


def test_with_simple_list_data(tmp_dir, dvc):
    """Testing a simple non-nested list as a foreach data"""
    resolver = DataResolver(dvc, tmp_dir, {})

    context = Context()
    data = {"foreach": ["foo", "bar", "baz"], "do": {"cmd": "echo ${item}"}}
    definition = ForeachDefinition(resolver, context, "build", data)

    assert definition.resolve_one("foo") == {"build@foo": {"cmd": "echo foo"}}
    assert definition.resolve_one("bar") == {"build@bar": {"cmd": "echo bar"}}
    # check that `foreach` item-key replacement didnot leave any leftovers.
    assert not context
    assert not resolver.tracked_vars["build@foo"]
    assert not resolver.tracked_vars["build@bar"]


def test_with_dict_data(tmp_dir, dvc):
    resolver = DataResolver(dvc, tmp_dir, {})
    context = Context()

    foreach_data = {"model1": "foo", "model2": "bar"}
    data = {"foreach": foreach_data, "do": {"cmd": "echo ${key} ${item}"}}
    definition = ForeachDefinition(resolver, context, "build", data)

    assert definition.resolve_one("model1") == {
        "build@model1": {"cmd": "echo model1 foo"}
    }
    assert definition.resolve_one("model2") == {
        "build@model2": {"cmd": "echo model2 bar"}
    }

    # check that `foreach` item-key replacement didnot leave any leftovers.
    assert not context
    assert not resolver.tracked_vars["build@model1"]
    assert not resolver.tracked_vars["build@model2"]


def test_with_composite_list(tmp_dir, dvc):
    resolver = DataResolver(dvc, tmp_dir, {})

    context = Context()
    foreach_data = [{"thresh": "foo"}, {"thresh": "bar"}]
    data = {"foreach": foreach_data, "do": {"cmd": "echo ${item.thresh}"}}
    definition = ForeachDefinition(resolver, context, "build", data)

    assert definition.resolve_one("0") == {"build@0": {"cmd": "echo foo"}}
    # check that `foreach` item-key replacement didnot leave any leftovers.
    assert not context

    assert definition.resolve_one("1") == {"build@1": {"cmd": "echo bar"}}
    assert not context
    assert not resolver.tracked_vars["build@0"]


def test_foreach_interpolated_simple_list(tmp_dir, dvc):
    foreach_data = ["foo", "bar", "baz"]
    vars_ = {"models": foreach_data}
    resolver = DataResolver(dvc, tmp_dir, {"vars": [vars_]})
    data = {"foreach": "${models}", "do": {"cmd": "echo ${item}"}}
    definition = ForeachDefinition(resolver, resolver.context, "build", data)

    assert definition.resolve_all() == {
        "build@foo": {"cmd": "echo foo"},
        "build@bar": {"cmd": "echo bar"},
        "build@baz": {"cmd": "echo baz"},
    }
    assert resolver.context == {"models": foreach_data}
    assert not any(item for item in resolver.tracked_vars.values())


@pytest.mark.parametrize("foreach_def", ["${item.thresh}", "${item[thresh]}"])
@pytest.mark.parametrize(
    "foreach_data, result",
    [
        (
            {"model1": {"thresh": "foo"}, "model2": {"thresh": "bar"}},
            {
                "build@model1": {"cmd": "echo foo"},
                "build@model2": {"cmd": "echo bar"},
            },
        ),
        (
            [{"thresh": "foo"}, {"thresh": "bar"}],
            {"build@0": {"cmd": "echo foo"}, "build@1": {"cmd": "echo bar"}},
        ),
    ],
)
def test_foreach_interpolate_with_composite_data(
    tmp_dir, dvc, foreach_def, foreach_data, result
):
    vars_ = [{"models": foreach_data}]
    resolver = DataResolver(dvc, tmp_dir, {"vars": vars_})
    data = {"foreach": "${models}", "do": {"cmd": f"echo {foreach_def}"}}
    definition = ForeachDefinition(resolver, resolver.context, "build", data)

    assert definition.resolve_all() == result
    assert resolver.context == {"models": foreach_data}
    assert not any(item for item in resolver.tracked_vars.values())


def test_params_file_with_dict_tracked(tmp_dir, dvc):
    foreach_data = {"model1": {"thresh": "foo"}, "model2": {"thresh": "bar"}}
    params = {"models": foreach_data}
    dump_yaml("params.yaml", params)

    resolver = DataResolver(dvc, tmp_dir, {})
    data = {"foreach": "${models}", "do": {"cmd": "echo ${item.thresh}"}}
    definition = ForeachDefinition(resolver, resolver.context, "build", data)

    assert definition.resolve_all() == {
        "build@model1": {"cmd": "echo foo"},
        "build@model2": {"cmd": "echo bar"},
    }
    # check that `foreach` item-key replacement didnot leave any leftovers.
    assert resolver.context == {"models": foreach_data}
    assert resolver.tracked_vars == {
        "build@model1": {"params.yaml": {"models.model1.thresh": "foo"}},
        "build@model2": {"params.yaml": {"models.model2.thresh": "bar"}},
    }


def test_params_file_tracked_for_composite_list(tmp_dir, dvc):
    foreach_data = [{"thresh": "foo"}, {"thresh": "bar"}]
    params = {"models": foreach_data}
    dump_yaml("params.yaml", params)

    resolver = DataResolver(dvc, tmp_dir, {})
    data = {"foreach": "${models}", "do": {"cmd": "echo ${item.thresh}"}}
    definition = ForeachDefinition(resolver, resolver.context, "build", data)

    assert definition.resolve_all() == {
        "build@0": {"cmd": "echo foo"},
        "build@1": {"cmd": "echo bar"},
    }
    assert resolver.context == {"models": foreach_data}
    assert resolver.tracked_vars == {
        "build@0": {"params.yaml": {"models.0.thresh": "foo"}},
        "build@1": {"params.yaml": {"models.1.thresh": "bar"}},
    }


def test_foreach_data_from_nested_vars(tmp_dir, dvc):
    vars_ = {"models": {"lst": [{"thresh": 10}, {"thresh": 15}]}}
    resolver = DataResolver(dvc, tmp_dir, {"vars": [vars_]})
    data = {"foreach": "${models.lst}", "do": {"cmd": "echo ${item.thresh}"}}
    definition = ForeachDefinition(resolver, resolver.context, "build", data)

    assert definition.resolve_all() == {
        "build@0": {"cmd": "echo 10"},
        "build@1": {"cmd": "echo 15"},
    }
    assert resolver.context == vars_
    assert not any(item for item in resolver.tracked_vars.values())


def test_foreach_partial_interpolations(tmp_dir, dvc):
    resolver = DataResolver(dvc, tmp_dir, {"vars": [{"bar": "bar"}]})
    foreach_data = {"model1": "foo", "model2": "${bar}"}
    data = {"foreach": foreach_data, "do": {"cmd": "echo ${item}"}}
    definition = ForeachDefinition(resolver, resolver.context, "build", data)

    assert definition.resolve_all() == {
        "build@model1": {"cmd": "echo foo"},
        "build@model2": {"cmd": "echo bar"},
    }
    assert resolver.context == {"bar": "bar"}
    assert not any(item for item in resolver.tracked_vars.values())


def test_mixed_vars_for_foreach_data(tmp_dir, dvc):
    dump_yaml("params.yaml", {"models": {"model1": "foo"}})
    dump_yaml("test_params.yaml", {"models": {"model2": "bar"}})

    resolver = DataResolver(dvc, tmp_dir, {"vars": ["test_params.yaml"]})
    data = {"foreach": "${models}", "do": {"cmd": "echo ${item}"}}
    definition = ForeachDefinition(resolver, resolver.context, "build", data)

    assert definition.resolve_all() == {
        "build@model1": {"cmd": "echo foo"},
        "build@model2": {"cmd": "echo bar"},
    }
    assert resolver.context == {"models": {"model1": "foo", "model2": "bar"}}
    assert resolver.tracked_vars == {
        "build@model1": {"params.yaml": {"models.model1": "foo"}},
        "build@model2": {"test_params.yaml": {"models.model2": "bar"}},
    }


def test_mixed_vars_for_foreach_data_2(tmp_dir, dvc):
    dump_yaml(
        "params.yaml",
        {"models": {"model1": {"thresh": 10}, "model2": {"thresh": 15}}},
    )
    dump_yaml(
        "test_params.yaml",
        {"models": {"model1": {"epochs": 5}, "model2": {"epochs": 10}}},
    )

    resolver = DataResolver(dvc, tmp_dir, {"vars": ["test_params.yaml"]})
    data = {
        "foreach": "${models}",
        "do": {"cmd": "echo ${item.thresh} ${item.epochs}"},
    }
    definition = ForeachDefinition(resolver, resolver.context, "build", data)

    assert definition.resolve_all() == {
        "build@model1": {"cmd": "echo 10 5"},
        "build@model2": {"cmd": "echo 15 10"},
    }
    assert resolver.context == {
        "models": {
            "model1": {"thresh": 10, "epochs": 5},
            "model2": {"thresh": 15, "epochs": 10},
        }
    }
    assert resolver.tracked_vars == {
        "build@model1": {
            "params.yaml": {"models.model1.thresh": 10},
            "test_params.yaml": {"models.model1.epochs": 5},
        },
        "build@model2": {
            "params.yaml": {"models.model2.thresh": 15},
            "test_params.yaml": {"models.model2.epochs": 10},
        },
    }


def test_foreach_with_interpolated_wdir(tmp_dir, dvc):
    resolver = DataResolver(dvc, tmp_dir / "data", {})
    foreach_data = ["foo", "bar"]
    data = {
        "foreach": foreach_data,
        "do": {"wdir": "${item}", "cmd": "echo hello"},
    }
    definition = ForeachDefinition(resolver, resolver.context, "build", data)

    assert definition.resolve_all() == {
        # note that the resolver generates `wdir` relative to file's wdir
        # so, this is just `foo`, not `data/foo`.
        # figuring out `wdir` is the responsibility of the `load_stage`/`Stage`
        "build@foo": {"wdir": "foo", "cmd": "echo hello"},
        "build@bar": {"wdir": "bar", "cmd": "echo hello"},
    }

    assert not resolver.context
    assert not any(item for item in resolver.tracked_vars.values())


def test_foreach_with_local_vars(tmp_dir, dvc):
    resolver = DataResolver(dvc, tmp_dir / "data", {})
    foreach_data = ["foo", "bar"]
    data = {
        "foreach": foreach_data,
        "do": {
            "vars": [{"foobar": "foobar"}],
            "cmd": "echo ${item} ${foobar}",
        },
    }
    definition = ForeachDefinition(resolver, resolver.context, "build", data)

    assert definition.resolve_all() == {
        # note that the resolver generates `wdir` relative to file's wdir
        # so, this is just `foo`, not `data/foo`.
        # figuring out `wdir` is the responsibility of the `load_stage`/`Stage`
        "build@foo": {"cmd": "echo foo foobar"},
        "build@bar": {"cmd": "echo bar foobar"},
    }
    assert not resolver.context
    assert not any(item for item in resolver.tracked_vars.values())


@pytest.mark.parametrize(
    "local_import",
    [
        "test_params.yaml",
        "test_params.yaml:train",
        "test_params.yaml:train,prepare",
    ],
)
def test_foreach_with_imported_vars(tmp_dir, dvc, local_import):
    dump_yaml("params.yaml", {"models": {"model1": {"thresh": "foo"}}})
    dump_yaml(
        "test_params.yaml", {"train": {"epochs": 10}, "prepare": {"nums": 25}}
    )
    resolver = DataResolver(dvc, tmp_dir, {})
    foreach_data = ["foo", "bar"]
    data = {
        "foreach": foreach_data,
        "do": {"vars": [local_import], "cmd": "echo ${item} ${train.epochs}"},
    }
    definition = ForeachDefinition(resolver, resolver.context, "build", data)

    assert definition.resolve_all() == {
        # note that the resolver generates `wdir` relative to file's wdir
        # so, this is just `foo`, not `data/foo`.
        # figuring out `wdir` is the responsibility of the `load_stage`/`Stage`
        "build@foo": {"cmd": "echo foo 10"},
        "build@bar": {"cmd": "echo bar 10"},
    }

    assert resolver.context == {"models": {"model1": {"thresh": "foo"}}}
    assert resolver.tracked_vars == {
        "build@foo": {"test_params.yaml": {"train.epochs": 10}},
        "build@bar": {"test_params.yaml": {"train.epochs": 10}},
    }


@pytest.mark.parametrize(
    "local_import", ["params.yaml", "params.yaml:train,prepare"]
)
def test_foreach_with_interpolated_wdir_and_local_vars(
    tmp_dir, dvc, local_import
):
    dump_yaml("params.yaml", {"models": {"model1": {"thresh": "foo"}}})

    for i in range(5):
        build_dir = tmp_dir / ("model-" + str(i))
        build_dir.mkdir()
        dump_yaml(
            build_dir / "params.yaml",
            {"train": {"epochs": 1 + i}, "prepare": {"nums": 10 * i}},
        )

    resolver = DataResolver(dvc, tmp_dir, {})
    data = {
        "foreach": [0, 1, 2, 3, 4],
        "do": {
            "wdir": "model-${item}",
            "vars": [local_import],
            "cmd": "echo ${item} ${train.epochs} ${prepare.nums}",
        },
    }
    definition = ForeachDefinition(resolver, resolver.context, "build", data)

    assert definition.resolve_all() == {
        # note that the resolver generates `wdir` relative to file's wdir
        # so, this is just `foo`, not `data/foo`.
        # figuring out `wdir` is the responsibility of the `load_stage`/`Stage`
        "build@0": {"wdir": "model-0", "cmd": "echo 0 1 0"},
        "build@1": {"wdir": "model-1", "cmd": "echo 1 2 10"},
        "build@2": {"wdir": "model-2", "cmd": "echo 2 3 20"},
        "build@3": {"wdir": "model-3", "cmd": "echo 3 4 30"},
        "build@4": {"wdir": "model-4", "cmd": "echo 4 5 40"},
    }

    assert resolver.context == {"models": {"model1": {"thresh": "foo"}}}
    assert resolver.tracked_vars == {
        "build@0": {
            os.path.join("model-0", "params.yaml"): {
                "train.epochs": 1,
                "prepare.nums": 0,
            }
        },
        "build@1": {
            os.path.join("model-1", "params.yaml"): {
                "train.epochs": 2,
                "prepare.nums": 10,
            }
        },
        "build@2": {
            os.path.join("model-2", "params.yaml"): {
                "train.epochs": 3,
                "prepare.nums": 20,
            }
        },
        "build@3": {
            os.path.join("model-3", "params.yaml"): {
                "train.epochs": 4,
                "prepare.nums": 30,
            }
        },
        "build@4": {
            os.path.join("model-4", "params.yaml"): {
                "train.epochs": 5,
                "prepare.nums": 40,
            }
        },
    }
    assert resolver.context.imports == {
        str(tmp_dir / DEFAULT_PARAMS_FILE): None
    }


def test_foreach_do_syntax_is_checked_once(tmp_dir, dvc, mocker):
    do_def = {"cmd": "python script.py --epochs ${item}"}
    data = {"foreach": [0, 1, 2, 3, 4], "do": do_def}
    definition = ForeachDefinition(
        DataResolver(dvc, tmp_dir, {}), Context(), "build", data
    )
    mock = mocker.patch("dvc.parsing.check_syntax_errors", return_value=True)
    definition.resolve_all()

    mock.assert_called_once_with(do_def, "build", "dvc.yaml")


def test_foreach_data_is_only_resolved_once(tmp_dir, dvc, mocker):
    context = Context(models=["foo", "bar", "baz"])
    data = {"foreach": "${models}", "do": {}}
    definition = ForeachDefinition(
        DataResolver(dvc, tmp_dir, {}), context, "build", data
    )
    mock = mocker.spy(definition, "_resolve_foreach_data")

    definition.resolve_all()

    mock.assert_called_once_with()
