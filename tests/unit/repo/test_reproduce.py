import mock


@mock.patch("dvc.repo.reproduce._reproduce_stage", returns=[])
def test_number_reproduces(reproduce_stage_mock, tmp_dir, dvc):
    tmp_dir.dvc_gen({"pre-foo": "pre-foo"})

    dvc.run(
        single_stage=True, deps=["pre-foo"], outs=["foo"], cmd="echo foo > foo"
    )
    dvc.run(
        single_stage=True, deps=["foo"], outs=["bar"], cmd="echo bar > bar"
    )
    dvc.run(
        single_stage=True, deps=["foo"], outs=["baz"], cmd="echo baz > baz"
    )
    dvc.run(
        single_stage=True, deps=["bar"], outs=["boop"], cmd="echo boop > boop"
    )

    reproduce_stage_mock.reset_mock()

    dvc.reproduce(all_pipelines=True)

    assert reproduce_stage_mock.call_count == 5
