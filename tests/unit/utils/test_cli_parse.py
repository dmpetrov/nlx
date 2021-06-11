from dvc.utils.cli_parse import parse_params


def test_parse_params():
    assert parse_params(
        [
            "param1",
            "file1:param1,param2",
            "file2:param2",
            "file1:param3,param4,",
            "param2,param10",
            "param3,",
            "file3:",
        ]
    ) == [
        {"params.yaml": ["param1", "param2", "param10", "param3"]},
        {"file1": ["param1", "param2", "param3", "param4"]},
        {"file2": ["param2"]},
        {"file3": []},
    ]
