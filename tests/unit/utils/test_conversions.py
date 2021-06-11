import pytest

from dvc.utils.conversions import human_readable_to_bytes

KB = 1024
MB = KB ** 2
GB = KB ** 3
TB = KB ** 4


@pytest.mark.parametrize(
    "test_input, expected",
    [
        ("10", 10),
        ("10   ", 10),
        ("1kb", 1 * KB),
        ("2kb", 2 * KB),
        ("1000mib", 1000 * MB),
        ("20gB", 20 * GB),
        ("10Tib", 10 * TB),
    ],
)
def test_conversions_human_readable_to_bytes(test_input, expected):
    assert human_readable_to_bytes(test_input) == expected


@pytest.mark.parametrize("invalid_input", ["foo", "10XB", "1000Pb", "fooMiB"])
def test_conversions_human_readable_to_bytes_invalid(invalid_input):
    with pytest.raises(ValueError):
        human_readable_to_bytes(invalid_input)
