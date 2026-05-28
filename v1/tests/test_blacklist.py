from unittest import mock
import pytest
import tempfile
import pandas as pd
from pathlib import Path

import tradepy
from tradepy.blacklist import Blacklist


@pytest.yield_fixture(autouse=True)
def temp_blacklist_file():
    with tempfile.NamedTemporaryFile("w") as f:
        stocks = pd.DataFrame(
            [
                ["000001", "2021-01-01"],
                ["000002", "2030-01-02"],
                ["000003", None],
            ],
            columns=["code", "until"],
        ).set_index("code")
        stocks.to_csv(f.name)

        with mock.patch("tradepy.config.common.blacklist_path", Path(f.name)):
            yield f.name
            Blacklist.purge_cache()


def test_contains_when_blacklist_is_empty():
    with mock.patch("tradepy.config.common.blacklist_path", None):
        assert not Blacklist.contains("000002")


def test_contains_when_code_is_not_blacklisted():
    assert not Blacklist.contains("11111")


def test_contains_when_code_is_blacklisted():
    assert Blacklist.contains("000002")


@pytest.mark.parametrize(
    "timestamp",
    [
        "1900-12-31",
        "2020-12-31",
        "2029-12-31",
        "2030-01-01",
    ],
)
def test_always_contains_when_no_due_date_is_set(timestamp):
    assert Blacklist.contains("000003", timestamp)


def test_contains_when_before_blacklist_before_due_date():
    assert Blacklist.contains("000001", "2020-12-31")


def test_not_contains_when_over_the_blacklist_due_date():
    assert not Blacklist.contains("000001", "2021-12-31")


def test_contains_with_invalid_timestamp_format():
    with pytest.raises(ValueError):
        Blacklist.contains("000001", "invalid-date-format")


def test_read_from_empty_file():
    with tempfile.NamedTemporaryFile("w") as f:
        with mock.patch("tradepy.config.common.blacklist_path", Path(f.name)):
            assert not Blacklist.read()


def test_read_from_nonexistent_file():
    with pytest.raises(FileNotFoundError):
        tradepy.config.common.blacklist_path = Path("/nonexistent/file/path.csv")
