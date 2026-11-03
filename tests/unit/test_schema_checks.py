"""Unit tests for header validation against fields_per_level.json."""
import pytest

from vstone_framework.schema_checks import assert_columns_match, compare_columns


def test_compare_columns_reports_both_directions():
    assert compare_columns(["Date", "RegionName", "ZHVI_AllHomes"], ["Date", "RegionName", "Extra"]) == {
        "missing": ["ZHVI_AllHomes"], "unexpected": ["Extra"]}


def test_assert_columns_match_passes_when_equal_in_any_order():
    assert_columns_match("zip", ["Date", "RegionName"], ["RegionName", "Date"])


def test_assert_columns_match_fails_with_clear_message():
    with pytest.raises(ValueError, match="Schema check failed for zip"):
        assert_columns_match("zip", ["Date", "RegionName"], ["Date"])
