"""Unit tests for the central configuration file."""
import copy

import pytest

from vstone_framework.config import load_config, validate_config


def test_config_loads_and_matches_pdf_chunk_plan():
    cfg = load_config()
    plan = {n: (c["format"], c["method"]) for n, c in cfg["chunks"].items()}
    assert plan == {"1": ("csv", "copy_into"), "2": ("csv", "dlt"), "3": ("json", "autoloader"),
                    "4": ("xml", "pyspark_xml")}
    assert cfg["kaggle"]["dataset"] == "zillow/zecon"
    assert set(cfg["metadata_files"]) == {"DataDictionary.csv", "all_available_metrics.json", "fields_per_level.json"}


def test_validate_config_rejects_missing_chunk():
    bad = copy.deepcopy(load_config())
    del bad["chunks"]["4"]
    with pytest.raises(ValueError, match="chunks 1-4"):
        validate_config(bad)


def test_validate_config_rejects_unmapped_level():
    bad = copy.deepcopy(load_config())
    bad["levels"] = bad["levels"] + ["country"]
    with pytest.raises(ValueError, match="one file to each level"):
        validate_config(bad)


def test_defaults_cover_every_notebook_parameter():
    from vstone_framework.runtime import STANDARD_PARAMS
    cfg = load_config()
    assert set(STANDARD_PARAMS) <= set(cfg["defaults"])
    assert sum(cfg["expected_source_rows"].values()) == 10_584_674        # profiled total
    assert set(cfg["schemas"]) == {"landing", "bronze", "silver", "gold", "governance", "ops"}
