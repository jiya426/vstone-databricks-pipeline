"""Loads conf/vstone_config.json - the single place for static pipeline definitions.

Environment-specific values (catalog, schemas, email, cut-off dates) are NOT here: they are
Databricks Asset Bundle variables, passed to notebooks as job parameters (widgets).
"""
from __future__ import annotations

import json
import os
from functools import lru_cache
from typing import Any, Dict

DEFAULT_PATH = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", "conf", "vstone_config.json"))


@lru_cache(maxsize=4)
def load_config(path: str = DEFAULT_PATH) -> Dict[str, Any]:
    with open(path, encoding="utf-8") as fh:
        cfg = json.load(fh)
    validate_config(cfg)
    return cfg


def validate_config(cfg: Dict[str, Any]) -> None:
    required = ["defaults", "schemas", "levels", "time_series_files", "reference_files", "chunks", "xml",
                "rescued_data_column", "expected_source_rows"]
    missing = [k for k in required if k not in cfg]
    if missing:
        raise ValueError(f"Config is missing keys: {missing}")
    if sorted(cfg["chunks"]) != ["1", "2", "3", "4"]:
        raise ValueError("Config must define exactly chunks 1-4")
    if sorted(cfg["time_series_files"].values()) != sorted(cfg["levels"]):
        raise ValueError("time_series_files must map one file to each level")
    if sorted(cfg["expected_source_rows"]) != sorted(cfg["levels"]):
        raise ValueError("expected_source_rows must have one count per level")
