"""Unity Catalog helpers: tags and comments (discoverability)."""
from __future__ import annotations

from typing import Dict

from .metadata import sql_string


def set_tags(spark, full_table: str, tags: Dict[str, str]) -> bool:
    """Apply table tags; returns False (with a warning) if tags are not allowed, never fails the load."""
    pairs = ", ".join(f"{sql_string(k)} = {sql_string(v)}" for k, v in tags.items())
    try:
        spark.sql(f"ALTER TABLE {full_table} SET TAGS ({pairs})")
        return True
    except Exception as e:  # noqa: BLE001 - tags are optional metadata
        print(f"WARN: could not set tags on {full_table}: {e}")
        return False


def comment_table(spark, full_table: str, comment: str) -> None:
    spark.sql(f"COMMENT ON TABLE {full_table} IS {sql_string(comment)}")


def load_level_fields(path: str) -> Dict[str, list]:
    """fields_per_level.json -> {"zip": [...], "city": [...], ...}"""
    import json
    with open(path, encoding="utf-8") as fh:
        return {k.lower(): v for k, v in json.load(fh).items()}
