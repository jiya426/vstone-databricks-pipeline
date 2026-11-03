"""Notebook runtime context: one call replaces the widget/path boilerplate in every notebook.

    ctx = get_context(dbutils)                      # standard parameters
    ctx = get_context(dbutils, {"force_rechunk": "false"})   # plus notebook-specific ones

Values come from job parameters (bundle variables) when run as a job, otherwise from the
interactive defaults in conf/vstone_config.json -> "defaults".
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional

from .config import load_config

STANDARD_PARAMS = ["catalog", "landing_schema", "bronze_schema", "silver_schema", "gold_schema",
                   "governance_schema", "ops_schema", "raw_volume", "chunks_volume", "run_id"]


@dataclass(frozen=True)
class Context:
    catalog: str
    landing_schema: str
    bronze_schema: str
    silver_schema: str
    gold_schema: str
    governance_schema: str
    ops_schema: str
    raw_volume: str
    chunks_volume: str
    run_id: str
    params: Dict[str, str] = field(default_factory=dict)   # notebook-specific parameters

    # ---- fully qualified schema names ----
    def fq(self, schema: str) -> str:
        return f"{self.catalog}.{schema}"

    @property
    def landing(self) -> str: return self.fq(self.landing_schema)
    @property
    def bronze(self) -> str: return self.fq(self.bronze_schema)
    @property
    def silver(self) -> str: return self.fq(self.silver_schema)
    @property
    def gold(self) -> str: return self.fq(self.gold_schema)
    @property
    def governance(self) -> str: return self.fq(self.governance_schema)
    @property
    def ops(self) -> str: return self.fq(self.ops_schema)

    # ---- Volume paths ----
    def volume_path(self, volume: str) -> str:
        return f"/Volumes/{self.catalog}/{self.landing_schema}/{volume}"

    @property
    def raw_path(self) -> str: return self.volume_path(self.raw_volume)
    @property
    def chunks_root(self) -> str: return self.volume_path(self.chunks_volume)
    @property
    def checkpoints_root(self) -> str: return self.volume_path(load_config()["volumes"]["checkpoints"])

    def param(self, name: str) -> str:
        return self.params[name]


def get_context(dbutils, extra: Optional[Dict[str, str]] = None) -> Context:
    """Declare the standard widgets (+ `extra` ones) and return their values as a Context."""
    defaults = load_config()["defaults"]
    for name in STANDARD_PARAMS:
        dbutils.widgets.text(name, str(defaults[name]))
    for name, default in (extra or {}).items():
        dbutils.widgets.text(name, str(defaults.get(name, default)))
    get = dbutils.widgets.get
    return Context(**{name: get(name) for name in STANDARD_PARAMS},
                   params={name: get(name) for name in (extra or {})})
