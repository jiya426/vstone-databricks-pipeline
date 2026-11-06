"""Notebook runtime context: turns widget values (job + task parameters) into names and paths.

Code parameterization, end to end:

    conf/vstone_config.json  ->  databricks.yml variables  ->  job parameters (all tasks)
                                                           ->  task parameters (one task, e.g. chunk_no)
                             ->  widgets declared at the top of every notebook  ->  get_context()

Every notebook declares its widgets explicitly (the "Parameters" cell), with defaults taken from
conf/vstone_config.json -> "defaults". When the notebook runs as a job task, the job and task
parameters override those defaults. `get_context` then only READS the widgets.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, Optional

from .config import load_config

# Job parameters every job passes (one per bundle variable used for names and paths)
STANDARD_PARAMS = ["catalog", "landing_schema", "bronze_schema", "silver_schema", "gold_schema",
                   "governance_schema", "ops_schema", "raw_volume", "chunks_volume", "env", "run_id"]


def widget_defaults() -> Dict[str, str]:
    """Default value of every widget, from the config file (used only for interactive runs)."""
    return {k: str(v) for k, v in load_config()["defaults"].items() if not k.startswith("_")}


def declare_widgets(dbutils, names: Iterable[str]) -> None:
    """Declare text widgets with config defaults. Notebooks call dbutils.widgets.text directly so the
    parameters are visible in the notebook; this helper exists for the DLT-free test harness."""
    defaults = widget_defaults()
    for name in names:
        dbutils.widgets.text(name, defaults[name])


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
    env: str
    run_id: str
    params: Dict[str, str] = field(default_factory=dict)   # task parameters of this notebook

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

    def ops_table(self, key: str) -> str:
        """Fully qualified ops table from config, e.g. ops_table('run_log') -> vstone.ops.pipeline_run_log"""
        return f"{self.ops}.{load_config()['ops_tables'][key]}"

    # ---- Volume paths ----
    def volume_path(self, volume: str) -> str:
        return f"/Volumes/{self.catalog}/{self.landing_schema}/{volume}"

    @property
    def raw_path(self) -> str: return self.volume_path(self.raw_volume)
    @property
    def chunks_root(self) -> str: return self.volume_path(self.chunks_volume)
    @property
    def checkpoints_root(self) -> str: return self.volume_path(load_config()["volumes"]["checkpoints"])

    def metadata_path(self, key: str) -> str:
        """Path of a metadata file in the raw Volume, e.g. metadata_path('fields_per_level')."""
        return f"{self.raw_path}/{load_config()['metadata'][key]}"

    def param(self, name: str) -> str:
        return self.params[name]

    def int_param(self, name: str) -> int:
        return int(self.params[name])

    def list_param(self, name: str, all_values: Optional[Iterable[str]] = None) -> list:
        """'a,b' -> ['a', 'b']; 'all' -> all_values (e.g. every level from config)."""
        raw = self.params[name].strip()
        if raw.lower() == "all":
            if all_values is None:
                raise ValueError(f"Parameter '{name}' = 'all' needs a list of allowed values")
            return list(all_values)
        values = [v.strip() for v in raw.split(",") if v.strip()]
        if all_values is not None:
            unknown = sorted(set(values) - set(all_values))
            if unknown:
                raise ValueError(f"Parameter '{name}' has unknown values {unknown}; allowed: {list(all_values)}")
        return values

    def bool_param(self, name: str) -> bool:
        return self.params[name].strip().lower() == "true"


def _read(dbutils, name: str, default: str) -> str:
    """Widget value; falls back to the config default when the widget is missing or empty."""
    try:
        value = dbutils.widgets.get(name)
    except Exception:  # noqa: BLE001 - widget not declared (interactive run of a partial notebook)
        value = None
    return default if value in (None, "") else value


def get_context(dbutils, task_params: Optional[Iterable[str]] = None) -> Context:
    """Read the job parameters (+ the named task parameters) from the widgets."""
    defaults = widget_defaults()
    names = list(task_params or [])
    return Context(**{name: _read(dbutils, name, defaults[name]) for name in STANDARD_PARAMS},
                   params={name: _read(dbutils, name, defaults[name]) for name in names})
