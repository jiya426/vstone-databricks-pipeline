"""Bronze-layer definitions and SQL/schema builders.

Every Bronze table is described once by a `BronzeSource`, generated from conf/vstone_config.json:
    4 chunk files -> 4 time-series tables (COPY INTO, DLT, Auto Loader, PySpark XML)
  + 2 crosswalk files loaded with COPY INTO straight from the raw Volume  = 6 Bronze tables

The 4 time-series tables share one column contract: the combined columns of all 6 levels
(Date, RegionName, RegionLevel, every metric) - see chunking.combined_columns.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence

from pyspark.sql.types import StringType, StructField, StructType

from .audit import AUDIT_COLUMN_COMMENTS, AUDIT_COLUMNS
from .chunking import COMBINED_ENTITY, LEVEL_COL, LEVELS, combined_columns
from .config import load_config
from .metadata import sql_string

_CFG = load_config()
RESCUED_COL: str = _CFG["rescued_data_column"]
CROSSWALK_COLUMNS: Dict[str, List[str]] = {k: list(v) for k, v in _CFG["reference_columns"].items()}
REFERENCE_FILE_OF: Dict[str, str] = {name: f for f, name in _CFG["reference_files"].items()}

METHOD_TEXT = {
    "copy_into": "chunk 1 CSV file loaded with COPY INTO",
    "dlt": "chunk 2 CSV file loaded with a Delta Live Tables streaming table",
    "autoloader": "chunk 3 JSON file loaded with Auto Loader",
    "pyspark_xml": "chunk 4 XML file loaded with PySpark read/write",
}


@dataclass(frozen=True)
class BronzeSource:
    entity: str                # time_series, county_crosswalk, city_crosswalk
    table: str                 # table name inside the bronze schema
    folder: str                # folder under its Volume (chunks Volume, or raw Volume for crosswalks)
    key_columns: tuple         # natural key used by sanity checks
    description: str
    method: str = "copy_into"  # copy_into | dlt | autoloader | pyspark_xml
    chunk_no: int = 0          # 1-4 for time series, 0 for crosswalks (not chunked)
    source: str = "copy_into_csv"   # value written to the audit column `source`
    file_format: str = "csv"
    file_name: str = ""        # the single file this table is loaded from
    has_rescued: bool = False  # True when the table keeps a _rescued_data column

    @property
    def is_reference(self) -> bool:
        return self.entity in CROSSWALK_COLUMNS

    def landing_path(self, chunks_root: str, raw_path: str) -> str:
        """Folder COPY INTO / Auto Loader / DLT / XML read from."""
        root = raw_path if self.is_reference else chunks_root
        return f"{root.rstrip('/')}/{self.folder}".rstrip("/")


def time_series_sources(chunk_nos: Optional[Iterable[int]] = None) -> List[BronzeSource]:
    """The 4 time-series Bronze tables, one per chunk file (optionally only some chunks)."""
    wanted = {int(n) for n in chunk_nos} if chunk_nos is not None else None
    out = []
    for n in sorted(_CFG["chunks"], key=int):
        if wanted is not None and int(n) not in wanted:
            continue
        c = _CFG["chunks"][n]
        out.append(BronzeSource(
            entity=COMBINED_ENTITY, table=c["table"], folder=c["folder"],
            key_columns=tuple(_CFG["time_series_key"]),
            description=(f"Bronze: raw Zillow monthly housing metrics for all {len(LEVELS)} geography levels "
                         f"({', '.join(LEVELS)}; column {LEVEL_COL}), {METHOD_TEXT[c['method']]} "
                         f"({c['file_name']}). All source columns kept as STRING."),
            method=c["method"], chunk_no=int(n), source=c["source"], file_format=c["format"],
            file_name=c["file_name"], has_rescued=c["method"] != "copy_into"))
    return out


def reference_sources() -> List[BronzeSource]:
    """The 2 crosswalk tables, loaded with COPY INTO from the raw Volume (not chunked)."""
    ref = _CFG["reference_load"]
    return [BronzeSource(entity=name, table=name, folder="", key_columns=tuple(_CFG["reference_keys"][name]),
                         description=f"Bronze: Zillow {name.replace('_', ' ')} reference data ({file}), "
                                     f"loaded with COPY INTO from the raw Volume (not chunked).",
                         method=ref["method"], chunk_no=0, source=ref["source"], file_format="csv", file_name=file)
            for file, name in _CFG["reference_files"].items()]


def bronze_sources(methods: Optional[Iterable[str]] = None) -> List[BronzeSource]:
    """All 6 Bronze tables (4 time series + 2 crosswalks), optionally filtered by ingestion method."""
    wanted = set(methods) if methods else None
    return [s for s in time_series_sources() + reference_sources() if wanted is None or s.method in wanted]


def source_for_chunk(chunk_no: int) -> BronzeSource:
    """The Bronze table fed by one chunk file - notebooks pick it from their `chunk_no` task parameter."""
    found = time_series_sources([chunk_no])
    if not found:
        raise ValueError(f"No chunk {chunk_no} in config; valid: {sorted(int(n) for n in _CFG['chunks'])}")
    return found[0]


def require_method(src: BronzeSource, method: str) -> BronzeSource:
    """Guard: a notebook built for one ingestion method must not be given another chunk."""
    if src.method != method:
        raise ValueError(f"chunk {src.chunk_no} is configured for '{src.method}', this notebook does '{method}'")
    return src


def contract_columns(src: BronzeSource, level_fields: Dict[str, List[str]]) -> List[str]:
    """Source columns a Bronze table must hold (before audit columns)."""
    return CROSSWALK_COLUMNS[src.entity] if src.is_reference else combined_columns(level_fields)


def table_columns(src: BronzeSource, level_fields: Dict[str, List[str]]) -> List[str]:
    """Every column of the Bronze table: contract + _rescued_data (if any) + audit columns."""
    return contract_columns(src, level_fields) + ([RESCUED_COL] if src.has_rescued else []) + AUDIT_COLUMNS


def header_ordered_columns(header: Sequence[str], contract: Sequence[str]) -> List[str]:
    """Return the contract columns in the FILE's header order.

    Spark applies an explicit CSV schema by POSITION, not by name. If the file's column order ever differs
    from the contract, the schema must follow the header order - otherwise values silently land in the
    wrong columns. The set of columns must still match the contract.
    """
    header = [h.lstrip("\ufeff") for h in header]
    if sorted(header) != sorted(contract):
        missing = sorted(set(contract) - set(header))
        unexpected = sorted(set(header) - set(contract))
        raise ValueError(f"CSV header does not match contract: missing={missing}, unexpected={unexpected}")
    return list(header)


def first_csv_header(path: str) -> List[str]:
    """Header row of a CSV file, or of the first .csv file in a folder (works on /Volumes paths)."""
    import csv
    import os
    if os.path.isdir(path):
        files = sorted(f for f in os.listdir(path) if f.endswith(".csv"))
        if not files:
            raise FileNotFoundError(f"No .csv files in {path}")
        path = os.path.join(path, files[0])
    with open(path, newline="", encoding="utf-8-sig") as fh:
        return next(csv.reader(fh))


def string_schema(columns: Sequence[str]) -> StructType:
    """Explicit all-STRING schema: Bronze keeps values exactly as delivered."""
    return StructType([StructField(c, StringType(), True) for c in columns])


def quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def column_definitions(columns: Sequence[str], comments: Dict[str, str], rescued: bool = False) -> List[str]:
    lines = [f"{quote_ident(c)} STRING COMMENT {sql_string(comments.get(c, c))}" for c in columns]
    if rescued:
        lines.append(f"{RESCUED_COL} STRING COMMENT "
                     f"{sql_string('Schema validation: JSON/XML/CSV values that did not fit the expected schema')}")
    lines += [f"{c} {'TIMESTAMP' if c == 'load_dt' else 'STRING'} COMMENT {sql_string(AUDIT_COLUMN_COMMENTS[c])}"
              for c in AUDIT_COLUMNS]
    return lines


def schema_ddl(columns: Sequence[str], comments: Dict[str, str], rescued: bool = False) -> str:
    """Column list as a DDL string - used for the DLT table schema (comments included)."""
    return ",\n".join(column_definitions(columns, comments, rescued))


def create_table_ddl(full_table: str, columns: Sequence[str], comments: Dict[str, str],
                     table_comment: str, rescued: bool = False) -> str:
    """CREATE TABLE IF NOT EXISTS with every source column as STRING plus audit columns, all commented."""
    body = ",\n".join("  " + line for line in column_definitions(columns, comments, rescued))
    return (f"CREATE TABLE IF NOT EXISTS {full_table} (\n{body}\n)\n"
            f"USING DELTA\n"
            f"COMMENT {sql_string(table_comment)}")


def copy_into_sql(full_table: str, source_path: str, columns: Sequence[str], source: str,
                  batch_id: str, pattern: str = "*.csv") -> str:
    """COPY INTO that keeps only the contracted columns and adds the audit columns.

    COPY INTO remembers which files it already loaded, so running it again loads 0 new rows.
    """
    select_cols = ",\n    ".join(quote_ident(c) for c in columns)
    return (f"COPY INTO {full_table}\n"
            f"FROM (\n  SELECT\n    {select_cols},\n"
            f"    current_timestamp() AS load_dt,\n"
            f"    {sql_string(source)} AS source,\n"
            f"    _metadata.file_path AS source_file,\n"
            f"    {sql_string(str(batch_id))} AS batch_id\n"
            f"  FROM {sql_string(source_path)}\n)\n"
            f"FILEFORMAT = CSV\n"
            f"PATTERN = {sql_string(pattern)}\n"
            f"FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false', 'mode' = 'PERMISSIVE')\n"
            f"COPY_OPTIONS ('mergeSchema' = 'false')")


def normalize_path(path: str) -> str:
    """'dbfs:/Volumes/a/b.xml' and '/Volumes/a/b.xml' are the same file."""
    return path[len("dbfs:"):] if path.startswith("dbfs:") else path


def new_files(available: Iterable[str], already_loaded: Iterable[str]) -> List[str]:
    """Files present in the landing folder but not yet in the Bronze table (incremental, idempotent)."""
    loaded = {normalize_path(p) for p in already_loaded if p}
    return sorted(p for p in (normalize_path(a) for a in available) if p not in loaded)
