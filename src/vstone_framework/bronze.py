"""Bronze-layer definitions and SQL/schema builders.

Every Bronze table is described once by a `BronzeSource`, generated from conf/vstone_config.json:
    6 levels x 4 chunk methods (copy_into, dlt, autoloader, pyspark_xml) = 24 time-series tables
  + 2 crosswalk tables (copy_into)                                       = 26 Bronze tables
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence

from pyspark.sql.types import StringType, StructField, StructType

from .audit import AUDIT_COLUMN_COMMENTS, AUDIT_COLUMNS
from .config import load_config
from .metadata import sql_string

_CFG = load_config()
RESCUED_COL: str = _CFG["rescued_data_column"]
TIME_SERIES_LEVELS: List[str] = list(_CFG["levels"])
CROSSWALK_COLUMNS: Dict[str, List[str]] = {k: list(v) for k, v in _CFG["reference_columns"].items()}

LEVEL_DESCRIPTIONS = {
    "state": "US states (plus a national 'UnitedStates' row)",
    "metro": "metropolitan areas keyed by CBSA code (plus 'United_States')",
    "county": "counties keyed by FIPS code",
    "city": "cities keyed by Zillow city slug",
    "neighborhood": "neighborhoods keyed by Zillow region id",
    "zip": "ZIP codes",
}
METHOD_TEXT = {
    "copy_into": "chunk 1 (initial load) from CSV with COPY INTO",
    "dlt": "chunk 2 (incremental) from CSV with a Delta Live Tables streaming table",
    "autoloader": "chunk 3 from JSON with Auto Loader",
    "pyspark_xml": "chunk 4 from XML with PySpark read/write",
}


@dataclass(frozen=True)
class BronzeSource:
    entity: str                # state ... zip, county_crosswalk, city_crosswalk
    table: str                 # table name inside the bronze schema
    folder: str                # folder under the chunks Volume
    key_columns: tuple         # natural key used by sanity checks
    description: str
    method: str = "copy_into"  # copy_into | dlt | autoloader | pyspark_xml
    chunk_no: int = 1
    source: str = "copy_into_csv"   # value written to the audit column `source`
    file_format: str = "csv"
    has_rescued: bool = False  # True when the table keeps a _rescued_data column

    @property
    def is_reference(self) -> bool:
        return self.entity in CROSSWALK_COLUMNS


def bronze_sources(methods: Optional[Iterable[str]] = None) -> List[BronzeSource]:
    """All Bronze tables, optionally filtered by ingestion method, in a stable order."""
    wanted = set(methods) if methods else None
    out: List[BronzeSource] = []
    for n in sorted(_CFG["chunks"], key=int):
        c = _CFG["chunks"][n]
        if wanted and c["method"] not in wanted:
            continue
        for lvl in TIME_SERIES_LEVELS:
            out.append(BronzeSource(
                entity=lvl, table=f"{lvl}_{c['table_suffix']}", folder=f"{c['folder']}/{lvl}",
                key_columns=tuple(_CFG["time_series_key"]),
                description=(f"Bronze: raw Zillow monthly housing metrics for {LEVEL_DESCRIPTIONS[lvl]}, "
                             f"{METHOD_TEXT[c['method']]}. All source columns kept as STRING."),
                method=c["method"], chunk_no=int(n), source=c["source"], file_format=c["format"],
                has_rescued=c["method"] != "copy_into"))
        if c["method"] == "copy_into":
            for ref, cols in CROSSWALK_COLUMNS.items():
                out.append(BronzeSource(
                    entity=ref, table=ref, folder=f"{c['folder']}/{ref}", key_columns=tuple(_CFG["reference_keys"][ref]),
                    description=(f"Bronze: Zillow {ref.replace('_', ' ')} reference data, loaded with COPY INTO."),
                    method="copy_into", chunk_no=int(n), source=c["source"], file_format="csv"))
    return out


def copy_into_sources() -> List[BronzeSource]:
    return bronze_sources(["copy_into"])


def contract_columns(src: BronzeSource, level_fields: Dict[str, List[str]]) -> List[str]:
    """Source columns a Bronze table must hold (before audit columns)."""
    return CROSSWALK_COLUMNS[src.entity] if src.is_reference else list(level_fields[src.entity])


def table_columns(src: BronzeSource, level_fields: Dict[str, List[str]]) -> List[str]:
    """Every column of the Bronze table: contract + _rescued_data (if any) + audit columns."""
    return contract_columns(src, level_fields) + ([RESCUED_COL] if src.has_rescued else []) + AUDIT_COLUMNS


def header_ordered_columns(header: Sequence[str], contract: Sequence[str]) -> List[str]:
    """Return the contract columns in the FILE's header order.

    Spark applies an explicit CSV schema by POSITION, not by name. fields_per_level.json lists the
    columns in a different order from the CSV header, so the schema must follow the header order -
    otherwise values silently land in the wrong columns. The set of columns must still match the contract.
    """
    header = [h.lstrip("\ufeff") for h in header]
    if sorted(header) != sorted(contract):
        missing = sorted(set(contract) - set(header))
        unexpected = sorted(set(header) - set(contract))
        raise ValueError(f"CSV header does not match contract: missing={missing}, unexpected={unexpected}")
    return list(header)


def first_csv_header(folder: str) -> List[str]:
    """Header row of the first .csv file in a folder (works on /Volumes paths via the local file API)."""
    import csv
    import os
    files = sorted(f for f in os.listdir(folder) if f.endswith(".csv"))
    if not files:
        raise FileNotFoundError(f"No .csv files in {folder}")
    with open(os.path.join(folder, files[0]), newline="", encoding="utf-8-sig") as fh:
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
