"""Bronze-layer configuration and SQL builders (Day 2: COPY INTO)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Sequence

from .audit import AUDIT_COLUMN_COMMENTS, AUDIT_COLUMNS
from .metadata import sql_string


@dataclass(frozen=True)
class BronzeSource:
    entity: str              # zip, city, ..., county_crosswalk
    table: str               # target table name inside the bronze schema
    folder: str              # folder under the chunks Volume
    key_columns: tuple       # natural key used by sanity checks
    description: str


TIME_SERIES_LEVELS = ["state", "metro", "county", "city", "neighborhood", "zip"]

LEVEL_DESCRIPTIONS = {
    "state": "US states (plus a national 'UnitedStates' row)",
    "metro": "metropolitan areas keyed by CBSA code (plus 'United_States')",
    "county": "counties keyed by FIPS code",
    "city": "cities keyed by Zillow city slug",
    "neighborhood": "neighborhoods keyed by Zillow region id",
    "zip": "ZIP codes",
}


def copy_into_sources() -> List[BronzeSource]:
    """Everything loaded by COPY INTO on Day 2: chunk 1 of each level + both crosswalks."""
    sources = [BronzeSource(
        entity=lvl, table=f"{lvl}_ts_csv", folder=f"csv_initial/{lvl}", key_columns=("Date", "RegionName"),
        description=(f"Bronze: raw Zillow monthly housing metrics for {LEVEL_DESCRIPTIONS[lvl]}, "
                     f"chunk 1 (initial load) ingested from CSV with COPY INTO. All source columns kept as STRING."))
        for lvl in TIME_SERIES_LEVELS]
    sources.append(BronzeSource("county_crosswalk", "county_crosswalk", "csv_initial/county_crosswalk", ("FIPS",),
                                "Bronze: Zillow county crosswalk (county, state, metro, CBSA), loaded with COPY INTO."))
    sources.append(BronzeSource("city_crosswalk", "city_crosswalk", "csv_initial/city_crosswalk", ("Unique_City_ID",),
                                "Bronze: Zillow city crosswalk (city slug to city, county, state), loaded with COPY INTO."))
    return sources


CROSSWALK_COLUMNS = {
    "county_crosswalk": ["CountyName", "StateName", "StateFIPS", "CountyFIPS", "MetroName_Zillow", "CBSAName",
                         "CountyRegionID_Zillow", "MetroRegionID_Zillow", "FIPS", "CBSACode"],
    "city_crosswalk": ["Unique_City_ID", "City", "County", "State"],
}


def quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def create_table_ddl(full_table: str, columns: Sequence[str], comments: Dict[str, str],
                     table_comment: str) -> str:
    """CREATE TABLE IF NOT EXISTS with every source column as STRING plus audit columns, all commented."""
    lines = [f"  {quote_ident(c)} STRING COMMENT {sql_string(comments.get(c, c))}" for c in columns]
    lines += [f"  {c} {'TIMESTAMP' if c == 'load_dt' else 'STRING'} COMMENT {sql_string(AUDIT_COLUMN_COMMENTS[c])}"
              for c in AUDIT_COLUMNS]
    return (f"CREATE TABLE IF NOT EXISTS {full_table} (\n" + ",\n".join(lines) + "\n)\n"
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
