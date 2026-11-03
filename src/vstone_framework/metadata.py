"""Business descriptions for tables and columns (PDF: "add descriptions/metadata about
enterprise data to make it more discoverable").

Metric column comments are generated from DataDictionary.csv: a column such as
`MedianListingPricePerSqft_3Bedroom` is split into a measure family
(`MedianListingPricePerSqft`) and a home segment (`3Bedroom`).
"""
from __future__ import annotations

import csv
from typing import Dict, Iterable, Optional

# Column families whose name differs from the DataDictionary term
FAMILY_ALIASES = {
    "Sale_Counts_Seas_Adj": "SalesSeasonallyAdjusted",
    "Sale_Counts": "SalesRaw",
    "Sale_Prices": "MedianSoldPrice",
    "InventoryTierShare": "TierShare",
    "Zri": "ZRI",
}

# Descriptions for columns that are not metrics
FIXED_COMMENTS = {
    "Date": "Month-end date of the observation (string yyyy-MM-dd in Bronze)",
    "RegionName": "Region identifier; meaning depends on level (state name, CBSA code, FIPS, city slug, Zillow neighborhood id, ZIP)",
    # CountyCrossWalk_Zillow.csv
    "CountyName": "County name", "StateName": "Full state name",
    "StateFIPS": "2-digit state FIPS code", "CountyFIPS": "3-digit county FIPS code within the state",
    "MetroName_Zillow": "Metro (MSA) name as used by Zillow", "CBSAName": "Core Based Statistical Area name (US Census)",
    "CountyRegionID_Zillow": "Zillow internal region id of the county",
    "MetroRegionID_Zillow": "Zillow internal region id of the metro",
    "FIPS": "5-digit county FIPS code (state + county); business key of the county",
    "CBSACode": "5-digit CBSA code; matches RegionName in Metro_time_series",
    # cities_crosswalk.csv
    "Unique_City_ID": "City slug (city + county + state); matches RegionName in City_time_series",
    "City": "City name", "County": "County name of the city", "State": "2-letter state code",
}


def load_data_dictionary(path: str, encoding: str = "cp1252") -> Dict[str, str]:
    """Read DataDictionary.csv into {term: definition}. The file is Windows-1252 encoded."""
    with open(path, encoding=encoding, newline="") as fh:
        return {row["Variable"].strip(): " ".join(row["Definition"].split()) for row in csv.DictReader(fh)}


def _match_family(column: str, dictionary: Dict[str, str]) -> Optional[tuple]:
    for alias, term in FAMILY_ALIASES.items():                 # longest aliases listed first
        if column == alias or column.startswith(alias + "_"):
            return term, column[len(alias):].lstrip("_")
    lower = {k.lower(): k for k in dictionary}
    parts = column.split("_")
    for i in range(len(parts), 0, -1):                        # longest prefix that is a known term
        prefix = "_".join(parts[:i])
        if prefix.lower() in lower:
            return lower[prefix.lower()], "_".join(parts[i:])
    return None


def column_comment(column: str, dictionary: Dict[str, str], max_len: int = 1000) -> str:
    if column in FIXED_COMMENTS:
        text = FIXED_COMMENTS[column]
    else:
        match = _match_family(column, dictionary)
        if match is None:
            text = f"Zillow metric {column}"
        else:
            term, segment = match
            text = dictionary[term]
            if segment:
                text = f"{text} Segment: {segment}."
    return text[:max_len]


def column_comments(columns: Iterable[str], dictionary: Dict[str, str]) -> Dict[str, str]:
    return {c: column_comment(c, dictionary) for c in columns}


def sql_string(text: str) -> str:
    """Quote a Python string as a SQL string literal."""
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"
