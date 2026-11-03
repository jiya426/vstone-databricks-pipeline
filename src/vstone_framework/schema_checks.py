"""Header validation against the expected-fields contract (fields_per_level.json)."""
from __future__ import annotations

from typing import Dict, Iterable, List


def compare_columns(expected: Iterable[str], actual: Iterable[str]) -> Dict[str, List[str]]:
    exp, act = set(expected), set(actual)
    return {"missing": sorted(exp - act), "unexpected": sorted(act - exp)}


def assert_columns_match(level: str, expected: Iterable[str], actual: Iterable[str]) -> None:
    diff = compare_columns(expected, actual)
    if diff["missing"] or diff["unexpected"]:
        raise ValueError(
            f"Schema check failed for {level}: missing={diff['missing']}, unexpected={diff['unexpected']}"
        )
