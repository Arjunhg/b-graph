from __future__ import annotations
"""Inspect raw JSONL datasets and print a Phase 1 data profile."""

"""Before building a DB or graph, we need to answer questions like:
    - How many rows does each table have?
    - Which columns never have missing values?
    - Do the foreign keys actually match across tables? (e.g., does every delivery actually have a matching billing doc?)

    This script answers all of that. Run it once, read the output, and it tells you if data is clean enough to build on.
"""

import json
from collections import Counter
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    import sys

    sys.path.append(str(Path(__file__).resolve().parents[2]))

from backend.db.schema_mapping import RELATIONSHIPS, TABLE_SCHEMAS, normalize_value


RAW_DATA_DIR = Path("data/raw")


def _read_rows(table_name: str) -> list[dict[str, Any]]:
    table_dir = RAW_DATA_DIR / table_name
    rows: list[dict[str, Any]] = []
    for part_file in sorted(table_dir.glob("*.jsonl")):
        with part_file.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
    return rows


def _value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, ensure_ascii=True)
    return value


def _candidate_single_keys(rows: list[dict[str, Any]], columns: list[str]) -> list[str]:
    """Finds which columns could be a primary key -- i.e., they have no missing values and all values are unique."""
    keys: list[str] = []
    for column in columns:
        values: list[Any] = []
        nullable = False
        """For each column loop through every row. If any value is missing, mark the column as nullable and stop checking. Otherwise, collect the values into a list. The compare length of list with lenght of set. If equal -> no duplicates -> candidate PK."""
        for row in rows:
            value = row.get(column)
            if value is None or value == "":
                nullable = True
                break
            values.append(_value(value))
        if not nullable and len(values) == len(set(values)):
            keys.append(column)
    return keys


def _table_profiles() -> dict[str, dict[str, Any]]:
    """For every table in your schema, builds a full profile dict containing row count, column names, which columns are never null, and candidate PKs."""
    profiles: dict[str, dict[str, Any]] = {}
    for table_name in sorted(TABLE_SCHEMAS):
        rows = _read_rows(table_name)
        columns = sorted({column for row in rows for column in row.keys()})
        """For each column, it counts how many rows have a null or empty string. .get(column) safely returns None if the column doesn't exist in a row (instead of crashing)."""
        null_counts = {
            column: sum(1 for row in rows if row.get(column) in (None, ""))
            for column in columns
        }
        non_null_columns = [column for column, misses in null_counts.items() if misses == 0]
        profiles[table_name] = {
            "rows": rows,
            "row_count": len(rows),
            "column_count": len(columns),
            "columns": columns,
            "non_null_columns": non_null_columns,
            "candidate_single_keys": _candidate_single_keys(rows, columns),
        }
    return profiles


def _distinct_values(
    rows: list[dict[str, Any]],
    columns: tuple[str, ...],
    transforms: tuple[str | None, ...] | None = None,
) -> set[tuple[Any, ...]]:
    """Collects all unique combinations of values for a given set of columns. Used to check FK overlap."""
    transforms = transforms or tuple(None for _ in columns)
    values: set[tuple[Any, ...]] = set()
    for row in rows:
        key: list[Any] = []
        skip = False
        for index, column in enumerate(columns):
            raw = row.get(column)
            if raw is None or raw == "":
                skip = True
                break
            key.append(_value(normalize_value(raw, transforms[index])))
        if not skip:
            values.add(tuple(key))
    return values


def print_profile() -> None:
    profiles = _table_profiles()

    print("Phase 1 data profile (raw JSONL)")
    print("=" * 80)
    for table_name, profile in profiles.items():
        print(
            f"{table_name}: rows={profile['row_count']} "
            f"columns={profile['column_count']} "
            f"pk={TABLE_SCHEMAS[table_name].primary_key}"
        )
        key_preview = ", ".join(profile["candidate_single_keys"][:5]) or "-"
        print(f"  candidate single keys: {key_preview}")
        print(f"  columns: {', '.join(profile['columns'])}")

    print("\nRelationship coverage")
    print("=" * 80)
    for relation in RELATIONSHIPS:
        child_rows = profiles[relation.child_table]["rows"]
        parent_rows = profiles[relation.parent_table]["rows"]

        child_values = _distinct_values(
            child_rows,
            relation.child_columns,
            relation.child_transforms if relation.child_transforms else None,
        )
        parent_values = _distinct_values(
            parent_rows,
            relation.parent_columns,
            relation.parent_transforms if relation.parent_transforms else None,
        )

        overlap = len(child_values & parent_values)
        coverage = (overlap / len(child_values)) if child_values else 0.0
        print(
            f"{relation.child_table}.{relation.child_columns} -> "
            f"{relation.parent_table}.{relation.parent_columns} "
            f"coverage={coverage:.2%} overlap={overlap}/{len(child_values)}"
        )


def print_distribution_examples() -> None:
    """Extra quick sanity checks for key ID columns."""
    profiles = _table_profiles()
    print("\nID distribution samples")
    print("=" * 80)
    for table_name in sorted(TABLE_SCHEMAS):
        rows = profiles[table_name]["rows"]
        id_like_columns = [
            column
            for column in profiles[table_name]["columns"]
            if any(
                token in column.lower()
                for token in (
                    "document",
                    "order",
                    "delivery",
                    "billing",
                    "customer",
                    "partner",
                    "product",
                    "plant",
                    "item",
                )
            )
        ]
        if not id_like_columns:
            continue
        column = id_like_columns[0]
        values = [str(row.get(column)) for row in rows if row.get(column) not in (None, "")]
        top = Counter(values).most_common(3)
        print(f"{table_name}.{column}: top_values={top}")


if __name__ == "__main__":
    print_profile()
    print_distribution_examples()
