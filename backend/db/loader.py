"""Raw JSONL -> SQLite ingestion logic with idempotency."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from backend.config import RAW_DATA_DIR
from backend.db.engine import ensure_metadata_tables, get_connection
from backend.db.schema_mapping import TABLE_SCHEMAS, TableSchema


LOADER_VERSION = "phase1-jsonl-loader-v1"


@dataclass(frozen=True)
class TableLoadResult:
    table_name: str
    status: str
    row_count: int
    source_signature: str
    message: str = ""


@dataclass(frozen=True)
class IngestionReport:
    table_results: tuple[TableLoadResult, ...]
    raw_counts: dict[str, int]
    sqlite_counts: dict[str, int]
    mismatches: dict[str, dict[str, int]]

    @property
    def loaded_tables(self) -> int:
        return sum(1 for result in self.table_results if result.status == "loaded")

    @property
    def skipped_tables(self) -> int:
        return sum(1 for result in self.table_results if result.status == "skipped")


def _quote(identifier: str) -> str:
    return f'"{identifier.replace(chr(34), chr(34) * 2)}"'


def _table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1;",
        (table_name,),
    ).fetchone()
    return row is not None


def _part_files_for_table(table: TableSchema) -> list[Path]:
    table_dir = RAW_DATA_DIR / Path(table.source_dir).name
    return sorted(table_dir.glob("*.jsonl"))


def _compute_source_signature(table: TableSchema) -> str:
    hasher = hashlib.sha256()
    hasher.update(LOADER_VERSION.encode("utf-8"))
    hasher.update(table.name.encode("utf-8"))
    hasher.update("|".join(table.primary_key).encode("utf-8"))

    for file_path in _part_files_for_table(table):
        stat = file_path.stat()
        hasher.update(file_path.name.encode("utf-8"))
        hasher.update(str(stat.st_size).encode("utf-8"))
        hasher.update(str(stat.st_mtime_ns).encode("utf-8"))
    return hasher.hexdigest()


def _normalize_cell(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, ensure_ascii=True)
    return value


def _read_jsonl_rows(table: TableSchema) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for file_path in _part_files_for_table(table):
        with file_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                payload = json.loads(line)
                rows.append({key: _normalize_cell(value) for key, value in payload.items()})
    return rows


def _column_order(rows: list[dict[str, Any]]) -> list[str]:
    ordered_columns: dict[str, None] = {}
    for row in rows:
        for key in row.keys():
            ordered_columns.setdefault(key, None)
    return list(ordered_columns.keys())


def _infer_sqlite_type(values: list[Any]) -> str:
    non_null = [value for value in values if value is not None]
    if not non_null:
        return "TEXT"

    if all(isinstance(value, int) for value in non_null):
        return "INTEGER"
    if all(isinstance(value, (int, float)) for value in non_null):
        return "REAL"
    return "TEXT"


def _create_table(
    connection: sqlite3.Connection,
    table: TableSchema,
    rows: list[dict[str, Any]],
) -> list[str]:
    columns = _column_order(rows)
    if not columns:
        raise ValueError(f"No columns found for table '{table.name}'")

    definitions: list[str] = []
    for column in columns:
        sample_values = [row.get(column) for row in rows]
        sqlite_type = _infer_sqlite_type(sample_values)
        definitions.append(f"{_quote(column)} {sqlite_type}")

    pk_columns = ", ".join(_quote(column) for column in table.primary_key)
    create_sql = (
        f"CREATE TABLE {_quote(table.name)} ("
        + ", ".join(definitions)
        + f", PRIMARY KEY ({pk_columns})"
        + ");"
    )

    connection.execute(f"DROP TABLE IF EXISTS {_quote(table.name)};")
    connection.execute(create_sql)
    return columns


def _insert_rows(
    connection: sqlite3.Connection,
    table_name: str,
    columns: list[str],
    rows: list[dict[str, Any]],
) -> None:
    quoted_columns = ", ".join(_quote(column) for column in columns)
    placeholders = ", ".join(f":{column}" for column in columns)
    sql = (
        f"INSERT INTO {_quote(table_name)} ({quoted_columns}) "
        f"VALUES ({placeholders});"
    )
    connection.executemany(sql, rows)


def _load_table(
    connection: sqlite3.Connection,
    table: TableSchema,
    force: bool = False,
) -> TableLoadResult:
    source_signature = _compute_source_signature(table)
    existing_state = connection.execute(
        "SELECT source_signature FROM ingestion_state WHERE table_name = ?;",
        (table.name,),
    ).fetchone()

    if (
        not force
        and existing_state is not None
        and existing_state["source_signature"] == source_signature
        and _table_exists(connection, table.name)
    ):
        row_count = connection.execute(
            f"SELECT COUNT(*) AS count FROM {_quote(table.name)};"
        ).fetchone()["count"]
        return TableLoadResult(
            table_name=table.name,
            status="skipped",
            row_count=row_count,
            source_signature=source_signature,
            message="No source changes detected.",
        )

    rows = _read_jsonl_rows(table)
    if not rows:
        raise ValueError(f"No input rows found for table '{table.name}'")

    with connection:
        columns = _create_table(connection, table, rows)
        _insert_rows(connection, table.name, columns, rows)
        connection.execute(
            """
            INSERT INTO ingestion_state (table_name, source_signature, row_count, loaded_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(table_name) DO UPDATE SET
                source_signature = excluded.source_signature,
                row_count = excluded.row_count,
                loaded_at = excluded.loaded_at;
            """,
            (
                table.name,
                source_signature,
                len(rows),
                datetime.now(UTC).isoformat(),
            ),
        )

    return TableLoadResult(
        table_name=table.name,
        status="loaded",
        row_count=len(rows),
        source_signature=source_signature,
    )


def raw_table_counts() -> dict[str, int]:
    counts: dict[str, int] = {}
    for table_name, table in sorted(TABLE_SCHEMAS.items()):
        total = 0
        for file_path in _part_files_for_table(table):
            with file_path.open("r", encoding="utf-8") as handle:
                total += sum(1 for line in handle if line.strip())
        counts[table_name] = total
    return counts


def sqlite_table_counts(connection: sqlite3.Connection) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table_name in sorted(TABLE_SCHEMAS):
        if _table_exists(connection, table_name):
            count = connection.execute(
                f"SELECT COUNT(*) AS count FROM {_quote(table_name)};"
            ).fetchone()["count"]
            counts[table_name] = count
        else:
            counts[table_name] = -1
    return counts


def verify_table_counts(connection: sqlite3.Connection) -> dict[str, dict[str, int]]:
    raw_counts = raw_table_counts()
    db_counts = sqlite_table_counts(connection)
    mismatches: dict[str, dict[str, int]] = {}

    for table_name in sorted(TABLE_SCHEMAS):
        raw_count = raw_counts[table_name]
        sqlite_count = db_counts[table_name]
        if raw_count != sqlite_count:
            mismatches[table_name] = {"raw": raw_count, "sqlite": sqlite_count}
    return mismatches


def ingest_raw_jsonl_to_sqlite(
    connection: sqlite3.Connection,
    force: bool = False,
) -> tuple[TableLoadResult, ...]:
    ensure_metadata_tables(connection)
    results: list[TableLoadResult] = []
    for table_name in sorted(TABLE_SCHEMAS):
        table = TABLE_SCHEMAS[table_name]
        result = _load_table(connection, table, force=force)
        results.append(result)
    return tuple(results)


def bootstrap_database(
    connection: sqlite3.Connection | None = None,
    force: bool = False,
) -> IngestionReport:
    """Load raw JSONL data into SQLite and validate final row counts."""
    close_connection = False
    if connection is None:
        connection = get_connection()
        close_connection = True

    try:
        table_results = ingest_raw_jsonl_to_sqlite(connection, force=force)
        raw_counts = raw_table_counts()
        db_counts = sqlite_table_counts(connection)
        mismatches = verify_table_counts(connection)
        return IngestionReport(
            table_results=table_results,
            raw_counts=raw_counts,
            sqlite_counts=db_counts,
            mismatches=mismatches,
        )
    finally:
        if close_connection:
            connection.close()


if __name__ == "__main__":
    report = bootstrap_database()
    print("Ingestion complete")
    print(f"Loaded tables: {report.loaded_tables}")
    print(f"Skipped tables: {report.skipped_tables}")
    if report.mismatches:
        print("Count mismatches detected:")
        for table_name, details in report.mismatches.items():
            print(f"- {table_name}: raw={details['raw']} sqlite={details['sqlite']}")
        raise SystemExit(1)

    print("Table counts verified.")
