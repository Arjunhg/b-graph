"""SQLite connection and database bootstrap helpers."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from backend.config import SQLITE_DB_PATH


def ensure_sqlite_dir(db_path: Path | None = None) -> Path:
    """Ensure the SQLite directory exists and return the resolved db path."""
    resolved = db_path or SQLITE_DB_PATH
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return resolved


def get_connection(db_path: Path | None = None) -> sqlite3.Connection:
    """Create a SQLite connection with practical defaults."""
    resolved = ensure_sqlite_dir(db_path)
    connection = sqlite3.connect(resolved)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON;")
    connection.execute("PRAGMA journal_mode = WAL;")
    connection.execute("PRAGMA synchronous = NORMAL;")
    return connection


def ensure_metadata_tables(connection: sqlite3.Connection) -> None:
    """Create metadata tables used by idempotent ingestion."""
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS ingestion_state (
            table_name TEXT PRIMARY KEY,
            source_signature TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            loaded_at TEXT NOT NULL
        );
        """
    )
    connection.commit()
