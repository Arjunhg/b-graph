"""Environment variables and project-level constants."""

from __future__ import annotations

import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover - fallback for non-synced envs
    def load_dotenv() -> bool:
        return False


load_dotenv()

ROOT_DIR = Path(__file__).resolve().parents[1]
RAW_DATA_DIR = ROOT_DIR / "data" / "raw"
SQLITE_DIR = ROOT_DIR / "data" / "sqlite"


def _resolve_path(path_value: str) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    return ROOT_DIR / path


SQLITE_DB_PATH = _resolve_path(
    os.getenv("DATABASE_PATH", str(SQLITE_DIR / "b_graph.db"))
)
REINGEST_ON_STARTUP = os.getenv("REINGEST_ON_STARTUP", "false").lower() in {
    "1",
    "true",
    "yes",
}
