"""FastAPI application entrypoint."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI

from backend.config import REINGEST_ON_STARTUP, SQLITE_DB_PATH
from backend.db.loader import bootstrap_database


@asynccontextmanager
async def lifespan(app: FastAPI):
    report = bootstrap_database(force=REINGEST_ON_STARTUP)
    if report.mismatches:
        raise RuntimeError(f"SQLite row-count mismatch detected: {report.mismatches}")
    app.state.ingestion_report = report
    yield


app = FastAPI(
    title="B Graph API",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health")
def health() -> dict[str, Any]:
    report = getattr(app.state, "ingestion_report", None)
    return {
        "ok": True,
        "database_path": str(SQLITE_DB_PATH),
        "loaded_tables": report.loaded_tables if report else 0,
        "skipped_tables": report.skipped_tables if report else 0,
    }
