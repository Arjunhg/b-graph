"""FastAPI application entrypoint."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.config import REINGEST_ON_STARTUP, SQLITE_DB_PATH
from backend.db.loader import bootstrap_database
from backend.graph.builder import build_graph
from backend.routes import chat_router, graph_router

# Comma-separated list of allowed origins, e.g.:
#   ALLOWED_ORIGINS="https://your-app.vercel.app,https://your-custom-domain.com"
# Defaults to localhost for local development.
_raw_origins = os.getenv("ALLOWED_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173")
ALLOWED_ORIGINS = [o.strip() for o in _raw_origins.split(",") if o.strip()]


@asynccontextmanager
async def lifespan(app: FastAPI):
    report = bootstrap_database(force=REINGEST_ON_STARTUP)
    if report.mismatches:
        raise RuntimeError(f"SQLite row-count mismatch detected: {report.mismatches}")
    app.state.ingestion_report = report
    graph, graph_report = build_graph()
    if not graph_report.flow_validation.is_traceable:
        raise RuntimeError(graph_report.flow_validation.message)
    app.state.graph = graph
    app.state.graph_report = graph_report
    yield


app = FastAPI(
    title="B Graph API",
    version="0.1.0",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(graph_router, prefix="/api/graph", tags=["graph"])
app.include_router(chat_router, prefix="/api", tags=["chat"])



@app.get("/health")
def health() -> dict[str, Any]:
    report = getattr(app.state, "ingestion_report", None)
    graph_report = getattr(app.state, "graph_report", None)
    return {
        "ok": True,
        "database_path": str(SQLITE_DB_PATH),
        "loaded_tables": report.loaded_tables if report else 0,
        "skipped_tables": report.skipped_tables if report else 0,
        "graph_nodes": graph_report.node_count if graph_report else 0,
        "graph_edges": graph_report.edge_count if graph_report else 0,
        "flow_traceable": graph_report.flow_validation.is_traceable if graph_report else False,
        "traceable_o2c_paths": (
            graph_report.flow_validation.traceable_path_count if graph_report else 0
        ),
    }
