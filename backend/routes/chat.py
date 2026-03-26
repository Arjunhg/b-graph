"""Chat/query endpoint with domain guardrail and graph highlights."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from backend.llm.pipeline import run_chat_query


router = APIRouter()


class ChatRequest(BaseModel):
    query: str = Field(min_length=1, max_length=2000)


class ChatResponse(BaseModel):
    answer: str
    highlighted_node_ids: list[str]
    traced_path: list[str]
    in_scope: bool
    debug: dict[str, Any]


@router.post("/chat", response_model=ChatResponse)
def chat(request: Request, payload: ChatRequest) -> ChatResponse:
    graph = getattr(request.app.state, "graph", None)
    if graph is None:
        raise HTTPException(status_code=503, detail="Graph is not initialized.")

    result = run_chat_query(payload.query, graph)
    return ChatResponse(
        answer=result.answer,
        highlighted_node_ids=list(result.highlighted_node_ids),
        traced_path=list(result.traced_path),
        in_scope=result.in_scope,
        debug=result.debug,
    )
