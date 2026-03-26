"""Rule-based chat pipeline for graph-backed API responses."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any

import networkx as nx

from backend.llm.prompts import DOMAIN_ONLY_REFUSAL, IN_SCOPE_KEYWORDS


ID_TOKEN_PATTERN = re.compile(r"\b[A-Za-z0-9]{5,20}\b")

TOPIC_TABLE_HINTS: dict[str, tuple[str, ...]] = {
    "order": ("sales_order_headers", "sales_order_items", "sales_order_schedule_lines"),
    "delivery": ("outbound_delivery_headers", "outbound_delivery_items"),
    "billing": ("billing_document_headers", "billing_document_items"),
    "invoice": ("billing_document_headers", "billing_document_items"),
    "journal": ("journal_entry_items_accounts_receivable",),
    "payment": ("payments_accounts_receivable",),
    "customer": (
        "business_partners",
        "business_partner_addresses",
        "customer_company_assignments",
        "customer_sales_area_assignments",
    ),
    "product": (
        "products",
        "product_descriptions",
        "product_plants",
        "product_storage_locations",
    ),
    "plant": ("plants", "product_plants", "product_storage_locations"),
    "address": ("business_partner_addresses",),
}


@dataclass(frozen=True)
class ChatResult:
    answer: str
    highlighted_node_ids: tuple[str, ...]
    in_scope: bool
    debug: dict[str, Any]


def _extract_id_like_tokens(text: str) -> list[str]:
    tokens = [token for token in ID_TOKEN_PATTERN.findall(text) if any(char.isdigit() for char in token)]
    # Preserve order while deduplicating.
    deduped: dict[str, None] = {}
    for token in tokens:
        deduped.setdefault(token, None)
    return list(deduped.keys())


def _is_in_scope(query: str, graph: nx.MultiDiGraph) -> tuple[bool, list[str]]:
    lower_query = query.lower()
    keyword_match = [keyword for keyword in IN_SCOPE_KEYWORDS if keyword in lower_query]
    if keyword_match:
        return True, keyword_match

    id_tokens = _extract_id_like_tokens(query)
    if not id_tokens:
        return False, []

    matched_by_id = _find_nodes_by_tokens(graph, id_tokens, limit=1)
    return bool(matched_by_id), []


def _find_nodes_by_tokens(
    graph: nx.MultiDiGraph,
    tokens: list[str],
    limit: int,
) -> list[str]:
    if not tokens:
        return []
    matches: list[str] = []
    lowered_tokens = [token.lower() for token in tokens]

    for node_id, attributes in graph.nodes(data=True):
        searchable = f"{node_id} {attributes.get('label', '')}".lower()
        if any(token in searchable for token in lowered_tokens):
            matches.append(node_id)
            if len(matches) >= limit:
                break
    return matches


def _table_hint_matches(
    graph: nx.MultiDiGraph,
    query: str,
    limit: int,
) -> tuple[list[str], list[str]]:
    lower_query = query.lower()
    selected_tables: list[str] = []
    for topic, tables in TOPIC_TABLE_HINTS.items():
        if topic in lower_query:
            for table in tables:
                if table not in selected_tables:
                    selected_tables.append(table)

    if not selected_tables:
        return [], []

    matches: list[str] = []
    for node_id, attributes in graph.nodes(data=True):
        if attributes.get("table") in selected_tables:
            matches.append(node_id)
            if len(matches) >= limit:
                break
    return matches, selected_tables


def _summarize_graph_context(graph: nx.MultiDiGraph) -> str:
    total_nodes = graph.number_of_nodes()
    total_edges = graph.number_of_edges()
    return (
        f"The graph currently has {total_nodes} nodes and {total_edges} relationships. "
        "Share a specific business document ID (order, delivery, billing, journal, or payment) "
        "for a more targeted answer."
    )


def run_chat_query(query: str, graph: nx.MultiDiGraph) -> ChatResult:
    in_scope, matched_keywords = _is_in_scope(query, graph)
    if not in_scope:
        return ChatResult(
            answer=DOMAIN_ONLY_REFUSAL,
            highlighted_node_ids=(),
            in_scope=False,
            debug={"matched_keywords": [], "matched_tokens": [], "selected_tables": []},
        )

    id_tokens = _extract_id_like_tokens(query)
    token_matches = _find_nodes_by_tokens(graph, id_tokens, limit=25)
    table_matches, selected_tables = _table_hint_matches(graph, query, limit=25)

    highlighted: list[str] = []
    for node_id in token_matches + table_matches:
        if node_id not in highlighted:
            highlighted.append(node_id)
        if len(highlighted) >= 25:
            break

    if highlighted:
        preview = ", ".join(highlighted[:5])
        answer = (
            f"I found {len(highlighted)} relevant graph nodes for your query. "
            f"Highlighted examples: {preview}."
        )
    else:
        answer = _summarize_graph_context(graph)

    return ChatResult(
        answer=answer,
        highlighted_node_ids=tuple(highlighted),
        in_scope=True,
        debug={
            "matched_keywords": matched_keywords,
            "matched_tokens": id_tokens,
            "selected_tables": selected_tables,
        },
    )
