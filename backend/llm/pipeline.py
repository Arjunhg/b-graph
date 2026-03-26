"""SQL-backed chat pipeline with validation, retry, and grounded answers."""

from __future__ import annotations

from dataclasses import dataclass
import json
import re
import sqlite3
from typing import Any

import networkx as nx

from backend.db.engine import get_connection
from backend.db.schema_mapping import TABLE_SCHEMAS
from backend.llm.client import generate_completion, is_llm_available
from backend.llm.prompts import (
    ANSWER_SYNTHESIS_SYSTEM_PROMPT,
    DOMAIN_ONLY_REFUSAL,
    IN_SCOPE_KEYWORDS,
    SQL_GENERATION_SYSTEM_PROMPT,
    render_schema_prompt,
)


ID_TOKEN_PATTERN = re.compile(r"\b[A-Za-z0-9]{5,20}\b")
FORBIDDEN_SQL_PATTERN = re.compile(
    r"\b(insert|update|delete|drop|alter|create|attach|detach|pragma|replace|vacuum|truncate)\b",
    re.IGNORECASE,
)
TABLE_REF_PATTERN = re.compile(r"\b(?:from|join)\s+([A-Za-z_][A-Za-z0-9_]*)", re.IGNORECASE)

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
    traced_path: tuple[str, ...]  # ordered sequence for animation, empty if not a flow query
    in_scope: bool
    debug: dict[str, Any]


@dataclass(frozen=True)
class SQLCandidate:
    sql: str
    interpretation: str
    note: str
    source: str


@dataclass(frozen=True)
class SQLExecutionResult:
    sql: str
    columns: tuple[str, ...]
    rows: tuple[dict[str, Any], ...]


def _extract_id_like_tokens(text: str) -> list[str]:
    tokens = [token for token in ID_TOKEN_PATTERN.findall(text) if any(char.isdigit() for char in token)]
    deduped: dict[str, None] = {}
    for token in tokens:
        deduped.setdefault(token, None)
    return list(deduped.keys())


def _find_nodes_by_tokens(graph: nx.MultiDiGraph, tokens: list[str], limit: int) -> list[str]:
    if not tokens:
        return []
    lowered_tokens = [token.lower() for token in tokens]
    matches: list[str] = []
    for node_id, attributes in graph.nodes(data=True):
        searchable = f"{node_id} {attributes.get('label', '')}".lower()
        if any(token in searchable for token in lowered_tokens):
            matches.append(node_id)
            if len(matches) >= limit:
                break
    return matches


def _is_in_scope(query: str, graph: nx.MultiDiGraph) -> tuple[bool, list[str]]:
    lower_query = query.lower()
    keyword_match = [keyword for keyword in IN_SCOPE_KEYWORDS if keyword in lower_query]
    if keyword_match:
        return True, keyword_match

    id_tokens = _extract_id_like_tokens(query)
    if not id_tokens:
        return False, []
    return bool(_find_nodes_by_tokens(graph, id_tokens, limit=1)), []


def _infer_identifier_focus(query: str, graph: nx.MultiDiGraph) -> tuple[str | None, str | None]:
    lower_query = query.lower()
    id_tokens = _extract_id_like_tokens(query)
    primary_id = id_tokens[0] if id_tokens else None

    for focus in ("order", "delivery", "billing", "invoice", "journal", "payment", "customer", "product", "plant"):
        if focus in lower_query:
            return focus, primary_id

    if primary_id:
        matched_nodes = _find_nodes_by_tokens(graph, [primary_id], limit=10)
        matched_tables = {
            graph.nodes[node_id]["table"]
            for node_id in matched_nodes
        }
        table_to_focus = {
            "sales_order_headers": "order",
            "sales_order_items": "order",
            "outbound_delivery_headers": "delivery",
            "outbound_delivery_items": "delivery",
            "billing_document_headers": "billing",
            "billing_document_items": "billing",
            "journal_entry_items_accounts_receivable": "journal",
            "payments_accounts_receivable": "payment",
            "business_partners": "customer",
            "products": "product",
            "plants": "plant",
        }
        for table_name in matched_tables:
            focus = table_to_focus.get(table_name)
            if focus:
                return focus, primary_id

        if primary_id.isdigit():
            if len(primary_id) == 6:
                return "order", primary_id
            if len(primary_id) == 8:
                return "billing", primary_id
            if len(primary_id) >= 10:
                return "payment", primary_id

    return None, primary_id


def _schema_by_table(connection: sqlite3.Connection) -> dict[str, list[str]]:
    schema: dict[str, list[str]] = {}
    for table_name in sorted(TABLE_SCHEMAS):
        rows = connection.execute(f'PRAGMA table_info("{table_name}");').fetchall()
        schema[table_name] = [row[1] for row in rows]
    return schema


def _clean_sql(raw_sql: str) -> str:
    sql = raw_sql.strip()
    sql = sql.removeprefix("```sql").removeprefix("```").removesuffix("```").strip()
    if ";" in sql:
        sql = sql.rstrip(";").strip()
    return sql


def _validate_sql(sql: str, connection: sqlite3.Connection, schema_by_table: dict[str, list[str]]) -> tuple[bool, str]:
    cleaned = _clean_sql(sql)
    upper_sql = cleaned.upper()

    if not cleaned:
        return False, "SQL is empty."
    if FORBIDDEN_SQL_PATTERN.search(cleaned):
        return False, "SQL contains forbidden write or schema-changing statements."
    if not (upper_sql.startswith("SELECT") or upper_sql.startswith("WITH")):
        return False, "Only SELECT queries are allowed."

    referenced_tables = {
        match.group(1)
        for match in TABLE_REF_PATTERN.finditer(cleaned)
    }
    unknown_tables = sorted(referenced_tables - set(schema_by_table))
    if unknown_tables:
        return False, f"SQL references unknown tables: {', '.join(unknown_tables)}"

    try:
        connection.execute(f"EXPLAIN QUERY PLAN {cleaned}")
    except sqlite3.Error as exc:
        return False, f"SQLite validation failed: {exc}"
    return True, "VALID"


def _candidate_count_query(topic: str) -> SQLCandidate:
    topic_to_table = {
        "order": ("sales_order_headers", "salesOrderCount"),
        "delivery": ("outbound_delivery_headers", "deliveryCount"),
        "billing": ("billing_document_headers", "billingDocumentCount"),
        "invoice": ("billing_document_headers", "billingDocumentCount"),
        "journal": ("journal_entry_items_accounts_receivable", "journalEntryCount"),
        "payment": ("payments_accounts_receivable", "paymentCount"),
        "customer": ("business_partners", "customerCount"),
        "product": ("products", "productCount"),
        "plant": ("plants", "plantCount"),
    }
    table_name, alias = topic_to_table[topic]
    return SQLCandidate(
        sql=f"SELECT COUNT(*) AS {alias} FROM {table_name}",
        interpretation=f"Count records for {topic}.",
        note="Aggregate count query.",
        source="deterministic",
    )


def _candidate_order_flow(order_id: str) -> SQLCandidate:
    return SQLCandidate(
        sql=f"""
            SELECT
                soh.salesOrder,
                soi.salesOrderItem,
                soh.soldToParty,
                soi.material,
                soi.requestedQuantity,
                odi.deliveryDocument,
                odi.deliveryDocumentItem,
                bdi.billingDocument,
                bdi.billingDocumentItem,
                je.accountingDocument AS journalAccountingDocument,
                p.accountingDocument AS paymentAccountingDocument,
                p.clearingDate,
                p.amountInTransactionCurrency AS paymentAmount
            FROM sales_order_headers AS soh
            LEFT JOIN sales_order_items AS soi
                ON soi.salesOrder = soh.salesOrder
            LEFT JOIN outbound_delivery_items AS odi
                ON odi.referenceSdDocument = soi.salesOrder
            AND ltrim(odi.referenceSdDocumentItem, '0') = soi.salesOrderItem
            LEFT JOIN billing_document_items AS bdi
                ON bdi.referenceSdDocument = odi.deliveryDocument
            AND bdi.referenceSdDocumentItem = ltrim(odi.deliveryDocumentItem, '0')
            LEFT JOIN journal_entry_items_accounts_receivable AS je
                ON je.referenceDocument = bdi.billingDocument
            LEFT JOIN payments_accounts_receivable AS p
                ON p.accountingDocument = je.accountingDocument
            WHERE soh.salesOrder = '{order_id}'
            ORDER BY CAST(soi.salesOrderItem AS INTEGER), odi.deliveryDocument, bdi.billingDocument
            LIMIT 100
        """.strip(),
        interpretation=f"Trace the Order-to-Cash flow for sales order {order_id}.",
        note="Flow query from sales order to payment.",
        source="deterministic",
    )


def _candidate_delivery_flow(delivery_id: str) -> SQLCandidate:
    return SQLCandidate(
        sql=f"""
            SELECT
                odi.deliveryDocument,
                odi.deliveryDocumentItem,
                odi.referenceSdDocument AS salesOrder,
                ltrim(odi.referenceSdDocumentItem, '0') AS salesOrderItem,
                bdi.billingDocument,
                je.accountingDocument AS journalAccountingDocument,
                p.accountingDocument AS paymentAccountingDocument,
                p.clearingDate
            FROM outbound_delivery_items AS odi
            LEFT JOIN billing_document_items AS bdi
                ON bdi.referenceSdDocument = odi.deliveryDocument
            AND bdi.referenceSdDocumentItem = ltrim(odi.deliveryDocumentItem, '0')
            LEFT JOIN journal_entry_items_accounts_receivable AS je
                ON je.referenceDocument = bdi.billingDocument
            LEFT JOIN payments_accounts_receivable AS p
                ON p.accountingDocument = je.accountingDocument
            WHERE odi.deliveryDocument = '{delivery_id}'
            ORDER BY CAST(ltrim(odi.deliveryDocumentItem, '0') AS INTEGER)
            LIMIT 100
        """.strip(),
        interpretation=f"Trace downstream records for delivery {delivery_id}.",
        note="Flow query from delivery to billing, journal, and payment.",
        source="deterministic",
    )


def _candidate_billing_flow(billing_id: str) -> SQLCandidate:
    return SQLCandidate(
        sql=f"""
                SELECT
                    bdh.billingDocument,
                    bdi.billingDocumentItem,
                    bdi.referenceSdDocument AS deliveryDocument,
                    odi.referenceSdDocument AS salesOrder,
                    ltrim(odi.referenceSdDocumentItem, '0') AS salesOrderItem,
                    je.accountingDocument AS journalAccountingDocument,
                    p.accountingDocument AS paymentAccountingDocument,
                    p.clearingDate,
                    p.amountInTransactionCurrency AS paymentAmount
                FROM billing_document_headers AS bdh
                LEFT JOIN billing_document_items AS bdi
                    ON bdi.billingDocument = bdh.billingDocument
                LEFT JOIN outbound_delivery_items AS odi
                    ON odi.deliveryDocument = bdi.referenceSdDocument
                AND ltrim(odi.deliveryDocumentItem, '0') = bdi.referenceSdDocumentItem
                LEFT JOIN journal_entry_items_accounts_receivable AS je
                    ON je.referenceDocument = bdh.billingDocument
                LEFT JOIN payments_accounts_receivable AS p
                    ON p.accountingDocument = je.accountingDocument
                WHERE bdh.billingDocument = '{billing_id}'
                ORDER BY CAST(bdi.billingDocumentItem AS INTEGER)
                LIMIT 100
            """.strip(),
        interpretation=f"Trace upstream and downstream records for billing document {billing_id}.",
        note="Flow query from billing to related order, journal, and payment.",
        source="deterministic",
    )


def _candidate_payment_flow(accounting_id: str) -> SQLCandidate:
    return SQLCandidate(
        sql=f"""
                SELECT
                    p.accountingDocument AS paymentAccountingDocument,
                    p.customer,
                    p.clearingDate,
                    p.amountInTransactionCurrency AS paymentAmount,
                    je.referenceDocument AS billingDocument,
                    bdi.referenceSdDocument AS deliveryDocument,
                    odi.referenceSdDocument AS salesOrder,
                    ltrim(odi.referenceSdDocumentItem, '0') AS salesOrderItem
                FROM payments_accounts_receivable AS p
                LEFT JOIN journal_entry_items_accounts_receivable AS je
                    ON je.accountingDocument = p.accountingDocument
                LEFT JOIN billing_document_items AS bdi
                    ON bdi.billingDocument = je.referenceDocument
                LEFT JOIN outbound_delivery_items AS odi
                    ON odi.deliveryDocument = bdi.referenceSdDocument
                AND ltrim(odi.deliveryDocumentItem, '0') = bdi.referenceSdDocumentItem
                WHERE p.accountingDocument = '{accounting_id}'
                ORDER BY salesOrder, deliveryDocument, billingDocument
                LIMIT 100
            """.strip(),
        interpretation=f"Trace upstream records for payment/journal accounting document {accounting_id}.",
        note="Flow query from payment back to billing and order.",
        source="deterministic",
    )


def _candidate_customer_summary(customer_id: str) -> SQLCandidate:
    return SQLCandidate(
        sql=f"""
                SELECT
                    bp.businessPartner,
                    bp.businessPartnerName,
                    COUNT(DISTINCT soh.salesOrder) AS salesOrderCount,
                    COUNT(DISTINCT bdh.billingDocument) AS billingDocumentCount,
                    COUNT(DISTINCT p.accountingDocument) AS paymentCount
                FROM business_partners AS bp
                LEFT JOIN sales_order_headers AS soh
                    ON soh.soldToParty = bp.businessPartner
                LEFT JOIN billing_document_headers AS bdh
                    ON bdh.soldToParty = bp.businessPartner
                LEFT JOIN payments_accounts_receivable AS p
                    ON p.customer = bp.businessPartner
                WHERE bp.businessPartner = '{customer_id}'
                GROUP BY bp.businessPartner, bp.businessPartnerName
                LIMIT 10
            """.strip(),
        interpretation=f"Summarize business activity for customer {customer_id}.",
        note="Customer summary query.",
        source="deterministic",
    )


def _candidate_product_summary(product_id: str) -> SQLCandidate:
    return SQLCandidate(
        sql=f"""
                SELECT
                    p.product,
                    pd.productDescription,
                    COUNT(DISTINCT soi.salesOrder) AS salesOrderCount,
                    COUNT(DISTINCT bdi.billingDocument) AS billingDocumentCount
                FROM products AS p
                LEFT JOIN product_descriptions AS pd
                    ON pd.product = p.product
                LEFT JOIN sales_order_items AS soi
                    ON soi.material = p.product
                LEFT JOIN billing_document_items AS bdi
                    ON bdi.material = p.product
                WHERE p.product = '{product_id}'
                GROUP BY p.product, pd.productDescription
                LIMIT 10
            """.strip(),
        interpretation=f"Summarize business activity for product {product_id}.",
        note="Product summary query.",
        source="deterministic",
    )


def _candidate_topic_preview(topic: str) -> SQLCandidate | None:
    topic_to_query = {
        "order": SQLCandidate(
            sql="""
                SELECT salesOrder, soldToParty, totalNetAmount, transactionCurrency
                FROM sales_order_headers
                ORDER BY salesOrder DESC
                LIMIT 25
            """.strip(),
            interpretation="Show recent sales order records.",
            note="Preview query for sales orders.",
            source="deterministic",
        ),
        "delivery": SQLCandidate(
            sql="""
                SELECT deliveryDocument, shippingPoint, overallGoodsMovementStatus, overallPickingStatus
                FROM outbound_delivery_headers
                ORDER BY deliveryDocument DESC
                LIMIT 25
            """.strip(),
            interpretation="Show recent delivery records.",
            note="Preview query for deliveries.",
            source="deterministic",
        ),
        "billing": SQLCandidate(
            sql="""
                SELECT billingDocument, soldToParty, totalNetAmount, transactionCurrency, billingDocumentIsCancelled
                FROM billing_document_headers
                ORDER BY billingDocument DESC
                LIMIT 25
            """.strip(),
            interpretation="Show recent billing document records.",
            note="Preview query for billing documents.",
            source="deterministic",
        ),
        "payment": SQLCandidate(
            sql="""
                SELECT accountingDocument, customer, clearingDate, amountInTransactionCurrency, transactionCurrency
                FROM payments_accounts_receivable
                ORDER BY accountingDocument DESC
                LIMIT 25
            """.strip(),
            interpretation="Show recent payment records.",
            note="Preview query for payments.",
            source="deterministic",
        ),
        "customer": SQLCandidate(
            sql="""
                SELECT businessPartner, businessPartnerName, customer
                FROM business_partners
                ORDER BY businessPartner
                LIMIT 25
            """.strip(),
            interpretation="Show customer master records.",
            note="Preview query for customers.",
            source="deterministic",
        ),
        "product": SQLCandidate(
            sql="""
                SELECT p.product, pd.productDescription, p.productType, p.baseUnit
                FROM products AS p
                LEFT JOIN product_descriptions AS pd ON pd.product = p.product
                ORDER BY p.product
                LIMIT 25
            """.strip(),
            interpretation="Show product master records.",
            note="Preview query for products.",
            source="deterministic",
        ),
    }
    return topic_to_query.get(topic)


def _deterministic_candidates(query: str, graph: nx.MultiDiGraph) -> list[SQLCandidate]:
    lower_query = query.lower()
    focus, primary_id = _infer_identifier_focus(query, graph)
    candidates: list[SQLCandidate] = []

    if any(token in lower_query for token in ("count", "how many", "number of")) and focus in {
        "order",
        "delivery",
        "billing",
        "invoice",
        "journal",
        "payment",
        "customer",
        "product",
        "plant",
    }:
        candidates.append(_candidate_count_query(focus if focus != "invoice" else "billing"))

    if primary_id and focus == "order":
        candidates.append(_candidate_order_flow(primary_id))
    if primary_id and focus == "delivery":
        candidates.append(_candidate_delivery_flow(primary_id))
    if primary_id and focus in {"billing", "invoice"}:
        candidates.append(_candidate_billing_flow(primary_id))
    if primary_id and focus in {"payment", "journal"}:
        candidates.append(_candidate_payment_flow(primary_id))
    if primary_id and focus == "customer":
        candidates.append(_candidate_customer_summary(primary_id))
    if primary_id and focus == "product":
        candidates.append(_candidate_product_summary(primary_id))

    if focus:
        preview = _candidate_topic_preview("billing" if focus == "invoice" else focus)
        if preview:
            candidates.append(preview)

    if not candidates:
        candidates.append(
            SQLCandidate(
                sql="""
                    SELECT salesOrder, soldToParty, totalNetAmount, transactionCurrency
                    FROM sales_order_headers
                    ORDER BY salesOrder DESC
                    LIMIT 25
                """.strip(),
                interpretation="Fallback to a general sales order preview because no specific SQL pattern matched.",
                note="Fallback preview query.",
                source="deterministic",
            )
        )
    return candidates


def _llm_candidate(
    query: str,
    schema_by_table: dict[str, list[str]],
    graph: nx.MultiDiGraph,
) -> SQLCandidate | None:
    if not is_llm_available():
        return None

    focus, primary_id = _infer_identifier_focus(query, graph)
    user_prompt = "\n\n".join(
        [
            render_schema_prompt(schema_by_table),
            f"User query: {query}",
            f"Inferred focus: {focus or 'unknown'}",
            f"Primary identifier: {primary_id or 'none'}",
        ]
    )
    response = generate_completion(SQL_GENERATION_SYSTEM_PROMPT, user_prompt)
    if response is None or not response.content:
        return None

    return SQLCandidate(
        sql=_clean_sql(response.content),
        interpretation="LLM-generated SQLite query based on the user request.",
        note="Generated by Groq model.",
        source=f"llm:{response.model}",
    )


def _execute_sql(connection: sqlite3.Connection, sql: str, max_rows: int = 200) -> SQLExecutionResult:
    cursor = connection.execute(_clean_sql(sql))
    columns = tuple(description[0] for description in cursor.description or [])
    rows = tuple(dict(zip(columns, row, strict=False)) for row in cursor.fetchmany(max_rows))
    return SQLExecutionResult(sql=_clean_sql(sql), columns=columns, rows=rows)


def _format_value(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def _synthesize_answer(query: str, execution: SQLExecutionResult) -> str:
    if not execution.rows:
        return "No matching records were found in the dataset."

    lower_query = query.lower()
    columns = set(execution.columns)

    if {
        "salesOrder",
        "deliveryDocument",
        "billingDocument",
        "journalAccountingDocument",
    }.intersection(columns) and any(word in lower_query for word in ("trace", "flow", "path", "payment", "billing", "delivery")):
        first_row = execution.rows[0]
        ordered_steps = [
            ("sales order", first_row.get("salesOrder")),
            ("sales order item", first_row.get("salesOrderItem")),
            ("delivery", first_row.get("deliveryDocument")),
            ("delivery item", first_row.get("deliveryDocumentItem")),
            ("billing", first_row.get("billingDocument")),
            ("billing item", first_row.get("billingDocumentItem")),
            ("journal entry", first_row.get("journalAccountingDocument")),
            ("payment", first_row.get("paymentAccountingDocument")),
        ]
        visible_steps = [f"{label}: {value}" for label, value in ordered_steps if value not in (None, "")]
        return "Trace from query results: " + " -> ".join(visible_steps) + "."

    if len(execution.rows) == 1:
        row = execution.rows[0]
        fragments = [f"{column}={_format_value(row[column])}" for column in execution.columns[:8]]
        return "One matching row was found: " + ", ".join(fragments) + "."

    preview_lines = []
    for row in execution.rows[:3]:
        preview = ", ".join(
            f"{column}={_format_value(row[column])}" for column in execution.columns[:6]
        )
        preview_lines.append(preview)
    return (
        f"The query returned {len(execution.rows)} rows. "
        f"Sample results: {' | '.join(preview_lines)}."
    )


def _collect_highlight_values(query: str, execution: SQLExecutionResult) -> set[str]:
    values = set(_extract_id_like_tokens(query))
    for row in execution.rows:
        for value in row.values():
            if value is None:
                continue
            text = str(value)
            # Require at least 5 chars so aggregate results like "100", "10", "1"
            # don't get treated as node IDs. Real IDs in this dataset (orders,
            # deliveries, billing docs, etc.) are all 5+ characters.
            if 5 <= len(text) <= 40 and any(char.isdigit() for char in text):
                values.add(text)
    return values


# Ordered column sequence that maps O2C flow row columns to graph node table names.
# Each tuple is (row_column, table_prefix) used to build a node ID like "table:value".
_FLOW_COLUMN_TO_TABLE: list[tuple[str, str]] = [
    ("salesOrder", "sales_order_headers"),
    ("salesOrderItem", None),          # composite key, handled separately
    ("deliveryDocument", "outbound_delivery_headers"),
    ("deliveryDocumentItem", None),     # composite key, handled separately
    ("billingDocument", "billing_document_headers"),
    ("journalAccountingDocument", "journal_entry_items_accounts_receivable"),
    ("paymentAccountingDocument", "payments_accounts_receivable"),
]


def  _extract_traced_path(execution: SQLExecutionResult) -> tuple[str, ...]:
    """Return an ordered list of node IDs for O2C flow animation.

    Only populated for flow queries whose result contains the standard O2C columns.
    Returns an empty tuple for non-flow queries.
    """
    cols = set(execution.columns)
    is_flow = {"salesOrder", "deliveryDocument", "billingDocument"}.issubset(cols)
    if not is_flow or not execution.rows:
        return ()

    seen: dict[str, None] = {}
    for row in execution.rows:
        sales_order = row.get("salesOrder")
        sales_order_item = row.get("salesOrderItem")
        delivery = row.get("deliveryDocument")
        delivery_item = row.get("deliveryDocumentItem")
        billing = row.get("billingDocument")
        journal = row.get("journalAccountingDocument")
        payment = row.get("paymentAccountingDocument")

        if sales_order:
            seen.setdefault(f"sales_order_headers:{sales_order}", None)
        if sales_order and sales_order_item:
            seen.setdefault(f"sales_order_items:{sales_order}|{sales_order_item}", None)
        if delivery:
            seen.setdefault(f"outbound_delivery_headers:{delivery}", None)
        if delivery and delivery_item:
            seen.setdefault(f"outbound_delivery_items:{delivery}|{delivery_item}", None)
        if billing:
            seen.setdefault(f"billing_document_headers:{billing}", None)
        if journal:
            seen.setdefault(f"journal_entry_items_accounts_receivable:{journal}", None)
        if payment:
            seen.setdefault(f"payments_accounts_receivable:{payment}", None)

    return tuple(seen.keys())


def _highlight_nodes(
    graph: nx.MultiDiGraph,
    query: str,
    execution: SQLExecutionResult,
    limit: int = 25,
) -> tuple[str, ...]:
    candidate_values = _collect_highlight_values(query, execution)
    if not candidate_values:
        return ()

    highlights: list[str] = []
    for node_id, attributes in graph.nodes(data=True):
        key_values = {str(value) for value in attributes.get("key", {}).values()}
        if candidate_values & key_values:
            highlights.append(node_id)
            if len(highlights) >= limit:
                break
    return tuple(highlights)


def _answer_via_llm(query: str, execution: SQLExecutionResult) -> str | None:
    if not is_llm_available():
        return None

    rows_json = json.dumps(list(execution.rows[:20]), ensure_ascii=True)
    user_prompt = "\n\n".join(
        [
            f"User query: {query}",
            f"Executed SQL: {execution.sql}",
            f"Returned columns: {', '.join(execution.columns)}",
            f"Returned rows (first 20): {rows_json}",
        ]
    )
    response = generate_completion(ANSWER_SYNTHESIS_SYSTEM_PROMPT, user_prompt)
    if response is None or not response.content:
        return None
    return response.content.strip()


def run_chat_query(query: str, graph: nx.MultiDiGraph) -> ChatResult:
    in_scope, matched_keywords = _is_in_scope(query, graph)
    if not in_scope:
        return ChatResult(
            answer=DOMAIN_ONLY_REFUSAL,
            highlighted_node_ids=(),
            traced_path=(),
            in_scope=False,
            debug={
                "matched_keywords": [],
                "matched_tokens": [],
                "selected_tables": [],
                "sql": None,
                "attempts": [],
            },
        )

    connection = get_connection()
    try:
        schema_by_table = _schema_by_table(connection)
        attempts: list[dict[str, Any]] = []
        deterministic_candidates = _deterministic_candidates(query, graph)
        llm_candidate = _llm_candidate(query, schema_by_table, graph)

        candidates: list[SQLCandidate] = list(deterministic_candidates)
        if llm_candidate is not None:
            is_fallback_first = (
                bool(deterministic_candidates)
                and deterministic_candidates[0].note == "Fallback preview query."
            )
            if is_fallback_first:
                candidates.insert(0, llm_candidate)
            else:
                candidates.append(llm_candidate)

        execution: SQLExecutionResult | None = None
        used_candidate: SQLCandidate | None = None

        for candidate in candidates:
            is_valid, validation_message = _validate_sql(candidate.sql, connection, schema_by_table)
            attempt_debug = {
                "source": candidate.source,
                "sql": _clean_sql(candidate.sql),
                "interpretation": candidate.interpretation,
                "note": candidate.note,
                "validation": validation_message,
            }
            attempts.append(attempt_debug)

            if not is_valid:
                continue

            try:
                execution = _execute_sql(connection, candidate.sql)
                used_candidate = candidate
                break
            except sqlite3.Error as exc:
                attempt_debug["execution_error"] = str(exc)
                continue

        if execution is None or used_candidate is None:
            return ChatResult(
                answer="I could not produce a valid dataset query for that request.",
                highlighted_node_ids=(),
                traced_path=(),
                in_scope=True,
                debug={
                    "matched_keywords": matched_keywords,
                    "matched_tokens": _extract_id_like_tokens(query),
                    "selected_tables": [],
                    "sql": None,
                    "attempts": attempts,
                },
            )

        highlighted_node_ids = _highlight_nodes(graph, query, execution)
        traced_path = _extract_traced_path(execution)
        answer = _answer_via_llm(query, execution) or _synthesize_answer(query, execution)

        selected_tables = sorted(
            {
                match.group(1)
                for match in TABLE_REF_PATTERN.finditer(execution.sql)
            }
        )
        return ChatResult(
            answer=answer,
            highlighted_node_ids=highlighted_node_ids,
            traced_path=traced_path,
            in_scope=True,
            debug={
                "matched_keywords": matched_keywords,
                "matched_tokens": _extract_id_like_tokens(query),
                "selected_tables": selected_tables,
                "sql": execution.sql,
                "row_count": len(execution.rows),
                "columns": list(execution.columns),
                "attempts": attempts,
                "llm_enabled": is_llm_available(),
            },
        )
    finally:
        connection.close()
