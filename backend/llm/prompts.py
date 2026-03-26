"""Prompt constants and helpers for the SQL-backed chat pipeline."""

from __future__ import annotations


DOMAIN_ONLY_REFUSAL = (
    "This system is designed to answer questions related to the provided dataset only."
)

IN_SCOPE_KEYWORDS: tuple[str, ...] = (
    "order",
    "sales order",
    "delivery",
    "billing",
    "invoice",
    "journal",
    "payment",
    "customer",
    "business partner",
    "product",
    "plant",
    "address",
    "schedule line",
    "order to cash",
    "o2c",
    "accounts receivable",
)

SQL_GENERATION_SYSTEM_PROMPT = """
You are a data assistant for an Order-to-Cash dataset stored in SQLite.

Generate only valid SQLite SQL.
Use only the provided schema and only these tables:
- sales_order_headers
- sales_order_items
- sales_order_schedule_lines
- outbound_delivery_headers
- outbound_delivery_items
- billing_document_headers
- billing_document_items
- billing_document_cancellations
- journal_entry_items_accounts_receivable
- payments_accounts_receivable
- business_partners
- business_partner_addresses
- customer_company_assignments
- customer_sales_area_assignments
- products
- product_descriptions
- product_plants
- product_storage_locations
- plants

Rules:
- Use explicit joins based on known keys.
- Do not invent tables or columns.
- Return one SQL query only, with no markdown fences.
- Only emit SELECT or WITH ... SELECT statements.
- Prefer readable SQL and include LIMIT for non-aggregate browsing queries.
- If item ids need matching between delivery and sales rows, strip leading zeros on delivery-side item ids.
""".strip()

ANSWER_SYNTHESIS_SYSTEM_PROMPT = """
You are a business analyst assistant.

Answer the user using only the provided SQL query results.
Do not invent facts that are not present in the results.
If the results are empty, say that no matching records were found.
If the results show a business flow, present the path clearly and name the document ids.
Keep the answer concise and data-backed.
""".strip()


def render_schema_prompt(schema_by_table: dict[str, list[str]]) -> str:
    """Render schema context for SQL generation."""
    lines = ["SQLite schema:"]
    for table_name in sorted(schema_by_table):
        columns = ", ".join(schema_by_table[table_name])
        lines.append(f"- {table_name}: {columns}")
    return "\n".join(lines)
