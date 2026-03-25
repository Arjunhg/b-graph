"""Graph node/edge schemas for row-level business graph modeling."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class GraphNode:
    node_id: str
    node_type: str
    table: str
    key: dict[str, Any]
    label: str
    metadata: dict[str, Any]


@dataclass(frozen=True)
class GraphEdge:
    source: str
    target: str
    edge_type: str
    relationship: str
    metadata: dict[str, Any]


@dataclass(frozen=True)
class FlowValidationResult:
    is_traceable: bool
    traceable_path_count: int
    sample_path: tuple[str, ...]
    message: str


@dataclass(frozen=True)
class GraphBuildReport:
    node_count: int
    edge_count: int
    nodes_by_table: dict[str, int]
    nodes_by_type: dict[str, int]
    edges_by_type: dict[str, int]
    unmatched_relationship_rows: dict[str, int]
    flow_validation: FlowValidationResult


TABLE_NODE_TYPES: dict[str, str] = {
    "sales_order_headers": "sales_order",
    "sales_order_items": "sales_order_item",
    "sales_order_schedule_lines": "sales_order_schedule_line",
    "outbound_delivery_headers": "outbound_delivery",
    "outbound_delivery_items": "outbound_delivery_item",
    "billing_document_headers": "billing_document",
    "billing_document_items": "billing_document_item",
    "billing_document_cancellations": "billing_cancellation",
    "journal_entry_items_accounts_receivable": "journal_entry",
    "payments_accounts_receivable": "payment",
    "business_partners": "customer",
    "business_partner_addresses": "customer_address",
    "customer_company_assignments": "customer_company_assignment",
    "customer_sales_area_assignments": "customer_sales_area_assignment",
    "products": "product",
    "product_descriptions": "product_description",
    "product_plants": "product_plant",
    "product_storage_locations": "product_storage_location",
    "plants": "plant",
}


RELATION_EDGE_TYPES: dict[tuple[str, str], str] = {
    ("sales_order_items", "sales_order_headers"): "ORDER_HAS_ITEM",
    ("sales_order_schedule_lines", "sales_order_items"): "ITEM_HAS_SCHEDULE_LINE",
    ("outbound_delivery_items", "outbound_delivery_headers"): "DELIVERY_HAS_ITEM",
    ("outbound_delivery_items", "sales_order_items"): "ORDER_ITEM_TO_DELIVERY_ITEM",
    ("billing_document_items", "billing_document_headers"): "BILLING_HAS_ITEM",
    ("billing_document_items", "outbound_delivery_items"): "DELIVERY_ITEM_TO_BILLING_ITEM",
    ("billing_document_cancellations", "billing_document_headers"): "BILLING_CANCELLATION_OF",
    ("journal_entry_items_accounts_receivable", "billing_document_headers"): "BILLING_TO_JOURNAL_ENTRY",
    ("payments_accounts_receivable", "journal_entry_items_accounts_receivable"): "JOURNAL_ENTRY_TO_PAYMENT",
    ("sales_order_headers", "business_partners"): "CUSTOMER_TO_ORDER",
    ("billing_document_headers", "business_partners"): "CUSTOMER_TO_BILLING",
    ("journal_entry_items_accounts_receivable", "business_partners"): "CUSTOMER_TO_JOURNAL_ENTRY",
    ("payments_accounts_receivable", "business_partners"): "CUSTOMER_TO_PAYMENT",
    ("business_partner_addresses", "business_partners"): "CUSTOMER_TO_ADDRESS",
    ("customer_company_assignments", "business_partners"): "CUSTOMER_TO_COMPANY_ASSIGNMENT",
    ("customer_sales_area_assignments", "business_partners"): "CUSTOMER_TO_SALES_AREA_ASSIGNMENT",
    ("sales_order_items", "products"): "PRODUCT_TO_ORDER_ITEM",
    ("billing_document_items", "products"): "PRODUCT_TO_BILLING_ITEM",
    ("product_descriptions", "products"): "PRODUCT_TO_DESCRIPTION",
    ("product_plants", "products"): "PRODUCT_TO_PLANT_ASSIGNMENT",
    ("product_plants", "plants"): "PLANT_TO_PRODUCT_ASSIGNMENT",
    ("product_storage_locations", "products"): "PRODUCT_TO_STORAGE_LOCATION",
    ("product_storage_locations", "plants"): "PLANT_TO_STORAGE_LOCATION",
}
