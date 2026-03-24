"""Canonical schema mapping for Phase 1 data inspection.

This module captures stable table keys and relationship hints discovered from the
raw JSONL dataset in ``data/raw``. It is intended to be reused by ingestion and
graph-building code in later phases.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


STRIP_LEADING_ZEROS = "strip_leading_zeros"


@dataclass(frozen=True)
class TableSchema:
    """Schema metadata for one raw table."""

    name: str
    source_dir: str
    primary_key: tuple[str, ...]
    notes: str = ""


@dataclass(frozen=True)
class Relationship:
    """Child-to-parent relationship definition."""

    child_table: str
    child_columns: tuple[str, ...]
    parent_table: str
    parent_columns: tuple[str, ...]
    child_transforms: tuple[str | None, ...] = ()
    parent_transforms: tuple[str | None, ...] = ()
    notes: str = ""


def normalize_value(value: Any, transform: str | None) -> Any:
    """Normalize a join value based on configured transform."""
    if value is None:
        return None
    if transform == STRIP_LEADING_ZEROS:
        text = str(value)
        if text.isdigit():
            return text.lstrip("0") or "0" # Or in case the value is all zeros, return a single zero
        return text
    return value 


TABLE_SCHEMAS: dict[str, TableSchema] = {
    "sales_order_headers": TableSchema(
        name="sales_order_headers",
        source_dir="data/raw/sales_order_headers",
        primary_key=("salesOrder",),
        notes="Header-level sales order records.",
    ),
    "sales_order_items": TableSchema(
        name="sales_order_items",
        source_dir="data/raw/sales_order_items",
        primary_key=("salesOrder", "salesOrderItem"),
        notes="Line items for sales orders.",
    ),
    "sales_order_schedule_lines": TableSchema(
        name="sales_order_schedule_lines",
        source_dir="data/raw/sales_order_schedule_lines",
        primary_key=("salesOrder", "salesOrderItem", "scheduleLine"),
        notes="Schedule line granularity for each sales order item.",
    ),
    "outbound_delivery_headers": TableSchema(
        name="outbound_delivery_headers",
        source_dir="data/raw/outbound_delivery_headers",
        primary_key=("deliveryDocument",),
    ),
    "outbound_delivery_items": TableSchema(
        name="outbound_delivery_items",
        source_dir="data/raw/outbound_delivery_items",
        primary_key=("deliveryDocument", "deliveryDocumentItem"),
    ),
    "billing_document_headers": TableSchema(
        name="billing_document_headers",
        source_dir="data/raw/billing_document_headers",
        primary_key=("billingDocument",),
        notes="Contains one billing document per row and accounting document id.",
    ),
    "billing_document_items": TableSchema(
        name="billing_document_items",
        source_dir="data/raw/billing_document_items",
        primary_key=("billingDocument", "billingDocumentItem"),
    ),
    "billing_document_cancellations": TableSchema(
        name="billing_document_cancellations",
        source_dir="data/raw/billing_document_cancellations",
        primary_key=("billingDocument",),
    ),
    "journal_entry_items_accounts_receivable": TableSchema(
        name="journal_entry_items_accounts_receivable",
        source_dir="data/raw/journal_entry_items_accounts_receivable",
        primary_key=("accountingDocument",),
        notes="Current dataset has accountingDocumentItem fixed to 1.",
    ),
    "payments_accounts_receivable": TableSchema(
        name="payments_accounts_receivable",
        source_dir="data/raw/payments_accounts_receivable",
        primary_key=("accountingDocument",),
        notes="Current dataset has accountingDocumentItem fixed to 1.",
    ),
    "business_partners": TableSchema(
        name="business_partners",
        source_dir="data/raw/business_partners",
        primary_key=("businessPartner",),
    ),
    "business_partner_addresses": TableSchema(
        name="business_partner_addresses",
        source_dir="data/raw/business_partner_addresses",
        primary_key=("addressId",),
    ),
    "customer_company_assignments": TableSchema(
        name="customer_company_assignments",
        source_dir="data/raw/customer_company_assignments",
        primary_key=("customer", "companyCode"),
    ),
    "customer_sales_area_assignments": TableSchema(
        name="customer_sales_area_assignments",
        source_dir="data/raw/customer_sales_area_assignments",
        primary_key=("customer", "salesOrganization", "distributionChannel", "division"),
    ),
    "products": TableSchema(
        name="products",
        source_dir="data/raw/products",
        primary_key=("product",),
    ),
    "product_descriptions": TableSchema(
        name="product_descriptions",
        source_dir="data/raw/product_descriptions",
        primary_key=("product", "language"),
    ),
    "product_plants": TableSchema(
        name="product_plants",
        source_dir="data/raw/product_plants",
        primary_key=("product", "plant"),
    ),
    "product_storage_locations": TableSchema(
        name="product_storage_locations",
        source_dir="data/raw/product_storage_locations",
        primary_key=("product", "plant", "storageLocation"),
    ),
    "plants": TableSchema(
        name="plants",
        source_dir="data/raw/plants",
        primary_key=("plant",),
    ),
}


RELATIONSHIPS: tuple[Relationship, ...] = (
    Relationship(
        child_table="sales_order_items",
        child_columns=("salesOrder",),
        parent_table="sales_order_headers",
        parent_columns=("salesOrder",),
    ),
    Relationship(
        child_table="sales_order_schedule_lines",
        child_columns=("salesOrder", "salesOrderItem"),
        parent_table="sales_order_items",
        parent_columns=("salesOrder", "salesOrderItem"),
    ),
    Relationship(
        child_table="outbound_delivery_items",
        child_columns=("deliveryDocument",),
        parent_table="outbound_delivery_headers",
        parent_columns=("deliveryDocument",),
    ),
    Relationship(
        child_table="outbound_delivery_items",
        child_columns=("referenceSdDocument", "referenceSdDocumentItem"),
        parent_table="sales_order_items",
        parent_columns=("salesOrder", "salesOrderItem"),
        child_transforms=(None, STRIP_LEADING_ZEROS),
        notes="referenceSdDocumentItem is zero-padded in delivery rows (e.g. 000010).",
    ),
    Relationship(
        child_table="billing_document_items",
        child_columns=("billingDocument",),
        parent_table="billing_document_headers",
        parent_columns=("billingDocument",),
    ),
    Relationship(
        child_table="billing_document_items",
        child_columns=("referenceSdDocument", "referenceSdDocumentItem"),
        parent_table="outbound_delivery_items",
        parent_columns=("deliveryDocument", "deliveryDocumentItem"),
        parent_transforms=(None, STRIP_LEADING_ZEROS),
        notes="deliveryDocumentItem is zero-padded in outbound deliveries.",
    ),
    Relationship(
        child_table="billing_document_cancellations",
        child_columns=("billingDocument",),
        parent_table="billing_document_headers",
        parent_columns=("cancelledBillingDocument",),
        notes="Cancellation row points to cancelledBillingDocument in headers.",
    ),
    Relationship(
        child_table="journal_entry_items_accounts_receivable",
        child_columns=("referenceDocument",),
        parent_table="billing_document_headers",
        parent_columns=("billingDocument",),
    ),
    Relationship(
        child_table="payments_accounts_receivable",
        child_columns=("accountingDocument",),
        parent_table="journal_entry_items_accounts_receivable",
        parent_columns=("accountingDocument",),
    ),
    Relationship(
        child_table="sales_order_headers",
        child_columns=("soldToParty",),
        parent_table="business_partners",
        parent_columns=("businessPartner",),
    ),
    Relationship(
        child_table="billing_document_headers",
        child_columns=("soldToParty",),
        parent_table="business_partners",
        parent_columns=("businessPartner",),
    ),
    Relationship(
        child_table="journal_entry_items_accounts_receivable",
        child_columns=("customer",),
        parent_table="business_partners",
        parent_columns=("businessPartner",),
    ),
    Relationship(
        child_table="payments_accounts_receivable",
        child_columns=("customer",),
        parent_table="business_partners",
        parent_columns=("businessPartner",),
    ),
    Relationship(
        child_table="business_partner_addresses",
        child_columns=("businessPartner",),
        parent_table="business_partners",
        parent_columns=("businessPartner",),
    ),
    Relationship(
        child_table="customer_company_assignments",
        child_columns=("customer",),
        parent_table="business_partners",
        parent_columns=("customer",),
    ),
    Relationship(
        child_table="customer_sales_area_assignments",
        child_columns=("customer",),
        parent_table="business_partners",
        parent_columns=("customer",),
    ),
    Relationship(
        child_table="sales_order_items",
        child_columns=("material",),
        parent_table="products",
        parent_columns=("product",),
    ),
    Relationship(
        child_table="billing_document_items",
        child_columns=("material",),
        parent_table="products",
        parent_columns=("product",),
    ),
    Relationship(
        child_table="product_descriptions",
        child_columns=("product",),
        parent_table="products",
        parent_columns=("product",),
    ),
    Relationship(
        child_table="product_plants",
        child_columns=("product",),
        parent_table="products",
        parent_columns=("product",),
    ),
    Relationship(
        child_table="product_plants",
        child_columns=("plant",),
        parent_table="plants",
        parent_columns=("plant",),
    ),
    Relationship(
        child_table="product_storage_locations",
        child_columns=("product",),
        parent_table="products",
        parent_columns=("product",),
    ),
    Relationship(
        child_table="product_storage_locations",
        child_columns=("plant",),
        parent_table="plants",
        parent_columns=("plant",),
    ),
)


CANONICAL_ENTITY_MAPPING: dict[str, tuple[str, ...]] = {
    "order": (
        "sales_order_headers",
        "sales_order_items",
        "sales_order_schedule_lines",
    ),
    "delivery": (
        "outbound_delivery_headers",
        "outbound_delivery_items",
    ),
    "billing": (
        "billing_document_headers",
        "billing_document_items",
        "billing_document_cancellations",
    ),
    "accounting": (
        "journal_entry_items_accounts_receivable",
        "payments_accounts_receivable",
    ),
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
    "plant": ("plants",),
}
