"""Prompt constants and guardrail text."""

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
