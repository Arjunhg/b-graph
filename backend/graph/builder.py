"""SQLite -> NetworkX row-level graph construction."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict
import sqlite3
from typing import Any

import networkx as nx

from backend.db.engine import get_connection
from backend.db.schema_mapping import RELATIONSHIPS, TABLE_SCHEMAS, normalize_value
from backend.graph.schema import (
    GraphBuildReport,
    GraphEdge,
    GraphNode,
    FlowValidationResult,
    RELATION_EDGE_TYPES,
    TABLE_NODE_TYPES,
)


def _quote(identifier: str) -> str:
    return f'"{identifier.replace(chr(34), chr(34) * 2)}"'


def _fetch_rows(connection: sqlite3.Connection, table_name: str) -> list[dict[str, Any]]:
    cursor = connection.execute(f"SELECT * FROM {_quote(table_name)};")
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]


def _pk_tuple(row: dict[str, Any], key_columns: tuple[str, ...]) -> tuple[Any, ...]:
    return tuple(row[column] for column in key_columns)


def _join_key(
    row: dict[str, Any],
    columns: tuple[str, ...],
    transforms: tuple[str | None, ...] | None = None,
) -> tuple[Any, ...] | None:
    transforms = transforms or tuple(None for _ in columns)
    values: list[Any] = []
    for index, column in enumerate(columns):
        value = row.get(column)
        if value is None or value == "":
            return None
        values.append(normalize_value(value, transforms[index]))
    return tuple(values)


def _make_node_id(table_name: str, primary_key_values: tuple[Any, ...]) -> str:
    pk_value = "|".join(str(value) for value in primary_key_values)
    return f"{table_name}:{pk_value}"


def _make_label(table_name: str, row: dict[str, Any], key_columns: tuple[str, ...]) -> str:
    key_text = ", ".join(f"{column}={row.get(column)}" for column in key_columns)
    return f"{table_name} ({key_text})"


def _relationship_name(child_table: str, parent_table: str) -> str:
    return f"{parent_table} -> {child_table}"


def _successors_by_edge_type(
    graph: nx.MultiDiGraph, node_id: str, edge_type: str
) -> list[str]:
    return [
        target
        for _, target, attributes in graph.out_edges(node_id, data=True)
        if attributes.get("edge_type") == edge_type
    ]


def _predecessors_by_edge_type(
    graph: nx.MultiDiGraph, node_id: str, edge_type: str
) -> list[str]:
    return [
        source
        for source, _, attributes in graph.in_edges(node_id, data=True)
        if attributes.get("edge_type") == edge_type
    ]


def _validate_key_flows(graph: nx.MultiDiGraph) -> FlowValidationResult:
    flow_paths: list[tuple[str, ...]] = []
    order_headers = [
        node_id
        for node_id, attributes in graph.nodes(data=True)
        if attributes.get("table") == "sales_order_headers"
    ]

    for order_header in order_headers:
        order_items = _successors_by_edge_type(graph, order_header, "ORDER_HAS_ITEM")
        for order_item in order_items:
            delivery_items = _successors_by_edge_type(
                graph, order_item, "ORDER_ITEM_TO_DELIVERY_ITEM"
            )
            for delivery_item in delivery_items:
                billing_items = _successors_by_edge_type(
                    graph, delivery_item, "DELIVERY_ITEM_TO_BILLING_ITEM"
                )
                for billing_item in billing_items:
                    billing_headers = _predecessors_by_edge_type(
                        graph, billing_item, "BILLING_HAS_ITEM"
                    )
                    for billing_header in billing_headers:
                        journal_entries = _successors_by_edge_type(
                            graph, billing_header, "BILLING_TO_JOURNAL_ENTRY"
                        )
                        for journal_entry in journal_entries:
                            payments = _successors_by_edge_type(
                                graph, journal_entry, "JOURNAL_ENTRY_TO_PAYMENT"
                            )
                            for payment in payments:
                                flow_paths.append(
                                    (
                                        order_header,
                                        order_item,
                                        delivery_item,
                                        billing_item,
                                        billing_header,
                                        journal_entry,
                                        payment,
                                    )
                                )

    if not flow_paths:
        return FlowValidationResult(
            is_traceable=False,
            traceable_path_count=0,
            sample_path=(),
            message="No explicit order -> delivery -> billing -> journal -> payment chain found.",
        )

    sample_path = flow_paths[0]

    return FlowValidationResult(
        is_traceable=True,
        traceable_path_count=len(flow_paths),
        sample_path=sample_path,
        message="Key Order-to-Cash flows are traceable in the graph.",
    )


def build_graph_from_sqlite(connection: sqlite3.Connection) -> tuple[nx.MultiDiGraph, GraphBuildReport]:
    graph = nx.MultiDiGraph()
    table_records: dict[str, list[tuple[dict[str, Any], str]]] = {}
    nodes_by_table: Counter[str] = Counter()
    nodes_by_type: Counter[str] = Counter()

    for table_name in sorted(TABLE_SCHEMAS):
        table_schema = TABLE_SCHEMAS[table_name]
        rows = _fetch_rows(connection, table_name)
        table_records[table_name] = []

        for row in rows:
            pk_values = _pk_tuple(row, table_schema.primary_key)
            node_id = _make_node_id(table_name, pk_values)
            node_type = TABLE_NODE_TYPES.get(table_name, "record")
            node = GraphNode(
                node_id=node_id,
                node_type=node_type,
                table=table_name,
                key=dict(zip(table_schema.primary_key, pk_values, strict=False)),
                label=_make_label(table_name, row, table_schema.primary_key),
                metadata=row,
            )
            graph.add_node(node_id, **asdict(node))
            table_records[table_name].append((row, node_id))
            nodes_by_table[table_name] += 1
            nodes_by_type[node_type] += 1

    edges_by_type: Counter[str] = Counter()
    unmatched_relationship_rows: dict[str, int] = {}

    for relation in RELATIONSHIPS:
        relation_id = (
            f"{relation.parent_table}.{relation.parent_columns}"
            f" -> {relation.child_table}.{relation.child_columns}"
        )
        parent_index: dict[tuple[Any, ...], list[str]] = defaultdict(list)

        for parent_row, parent_node_id in table_records[relation.parent_table]:
            key = _join_key(
                parent_row,
                relation.parent_columns,
                relation.parent_transforms if relation.parent_transforms else None,
            )
            if key is not None:
                parent_index[key].append(parent_node_id)

        unmatched_count = 0
        edge_type = RELATION_EDGE_TYPES.get(
            (relation.child_table, relation.parent_table), "RELATED_TO"
        )

        for child_row, child_node_id in table_records[relation.child_table]:
            child_key = _join_key(
                child_row,
                relation.child_columns,
                relation.child_transforms if relation.child_transforms else None,
            )
            if child_key is None:
                unmatched_count += 1
                continue

            parent_node_ids = parent_index.get(child_key, [])
            if not parent_node_ids:
                unmatched_count += 1
                continue

            for parent_node_id in parent_node_ids:
                edge = GraphEdge(
                    source=parent_node_id,
                    target=child_node_id,
                    edge_type=edge_type,
                    relationship=_relationship_name(
                        relation.child_table, relation.parent_table
                    ),
                    metadata={
                        "parent_table": relation.parent_table,
                        "child_table": relation.child_table,
                        "parent_columns": relation.parent_columns,
                        "child_columns": relation.child_columns,
                        "notes": relation.notes,
                    },
                )
                graph.add_edge(parent_node_id, child_node_id, **asdict(edge))
                edges_by_type[edge_type] += 1

        unmatched_relationship_rows[relation_id] = unmatched_count

    flow_validation = _validate_key_flows(graph)
    report = GraphBuildReport(
        node_count=graph.number_of_nodes(),
        edge_count=graph.number_of_edges(),
        nodes_by_table=dict(sorted(nodes_by_table.items())),
        nodes_by_type=dict(sorted(nodes_by_type.items())),
        edges_by_type=dict(sorted(edges_by_type.items())),
        unmatched_relationship_rows=unmatched_relationship_rows,
        flow_validation=flow_validation,
    )
    return graph, report


def build_graph() -> tuple[nx.MultiDiGraph, GraphBuildReport]:
    """Build graph from the configured SQLite database."""
    connection = get_connection()
    try:
        return build_graph_from_sqlite(connection)
    finally:
        connection.close()


if __name__ == "__main__":
    graph, report = build_graph()
    print(f"Nodes: {report.node_count}")
    print(f"Edges: {report.edge_count}")
    print(f"Flow traceable: {report.flow_validation.is_traceable}")
    print(f"Traceable O2C paths: {report.flow_validation.traceable_path_count}")
    if report.flow_validation.sample_path:
        print("Sample O2C path:")
        for node_id in report.flow_validation.sample_path:
            print(f"- {node_id}")
