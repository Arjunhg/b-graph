"""Graph fetch, expansion, and node metadata endpoints."""

from __future__ import annotations

from collections import deque
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
import networkx as nx


router = APIRouter()


def _get_graph(request: Request) -> nx.MultiDiGraph:
    graph = getattr(request.app.state, "graph", None)
    if graph is None:
        raise HTTPException(status_code=503, detail="Graph is not initialized.")
    return graph


def _serialize_node(node_id: str, attributes: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": node_id,
        "node_type": attributes.get("node_type"),
        "table": attributes.get("table"),
        "label": attributes.get("label"),
        "key": attributes.get("key", {}),
        "metadata": attributes.get("metadata", {}),
    }


def _serialize_edge(
    source: str,
    target: str,
    key: int,
    attributes: dict[str, Any],
) -> dict[str, Any]:
    edge_type = attributes.get("edge_type", "RELATED_TO")
    return {
        "id": f"{source}|{target}|{key}|{edge_type}",
        "source": source,
        "target": target,
        "edge_type": edge_type,
        "relationship": attributes.get("relationship"),
        "metadata": attributes.get("metadata", {}),
    }


def _subgraph_payload(
    graph: nx.MultiDiGraph,
    node_ids: set[str],
    max_edges: int,
) -> dict[str, Any]:
    nodes = [
        _serialize_node(node_id, graph.nodes[node_id])
        for node_id in node_ids
    ]

    edges: list[dict[str, Any]] = []
    for source, target, key, attributes in graph.edges(keys=True, data=True):
        if source in node_ids and target in node_ids:
            edges.append(_serialize_edge(source, target, key, attributes))
            if len(edges) >= max_edges:
                break

    return {"nodes": nodes, "edges": edges}


def _collect_overview_nodes(graph: nx.MultiDiGraph, max_nodes: int) -> set[str]:
    selected: set[str] = set()
    for seed in graph.nodes():
        if seed in selected:
            continue
        queue: deque[str] = deque([seed])
        while queue and len(selected) < max_nodes:
            current = queue.popleft()
            if current in selected:
                continue
            selected.add(current)
            neighbors = set(graph.successors(current)) | set(graph.predecessors(current))
            for neighbor in neighbors:
                if neighbor not in selected:
                    queue.append(neighbor)
        if len(selected) >= max_nodes:
            break
    return selected


@router.get("")
def get_graph(
    request: Request,
    max_nodes: int = Query(default=2000, ge=1, le=50000),
    max_edges: int = Query(default=6000, ge=1, le=150000),
) -> dict[str, Any]:
    graph = _get_graph(request)
    node_ids = _collect_overview_nodes(graph, max_nodes=max_nodes)
    payload = _subgraph_payload(graph, node_ids=node_ids, max_edges=max_edges)
    payload["truncated"] = graph.number_of_nodes() > max_nodes
    payload["total_nodes"] = graph.number_of_nodes()
    payload["total_edges"] = graph.number_of_edges()
    return payload


@router.get("/nodes")
def get_graph_nodes(
    request: Request,
    table: str | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=10000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    graph = _get_graph(request)

    nodes: list[dict[str, Any]] = []
    for node_id, attributes in graph.nodes(data=True):
        if table and attributes.get("table") != table:
            continue
        nodes.append(_serialize_node(node_id, attributes))

    total = len(nodes)
    sliced = nodes[offset : offset + limit]
    return {"total": total, "nodes": sliced}


@router.get("/edges")
def get_graph_edges(
    request: Request,
    edge_type: str | None = Query(default=None),
    limit: int = Query(default=1000, ge=1, le=20000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    graph = _get_graph(request)

    edges: list[dict[str, Any]] = []
    for source, target, key, attributes in graph.edges(keys=True, data=True):
        if edge_type and attributes.get("edge_type") != edge_type:
            continue
        edges.append(_serialize_edge(source, target, key, attributes))

    total = len(edges)
    sliced = edges[offset : offset + limit]
    return {"total": total, "edges": sliced}


@router.get("/node/{node_id:path}")
def get_node(request: Request, node_id: str) -> dict[str, Any]:
    graph = _get_graph(request)
    if node_id not in graph:
        raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found.")

    attributes = dict(graph.nodes[node_id])
    return {
        "node": _serialize_node(node_id, attributes),
        "in_degree": graph.in_degree(node_id),
        "out_degree": graph.out_degree(node_id),
        "degree": graph.degree(node_id),
    }


@router.get("/expand/{node_id:path}")
def expand_node(
    request: Request,
    node_id: str,
    hops: int = Query(default=1, ge=1, le=3),
    max_nodes: int = Query(default=250, ge=1, le=5000),
    max_edges: int = Query(default=1000, ge=1, le=20000),
) -> dict[str, Any]:
    graph = _get_graph(request)
    if node_id not in graph:
        raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found.")

    visited: set[str] = {node_id}
    queue: deque[tuple[str, int]] = deque([(node_id, 0)])

    while queue and len(visited) < max_nodes:
        current, depth = queue.popleft()
        if depth >= hops:
            continue

        neighbors = set(graph.successors(current)) | set(graph.predecessors(current))
        for neighbor in neighbors:
            if neighbor in visited:
                continue
            visited.add(neighbor)
            queue.append((neighbor, depth + 1))
            if len(visited) >= max_nodes:
                break

    payload = _subgraph_payload(graph, node_ids=visited, max_edges=max_edges)
    payload["center_node_id"] = node_id
    payload["hops"] = hops
    payload["truncated"] = len(visited) >= max_nodes
    return payload
