"""Pure parsers for TigerGraph GSQL and REST catalog output."""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

LS_INSTALLED_QUERY_PATTERN = re.compile(
    r"^\s*-\s*([A-Za-z_][A-Za-z0-9_]*)\s*\([^)]*\)\s*\(installed",
    re.IGNORECASE,
)
SHOW_QUERY_CREATE_PATTERN = re.compile(
    r"CREATE\s+QUERY\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(",
    re.IGNORECASE,
)
SHOW_QUERY_INSTALLED_MARKER = re.compile(r"#\s*installed", re.IGNORECASE)


def rest_response_is_error(result: dict[str, Any] | list[dict]) -> bool:
    return isinstance(result, dict) and result.get("error") is True


def rest_error_suggests_auth_or_gateway(result: dict[str, Any]) -> bool:
    message = str(result.get("message", "")).lower()
    return any(
        token in message
        for token in (
            "403",
            "401",
            "502",
            "forbidden",
            "bad gateway",
            "unauthorized",
        )
    )


def gsql_result_has_error(result: str) -> bool:
    """Return True when a GSQL response text signals a semantic/runtime failure."""
    lowered = result.lower()
    return (
        "semantic check fails" in lowered
        or "failed to" in lowered
        or "parse error" in lowered
        or "syntax error" in lowered
    )


def is_missing_query_endpoint_error(result: dict[str, Any]) -> bool:
    """Return True when REST++ reports an installed query endpoint is missing."""
    message = str(result.get("message", "")).lower()
    details = str(result.get("details", "")).lower()
    return (
        "endpoint is not found" in message
        or "endpoint is not found" in details
        or "no such endpoint" in message
        or "no such endpoint" in details
    )


def is_not_found_error(error: Exception | str) -> bool:
    """Return True if the error indicates that an object doesn't exist."""
    err_str = str(error).lower()
    return "does not exist" in err_str or "not found" in err_str


def parse_show_output(result_str: str, prefix: str) -> list[str]:
    """Parse SHOW * output to extract type names."""
    names: list[str] = []
    pattern = rf"(?:^|\s)-?\s*{re.escape(prefix)}\s+(\w+)\s*\("

    for line in result_str.split("\n"):
        line = line.strip()
        if not line:
            continue
        match = re.search(pattern, line, re.IGNORECASE)
        if match:
            name = match.group(1)
            if name and name not in names:
                names.append(name)

    return names


def parse_show_vertex_output(result_str: str) -> list[str]:
    """Parse SHOW VERTEX * output to extract vertex type names."""
    return parse_show_output(result_str, "VERTEX")


def parse_show_job_output(result_str: str) -> list[str]:
    """Parse SHOW JOB * output to extract job names."""
    return parse_show_output(result_str, "JOB")


def parse_show_graph_output(result_str: str) -> list[str]:
    """Parse SHOW GRAPH * output to extract graph names."""
    names: list[str] = []
    graph_pattern = re.compile(
        r"(?:^|\s)-?\s*GRAPH\s+([A-Za-z_][A-Za-z0-9_]*)\s*(?:\(|$)",
        re.IGNORECASE,
    )
    for line in result_str.split("\n"):
        line = line.strip()
        if not line:
            continue
        match = graph_pattern.search(line)
        if not match:
            continue
        graph_name = match.group(1)
        if graph_name not in names:
            names.append(graph_name)
    return names


def parse_show_edge_output(result_str: str) -> list[tuple[str, bool]]:
    """Parse SHOW EDGE * output to extract edge type names and direction."""
    edge_types: list[tuple[str, bool]] = []
    directed_pattern = r"(?:^|\s)-?\s*DIRECTED\s+EDGE\s+(\w+)\s*\("
    undirected_pattern = r"(?:^|\s)-?\s*UNDIRECTED\s+EDGE\s+(\w+)\s*\("

    for line in result_str.split("\n"):
        line = line.strip()
        if not line:
            continue

        match = re.search(directed_pattern, line, re.IGNORECASE)
        if match:
            edge_name = match.group(1)
            if edge_name:
                edge_types.append((edge_name, True))
            continue

        match = re.search(undirected_pattern, line, re.IGNORECASE)
        if match:
            edge_name = match.group(1)
            if edge_name:
                edge_types.append((edge_name, False))

    return edge_types


def parse_show_edge_output_with_vertices(
    output: str,
) -> dict[str, list[tuple[str, str]]]:
    """Parse SHOW EDGE * output (compact TigerGraph format)."""
    edge_map: dict[str, list[tuple[str, str]]] = defaultdict(list)

    edge_line_pattern = re.compile(
        r"-\s+(?:DIRECTED|UNDIRECTED)\s+EDGE\s+(\w+)\(([^)]+)\)"
    )
    from_to_pattern = re.compile(r"FROM\s+(\w+)\s*,\s*TO\s+(\w+)")

    for line in output.splitlines():
        line = line.strip()
        if not line.startswith("-"):
            continue

        edge_match = edge_line_pattern.search(line)
        if not edge_match:
            continue

        edge_name = edge_match.group(1)
        endpoints_blob = edge_match.group(2)

        for endpoint in endpoints_blob.split("|"):
            ft_match = from_to_pattern.search(endpoint)
            if ft_match:
                source, target = ft_match.groups()
                edge_map[edge_name].append((source, target))

    return dict(edge_map)


def parse_installed_queries_from_ls(result_str: str) -> list[str]:
    """Parse ``ls`` output lines like ``- my_query() (installed v2)``."""
    queries: list[str] = []
    for line in result_str.split("\n"):
        match = LS_INSTALLED_QUERY_PATTERN.search(line.strip())
        if not match:
            continue
        query_name = match.group(1)
        if query_name not in queries:
            queries.append(query_name)
    return queries


def parse_installed_queries_from_show_query(result_str: str) -> list[str]:
    """Parse ``SHOW QUERY *`` output, keeping only blocks marked ``# installed``."""
    queries: list[str] = []
    pending_installed = False
    for line in result_str.split("\n"):
        stripped = line.strip()
        if not stripped:
            continue
        if SHOW_QUERY_INSTALLED_MARKER.search(stripped):
            pending_installed = True
            continue
        create_match = SHOW_QUERY_CREATE_PATTERN.search(stripped)
        if create_match:
            if pending_installed:
                query_name = create_match.group(1)
                if query_name not in queries:
                    queries.append(query_name)
            pending_installed = False
    return queries


def parse_installed_queries_from_rest_endpoints(
    result: dict[str, Any] | list[dict],
    graph_name: str,
) -> list[str]:
    """Extract installed query names from ``GET /endpoints/{graph}?dynamic=true``."""
    if not isinstance(result, dict) or rest_response_is_error(result):
        return []

    queries: list[str] = []
    query_prefix = f"/query/{graph_name}/"
    for endpoint_path in result.keys():
        if query_prefix not in endpoint_path:
            continue
        idx = endpoint_path.find(query_prefix)
        if idx < 0:
            continue
        query_part = endpoint_path[idx + len(query_prefix) :]
        query_name = query_part.split()[0] if query_part else ""
        query_name = query_name.rstrip("/").strip()
        if query_name and query_name not in queries:
            queries.append(query_name)
    return queries


def parse_restpp_response(response: dict | list, is_edge: bool = False) -> list[dict]:
    """Parse REST++ API response into list of documents."""
    result: list[dict] = []
    if isinstance(response, dict):
        if "results" in response:
            for data in response["results"]:
                if is_edge:
                    edge_type = data.get("e_type", "")
                    from_id = data.get("from_id", data.get("from", ""))
                    to_id = data.get("to_id", data.get("to", ""))
                    attributes = data.get("attributes", {})
                    doc = {
                        **attributes,
                        "edge_type": edge_type,
                        "from_id": from_id,
                        "to_id": to_id,
                    }
                else:
                    vertex_id = data.get("v_id", data.get("id"))
                    attributes = data.get("attributes", {})
                    doc = {**attributes, "id": vertex_id}
                result.append(doc)
    elif isinstance(response, list):
        for data in response:
            if isinstance(data, dict):
                if is_edge:
                    edge_type = data.get("e_type", "")
                    from_id = data.get("from_id", data.get("from", ""))
                    to_id = data.get("to_id", data.get("to", ""))
                    attributes = data.get("attributes", data)
                    doc = {
                        **attributes,
                        "edge_type": edge_type,
                        "from_id": from_id,
                        "to_id": to_id,
                    }
                else:
                    vertex_id = data.get("v_id", data.get("id"))
                    attributes = data.get("attributes", data)
                    doc = {**attributes, "id": vertex_id}
                result.append(doc)
    return result
