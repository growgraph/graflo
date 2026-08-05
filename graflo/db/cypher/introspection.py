"""Sampling introspection for the Cypher family.

Neo4j, Memgraph and FalkorDB answer "what is in this graph?" with the same four
queries -- list labels, sample a label's property keys, list the distinct
``(source, type, target)`` patterns, sample a pattern's property keys -- so one
collector serves all three, the same reason :func:`cypher_rel_pattern` and
:func:`cypher_graph_neighbors` are shared.

What genuinely differs is only how a driver hands back rows: the Neo4j driver
returns records addressable by name, while the Memgraph and FalkorDB wrappers
return tuples plus a column list. Callers supply *run* to bridge that, exactly as
they already do for traversal.

This is **sampling**, not a catalogue read. A property that appears on no sampled
node is not reported, so the recovered schema is a lower bound on the real one --
which is why :attr:`GraphIntrospectionResult.sample_limit` travels with the
result rather than being discarded.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from typing import Any

from graflo.db.graph_introspection import (
    GraphEdgeIntrospection,
    GraphIntrospectionResult,
    GraphVertexIntrospection,
    infer_identity_fields,
)

logger = logging.getLogger(__name__)

#: Rows returned by *run*, keyed by the ``RETURN`` aliases passed alongside.
CypherRunner = Callable[[str, Sequence[str]], list[dict[str, Any]]]

#: Preferred label listing. Every backend in this family ships it, but Memgraph
#: gates it behind a config flag and FalkorDB's older builds omit it, so a caller
#: must never depend on it succeeding.
LABELS_PROCEDURE = "CALL db.labels() YIELD label RETURN label"

#: Works everywhere but scans every node, so it is a fallback, not the default.
LABELS_FALLBACK = "MATCH (n) UNWIND labels(n) AS label RETURN DISTINCT label"


def escape_label(name: str) -> str:
    """Strip backticks so a label cannot break out of its quoting."""
    return name.replace("`", "")


def _vertex_property_query(label: str, sample_limit: int) -> str:
    return (
        f"MATCH (n:`{escape_label(label)}`)\n"
        f"WITH n LIMIT {int(sample_limit)}\n"
        "UNWIND keys(n) AS key\n"
        "RETURN DISTINCT key"
    )


def _edge_pattern_query() -> str:
    return (
        "MATCH (a)-[r]->(b)\n"
        "WITH labels(a)[0] AS source, type(r) AS relation, labels(b)[0] AS target\n"
        "WHERE source IS NOT NULL AND target IS NOT NULL\n"
        "RETURN DISTINCT source, relation, target"
    )


def _edge_property_query(
    source: str, target: str, relation: str | None, sample_limit: int
) -> str:
    rel_clause = f":`{escape_label(relation)}`" if relation else ""
    return (
        f"MATCH (a:`{escape_label(source)}`)-[r{rel_clause}]->"
        f"(b:`{escape_label(target)}`)\n"
        f"WITH r LIMIT {int(sample_limit)}\n"
        "UNWIND keys(r) AS key\n"
        "RETURN DISTINCT key"
    )


def collect_cypher_labels(run: CypherRunner) -> list[str]:
    """List node labels, falling back to a full scan when the procedure is absent."""
    try:
        rows = run(LABELS_PROCEDURE, ("label",))
    except Exception:
        logger.debug("db.labels() unavailable; falling back to a label scan")
        rows = run(LABELS_FALLBACK, ("label",))
    return [row["label"] for row in rows if row.get("label")]


def collect_cypher_introspection(
    *,
    name: str,
    run: CypherRunner,
    sample_limit: int = 100,
) -> GraphIntrospectionResult:
    """Sample a Cypher-family graph into a :class:`GraphIntrospectionResult`.

    Args:
        name: Name for the recovered schema.
        run: Executes a query and returns rows as dicts keyed by the given
            ``RETURN`` aliases.
        sample_limit: Nodes/relationships examined per type. Higher costs more
            and recovers rarer properties; it never affects which *types* are
            found, only which properties.

    Returns:
        GraphIntrospectionResult: with ``sample_limit`` recorded, since a
        consumer cannot judge the result without knowing how it was obtained.

    Note:
        ``directed`` is left at ``True`` throughout. An undirected edge is stored
        in one orientation on this family and observed in one orientation, so
        sampling cannot distinguish it from a directed one -- claiming otherwise
        would silently widen every query built on the recovered schema.
    """
    vertices: list[GraphVertexIntrospection] = []
    for label in collect_cypher_labels(run):
        rows = run(_vertex_property_query(label, sample_limit), ("key",))
        properties = [row["key"] for row in rows if row.get("key")]
        vertices.append(
            GraphVertexIntrospection(
                name=label,
                properties=properties,
                identity=infer_identity_fields(properties),
            )
        )

    edges: list[GraphEdgeIntrospection] = []
    for row in run(_edge_pattern_query(), ("source", "relation", "target")):
        source, target = row.get("source"), row.get("target")
        relation = row.get("relation")
        if not source or not target:
            continue
        prop_rows = run(
            _edge_property_query(source, target, relation, sample_limit), ("key",)
        )
        edges.append(
            GraphEdgeIntrospection(
                source=source,
                target=target,
                relation=relation,
                properties=[r["key"] for r in prop_rows if r.get("key")],
            )
        )

    return GraphIntrospectionResult(
        name=name,
        vertices=vertices,
        edges=edges,
        sample_limit=sample_limit,
    )
