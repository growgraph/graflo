"""Endpoint matching for one-hop edge reads on Cypher-family backends.

Neo4j, Memgraph and FalkorDB have no native vertex key: a vertex is found by a
property, and which property is the caller's to say (the vertex's identity
field). Names are escaped as single tokens; values always travel as parameters.
"""

from __future__ import annotations

from graflo.db.cypher.escape import cypher_name


def cypher_keyed_node(variable: str, label: str | None, key: str, param: str) -> str:
    """A node pattern matched on one property: ``(variable:Label {key: $param})``.

    Args:
        variable: Node variable bound in the pattern.
        label: Node label, or None to match any label.
        key: Property the node is matched on.
        param: Name of the query parameter holding the value.
    """
    head = f"{variable}:{cypher_name(label)}" if label else variable
    return f"({head} {{{cypher_name(key)}: ${param}}})"


def cypher_key_equals(variable: str, key: str, param: str) -> str:
    """A predicate on one property of a bound node: ``variable.key = $param``."""
    return f"{variable}.{cypher_name(key)} = ${param}"


def cypher_key_value(variable: str, key: str) -> str:
    """The value of one property of a bound node: ``variable.key``."""
    return f"{variable}.{cypher_name(key)}"
