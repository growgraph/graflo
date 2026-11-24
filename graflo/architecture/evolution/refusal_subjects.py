"""How an op refusal names what it is about, in its ``subjects``.

Plain names, no prefix: the refusal's ``check`` already says what kind of
thing a subject is.

* a vertex type, relation, resource or block: its name (``Firm``, ``knows``,
  ``orders``, ``schema``)
* a vertex property: ``Firm.firm_id``
* an edge: its key as :func:`~graflo.architecture.graph_types.identifiers.serialize_edge_key`
  writes it, ``["person","company","knows"]``
* an edge property: the edge key, then ``.weight``
"""

from __future__ import annotations

from collections.abc import Iterable

from graflo.architecture.graph_types.identifiers import serialize_edge_key


def edge_subject(source: str, target: str, relation: str | None) -> str:
    """An edge as a refusal subject."""
    return serialize_edge_key((source, target, relation))


def prop_subject(owner: str, prop: str) -> str:
    """A property of a vertex type, or of an edge given by :func:`edge_subject`."""
    return f"{owner}.{prop}"


def names(values: Iterable[object]) -> tuple[str, ...]:
    """Subjects from names, sorted, so a refusal does not depend on set order."""
    return tuple(sorted(str(value) for value in values))


def edges(edge_ids: Iterable[tuple[str, str, str | None]]) -> tuple[str, ...]:
    """Subjects from edge ids, sorted."""
    return tuple(sorted(edge_subject(*edge_id) for edge_id in edge_ids))
