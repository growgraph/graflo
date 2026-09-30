"""Pipeline steps for a record that is one vertex and refers to others."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

#: ``(target vertex, relation, {target field: record field})``.
Reference = tuple[str, str, Mapping[str, str]]


def subject_role(relations: set[str]) -> str:
    """A role for the subject vertex that no relation is named like."""
    role = "subject"
    while role in relations:
        role = f"_{role}"
    return role


def subject_with_references(
    vertex: str, references: Sequence[Reference]
) -> list[dict[str, Any]]:
    """Steps that read a record as *vertex* plus an edge per reference.

    The subject and the targets of each relation are kept in their own role,
    and every edge names both. So an edge joins the subject to the targets of
    its own relation: a target of the subject's type is not read as a second
    subject, and two relations to one type do not take each other's targets.

    Every edge of the record is written by a step, so a resource built from
    these steps sets ``infer_edges=False``.
    """
    if not references:
        return [{"vertex": vertex}]
    subject = subject_role({relation for _, relation, _ in references})
    pipeline: list[dict[str, Any]] = [{"vertex": vertex, "role": subject}]
    for target, relation, fields in references:
        pipeline.append(
            {
                "vertex": target,
                "from": dict(fields),
                "extraction_scope": "mapped_only",
                "role": relation,
            }
        )
        pipeline.append(
            {
                "edge": {
                    "from": vertex,
                    "to": target,
                    "relation": relation,
                    "match_source": subject,
                    "match_target": relation,
                }
            }
        )
    return pipeline
