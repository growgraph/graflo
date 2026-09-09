"""Optional grounding of schema elements in external vocabularies.

Purely additive and never consulted at execution time: identity, storage naming
and ingestion behave identically whether or not these blocks are present. Their
audience is the reader — human or agent — deciding what a type *means* before
deciding what to ask about it.

Serialized through the ``gf:`` meta-ontology, reusing ``skos:exactMatch`` and
``skos:altLabel`` rather than minting GraFlo-specific equivalents.
"""

from __future__ import annotations

from collections.abc import Iterable

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel


class Semantics(ConfigBaseModel):
    """External-vocabulary anchors for a vertex, edge, or whole schema."""

    iri: str | None = PydanticField(
        default=None,
        description=(
            "IRI of the concept this element denotes, e.g. "
            "``https://schema.org/Person``."
        ),
    )
    exact_match: list[str] = PydanticField(
        default_factory=list,
        description="IRIs asserted equivalent to this element (``skos:exactMatch``).",
    )
    synonyms: list[str] = PydanticField(
        default_factory=list,
        description="Alternative labels an agent may encounter (``skos:altLabel``).",
    )


class FieldSemantics(Semantics):
    """Anchors for a property, which additionally may carry a unit.

    Separate from :class:`Semantics` on purpose: a unit on a vertex or an edge is
    meaningless, and because these models forbid extra keys, the split makes
    ``unit:`` on a vertex a validation error without any custom validator.
    """

    unit: str | None = PydanticField(
        default=None,
        description="Unit of measure as a UCUM or QUDT token, e.g. ``m/s``, ``USD``.",
    )


def union_anchors(values: Iterable[str]) -> list[str]:
    """Order-preserving de-duplication, for the list-valued anchor fields."""
    out: list[str] = []
    for value in values:
        if value and value not in out:
            out.append(value)
    return out


def merge_semantics(
    left: Semantics | None, right: Semantics | None
) -> Semantics | None:
    """Union two semantic anchor blocks.

    ``exact_match`` and ``synonyms`` are sets of claims and simply union.
    ``iri`` is single-valued and cannot: a type composed from one denoting
    ``schema.org/Person`` and one denoting ``foaf:Agent`` denotes neither
    exactly, so a disagreement clears it rather than silently electing the left
    side's concept as the merged type's meaning.
    """
    if left is None or right is None:
        source = left if right is None else right
        return source.model_copy(deep=True) if source is not None else None
    return Semantics(
        iri=left.iri if left.iri == right.iri else None,
        exact_match=union_anchors(list(left.exact_match) + list(right.exact_match)),
        synonyms=union_anchors(list(left.synonyms) + list(right.synonyms)),
    )


def merge_field_semantics(
    left: FieldSemantics | None,
    right: FieldSemantics | None,
    *,
    owner: str,
    field: str,
) -> FieldSemantics | None:
    """Union two property grounding blocks, refusing a unit clash.

    The anchors fold exactly as :func:`merge_semantics` folds them. ``unit`` does
    not: unlike a disputed ``iri``, which costs the reader a claim about meaning,
    two units mean the merged property would hold numerically incomparable
    values -- the one place in this module where a disagreement is a defect in
    the data rather than in its description.
    """
    if left is None or right is None:
        source = left if right is None else right
        return source.model_copy(deep=True) if source is not None else None
    if left.unit and right.unit and left.unit != right.unit:
        raise ValueError(
            f"Conflicting units for {owner}, property {field!r}: "
            f"{left.unit!r} vs {right.unit!r}. The merged property would hold "
            "numerically incomparable values -- convert one side or drop the unit."
        )
    return FieldSemantics(
        iri=left.iri if left.iri == right.iri else None,
        exact_match=union_anchors(list(left.exact_match) + list(right.exact_match)),
        synonyms=union_anchors(list(left.synonyms) + list(right.synonyms)),
        unit=left.unit or right.unit,
    )
