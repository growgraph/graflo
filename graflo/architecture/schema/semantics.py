"""Optional grounding of schema elements in external vocabularies.

Purely additive and never consulted at execution time: identity, storage naming
and ingestion behave identically whether or not these blocks are present. Their
audience is the reader — human or agent — deciding what a type *means* before
deciding what to ask about it.

Serialized through the ``gf:`` meta-ontology, reusing ``skos:exactMatch`` and
``skos:altLabel`` rather than minting GraFlo-specific equivalents.
"""

from __future__ import annotations

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
