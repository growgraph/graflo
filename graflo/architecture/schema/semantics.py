"""Optional grounding of schema elements in external vocabularies.

Purely additive and never consulted at execution time: identity, storage naming
and ingestion behave identically whether or not these blocks are present. Their
audience is the reader — human or agent — deciding what a type *means* before
deciding what to ask about it.

Serialized through the ``gf:`` meta-ontology, reusing ``rdfs:label`` and the SKOS
mapping and labelling properties rather than minting GraFlo-specific equivalents.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

from pydantic import Field as PydanticField
from pydantic import field_validator

from graflo.architecture.base import ConfigBaseModel

_ABSOLUTE_IRI = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*:[^\s<>\"{}|\\^`]+$")

#: Vocabulary prefixes that read as a URI scheme in a prefixed name such as
#: ``foaf:Agent``. None is a registered scheme (``geo`` is, so it is absent).
VOCABULARY_PREFIXES: frozenset[str] = frozenset(
    {
        "dc",
        "dcat",
        "dcterms",
        "foaf",
        "gf",
        "owl",
        "prov",
        "qudt",
        "rdf",
        "rdfs",
        "schema",
        "sh",
        "skos",
        "sosa",
        "ssn",
        "xsd",
    }
)


def iri_scheme(value: str) -> str:
    """The part of *value* before its first colon."""
    return value.split(":", 1)[0]


def is_absolute_iri(value: str) -> bool:
    """Whether *value* is an absolute IRI: a scheme, a colon, and no whitespace.

    A prefixed name over a well-known vocabulary (``foaf:Agent``) is not one:
    the contract stores full IRIs only.
    """
    return bool(_ABSOLUTE_IRI.match(value)) and (
        iri_scheme(value).lower() not in VOCABULARY_PREFIXES
    )


def check_iri(value: str, what: str) -> str:
    """Return *value*, or raise when it is not an absolute IRI."""
    if not is_absolute_iri(value):
        raise ValueError(
            f"{what} {value!r} is not an absolute IRI; write the full IRI, "
            "not a prefixed name"
        )
    return value


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
    label: str | None = PydanticField(
        default=None,
        description="Human-readable name of the element (``rdfs:label``).",
    )
    close_match: list[str] = PydanticField(
        default_factory=list,
        description=(
            "IRIs of concepts close enough to interchange in some applications, "
            "but not asserted equivalent (``skos:closeMatch``)."
        ),
    )
    broader: list[str] = PydanticField(
        default_factory=list,
        description="IRIs of broader concepts in an external vocabulary (``skos:broader``).",
    )

    @field_validator("iri")
    @classmethod
    def _check_iri(cls, v: str | None) -> str | None:
        return None if v is None else check_iri(v, "semantics iri")

    @field_validator("exact_match", "close_match", "broader")
    @classmethod
    def _check_iris(cls, v: list[str]) -> list[str]:
        for iri in v:
            check_iri(iri, "semantics link")
        return v


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
    """Merge two semantic anchor blocks, unioning their anchors.

    The list-valued anchors are sets of claims and simply union. ``iri`` is
    single-valued and cannot: a type composed from one denoting
    ``schema.org/Person`` and one denoting ``foaf:Agent`` denotes neither
    exactly, so a disagreement clears it rather than silently electing the left
    side's concept as the merged type's meaning. ``label`` folds the same way.
    """
    if left is None or right is None:
        source = left if right is None else right
        return source.model_copy(deep=True) if source is not None else None
    return _merged_anchors(left, right)


def _merged_anchors(left: Semantics, right: Semantics) -> Semantics:
    """The fields :class:`Semantics` declares, merged as :func:`merge_semantics` says."""
    return Semantics(
        iri=left.iri if left.iri == right.iri else None,
        label=left.label if left.label == right.label else None,
        exact_match=union_anchors([*left.exact_match, *right.exact_match]),
        synonyms=union_anchors([*left.synonyms, *right.synonyms]),
        close_match=union_anchors([*left.close_match, *right.close_match]),
        broader=union_anchors([*left.broader, *right.broader]),
    )


def merge_field_semantics(
    left: FieldSemantics | None,
    right: FieldSemantics | None,
    *,
    owner: str,
    field: str,
) -> FieldSemantics | None:
    """Merge two property grounding blocks, refusing a unit clash.

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
        **_merged_anchors(left, right).model_dump(), unit=left.unit or right.unit
    )
