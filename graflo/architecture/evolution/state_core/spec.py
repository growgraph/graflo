"""What a caller must declare for a lift, and what the lift works out itself.

The split is not arbitrary. A lift can see *structure* -- which edges leave
``directed`` unstated, which types have no grounding, where a property is a
float with no unit -- but it cannot see *meaning*. Nothing in a schema says that
``ConfigurationItem`` denotes a ``sosa:FeatureOfInterest``, or that ``temp_c``
is degrees Celsius rather than a count, or that ``status`` is a fact that
changes over time while ``serial_number`` is not. Those are the four things this
spec carries, and the reason it is required rather than optional.

Everything else is mechanical and never appears here: undeclared directionality,
the scaffolding types and their groundings, the provenance edges.
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field as PydanticField
from pydantic import model_validator

from graflo.architecture.base import ConfigBaseModel


class Grounding(ConfigBaseModel):
    """An external-vocabulary anchor a caller asserts for one element."""

    iri: str | None = PydanticField(
        default=None, description="IRI of the concept this element denotes."
    )
    exact_match: list[str] = PydanticField(
        default_factory=list, description="IRIs asserted equivalent to it."
    )
    synonyms: list[str] = PydanticField(
        default_factory=list, description="Alternative labels an agent may meet."
    )

    @model_validator(mode="after")
    def _validate_says_something(self) -> Grounding:
        if not self.iri and not self.exact_match and not self.synonyms:
            raise ValueError(
                "a grounding must carry at least one of iri, exact_match, synonyms"
            )
        return self


class EdgeGrounding(Grounding):
    """A grounding addressed at one edge triple."""

    source: str = PydanticField(..., description="Source vertex type name.")
    target: str = PydanticField(..., description="Target vertex type name.")
    relation: str | None = PydanticField(default=None, description="Relation name.")


class LiftSpec(ConfigBaseModel):
    """The semantic input to :func:`~graflo.architecture.evolution.state_core.plan_lift`."""

    grounding: dict[str, Grounding] = PydanticField(
        default_factory=dict,
        description="Per-vertex grounding: ``{type_name: Grounding}``.",
    )
    edge_grounding: list[EdgeGrounding] = PydanticField(
        default_factory=list,
        description="Per-edge grounding, addressed by triple.",
    )
    identity: dict[str, list[str]] = PydanticField(
        default_factory=dict,
        description=(
            "Explicit identity for types that would otherwise fall back to "
            "``identity_from_all_properties``. Required only for those."
        ),
    )
    stateful: dict[str, list[str]] = PydanticField(
        default_factory=dict,
        description=(
            "Per-type mutable properties: ``{type_name: [property, ...]}``. Each "
            "named type gains a ``<Type>State`` carrying those properties over a "
            "validity interval."
        ),
    )
    observed: list[str] = PydanticField(
        default_factory=list,
        description=(
            "Types that gain a ``<Type>Observation`` scaffold for measurements "
            "taken of them at a time."
        ),
    )
    measured: dict[str, str] = PydanticField(
        default_factory=dict,
        description=(
            "Units for existing properties, as ``{'Type.property': ucum_token}``. "
            "UCUM has no currency, so currency uses ISO-4217 (``USD``)."
        ),
    )
    provenance: bool = PydanticField(
        default=True,
        description=(
            "Add ``Evidence`` and ``Agent`` with the provenance edges that make "
            "'where did this fact come from' answerable."
        ),
    )
    retire: Literal["move", "keep"] = PydanticField(
        default="move",
        description=(
            "What happens to a property named in ``stateful``. ``move`` removes it "
            "from the entity -- the honest lift, since a fact that changes over "
            "time does not belong on the thing it is about. ``keep`` leaves it as "
            "a denormalized current value beside the history."
        ),
    )

    @model_validator(mode="after")
    def _validate_measured_addresses(self) -> LiftSpec:
        bad = sorted(key for key in self.measured if key.count(".") != 1)
        if bad:
            raise ValueError(f"measured keys must read 'Type.property', got {bad}")
        return self

    def measured_for(self, vertex: str) -> dict[str, str]:
        """``{property: unit}`` for one type."""
        return {
            key.split(".", 1)[1]: unit
            for key, unit in self.measured.items()
            if key.split(".", 1)[0] == vertex
        }


__all__ = ["EdgeGrounding", "Grounding", "LiftSpec"]
