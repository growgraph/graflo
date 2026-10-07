"""Ops that set semantic annotations and descriptions."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import Field as PydanticField
from pydantic import model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.schema.semantics import FieldSemantics, Semantics

from .edges import EdgeSelector, _validate_unique_edge_selectors


class SetVertexSemanticsOp(ConfigBaseModel):
    """Ground vertex types in an external vocabulary.

    Semantics were authorable only when a type was first written: no operation
    could attach an ``iri`` to a type that already existed, so a manifest that
    arrived ungrounded — an inferred one, or anything predating the block — could
    never be grounded through the op system, only rewritten by hand.

    Grounding is purely additive and never consulted at execution time, so this
    op cannot change how anything ingests or stores. What it changes is whether a
    reader who did not author the schema can tell what a type denotes.
    """

    op: Literal["set_vertex_semantics"] = "set_vertex_semantics"
    semantics: dict[str, Semantics | None] = PydanticField(
        ...,
        description=(
            "Per-vertex grounding: ``{vertex_name: Semantics}``. ``None`` clears "
            "the block, which is what makes the op invertible."
        ),
        min_length=1,
    )


class SetVertexDescriptionsOp(ConfigBaseModel):
    """Set or clear the human-readable description of existing vertex types.

    A description was authorable only when a type was first written, so a
    change to one -- a merge folding two types together and joining what each
    side said about it, or a correction to what an inferred type means -- had no
    operation and could only be diffed as inexpressible. Like grounding, a
    description is never consulted at execution time: this op cannot change how
    anything ingests or stores.
    """

    op: Literal["set_vertex_descriptions"] = "set_vertex_descriptions"
    descriptions: dict[str, str | None] = PydanticField(
        ...,
        description=(
            "Per-vertex description: ``{vertex_name: text}``. ``None`` clears it, "
            "which is what makes the op invertible."
        ),
        min_length=1,
    )


class SetEdgeSemanticsOp(ConfigBaseModel):
    """Ground edge relations in an external vocabulary.

    The vertex op's counterpart. Relations carry as much meaning as types --
    ``wasDerivedFrom`` and ``dependsOn`` are not interchangeable -- and a
    conformance profile that asks whether types are grounded has to be able to
    ask it of edges too.
    """

    op: Literal["set_edge_semantics"] = "set_edge_semantics"
    edges: list[EdgeSelector] = PydanticField(
        ...,
        description="Edge triples whose grounding changes.",
        min_length=1,
    )
    semantics: Semantics | None = PydanticField(
        default=None,
        description=(
            "Grounding applied to every selected edge; ``None`` clears it. Not "
            "``FieldSemantics``: a unit on an edge is meaningless, and the model "
            "split is what makes ``unit:`` here a validation error."
        ),
    )

    @model_validator(mode="after")
    def _validate_unique_selectors(self) -> SetEdgeSemanticsOp:
        _validate_unique_edge_selectors(self.edges, kind="set_edge_semantics")
        return self


class FieldSemanticsTarget(ConfigBaseModel):
    """One property of one vertex, and the grounding to put on it."""

    vertex: str = PydanticField(..., description="Vertex type name.")
    field: str = PydanticField(..., description="Property name on that vertex.")
    semantics: FieldSemantics | None = PydanticField(
        default=None,
        description="Grounding for the property; ``None`` clears it.",
    )

    def key(self) -> tuple[Any, ...]:
        return ("vertex", self.vertex, self.field)


class EdgeFieldSemanticsTarget(ConfigBaseModel):
    """One property of one edge triple, and the grounding to put on it.

    Edge properties carry the same ``FieldSemantics`` as vertex properties --
    a ``since`` on an edge has a unit as much as a ``temperature`` on a vertex
    does -- but until this target existed no op could reach them.
    """

    source: str = PydanticField(..., description="Source vertex type name.")
    target: str = PydanticField(..., description="Target vertex type name.")
    relation: str | None = PydanticField(
        default=None,
        description="Relation name; ``None`` matches the edge with no relation set.",
    )
    field: str = PydanticField(..., description="Property name on that edge.")
    semantics: FieldSemantics | None = PydanticField(
        default=None,
        description="Grounding for the property; ``None`` clears it.",
    )

    def edge_id(self) -> tuple[str, str, str | None]:
        return self.source, self.target, self.relation

    def key(self) -> tuple[Any, ...]:
        return ("edge", self.source, self.target, self.relation, self.field)


class SetFieldSemanticsOp(ConfigBaseModel):
    """Ground vertex and edge properties, including their unit of measure.

    Takes :class:`~graflo.architecture.schema.semantics.FieldSemantics` rather
    than :class:`~graflo.architecture.schema.semantics.Semantics`, which is the
    entire reason this is a third op rather than a mode of the vertex one: only
    a property may carry ``unit``, and the two models are kept apart so that
    ``unit:`` on a type is a validation error rather than a silent no-op.

    A target names either a vertex property (``vertex`` + ``field``) or an
    edge property (``source`` / ``target`` / ``relation`` + ``field``).
    """

    op: Literal["set_field_semantics"] = "set_field_semantics"
    targets: list[FieldSemanticsTarget | EdgeFieldSemanticsTarget] = PydanticField(
        ...,
        description="Properties whose grounding changes.",
        min_length=1,
    )

    @model_validator(mode="after")
    def _validate_unique_targets(self) -> SetFieldSemanticsOp:
        keys = [t.key() for t in self.targets]
        if len(keys) != len(set(keys)):
            raise ValueError(
                "set_field_semantics targets must be unique by (vertex, field) or "
                "(source, target, relation, field)"
            )
        return self
