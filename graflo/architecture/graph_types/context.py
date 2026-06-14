"""Extraction and assembly runtime contexts."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from pydantic import ConfigDict, Field

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.edge_derivation import EdgeDerivation
from graflo.architecture.graph_types.identifiers import GraphEntity
from graflo.architecture.graph_types.location import LocationIndex, ProvenancePath
from graflo.architecture.graph_types.transform import TransformPayload


def inner_factory_vertex() -> defaultdict[LocationIndex, list]:
    """Create a default dictionary for vertex data."""
    return defaultdict(list)


def outer_factory() -> defaultdict[str, defaultdict[LocationIndex, list]]:
    """Create a nested default dictionary for vertex data."""
    return defaultdict(inner_factory_vertex)


def dd_factory() -> defaultdict[GraphEntity, list]:
    """Create a default dictionary for graph entity data."""
    return defaultdict(list)


def _default_dict_transforms() -> defaultdict[LocationIndex, list[Any]]:
    return defaultdict(list)


def _default_dict_observations() -> dict[LocationIndex, dict[str, Any]]:
    return {}


def _default_vertex_observations() -> list[VertexObservation]:
    return []


def _default_transform_observations() -> list[TransformObservation]:
    return []


def _default_edge_intents() -> list[EdgeIntent]:
    return []


def _default_transform_failures() -> list[TransformCastFailure]:
    return []


class VertexObservation(ConfigBaseModel):
    """Typed vertex observation emitted during extraction."""

    vertex_name: str
    location: LocationIndex
    vertex: dict[str, Any]
    ctx: dict[str, Any]
    provenance: ProvenancePath


class TransformObservation(ConfigBaseModel):
    """Typed transform observation emitted during extraction."""

    location: LocationIndex
    payload: TransformPayload
    provenance: ProvenancePath


class TransformCastFailure(ConfigBaseModel):
    """One transform step that failed during extraction (tolerance mode)."""

    location: LocationIndex
    transform_label: str
    exception_type: str
    message: str
    traceback: str = ""
    nulled_fields: tuple[str, ...] = Field(default_factory=tuple)


class EdgeIntent(ConfigBaseModel):
    """Typed edge assembly request emitted during extraction."""

    edge: Any
    location: LocationIndex | None = None
    provenance: ProvenancePath | None = None
    derivation: EdgeDerivation | None = None


class ExtractionContext(ConfigBaseModel):
    """Extraction-phase context.

    Attributes:
        acc_vertex: Local accumulation of extracted vertices
        transform_buffer: Buffer for transform payloads (defaultdict[LocationIndex, list])
        obs_buffer: Merged observation context per location (dict[LocationIndex, dict])
        vertex_observations: Explicit extracted vertex observations
        transform_observations: Explicit extracted transform observations
        edge_intents: Explicit edge intents for assembly phase
    """

    model_config = ConfigDict(kw_only=True)  # type: ignore[assignment]

    # Pydantic cannot schema nested defaultdict with custom key types (e.g. LocationIndex),
    # so we use Any; runtime type is as documented in Attributes
    acc_vertex: Any = Field(default_factory=outer_factory)
    transform_buffer: Any = Field(default_factory=_default_dict_transforms)
    obs_buffer: Any = Field(default_factory=_default_dict_observations)
    vertex_observations: list[VertexObservation] = Field(
        default_factory=_default_vertex_observations
    )
    transform_observations: list[TransformObservation] = Field(
        default_factory=_default_transform_observations
    )
    edge_intents: list[EdgeIntent] = Field(default_factory=_default_edge_intents)
    transform_failures: list[TransformCastFailure] = Field(
        default_factory=_default_transform_failures
    )

    def record_vertex_observation(
        self, *, vertex_name: str, location: LocationIndex, vertex: dict, ctx: dict
    ) -> None:
        self.vertex_observations.append(
            VertexObservation(
                vertex_name=vertex_name,
                location=location,
                vertex=vertex,
                ctx=ctx,
                provenance=ProvenancePath.from_lindex(location),
            )
        )

    def record_transform_observation(
        self, *, location: LocationIndex, payload: TransformPayload
    ) -> None:
        self.transform_observations.append(
            TransformObservation(
                location=location,
                payload=payload,
                provenance=ProvenancePath.from_lindex(location),
            )
        )

    def record_edge_intent(
        self,
        *,
        edge: Any,
        location: LocationIndex,
        derivation: EdgeDerivation | None = None,
    ) -> None:
        self.edge_intents.append(
            EdgeIntent(
                edge=edge,
                location=location,
                provenance=ProvenancePath.from_lindex(location),
                derivation=derivation,
            )
        )

    def record_transform_failure(
        self,
        *,
        location: LocationIndex,
        transform_label: str,
        exc: BaseException,
        traceback_text: str,
        nulled_fields: tuple[str, ...],
    ) -> None:
        self.transform_failures.append(
            TransformCastFailure(
                location=location,
                transform_label=transform_label,
                exception_type=type(exc).__name__,
                message=str(exc),
                traceback=traceback_text,
                nulled_fields=nulled_fields,
            )
        )


class AssemblyContext(ConfigBaseModel):
    """Assembly-phase context built from extraction outputs."""

    model_config = ConfigDict(kw_only=True)  # type: ignore[assignment]

    extraction: ExtractionContext
    acc_global: Any = Field(default_factory=dd_factory)

    @property
    def acc_vertex(self) -> Any:
        return self.extraction.acc_vertex

    @property
    def transform_buffer(self) -> Any:
        return self.extraction.transform_buffer

    @property
    def obs_buffer(self) -> Any:
        return self.extraction.obs_buffer

    @property
    def edge_intents(self) -> list[EdgeIntent]:
        return self.extraction.edge_intents

    @classmethod
    def from_extraction(cls, extraction: ExtractionContext) -> AssemblyContext:
        return cls(extraction=extraction)


class GraphAssemblyResult(ConfigBaseModel):
    """Result of graph assembly phase."""

    entities: Any = Field(default_factory=dd_factory)


class ResourceCastResult(ConfigBaseModel):
    """Outcome of casting one document through a resource pipeline."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    entities: Any
    transform_failures: list[TransformCastFailure] = Field(default_factory=list)


class ActionContext(ExtractionContext):
    """Backward-compatible extraction+assembly context.

    Kept for existing callers/tests while Wave 5 migrates call surfaces.
    """

    acc_global: Any = Field(default_factory=dd_factory)
