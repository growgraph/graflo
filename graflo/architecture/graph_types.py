"""Graph runtime types and data structures (extraction, assembly, containers).

This module defines graph-processing data structures used across the ingestion
pipeline and database adapters. It provides:

- Core data types for vertices and edges
- Database index configurations
- Graph container implementations
- Edge mapping and casting utilities
- Action context for graph transformations

The module is designed to be database-agnostic, supporting both ArangoDB and Neo4j through
the DBType enum. It provides a unified interface for working with graph data structures
while allowing for database-specific optimizations and features.

Key Components:
    - EdgeMapping: Defines how edges are mapped between vertices
    - IndexType: Supported database index types
    - EdgeType: Types of edge handling in the graph database
    - GraphContainer: Main container for graph data
    - ActionContext: Context for graph transformation operations

Example:
    >>> container = GraphContainer(vertices={}, edges={}, linear=[])
    >>> index = Index(fields=["name", "age"], type=IndexType.PERSISTENT)
    >>> context = ActionContext()
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, TypeAlias

from pydantic import ConfigDict, Field, model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.edge_derivation import EdgeDerivation
from graflo.onto import BaseEnum
from graflo.onto import DBType


def _pick_unique_dict(docs: list) -> list:
    """Deduplicate dicts by structure; preserves original objects (local copy of merge logic)."""

    def make_hashable(obj: object) -> object:
        if isinstance(obj, dict):
            return tuple(sorted((k, make_hashable(v)) for k, v in obj.items()))
        if isinstance(obj, (list, tuple)):
            return tuple(make_hashable(item) for item in obj)
        if isinstance(obj, (datetime, date, time)):
            return ("__datetime__", obj.isoformat())
        if isinstance(obj, Decimal):
            return ("__decimal__", str(obj))
        if isinstance(obj, set):
            return tuple(sorted(make_hashable(item) for item in obj))
        return obj

    seen: dict[object, object] = {}
    for doc in docs:
        key = make_hashable(doc)
        if key not in seen:
            seen[key] = doc
    return list(seen.values())


# Edge identifier layers:
# - EdgeId: schema-level edge definition key (source, target, relation)
EdgeId: TypeAlias = tuple[str, str, str | None]
GraphEntity: TypeAlias = str | EdgeId

logger = logging.getLogger(__name__)


class EdgeMapping(BaseEnum):
    """Defines how edges are mapped between vertices.

    ALL: Maps all vertices to all vertices
    ONE_N: Maps one vertex to many vertices
    """

    ALL = "all"
    ONE_N = "1-n"


class EncodingType(BaseEnum):
    """Supported character encodings for data input/output."""

    ISO_8859 = "ISO-8859-1"
    UTF_8 = "utf-8"


class IndexType(BaseEnum):
    """Types of database indexes supported.

    PERSISTENT: Standard persistent index
    HASH: Hash-based index for fast lookups
    SKIPLIST: Sorted index using skip list data structure
    FULLTEXT: Index optimized for text search
    """

    PERSISTENT = "persistent"
    HASH = "hash"
    SKIPLIST = "skiplist"
    FULLTEXT = "fulltext"


class EdgeType(BaseEnum):
    """Defines how edges are handled in the graph database.

    INDIRECT: Uses pre-existing DB structures and may be used after data ingestion
    DIRECT: Generated during ingestion from resource pipelines
    """

    INDIRECT = "indirect"
    DIRECT = "direct"


class ABCFields(ConfigBaseModel):
    """Base model for entities that have fields.

    Attributes:
        name: Optional name of the entity
        fields: List of field names
    """

    name: str | None = Field(
        default=None,
        description="Optional name of the entity (e.g. vertex name for composite field prefix).",
    )
    fields: list[str] = Field(
        default_factory=list,
        description="List of field names for this entity.",
    )
    keep_vertex_name: bool = Field(
        default=True,
        description="If True, composite field names use entity@field format; otherwise use field only.",
    )

    def cfield(self, x: str) -> str:
        """Creates a composite field name by combining the entity name with a field name.

        Args:
            x: Field name to combine with entity name

        Returns:
            Composite field name in format "entity@field"
        """
        return f"{self.name}@{x}" if self.keep_vertex_name else x


class Weight(ABCFields):
    """Defines weight configuration for edges.

    Attributes:
        map: Dictionary mapping field values to weights
        filter: Dictionary of filter conditions for weights
    """

    map: dict = Field(
        default_factory=dict,
        description="Mapping of field values to weight values for vertex-based edge attributes.",
    )
    filter: dict = Field(
        default_factory=dict,
        description="Filter conditions applied when resolving vertex-based weights.",
    )


class Index(ConfigBaseModel):
    """Configuration for database indexes.

    Attributes:
        name: Optional name of the index
        fields: List of fields to index
        unique: Whether the index enforces uniqueness
        type: Type of index to create
        deduplicate: Whether to deduplicate index entries
        sparse: Whether to create a sparse index
        exclude_edge_endpoints: Whether to exclude edge endpoints from index
    """

    name: str | None = Field(
        default=None,
        description="Optional index name. For edges, can reference a vertex name for composite fields.",
    )
    fields: list[str] = Field(
        default_factory=list,
        description="List of field names included in this index.",
    )
    unique: bool = Field(
        default=True,
        description="If True, index enforces uniqueness on the field combination.",
    )
    type: IndexType = Field(
        default=IndexType.PERSISTENT,
        description="Index type (PERSISTENT, HASH, SKIPLIST, FULLTEXT).",
    )
    deduplicate: bool = Field(
        default=True,
        description="Whether to deduplicate index entries (e.g. ArangoDB).",
    )
    sparse: bool = Field(
        default=False,
        description="If True, create a sparse index (exclude null/missing values).",
    )
    exclude_edge_endpoints: bool = Field(
        default=False,
        description="If True, do not add _from/_to to edge index (e.g. ArangoDB).",
    )

    def __iter__(self):
        """Iterate over the indexed fields."""
        return iter(self.fields)

    def db_form(self, db_type: DBType) -> dict:
        """Convert index configuration to database-specific format.

        Args:
            db_type: Type of database (ARANGO or NEO4J)

        Returns:
            Dictionary of index configuration in database-specific format

        Raises:
            ValueError: If db_type is not supported
        """
        r = dict(self.to_dict())
        if db_type == DBType.ARANGO:
            r.pop("name", None)
            r.pop("exclude_edge_endpoints", None)
        return r


class ItemsView:
    """View class for iterating over vertices and edges in a GraphContainer."""

    def __init__(self, gc: GraphContainer):
        self._dictlike = gc

    def __iter__(self):
        """Iterate over vertices and edges in the container."""
        for key in self._dictlike.vertices:
            yield key, self._dictlike.vertices[key]
        for key in self._dictlike.edges:
            yield key, self._dictlike.edges[key]


class GraphContainer(ConfigBaseModel):
    """Container for graph data including vertices and edges.

    Attributes:
        vertices: Dictionary mapping vertex names to lists of vertex data
        edges: Dictionary mapping edge IDs to lists of edge data
        linear: List of default dictionaries containing linear data
    """

    vertices: dict[str, list] = Field(default_factory=dict)
    edges: dict[tuple[str, str, str | None], list] = Field(default_factory=dict)
    linear: list[defaultdict[str | tuple[str, str, str | None], list[Any]]] = Field(
        default_factory=list
    )

    def items(self):
        """Get an ItemsView of the container's contents."""
        return ItemsView(self)

    def pick_unique(self):
        """Remove duplicate entries from vertices and edges."""
        for k, v in self.vertices.items():
            self.vertices[k] = _pick_unique_dict(v)
        for k, v in self.edges.items():
            self.edges[k] = _pick_unique_dict(v)

    def loop_over_relations(self, edge_def: tuple[str, str, str | None]):
        """Iterate over edges matching the given edge definition.

        Args:
            edge_def: Tuple of (source, target, optional_relation)

        Returns:
            Generator yielding matching edge IDs
        """
        source, target, _ = edge_def
        return (ed for ed in self.edges if source == ed[0] and target == ed[1])

    @classmethod
    def from_docs_list(
        cls, list_default_dicts: list[defaultdict[GraphEntity, list]]
    ) -> GraphContainer:
        """Create a GraphContainer from a list of default dictionaries.

        Args:
            list_default_dicts: List of default dictionaries containing vertex and edge data

        Returns:
            New GraphContainer instance

        Raises:
            AssertionError: If edge IDs are not properly formatted
        """
        vdict: defaultdict[str, list] = defaultdict(list)
        edict: defaultdict[tuple[str, str, str | None], list] = defaultdict(list)

        for d in list_default_dicts:
            for k, v in d.items():
                if isinstance(k, str):
                    vdict[k].extend(v)
                elif isinstance(k, tuple):
                    if not (
                        len(k) == 3
                        and all(isinstance(item, str) for item in k[:-1])
                        and isinstance(k[-1], (str, type(None)))
                    ):
                        raise ValueError(
                            f"edge key must be (str, str, str|None), got {k}"
                        )
                    edict[k].extend(v)
        return GraphContainer(
            vertices=dict(vdict.items()),
            edges=dict(edict.items()),
            linear=list_default_dicts,
        )


class EdgeCastingType(BaseEnum):
    """Types of edge casting supported.

    PAIR: Edges are cast as pairs of vertices
    PRODUCT: Edges are cast as combinations of vertex sets
    """

    PAIR = "pair"
    PRODUCT = "product"
    COMBINATIONS = "combinations"


def inner_factory_vertex() -> defaultdict[LocationIndex, list]:
    """Create a default dictionary for vertex data."""
    return defaultdict(list)


def outer_factory() -> defaultdict[str, defaultdict[LocationIndex, list]]:
    """Create a nested default dictionary for vertex data."""
    return defaultdict(inner_factory_vertex)


def dd_factory() -> defaultdict[GraphEntity, list]:
    """Create a default dictionary for graph entity data."""
    return defaultdict(list)


class VertexRep(ConfigBaseModel):
    """Context for graph transformation actions.

    Attributes:
        vertex: doc representing a vertex
        ctx: context (for edge definition upstream
    """

    model_config = ConfigDict(kw_only=True)  # type: ignore[assignment]

    vertex: dict
    ctx: dict


class TransformPayload(ConfigBaseModel):
    """Typed transform output shared between extraction and assembly phases."""

    named: dict[str, Any] = Field(default_factory=dict)
    positional: tuple[Any, ...] = Field(default_factory=tuple)

    @classmethod
    def from_result(cls, result: Any) -> TransformPayload:
        if isinstance(result, dict):
            return cls(named=dict(result))
        if isinstance(result, tuple):
            return cls(positional=tuple(result))
        return cls(positional=(result,))

    def context_doc(self) -> dict[str, Any]:
        """Named values available for weight and relation extraction."""
        return dict(self.named)


class ProvenancePath(ConfigBaseModel):
    """Explicit provenance path for extracted observations."""

    path: tuple[str | int | None, ...] = Field(default_factory=tuple)

    @classmethod
    def from_lindex(cls, lindex: LocationIndex) -> ProvenancePath:
        return cls(path=lindex.path)


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


class EdgeIntent(ConfigBaseModel):
    """Typed edge assembly request emitted during extraction."""

    edge: Any
    location: LocationIndex | None = None
    provenance: ProvenancePath | None = None
    derivation: EdgeDerivation | None = None


class LocationIndex(ConfigBaseModel):
    """Immutable location index for nested graph traversal."""

    model_config = ConfigDict(frozen=True)

    path: tuple[str | int | None, ...] = Field(default_factory=tuple)

    @model_validator(mode="before")
    @classmethod
    def accept_tuple(cls, data: Any) -> Any:
        """Accept a single tuple as positional path (e.g. LocationIndex((0,)))."""
        if isinstance(data, tuple):
            return {"path": data}
        return data

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Allow LocationIndex((0,)) or LocationIndex(path=(0,))."""
        if args and len(args) == 1 and isinstance(args[0], tuple) and not kwargs:
            kwargs = {"path": args[0]}
        super().__init__(**kwargs)

    def extend(self, extension: tuple[str | int | None, ...]) -> LocationIndex:
        return LocationIndex(path=(*self.path, *extension))

    def depth(self) -> int:
        return len(self.path)

    def congruence_measure(self, other: LocationIndex) -> int:
        neq_position = 0
        for step_a, step_b in zip(self.path, other.path):
            if step_a != step_b:
                break
            neq_position += 1
        return neq_position

    def filter(self, lindex_list: list[LocationIndex]) -> list[LocationIndex]:
        return [
            t
            for t in lindex_list
            if t.depth() >= self.depth() and t.path[: self.depth()] == self.path
        ]

    def __lt__(self, other: LocationIndex) -> bool:
        return len(self.path) < len(other.path)

    def __contains__(self, item: object) -> bool:
        return item in self.path

    def __len__(self) -> int:
        return len(self.path)

    def __iter__(self):
        return iter(self.path)

    def __getitem__(self, item: int | slice):
        return self.path[item]


def _default_dict_transforms() -> defaultdict[LocationIndex, list[Any]]:
    return defaultdict(list)


def _default_edge_requests() -> list[tuple[Any, LocationIndex]]:
    return []


def _default_vertex_observations() -> list[VertexObservation]:
    return []


def _default_transform_observations() -> list[TransformObservation]:
    return []


def _default_edge_intents() -> list[EdgeIntent]:
    return []


class ExtractionContext(ConfigBaseModel):
    """Extraction-phase context.

    Attributes:
        acc_vertex: Local accumulation of extracted vertices
        buffer_transforms: Buffer for transform payloads (defaultdict[LocationIndex, list])
        edge_requests: Explicit edge assembly requests collected during extraction
        vertex_observations: Explicit extracted vertex observations
        transform_observations: Explicit extracted transform observations
        edge_intents: Explicit edge intents for assembly phase
    """

    model_config = ConfigDict(kw_only=True)  # type: ignore[assignment]

    # Pydantic cannot schema nested defaultdict with custom key types (e.g. LocationIndex),
    # so we use Any; runtime type is as documented in Attributes
    acc_vertex: Any = Field(default_factory=outer_factory)
    buffer_transforms: Any = Field(default_factory=_default_dict_transforms)
    edge_requests: Any = Field(default_factory=_default_edge_requests)
    vertex_observations: list[VertexObservation] = Field(
        default_factory=_default_vertex_observations
    )
    transform_observations: list[TransformObservation] = Field(
        default_factory=_default_transform_observations
    )
    edge_intents: list[EdgeIntent] = Field(default_factory=_default_edge_intents)

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


class AssemblyContext(ConfigBaseModel):
    """Assembly-phase context built from extraction outputs."""

    model_config = ConfigDict(kw_only=True)  # type: ignore[assignment]

    extraction: ExtractionContext
    acc_global: Any = Field(default_factory=dd_factory)

    @property
    def acc_vertex(self) -> Any:
        return self.extraction.acc_vertex

    @property
    def buffer_transforms(self) -> Any:
        return self.extraction.buffer_transforms

    @property
    def edge_requests(self) -> Any:
        return self.extraction.edge_requests

    @edge_requests.setter
    def edge_requests(self, value: Any) -> None:
        self.extraction.edge_requests = value

    @property
    def edge_intents(self) -> list[EdgeIntent]:
        return self.extraction.edge_intents

    @classmethod
    def from_extraction(cls, extraction: ExtractionContext) -> AssemblyContext:
        return cls(extraction=extraction)


class GraphAssemblyResult(ConfigBaseModel):
    """Result of graph assembly phase."""

    entities: Any = Field(default_factory=dd_factory)


class ActionContext(ExtractionContext):
    """Backward-compatible extraction+assembly context.

    Kept for existing callers/tests while Wave 5 migrates call surfaces.
    """

    acc_global: Any = Field(default_factory=dd_factory)
