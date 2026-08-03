"""Abstract database connection interface for graph databases.

This module defines the abstract interface for database connections, providing
a unified API for different graph database implementations. It includes methods
for database management, graph structure operations, and data manipulation.

Key Components:

    - Connection: Abstract base class for database connections
    - ConnectionType: Type variable for connection implementations

The connection interface supports:

    - Database/Graph creation and deletion
    - Graph structure management (vertex types, edge types)
    - Index definition
    - Document operations (insert, update, fetch)
    - Edge operations
    - Aggregation queries

Database Organization Terminology:
    Different databases organize graph data differently:

    - ArangoDB:
        * Database: Top-level container (like a schema)
        * Collections (ArangoDB-specific): Container for vertices (vertex collections)
        * Edge Collections (ArangoDB-specific): Container for edges
        * Graph: Named graph that connects vertex and edge collections

    - Neo4j:
        * Database: Top-level container
        * Labels: Categories for nodes (equivalent to vertex types)
        * Relationship Types: Types of relationships (equivalent to edge types)
        * No explicit "graph" concept - all nodes/relationships are in the database

    - TigerGraph:
        * Graph: Top-level container (functions like a database in ArangoDB)
        * Vertex Types: Global vertex type definitions (can be shared across graphs)
        * Edge Types: Global edge type definitions (can be shared across graphs)
        * Vertex and edge types are associated with graphs

    When using the Connection interface, the terms "vertex type" and "edge type"
    are used generically to refer to the appropriate concept in each database.

Example:
    >>> class MyConnection(Connection):
    ...     def create_database(self, name: str):
    ...         # Implementation
    ...     def execute(self, query, **kwargs):
    ...         # Implementation
"""

import abc
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Literal, TypeVar

from graflo.architecture.graph_types import EdgeDirection, GraphContainer
from graflo.architecture.schema import Schema
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.vertex import VertexConfig
from graflo.connections.onto import TigergraphBulkLoadConfig
from graflo.db.bulk_exc import UnsupportedBulkLoad
from graflo.db.resolve import (
    DEFAULT_RESOLVE_CHUNK_SIZE,
    bucket_by_key,
    build_match_filter,
    chunked,
    distinct_keys,
    index_matches_by_doc,
)
from graflo.onto import (
    DB_TYPE_TO_EXPRESSION_FLAVOR,
    AggregationType,
    DBType,
    ExpressionFlavor,
)

if TYPE_CHECKING:
    from graflo.architecture.contract.bindings import Bindings
    from graflo.architecture.graph_types import EdgeId
    from graflo.connections.provider import ConnectionProvider
    from graflo.db.edge_direction_support import EdgeDirectionDiagnostic

logger = logging.getLogger(__name__)
ConnectionType = TypeVar("ConnectionType", bound="Connection")


def _parse_on_duplicate(value: Any) -> Literal["upsert", "ignore"]:
    if not isinstance(value, str):
        raise TypeError(
            "on_duplicate must be str ('upsert' or 'ignore'), "
            f"got {type(value).__name__}"
        )
    if value == "upsert":
        return "upsert"
    if value == "ignore":
        return "ignore"
    raise ValueError(f"on_duplicate must be 'upsert' or 'ignore', got {value!r}")


@dataclass(frozen=True)
class InsertEdgesKwArgs:
    """Keyword arguments shared by :meth:`Connection.insert_edges_batch` implementations."""

    dry: bool
    collection_name: str | None
    uniq_weight_fields: Any
    uniq_weight_collections: Any
    on_duplicate: Literal["upsert", "ignore"]
    relationship_merge_properties: Any


def consume_insert_edges_kwargs(kwargs: dict[str, Any]) -> InsertEdgesKwArgs:
    """Pop standard ``insert_edges_batch`` keys from *kwargs* and warn on unknown keys.

    Mutates *kwargs* in place (removes consumed keys). Callers should not pass
    additional keyword arguments beyond those documented on
    :meth:`Connection.insert_edges_batch`.
    """
    result = InsertEdgesKwArgs(
        dry=bool(kwargs.pop("dry", False)),
        collection_name=kwargs.pop("collection_name", None),
        uniq_weight_fields=kwargs.pop("uniq_weight_fields", None),
        uniq_weight_collections=kwargs.pop("uniq_weight_collections", None),
        on_duplicate=_parse_on_duplicate(kwargs.pop("on_duplicate", "ignore")),
        relationship_merge_properties=kwargs.pop("relationship_merge_properties", None),
    )
    if kwargs:
        logger.warning(
            "insert_edges_batch: unsupported keyword arguments ignored: %s",
            sorted(kwargs.keys()),
        )
        kwargs.clear()
    return result


class SchemaExistsError(RuntimeError):
    """Raised when schema artifacts already exist and recreate is False.

    Set recreate=True in apply_target_schema (or recreate_schema=True in init_db)
    to replace the existing schema, or use clear_data=True before ingestion to
    only clear data without touching the schema.
    """


class NamespaceNotFoundError(RuntimeError):
    """Raised when create=False and the target graph/database/space does not exist."""


class Connection(abc.ABC):
    """Abstract base class for database connections.

    This class defines the interface that all database connection implementations
    must follow. It provides methods for database/graph operations, graph structure
    management (vertex types, edge types), and data manipulation.

    Note:
        All methods marked with @abc.abstractmethod must be implemented by
        concrete connection classes. Subclasses must set the class attribute
        `flavor` to their DBType.
    """

    flavor: ClassVar[DBType] = DBType.ARANGO  # Overridden by subclasses
    supports_graph_export: ClassVar[bool] = False

    def __init__(self):
        """Initialize the connection."""

    @classmethod
    def expression_flavor(cls) -> ExpressionFlavor:
        """Expression flavor for filter rendering (AQL, CYPHER, GSQL).

        Graph connection subclasses must set class attribute `flavor` to a
        DBType present in DB_TYPE_TO_EXPRESSION_FLAVOR.
        """
        return DB_TYPE_TO_EXPRESSION_FLAVOR[cls.flavor]

    @abc.abstractmethod
    def create_database(self, name: str):
        """Create a new database.

        Args:
            name: Name of the database to create
        """

    @abc.abstractmethod
    def delete_database(self, name: str):
        """Delete a database.

        Args:
            name: Name of the database to delete
        """

    @abc.abstractmethod
    def execute(self, query: str | Any, **kwargs: Any) -> Any:
        """Execute a database query.

        Args:
            query: Query to execute
            **kwargs: Additional query parameters

        Returns:
            Query result (database-specific)
        """

    @abc.abstractmethod
    def close(self):
        """Close the database connection."""

    def define_indexes(self, schema: Schema):
        """Define indexes for vertices and edges in the schema.

        Args:
            schema: Schema containing vertex and edge configurations
        """
        self.define_vertex_indexes(schema.core_schema.vertex_config, schema=schema)
        self.define_edge_indexes(
            list(schema.core_schema.edge_config.values()), schema=schema
        )

    @abc.abstractmethod
    def define_schema(self, schema: Schema):
        """Define vertex and edge classes based on the schema.

        Args:
            schema: Schema containing vertex and edge class definitions
        """

    @abc.abstractmethod
    def delete_graph_structure(
        self,
        vertex_types: tuple[str, ...] | list[str] = (),
        graph_names: tuple[str, ...] | list[str] = (),
        delete_all: bool = False,
    ) -> None:
        """Delete graph structure (graphs, vertex types, edge types) from the database.

        This method deletes graphs and their associated vertex/edge types.
        The exact behavior depends on the database implementation:

        - ArangoDB: Deletes graphs and collections (vertex/edge collections)
        - Neo4j: Deletes nodes from labels (vertex types) and relationships
        - TigerGraph: Deletes graphs, vertex types, edge types, and jobs

        Args:
            vertex_types: Vertex type names to delete (database-specific interpretation)
            graph_names: Graph/database names to delete
            delete_all: If True, delete all targeted graph structures.
                This is destructive and should only be used with explicit intent.
        """

    @abc.abstractmethod
    def ensure_target_namespace(self, schema: Schema, *, create: bool) -> None:
        """Ensure the target graph/database/space namespace exists (op 1).

        Args:
            schema: Schema whose metadata/config resolves the namespace name.
            create: If True, create the namespace when missing (idempotent where
                supported). If False, require an existing namespace or raise
                NamespaceNotFoundError.
        """

    @abc.abstractmethod
    def apply_target_schema(
        self,
        schema: Schema,
        *,
        recreate: bool,
        create_namespace: bool = True,
    ) -> None:
        """Define vertex/edge schema artifacts and indexes (op 2).

        Args:
            schema: Schema to apply.
            recreate: If True, drop existing schema artifacts before defining.
                If False and artifacts already exist, raises SchemaExistsError.
            create_namespace: Whether namespace creation is allowed. Backends use
                this during recreate to decide if the graph/db shell may be dropped.
        """

    @property
    def _reported_edge_directions(self) -> set["EdgeId"]:
        """Edges already reported by :meth:`report_edge_direction_support`.

        Held in ``__dict__`` rather than set in ``__init__`` so every backend
        gets it without touching eight constructors.
        """
        seen = self.__dict__.get("_edge_direction_seen")
        if seen is None:
            seen = set()
            self.__dict__["_edge_direction_seen"] = seen
        return seen

    def edge_direction_diagnostics(
        self, schema: Schema
    ) -> list["EdgeDirectionDiagnostic"]:
        """How this backend will treat each logically undirected edge in ``schema``.

        Returned as data, not just logged, so an API or UI can surface it —
        ``Edge.directed`` is authored far from where its consequences land, and
        a log line reaches nobody driving GraFlo over HTTP. Empty when the
        schema declares no undirected edges, or when the backend represents them
        natively. See :mod:`graflo.db.edge_direction_support`.
        """
        from graflo.db.edge_direction_support import check_schema_edge_directions

        return check_schema_edge_directions(self.flavor, schema)

    def report_edge_direction_support(self, schema: Schema) -> None:
        """Log :meth:`edge_direction_diagnostics` once per distinct edge.

        Backends call this from :meth:`apply_target_schema`, which runs on every
        define/recreate; the diagnostics are a static property of the schema and
        the target, so repeating them each time is noise.
        """
        for diagnostic in self.edge_direction_diagnostics(schema):
            if diagnostic.edge_id in self._reported_edge_directions:
                continue
            self._reported_edge_directions.add(diagnostic.edge_id)
            level = (
                logging.WARNING if diagnostic.severity == "warning" else logging.INFO
            )
            logger.log(level, "%s %s", diagnostic.message, diagnostic.remedy)

    def init_db(
        self,
        schema: Schema,
        recreate_schema: bool = False,
        *,
        create_namespace: bool = True,
    ) -> None:
        """Convenience wrapper: ensure namespace then apply schema.

        Prefer calling ensure_target_namespace and apply_target_schema directly.
        """
        self.ensure_target_namespace(schema, create=create_namespace)
        self.apply_target_schema(
            schema, recreate=recreate_schema, create_namespace=create_namespace
        )

    @abc.abstractmethod
    def clear_data(self, schema: Schema) -> None:
        """Remove all data from the graph without dropping or changing the schema.

        Args:
            schema: Schema describing the graph (used to identify collections/labels).
        """

    @abc.abstractmethod
    def upsert_docs_batch(
        self,
        docs: list[dict[str, Any]],
        class_name: str,
        match_keys: list[str] | tuple[str, ...],
        **kwargs: Any,
    ) -> None:
        """Upsert a batch of documents.

        Args:
            docs: Documents to upsert
            class_name: Name of the vertex type (or collection/label in database-specific terms)
            match_keys: Keys to match for upsert
            **kwargs: Additional upsert parameters
        """

    @abc.abstractmethod
    def insert_edges_batch(
        self,
        docs_edges: list[list[dict[str, Any]]] | list[Any] | None,
        source_class: str,
        target_class: str,
        relation_name: str,
        match_keys_source: tuple[str, ...],
        match_keys_target: tuple[str, ...],
        filter_uniques: bool = True,
        head: int | None = None,
        **kwargs: Any,
    ) -> None:
        """Insert a batch of edges.

        Args:
            docs_edges: Edge documents to insert
            source_class: Source vertex type/class
            target_class: Target vertex type/class
            relation_name: Name of the edge type/relation
            match_keys_source: Keys to match source vertices
            match_keys_target: Keys to match target vertices
            filter_uniques: Whether to filter unique edges
            head: Optional limit on number of edges to insert
            **kwargs: Additional insertion parameters (see also
                :func:`consume_insert_edges_kwargs`):
                - dry: If True, do not execute writes (supported where implemented)
                - collection_name: Edge collection (ArangoDB) or unused type-specific name
                - uniq_weight_fields: Uniqueness fields (ArangoDB UPSERT match)
                - uniq_weight_collections: Uniqueness collections (ArangoDB UPSERT)
                - on_duplicate: ArangoDB only. ``\"ignore\"`` (default): ``INSERT`` with
                  ``ignoreErrors``; ``\"upsert\"``: AQL ``UPSERT`` when a matching edge
                  may already exist (align match keys with a unique index).
                - relationship_merge_properties: Property names for Cypher MERGE
                  (Neo4j, FalkorDB, Memgraph) so parallel edges differ by weights
        """

    @abc.abstractmethod
    def insert_return_batch(
        self, docs: list[dict[str, Any]], class_name: str
    ) -> list[dict[str, Any]] | str:
        """Insert documents and return the inserted documents.

        Args:
            docs: Documents to insert
            class_name: Name of the vertex type (or collection/label in database-specific terms)

        Returns:
            list | str: Inserted documents, or a query string (database-specific behavior).
                Most implementations return a list of inserted documents. ArangoDB returns
                an AQL query string for deferred execution.
        """

    @abc.abstractmethod
    def fetch_docs(
        self,
        class_name: str,
        filters: list[Any] | dict[str, Any] | None = None,
        limit: int | None = None,
        return_keys: list[str] | None = None,
        unset_keys: list[str] | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Fetch documents from a vertex type.

        Args:
            class_name: Name of the vertex type (or collection/label in database-specific terms)
            filters: Query filters
            limit: Maximum number of documents to return
            return_keys: Keys to return
            unset_keys: Keys to unset
            **kwargs: Additional database-specific parameters (e.g., field_types for TigerGraph)

        Returns:
            list: Fetched documents
        """

    @abc.abstractmethod
    def fetch_edges(
        self,
        from_type: str,
        from_id: str,
        edge_type: str | None = None,
        to_type: str | None = None,
        to_id: str | None = None,
        filters: list[Any] | dict[str, Any] | None = None,
        limit: int | None = None,
        return_keys: list[str] | None = None,
        unset_keys: list[str] | None = None,
        direction: EdgeDirection = EdgeDirection.OUT,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Fetch edges incident to one vertex.

        ``from_type`` / ``from_id`` name the **anchor** vertex; ``direction``
        decides which orientations are followed from it, and ``to_type`` /
        ``to_id`` constrain the vertex at the *other* end — whichever end that
        is. For a logically undirected edge the correct value is
        :attr:`EdgeDirection.ANY`; :func:`~graflo.db.edge_direction_support.default_direction_for_edge`
        derives it from the schema edge.

        Args:
            from_type: Anchor vertex type
            from_id: Anchor vertex ID (required)
            edge_type: Optional edge type to filter by
            to_type: Optional vertex type of the other endpoint
            to_id: Optional vertex ID of the other endpoint
            filters: Additional query filters
            limit: Maximum number of edges to return
            return_keys: Keys to return (projection)
            unset_keys: Keys to exclude (projection)
            direction: Orientations to follow from the anchor. Defaults to
                :attr:`EdgeDirection.OUT`, the historical behaviour.
            **kwargs: Additional database-specific parameters

        Returns:
            list: List of fetched edges

        Raises:
            UnsupportedEdgeDirectionError: if the backend cannot follow the edge
                in the requested direction (TigerGraph without a reverse type).
        """

    @abc.abstractmethod
    def fetch_present_documents(
        self,
        batch: list[dict[str, Any]],
        class_name: str,
        match_keys: list[str] | tuple[str, ...],
        keep_keys: list[str] | tuple[str, ...] | None = None,
        flatten: bool = False,
        filters: list[Any] | dict[str, Any] | None = None,
    ) -> list[dict[str, Any]] | dict[int, list[dict[str, Any]]]:
        """Fetch documents that exist in the database.

        Args:
            batch: Batch of documents to check
            class_name: Name of the collection
            match_keys: Keys to match
            keep_keys: Keys to keep in result
            flatten: Whether to flatten the result. If True, returns a flat list.
                If False, returns a dict mapping batch indices to matching documents.
            filters: Additional query filters

        Returns:
            list | dict: Documents that exist in the database. Returns a list if
                flatten=True, otherwise returns a dict mapping batch indices to documents.
        """

    def resolve_vertices(
        self,
        class_name: str,
        key_docs: list[dict[str, Any]],
        match_keys: tuple[str, ...],
        return_keys: tuple[str, ...],
        *,
        chunk_size: int = DEFAULT_RESOLVE_CHUNK_SIZE,
    ) -> dict[int, list[dict[str, Any]]]:
        """Locate vertices by an arbitrary field-set, preserving multiplicity.

        Used to attach edge endpoints declared by a *secondary identity*: the
        caller passes documents carrying the secondary fields and gets back the
        matching vertices projected onto *return_keys* (the primary identity),
        so the edge write itself stays a plain primary-key operation.

        Unlike :meth:`fetch_present_documents`, every match is returned rather
        than the first, because the caller's ambiguity policy needs the count.

        This default implementation issues one filtered
        :meth:`fetch_docs` per chunk of distinct keys and works on any backend
        whose ``fetch_docs`` honours ``filters``. Backends override it where a
        cheaper or more expressive lookup exists.

        Args:
            class_name: Storage name of the vertex type to search
            key_docs: Documents carrying values for *match_keys*
            match_keys: Field-set to match on (the secondary identity)
            return_keys: Fields to project onto the matched vertices
            chunk_size: Distinct keys per lookup query

        Returns:
            dict: Position in *key_docs* -> every vertex it matched. Positions
                with an unresolvable (partial) key or no match are absent.
        """
        if not key_docs or not match_keys:
            return {}

        keys = distinct_keys(key_docs, match_keys)
        if not keys:
            return {}

        fetch_keys = list(dict.fromkeys([*match_keys, *return_keys]))
        buckets: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        for chunk in chunked(keys, chunk_size):
            filters = build_match_filter(match_keys, chunk)
            docs = self.fetch_docs(
                class_name,
                filters=filters,
                return_keys=fetch_keys,
            )
            for key, matched in bucket_by_key(list(docs or []), match_keys).items():
                buckets.setdefault(key, []).extend(matched)

        return index_matches_by_doc(key_docs, match_keys, buckets)

    @abc.abstractmethod
    def aggregate(
        self,
        class_name: str,
        aggregation_function: AggregationType,
        discriminant: str | None = None,
        aggregated_field: str | None = None,
        filters: list[Any] | dict[str, Any] | None = None,
    ) -> int | float | list[dict[str, Any]] | dict[str, int | float] | None:
        """Perform aggregation on a collection.

        Args:
            class_name: Name of the collection
            aggregation_function: Type of aggregation to perform
            discriminant: Field to group by
            aggregated_field: Field to aggregate
            filters: Query filters

        Returns:
            Aggregation results (type depends on aggregation function)
        """

    @abc.abstractmethod
    def keep_absent_documents(
        self,
        batch: list[dict[str, Any]],
        class_name: str,
        match_keys: list[str] | tuple[str, ...],
        keep_keys: list[str] | tuple[str, ...] | None = None,
        filters: list[Any] | dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Keep documents that don't exist in the database.

        Args:
            batch: Batch of documents to check
            class_name: Name of the collection
            match_keys: Keys to match
            keep_keys: Keys to keep in result
            filters: Additional query filters

        Returns:
            list: Documents that don't exist in the database
        """

    @abc.abstractmethod
    def define_vertex_indexes(
        self, vertex_config: VertexConfig, schema: Schema | None = None
    ):
        """Define indexes for vertex classes.

        Args:
            vertex_config: Vertex configuration containing index definitions
        """

    @abc.abstractmethod
    def define_edge_indexes(self, edges: list[Edge], schema: Schema | None = None):
        """Define indexes for edge classes.

        Args:
            edges: List of edge configurations containing index definitions
        """

    def define_vertex_classes(self, schema: Schema) -> None:
        """Define vertex classes based on schema.

        This method is called from define_schema() to create vertex types/collections.
        Most implementations take a Schema. Some implementations (like TigerGraph)
        may override with a more specific signature (VertexConfig).

        Default implementation is a no-op. Override in subclasses as needed.

        Args:
            schema: Schema containing vertex definitions
        """

    def define_edge_classes(self, edges: list[Edge]) -> None:
        """Define edge classes based on edge configurations.

        This method is called from define_schema() to create edge types/collections.

        Default implementation is a no-op. Override in subclasses as needed.

        Args:
            edges: List of edge configurations to create
        """

    def bulk_load_begin(
        self, schema: Schema, bulk_cfg: TigergraphBulkLoadConfig
    ) -> str:
        """Start a native bulk-load session (CSV staging + LOADING JOB).

        Raises:
            UnsupportedBulkLoad: For backends that only support REST/document APIs.
        """
        raise UnsupportedBulkLoad(
            f"Database flavor {self.flavor!r} does not support native bulk load"
        )

    def bulk_load_append(
        self, session_id: str, gc: GraphContainer, schema: Schema
    ) -> None:
        """Append one cast batch to the active bulk-load session."""
        raise UnsupportedBulkLoad(
            f"Database flavor {self.flavor!r} does not support native bulk load"
        )

    def bulk_load_finalize(
        self,
        session_id: str,
        schema: Schema,
        *,
        bindings: "Bindings | None" = None,
        connection_provider: "ConnectionProvider | None" = None,
    ) -> str:
        """Close staging files, optionally upload to S3, run LOADING JOB, return GSQL log text."""
        raise UnsupportedBulkLoad(
            f"Database flavor {self.flavor!r} does not support native bulk load"
        )

    def introspect_graph_schema(
        self,
        schema_name: str | None = None,
        *,
        sample_limit: int = 100,
    ) -> Schema:
        """Infer a graflo :class:`Schema` from this graph database.

        Graph connection subclasses implement sampling-based introspection.
        """
        raise NotImplementedError(
            f"introspect_graph_schema is not implemented for {type(self).__name__}"
        )

    def fetch_all_docs(
        self,
        class_name: str,
        *,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch all documents for a vertex type/collection."""
        raise NotImplementedError(
            f"fetch_all_docs is not implemented for {type(self).__name__}"
        )

    def fetch_all_edges(
        self,
        source_class: str,
        target_class: str,
        relation_name: str | None,
        *,
        match_keys_source: tuple[str, ...] | None = None,
        match_keys_target: tuple[str, ...] | None = None,
        limit: int | None = None,
        collection_name: str | None = None,
    ) -> list[list[dict[str, Any]]]:
        """Fetch all edges between two vertex types.

        Returns:
            List of ``[source_doc, target_doc, edge_properties]`` triples.
        """
        raise NotImplementedError(
            f"fetch_all_edges is not implemented for {type(self).__name__}"
        )
