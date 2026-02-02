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
from typing import Any, ClassVar, TypeVar

from graflo.architecture.edge import Edge
from graflo.architecture.schema import Schema
from graflo.architecture.vertex import VertexConfig
from graflo.onto import (
    AggregationType,
    DB_TYPE_TO_EXPRESSION_FLAVOR,
    DBType,
    ExpressionFlavor,
)

logger = logging.getLogger(__name__)
ConnectionType = TypeVar("ConnectionType", bound="Connection")


class SchemaExistsError(RuntimeError):
    """Raised when schema/graph already exists and recreate_schema is False.

    Set recreate_schema=True to replace the existing schema, or use clear_data=True
    before ingestion to only clear data without touching the schema.
    """


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

    def __init__(self):
        """Initialize the connection."""
        pass

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
        pass

    @abc.abstractmethod
    def delete_database(self, name: str):
        """Delete a database.

        Args:
            name: Name of the database to delete
        """
        pass

    @abc.abstractmethod
    def execute(self, query: str | Any, **kwargs: Any) -> Any:
        """Execute a database query.

        Args:
            query: Query to execute
            **kwargs: Additional query parameters

        Returns:
            Query result (database-specific)
        """
        pass

    @abc.abstractmethod
    def close(self):
        """Close the database connection."""
        pass

    def define_indexes(self, schema: Schema):
        """Define indexes for vertices and edges in the schema.

        Args:
            schema: Schema containing vertex and edge configurations
        """
        self.define_vertex_indices(schema.vertex_config)
        self.define_edge_indices(schema.edge_config.edges_list(include_aux=True))

    @abc.abstractmethod
    def define_schema(self, schema: Schema):
        """Define vertex and edge classes based on the schema.

        Args:
            schema: Schema containing vertex and edge class definitions
        """
        pass

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
            delete_all: If True, delete all graphs and their associated structures
        """
        pass

    @abc.abstractmethod
    def init_db(self, schema: Schema, recreate_schema: bool) -> None:
        """Initialize the database with the given schema.

        If the schema/graph already exists and recreate_schema is False, raises
        SchemaExistsError and the script halts.

        Args:
            schema: Schema to initialize the database with
            recreate_schema: If True, drop existing schema and define new one.
                If False and schema/graph already exists, raises SchemaExistsError.
        """
        pass

    @abc.abstractmethod
    def clear_data(self, schema: Schema) -> None:
        """Remove all data from the graph without dropping or changing the schema.

        Args:
            schema: Schema describing the graph (used to identify collections/labels).
        """
        pass

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
        pass

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
            **kwargs: Additional insertion parameters, including:
                - collection_name: Name of the edge type (database-specific: collection/relationship type).
                  Required for ArangoDB (defaults to {source_class}_{target_class}_edges if not provided),
                  optional for other databases.
                - uniq_weight_fields: Fields to consider for uniqueness (ArangoDB-specific)
                - uniq_weight_collections: Vertex/edge types to consider for uniqueness (ArangoDB-specific)
                - upsert_option: Whether to upsert existing edges (ArangoDB-specific)
        """
        pass

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
        pass

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
        pass

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
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Fetch edges from the database.

        Args:
            from_type: Source vertex type
            from_id: Source vertex ID (required)
            edge_type: Optional edge type to filter by
            to_type: Optional target vertex type to filter by
            to_id: Optional target vertex ID to filter by
            filters: Additional query filters
            limit: Maximum number of edges to return
            return_keys: Keys to return (projection)
            unset_keys: Keys to exclude (projection)
            **kwargs: Additional database-specific parameters

        Returns:
            list: List of fetched edges
        """
        pass

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
        pass

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
        pass

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
        pass

    @abc.abstractmethod
    def define_vertex_indices(self, vertex_config: VertexConfig):
        """Define indices for vertex classes.

        Args:
            vertex_config: Vertex configuration containing index definitions
        """
        pass

    @abc.abstractmethod
    def define_edge_indices(self, edges: list[Edge]):
        """Define indices for edge classes.

        Args:
            edges: List of edge configurations containing index definitions
        """
        pass

    def define_vertex_classes(self, schema: Schema) -> None:
        """Define vertex classes based on schema.

        This method is called from define_schema() to create vertex types/collections.
        Most implementations take a Schema. Some implementations (like TigerGraph)
        may override with a more specific signature (VertexConfig).

        Default implementation is a no-op. Override in subclasses as needed.

        Args:
            schema: Schema containing vertex definitions
        """
        pass

    def define_edge_classes(self, edges: list[Edge]) -> None:
        """Define edge classes based on edge configurations.

        This method is called from define_schema() to create edge types/collections.

        Default implementation is a no-op. Override in subclasses as needed.

        Args:
            edges: List of edge configurations to create
        """
        pass
