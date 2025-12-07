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
        * Collections: Container for vertices (vertex collections)
        * Edge Collections: Container for edges
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
from typing import TypeVar

from graflo.architecture.edge import Edge
from graflo.architecture.schema import Schema
from graflo.architecture.vertex import VertexConfig
from graflo.onto import AggregationType

logger = logging.getLogger(__name__)
ConnectionType = TypeVar("ConnectionType", bound="Connection")


class Connection(abc.ABC):
    """Abstract base class for database connections.

    This class defines the interface that all database connection implementations
    must follow. It provides methods for database/graph operations, graph structure
    management (vertex types, edge types), and data manipulation.

    Note:
        All methods marked with @abc.abstractmethod must be implemented by
        concrete connection classes.
    """

    def __init__(self):
        """Initialize the connection."""
        pass

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
    def execute(self, query, **kwargs):
        """Execute a database query.

        Args:
            query: Query to execute
            **kwargs: Additional query parameters
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
        """Define collections based on the schema.

        Args:
            schema: Schema containing collection definitions
        """
        pass

    @abc.abstractmethod
    def delete_graph_structure(self, vertex_types=(), graph_names=(), delete_all=False):
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
    def init_db(self, schema: Schema, clean_start):
        """Initialize the database with the given schema.

        Args:
            schema: Schema to initialize the database with
            clean_start: Whether to clean existing data
        """
        pass

    @abc.abstractmethod
    def upsert_docs_batch(self, docs, class_name, match_keys, **kwargs):
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
        docs_edges,
        source_class,
        target_class,
        relation_name,
        collection_name,
        match_keys_source,
        match_keys_target,
        filter_uniques=True,
        uniq_weight_fields=None,
        uniq_weight_collections=None,
        upsert_option=False,
        head=None,
        **kwargs,
    ):
        """Insert a batch of edges.

        Args:
            docs_edges: Edge documents to insert
            source_class: Source vertex type/class
            target_class: Target vertex type/class
            relation_name: Name of the edge type/relation
            collection_name: Name of the edge type (database-specific: collection/relationship type)
            match_keys_source: Keys to match source vertices
            match_keys_target: Keys to match target vertices
            filter_uniques: Whether to filter unique edges
            uniq_weight_fields: Fields to consider for uniqueness
            uniq_weight_collections: Vertex/edge types to consider for uniqueness (database-specific)
            upsert_option: Whether to upsert existing edges
            head: Optional head document
            **kwargs: Additional insertion parameters
        """
        pass

    @abc.abstractmethod
    def insert_return_batch(self, docs, class_name):
        """Insert documents and return the inserted documents.

        Args:
            docs: Documents to insert
            class_name: Name of the vertex type (or collection/label in database-specific terms)

        Returns:
            list: Inserted documents
        """
        pass

    @abc.abstractmethod
    def fetch_docs(
        self,
        class_name,
        filters,
        limit,
        return_keys,
        unset_keys,
        **kwargs,
    ):
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
        filters: list | dict | None = None,
        limit: int | None = None,
        return_keys: list | None = None,
        unset_keys: list | None = None,
        **kwargs,
    ):
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
        batch,
        class_name,
        match_keys,
        keep_keys,
        flatten=False,
        filters: list | dict | None = None,
    ):
        """Fetch documents that exist in the database.

        Args:
            batch: Batch of documents to check
            class_name: Name of the collection
            match_keys: Keys to match
            keep_keys: Keys to keep in result
            flatten: Whether to flatten the result
            filters: Additional query filters

        Returns:
            list: Documents that exist in the database
        """
        pass

    @abc.abstractmethod
    def aggregate(
        self,
        class_name,
        aggregation_function: AggregationType,
        discriminant: str | None = None,
        aggregated_field: str | None = None,
        filters: list | dict | None = None,
    ):
        """Perform aggregation on a collection.

        Args:
            class_name: Name of the collection
            aggregation_function: Type of aggregation to perform
            discriminant: Field to group by
            aggregated_field: Field to aggregate
            filters: Query filters

        Returns:
            dict: Aggregation results
        """
        pass

    @abc.abstractmethod
    def keep_absent_documents(
        self,
        batch,
        class_name,
        match_keys,
        keep_keys,
        filters: list | dict | None = None,
    ):
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
        """Define indices for vertex collections.

        Args:
            vertex_config: Vertex configuration containing index definitions
        """
        pass

    @abc.abstractmethod
    def define_edge_indices(self, edges: list[Edge]):
        """Define indices for edge collections.

        Args:
            edges: List of edge configurations containing index definitions
        """
        pass

    # @abc.abstractmethod
    # def define_vertex_collections(self, graph_config, vertex_config):
    #     pass
    #
    # @abc.abstractmethod
    # def define_edge_collections(self, graph_config):
    #     pass

    # @abc.abstractmethod
    # def create_collection_if_absent(self, g, vcol, index, unique=True):
    #     pass
