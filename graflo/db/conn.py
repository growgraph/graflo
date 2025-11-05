"""Abstract database connection interface for graph databases.

This module defines the abstract interface for database connections, providing
a unified API for different graph database implementations. It includes methods
for database management, collection operations, and data manipulation.

Key Components:
    - Connection: Abstract base class for database connections
    - ConnectionType: Type variable for connection implementations

The connection interface supports:
    - Database creation and deletion
    - Collection management
    - Index definition
    - Document operations (insert, update, fetch)
    - Edge operations
    - Aggregation queries

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
    must follow. It provides methods for database operations, collection management,
    and data manipulation.

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
    def delete_collections(self, cnames=(), gnames=(), delete_all=False):
        """Delete collections from the database.

        Args:
            cnames: Collection names to delete
            gnames: Graph names to delete
            delete_all: Whether to delete all collections
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
            class_name: Name of the collection
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
            source_class: Source vertex class
            target_class: Target vertex class
            relation_name: Name of the relation
            collection_name: Name of the edge collection
            match_keys_source: Keys to match source vertices
            match_keys_target: Keys to match target vertices
            filter_uniques: Whether to filter unique edges
            uniq_weight_fields: Fields to consider for uniqueness
            uniq_weight_collections: Collections to consider for uniqueness
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
            class_name: Name of the collection

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
        """Fetch documents from a collection.

        Args:
            class_name: Name of the collection
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
