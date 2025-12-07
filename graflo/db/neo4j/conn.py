"""Neo4j connection implementation for graph database operations.

This module implements the Connection interface for Neo4j, providing
specific functionality for graph operations in Neo4j. It handles:
- Node and relationship management
- Cypher query execution
- Index creation and management
- Batch operations
- Graph traversal and pattern matching

Key Features:
    - Label-based node organization
    - Relationship type management
    - Property indices
    - Cypher query execution
    - Batch node and relationship operations

Example:
    >>> conn = Neo4jConnection(config)
    >>> conn.init_db(schema, clean_start=True)
    >>> conn.upsert_docs_batch(docs, "User", match_keys=["email"])
"""

import logging

from neo4j import GraphDatabase

from graflo.architecture.edge import Edge
from graflo.architecture.onto import Index
from graflo.architecture.schema import Schema
from graflo.architecture.vertex import VertexConfig
from graflo.db.conn import Connection
from graflo.filter.onto import Expression
from graflo.onto import AggregationType, DBFlavor, ExpressionFlavor

from ..connection.onto import Neo4jConfig

logger = logging.getLogger(__name__)


class Neo4jConnection(Connection):
    """Neo4j-specific implementation of the Connection interface.

    This class provides Neo4j-specific implementations for all database
    operations, including node management, relationship operations, and
    Cypher query execution. It uses the Neo4j Python driver for all operations.

    Attributes:
        flavor: Database flavor identifier (NEO4J)
        conn: Neo4j session instance
    """

    flavor = DBFlavor.NEO4J

    def __init__(self, config: Neo4jConfig):
        """Initialize Neo4j connection.

        Args:
            config: Neo4j connection configuration containing URL and credentials
        """
        super().__init__()
        # Store config for later use
        self.config = config
        # Ensure url is not None - GraphDatabase.driver requires a non-None URI
        if config.url is None:
            raise ValueError("Neo4j connection requires a URL to be configured")
        self._driver = GraphDatabase.driver(
            uri=config.url, auth=(config.username, config.password)
        )
        self.conn = self._driver.session()

    def execute(self, query, **kwargs):
        """Execute a Cypher query.

        Args:
            query: Cypher query string to execute
            **kwargs: Additional query parameters

        Returns:
            Result: Neo4j query result
        """
        cursor = self.conn.run(query, **kwargs)
        return cursor

    def close(self):
        """Close the Neo4j connection and session."""
        # Close session first, then the underlying driver
        try:
            self.conn.close()
        finally:
            # Ensure the driver is also closed to release resources
            self._driver.close()

    def create_database(self, name: str):
        """Create a new Neo4j database.

        Note: This operation is only supported in Neo4j Enterprise Edition.
        Community Edition only supports one database per instance.

        Args:
            name: Name of the database to create
        """
        try:
            self.execute(f"CREATE DATABASE {name}")
            logger.info(f"Successfully created Neo4j database '{name}'")
        except Exception as e:
            raise e

    def delete_database(self, name: str):
        """Delete a Neo4j database.

        Note: This operation is only supported in Neo4j Enterprise Edition.
        As a fallback, it deletes all nodes and relationships.

        Args:
            name: Name of the database to delete (unused, deletes all data)
        """
        try:
            self.execute("MATCH (n) DETACH DELETE n")
            logger.info("Successfully cleaned Neo4j database")
        except Exception as e:
            logger.error(
                f"Failed to clean Neo4j database: {e}",
                exc_info=True,
            )
            raise

    def define_vertex_indices(self, vertex_config: VertexConfig):
        """Define indices for vertex labels.

        Creates indices for each vertex label based on the configuration.

        Args:
            vertex_config: Vertex configuration containing index definitions
        """
        for c in vertex_config.vertex_set:
            for index_obj in vertex_config.indexes(c):
                self._add_index(c, index_obj)

    def define_edge_indices(self, edges: list[Edge]):
        """Define indices for relationship types.

        Creates indices for each relationship type based on the configuration.

        Args:
            edges: List of edge configurations containing index definitions
        """
        for edge in edges:
            for index_obj in edge.indexes:
                if edge.relation is not None:
                    self._add_index(edge.relation, index_obj, is_vertex_index=False)

    def _add_index(self, obj_name, index: Index, is_vertex_index=True):
        """Add an index to a label or relationship type.

        Args:
            obj_name: Label or relationship type name
            index: Index configuration to create
            is_vertex_index: If True, create index on nodes, otherwise on relationships
        """
        fields_str = ", ".join([f"x.{f}" for f in index.fields])
        fields_str2 = "_".join(index.fields)
        index_name = f"{obj_name}_{fields_str2}"
        if is_vertex_index:
            formula = f"(x:{obj_name})"
        else:
            formula = f"()-[x:{obj_name}]-()"

        q = f"CREATE INDEX {index_name} IF NOT EXISTS FOR {formula} ON ({fields_str});"

        self.execute(q)

    def define_schema(self, schema: Schema):
        """Define collections based on schema.

        Note: This is a no-op in Neo4j as collections are implicit.

        Args:
            schema: Schema containing collection definitions
        """
        pass

    def define_vertex_collections(self, schema: Schema):
        """Define vertex collections based on schema.

        Note: This is a no-op in Neo4j as vertex collections are implicit.

        Args:
            schema: Schema containing vertex definitions
        """
        pass

    def define_edge_collections(self, edges: list[Edge]):
        """Define edge collections based on schema.

        Note: This is a no-op in Neo4j as edge collections are implicit.

        Args:
            edges: List of edge configurations
        """
        pass

    def delete_graph_structure(self, vertex_types=(), graph_names=(), delete_all=False):
        """Delete graph structure (nodes and relationships) from Neo4j.

        In Neo4j:
        - Labels: Categories for nodes (equivalent to vertex types)
        - Relationship Types: Types of relationships (equivalent to edge types)
        - No explicit "graph" concept - all nodes/relationships are in the database

        Args:
            vertex_types: Label names to delete nodes for
            graph_names: Unused in Neo4j (no explicit graph concept)
            delete_all: If True, delete all nodes and relationships
        """
        cnames = vertex_types
        if cnames:
            for c in cnames:
                q = f"MATCH (n:{c}) DELETE n"
                self.execute(q)
        else:
            q = "MATCH (n) DELETE n"
            self.execute(q)

    def init_db(self, schema: Schema, clean_start):
        """Initialize Neo4j with the given schema.

        Checks if the database exists and creates it if it doesn't.
        Uses schema.general.name if database is not set in config.
        Note: Database creation is only supported in Neo4j Enterprise Edition.

        Args:
            schema: Schema containing graph structure definitions
            clean_start: If True, delete all existing data before initialization
        """
        # Determine database name: use config.database if set, otherwise use schema.general.name
        db_name = self.config.database
        if not db_name:
            db_name = schema.general.name
            # Update config for subsequent operations
            self.config.database = db_name

        # Check if database exists and create it if it doesn't
        # Note: This only works in Neo4j Enterprise Edition
        # For Community Edition, we'll try to create it but it may fail gracefully
        # Community Edition only allows one database per instance
        try:
            # Try to check if database exists (Enterprise feature)
            try:
                result = self.execute("SHOW DATABASES")
                # Neo4j result is a cursor-like object, iterate to get records
                databases = []
                for record in result:
                    # Record structure may vary, try common field names
                    if hasattr(record, "get"):
                        db_name_field = (
                            record.get("name")
                            or record.get("database")
                            or record.get("db")
                        )
                    else:
                        # If record is a dict-like object, try direct access
                        db_name_field = getattr(record, "name", None) or getattr(
                            record, "database", None
                        )
                    if db_name_field:
                        databases.append(db_name_field)

                if db_name not in databases:
                    logger.info(
                        f"Database '{db_name}' does not exist, attempting to create it..."
                    )
                    try:
                        self.create_database(db_name)
                        logger.info(f"Successfully created database '{db_name}'")
                    except Exception as create_error:
                        logger.info(
                            f"Neo4j Community Edition? Could not create database '{db_name}': {create_error}. "
                            f"This may be Neo4j Community Edition which only supports one database per instance.",
                            exc_info=True,
                        )
                        # Continue with default database for Community Edition
            except Exception as show_error:
                # If SHOW DATABASES fails (Community Edition or older versions), try to create anyway
                logger.debug(
                    f"Could not check database existence (may be Community Edition): {show_error}"
                )
                try:
                    self.create_database(db_name)
                    logger.info(f"Successfully created database '{db_name}'")
                except Exception as create_error:
                    logger.info(
                        f"Neo4j Community Edition? Could not create database '{db_name}': {create_error}. "
                        f"This may be Neo4j Community Edition which only supports one database per instance. "
                        f"Continuing with default database.",
                        exc_info=True,
                    )
                    # Continue with default database for Community Edition
        except Exception as e:
            logger.error(
                f"Error during database initialization for '{db_name}': {e}",
                exc_info=True,
            )
            # Don't raise - allow operation to continue with default database
            logger.warning(
                "Continuing with default database due to initialization error"
            )

        try:
            if clean_start:
                try:
                    self.delete_database("")
                    logger.debug(f"Cleaned database '{db_name}' for fresh start")
                except Exception as clean_error:
                    logger.warning(
                        f"Error during clean_start for database '{db_name}': {clean_error}",
                        exc_info=True,
                    )
                    # Continue - may be first run or already clean

            try:
                self.define_indexes(schema)
                logger.debug(f"Defined indexes for database '{db_name}'")
            except Exception as index_error:
                logger.error(
                    f"Failed to define indexes for database '{db_name}': {index_error}",
                    exc_info=True,
                )
                raise
        except Exception as e:
            logger.error(
                f"Error during database schema initialization for '{db_name}': {e}",
                exc_info=True,
            )
            raise

    def upsert_docs_batch(self, docs, class_name, match_keys, **kwargs):
        """Upsert a batch of nodes using Cypher.

        Performs an upsert operation on a batch of nodes, using the specified
        match keys to determine whether to update existing nodes or create new ones.

        Args:
            docs: List of node documents to upsert
            class_name: Label to upsert into
            match_keys: Keys to match for upsert operation
            **kwargs: Additional options:
                - dry: If True, don't execute the query
        """
        dry = kwargs.pop("dry", False)

        index_str = ", ".join([f"{k}: row.{k}" for k in match_keys])
        q = f"""
            WITH $batch AS batch 
            UNWIND batch as row 
            MERGE (n:{class_name} {{ {index_str} }}) 
            ON MATCH set n += row 
            ON CREATE set n += row
        """
        if not dry:
            self.execute(q, batch=docs)

    def insert_edges_batch(
        self,
        docs_edges,
        source_class,
        target_class,
        relation_name,
        collection_name=None,
        match_keys_source=("_key",),
        match_keys_target=("_key",),
        filter_uniques=True,
        uniq_weight_fields=None,
        uniq_weight_collections=None,
        upsert_option=False,
        head=None,
        **kwargs,
    ):
        """Insert a batch of relationships using Cypher.

        Creates relationships between source and target nodes, with support for
        property matching and unique constraints.

        Args:
            docs_edges: List of edge documents in format [{__source: source_doc, __target: target_doc}]
            source_class: Source node label
            target_class: Target node label
            relation_name: Relationship type name
            collection_name: Unused in Neo4j
            match_keys_source: Keys to match source nodes
            match_keys_target: Keys to match target nodes
            filter_uniques: Unused in Neo4j
            uniq_weight_fields: Unused in Neo4j
            uniq_weight_collections: Unused in Neo4j
            upsert_option: Unused in Neo4j
            head: Optional limit on number of relationships to insert
            **kwargs: Additional options:
                - dry: If True, don't execute the query
        """
        dry = kwargs.pop("dry", False)

        source_match_str = [f"source.{key} = row[0].{key}" for key in match_keys_source]
        target_match_str = [f"target.{key} = row[1].{key}" for key in match_keys_target]

        match_clause = "WHERE " + " AND ".join(source_match_str + target_match_str)

        q = f"""
            WITH $batch AS batch 
            UNWIND batch as row 
            MATCH (source:{source_class}), 
                  (target:{target_class}) {match_clause} 
                        MERGE (source)-[r:{relation_name}]->(target)
                SET r += row[2]
        
        """
        if not dry:
            self.execute(q, batch=docs_edges)

    def insert_return_batch(self, docs, class_name):
        """Insert nodes and return their properties.

        Note: Not implemented in Neo4j.

        Args:
            docs: Documents to insert
            class_name: Label to insert into

        Raises:
            NotImplementedError: This method is not implemented for Neo4j
        """
        raise NotImplementedError()

    def fetch_docs(
        self,
        class_name,
        filters: list | dict | None = None,
        limit: int | None = None,
        return_keys: list | None = None,
        unset_keys: list | None = None,
        **kwargs,
    ):
        """Fetch nodes from a label.

        Args:
            class_name: Label to fetch from
            filters: Query filters
            limit: Maximum number of nodes to return
            return_keys: Keys to return
            unset_keys: Unused in Neo4j

        Returns:
            list: Fetched nodes
        """
        if filters is not None:
            ff = Expression.from_dict(filters)
            filter_clause = f"WHERE {ff(doc_name='n', kind=DBFlavor.NEO4J)}"
        else:
            filter_clause = ""

        if return_keys is not None:
            keep_clause_ = ", ".join([f".{item}" for item in return_keys])
            keep_clause = f"{{ {keep_clause_} }}"
        else:
            keep_clause = ""

        if limit is not None and isinstance(limit, int):
            limit_clause = f"LIMIT {limit}"
        else:
            limit_clause = ""

        q = (
            f"MATCH (n:{class_name})"
            f"  {filter_clause}"
            f"  RETURN n {keep_clause}"
            f"  {limit_clause}"
        )
        cursor = self.execute(q)
        r = [item["n"] for item in cursor.data()]
        return r

    # TODO test
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
        """Fetch edges from Neo4j using Cypher.

        Args:
            from_type: Source node label
            from_id: Source node ID (property name depends on match_keys used)
            edge_type: Optional relationship type to filter by
            to_type: Optional target node label to filter by
            to_id: Optional target node ID to filter by
            filters: Additional query filters
            limit: Maximum number of edges to return
            return_keys: Keys to return (projection)
            unset_keys: Keys to exclude (projection) - not supported in Neo4j
            **kwargs: Additional parameters

        Returns:
            list: List of fetched edges
        """
        # Build Cypher query to fetch edges
        # Match source node first
        source_match = f"(source:{from_type} {{id: '{from_id}'}})"

        # Build relationship pattern
        if edge_type:
            rel_pattern = f"-[r:{edge_type}]->"
        else:
            rel_pattern = "-[r]->"

        # Build target node match
        if to_type:
            target_match = f"(target:{to_type})"
        else:
            target_match = "(target)"

        # Add target ID filter if provided
        where_clauses = []
        if to_id:
            where_clauses.append(f"target.id = '{to_id}'")

        # Add additional filters if provided
        if filters is not None:
            from graflo.filter.onto import Expression

            ff = Expression.from_dict(filters)
            filter_clause = ff(doc_name="r", kind=ExpressionFlavor.NEO4J)
            where_clauses.append(filter_clause)

        where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        # Build return clause
        if return_keys is not None:
            return_clause = ", ".join([f"r.{key} as {key}" for key in return_keys])
            return_clause = f"RETURN {return_clause}"
        else:
            return_clause = "RETURN r"

        limit_clause = f"LIMIT {limit}" if limit else ""

        query = f"""
            MATCH {source_match}{rel_pattern}{target_match}
            {where_clause}
            {return_clause}
            {limit_clause}
        """

        cursor = self.execute(query)
        result = [item["r"] for item in cursor.data()]

        # Note: unset_keys is not supported in Neo4j as we can't modify the result structure
        # after the query

        return result

    def fetch_present_documents(
        self,
        batch,
        class_name,
        match_keys,
        keep_keys,
        flatten=False,
        filters: list | dict | None = None,
    ):
        """Fetch nodes that exist in the database.

        Note: Not implemented in Neo4j.

        Args:
            batch: Batch of documents to check
            class_name: Label to check in
            match_keys: Keys to match nodes
            keep_keys: Keys to keep in result
            flatten: Unused in Neo4j
            filters: Additional query filters

        Raises:
            NotImplementedError: This method is not implemented for Neo4j
        """
        raise NotImplementedError

    def aggregate(
        self,
        class_name,
        aggregation_function: AggregationType,
        discriminant: str | None = None,
        aggregated_field: str | None = None,
        filters: list | dict | None = None,
    ):
        """Perform aggregation on nodes.

        Note: Not implemented in Neo4j.

        Args:
            class_name: Label to aggregate
            aggregation_function: Type of aggregation to perform
            discriminant: Field to group by
            aggregated_field: Field to aggregate
            filters: Query filters

        Raises:
            NotImplementedError: This method is not implemented for Neo4j
        """
        raise NotImplementedError

    def keep_absent_documents(
        self,
        batch,
        class_name,
        match_keys,
        keep_keys,
        filters: list | dict | None = None,
    ):
        """Keep nodes that don't exist in the database.

        Note: Not implemented in Neo4j.

        Args:
            batch: Batch of documents to check
            class_name: Label to check in
            match_keys: Keys to match nodes
            keep_keys: Keys to keep in result
            filters: Additional query filters

        Raises:
            NotImplementedError: This method is not implemented for Neo4j
        """
        raise NotImplementedError
