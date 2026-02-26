"""FalkorDB connection implementation for graph database operations.

This module implements the Connection interface for FalkorDB, providing
specific functionality for graph operations in FalkorDB. It handles:

- Node and relationship management
- Cypher query execution
- Index creation and management
- Batch operations
- Input sanitization and validation

Key Features:

    - Label-based node organization (like Neo4j)
    - Relationship type management
    - Property indices
    - OpenCypher query execution
    - Batch node and relationship operations
    - Redis-based storage with graph namespacing

Example:
    >>> conn = FalkordbConnection(config)
    >>> conn.init_db(schema, recreate_schema=True)
    >>> conn.upsert_docs_batch(docs, "Person", match_keys=["id"])
"""

import logging
from typing import Any
from urllib.parse import urlparse

from falkordb import FalkorDB
from falkordb.graph import Graph

from graflo.architecture.edge import Edge
from graflo.architecture.onto import Index
from graflo.architecture.schema import Schema
from graflo.architecture.vertex import VertexConfig
from graflo.db.conn import Connection, SchemaExistsError
from graflo.db.util import serialize_value
from graflo.filter.onto import FilterExpression
from graflo.onto import AggregationType
from graflo.onto import DBType


from ..connection.onto import FalkordbConfig

logger = logging.getLogger(__name__)


class FalkordbConnection(Connection):
    """FalkorDB-specific implementation of the Connection interface.

    This class provides FalkorDB-specific implementations for all database
    operations, including node management, relationship operations, and
    Cypher query execution. It uses the FalkorDB Python client for all operations.

    Attributes:
        flavor: Database flavor identifier (FALKORDB)
        config: FalkorDB connection configuration (URI, database, credentials)
        client: Underlying FalkorDB client instance
        graph: Active graph object for query execution
        _graph_name: Name of the currently selected graph
    """

    flavor = DBType.FALKORDB

    # Type annotations for instance attributes
    client: FalkorDB | None
    graph: Graph | None
    _graph_name: str

    def __init__(self, config: FalkordbConfig):
        """Initialize FalkorDB connection.

        Args:
            config: FalkorDB connection configuration containing URI, database,
                and optional password
        """
        super().__init__()
        self.config = config

        if config.uri is None:
            raise ValueError("FalkorDB connection requires a URI to be configured")

        # Parse URI to extract host and port
        parsed = urlparse(config.uri)
        host = parsed.hostname or "localhost"
        port = parsed.port or 6379

        # Initialize FalkorDB client
        if config.password:
            self.client = FalkorDB(host=host, port=port, password=config.password)
        else:
            self.client = FalkorDB(host=host, port=port)

        # Select the graph (database in config maps to graph name)
        graph_name = config.database or "default"
        self.graph = self.client.select_graph(graph_name)
        self._graph_name = graph_name

    def execute(self, query: str, **kwargs):
        """Execute a raw OpenCypher query against the graph.

        Args:
            query: OpenCypher query string. Can include parameter placeholders
                using $name syntax (e.g., "MATCH (n) WHERE n.id = $id")
            **kwargs: Query parameters as keyword arguments

        Returns:
            QueryResult: FalkorDB result object containing result_set and statistics
        """
        assert self.graph is not None, "Connection is closed"
        # Pass params as keyword argument if the client supports it, otherwise as positional
        # Try params keyword first, fall back to positional
        if kwargs:
            try:
                result = self.graph.query(query, params=kwargs)
            except TypeError:
                # Fall back to positional argument if params keyword not supported
                result = self.graph.query(query, kwargs)
        else:
            result = self.graph.query(query)
        return result

    def close(self):
        """Close the FalkorDB connection."""
        # FalkorDB client handles connection pooling internally
        # No explicit close needed, but we can delete the reference
        self.graph = None
        self.client = None

    @staticmethod
    def _is_valid_property_value(value) -> bool:
        """Validate that a value can be stored as a FalkorDB property.

        Rejects NaN and infinity values that cannot be stored.

        Args:
            value: Value to validate

        Returns:
            True if value can be safely stored, False otherwise
        """
        import math

        if isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                return False
        return True

    @staticmethod
    def _sanitize_string_value(value: str) -> str:
        """Remove characters that break the Cypher parser.

        Args:
            value: String value to sanitize

        Returns:
            Sanitized string with problematic characters removed
        """
        if "\x00" in value:
            value = value.replace("\x00", "")
        return value

    def _sanitize_value(self, value):
        """Recursively sanitize a value, handling nested structures.

        Args:
            value: Value to sanitize (can be dict, list, or primitive)

        Returns:
            Sanitized value with datetime objects serialized to epoch microseconds
        """
        # Handle nested dictionaries recursively
        if isinstance(value, dict):
            sanitized_dict = {}
            for k, v in value.items():
                # Filter non-string keys in nested dicts too
                if not isinstance(k, str):
                    logger.warning(
                        f"Skipping non-string nested key: {k!r} (type: {type(k).__name__})"
                    )
                    continue
                sanitized_v = self._sanitize_value(v)
                # Check for invalid float values
                if self._is_valid_property_value(sanitized_v):
                    sanitized_dict[k] = sanitized_v
            return sanitized_dict

        # Handle lists recursively
        elif isinstance(value, list):
            sanitized_list = []
            for item in value:
                sanitized_item = self._sanitize_value(item)
                # Check for invalid float values
                if self._is_valid_property_value(sanitized_item):
                    sanitized_list.append(sanitized_item)
            return sanitized_list

        # Handle primitive values
        else:
            # Convert datetime objects to ISO-8601 strings for FalkorDB
            # These will be wrapped with datetime() function in Cypher queries
            from datetime import date, datetime, time

            if isinstance(value, (datetime, date, time)):
                # Use ISO format - will be wrapped with datetime() in Cypher
                serialized = serialize_value(value)  # This returns ISO-8601 string
            else:
                # Use shared serialize_value for other types (Decimal, etc.)
                serialized = serialize_value(value)

            # Sanitize string values (remove null bytes that break Cypher)
            if isinstance(serialized, str):
                serialized = self._sanitize_string_value(serialized)

            return serialized

    def _sanitize_document(
        self, doc: dict, match_keys: list[str] | None = None
    ) -> dict:
        """Sanitize a document for safe FalkorDB insertion.

        Filters invalid keys/values, serializes datetime objects (including nested ones),
        and validates required match keys.

        Args:
            doc: Document to sanitize
            match_keys: Optional list of keys that must be present with valid values

        Returns:
            Sanitized copy of the document

        Raises:
            ValueError: If a required match_key is missing or has None value
        """
        sanitized = {}

        for key, value in doc.items():
            # Filter non-string keys
            if not isinstance(key, str):
                logger.warning(
                    f"Skipping non-string property key: {key!r} (type: {type(key).__name__})"
                )
                continue

            # Recursively sanitize the value (handles nested dicts/lists and datetime objects)
            sanitized_value = self._sanitize_value(value)

            # Check for invalid float values
            if not self._is_valid_property_value(sanitized_value):
                logger.warning(
                    f"Skipping property '{key}' with invalid value: {sanitized_value}"
                )
                continue

            sanitized[key] = sanitized_value

        # Validate match_keys presence
        if match_keys:
            for key in match_keys:
                if key not in sanitized:
                    raise ValueError(
                        f"Required match key '{key}' is missing or has invalid value in document: {doc}"
                    )
                if sanitized[key] is None:
                    raise ValueError(
                        f"Match key '{key}' cannot be None in document: {doc}"
                    )

        return sanitized

    def _sanitize_batch(
        self, docs: list[dict], match_keys: list[str] | None = None
    ) -> list[dict]:
        """Sanitize a batch of documents.

        Args:
            docs: List of documents to sanitize
            match_keys: Optional list of required keys to validate

        Returns:
            List of sanitized documents
        """
        return [self._sanitize_document(doc, match_keys) for doc in docs]

    def create_database(self, name: str):
        """Create a new graph in FalkorDB.

        Note: In FalkorDB, graphs are created implicitly when data is first inserted.

        Args:
            name: Name of the graph to create
        """
        # In FalkorDB, graphs are created implicitly when you first insert data
        # We just need to select the graph
        assert self.client is not None, "Connection is closed"
        self.graph = self.client.select_graph(name)
        self._graph_name = name
        logger.info(f"Selected FalkorDB graph '{name}'")

    def delete_database(self, name: str):
        """Delete a graph from FalkorDB.

        Args:
            name: Name of the graph to delete (if empty, uses current graph)
        """
        graph_to_delete = name if name else self._graph_name
        assert self.client is not None, "Connection is closed"
        try:
            # Delete the graph using the FalkorDB API
            graph = self.client.select_graph(graph_to_delete)
            graph.delete()
            logger.info(f"Successfully deleted FalkorDB graph '{graph_to_delete}'")
        except Exception as e:
            logger.error(
                f"Failed to delete FalkorDB graph '{graph_to_delete}': {e}",
                exc_info=True,
            )
            raise

    def define_vertex_indices(
        self, vertex_config: VertexConfig, schema: Schema | None = None
    ):
        """Define indices for vertex labels.

        Creates indices for each vertex label based on the configuration.
        FalkorDB supports range indices on node properties.

        Args:
            vertex_config: Vertex configuration containing index definitions
        """
        for c in vertex_config.vertex_set:
            index_list = (
                schema.database_features.vertex_secondary_indexes(c)
                if schema is not None
                else []
            )
            for index_obj in index_list:
                self._add_index(c, index_obj)

    def define_edge_indices(self, edges: list[Edge], schema: Schema | None = None):
        """Define indices for relationship types.

        Creates indices for each relationship type based on the configuration.
        FalkorDB supports range indices on relationship properties.

        Args:
            edges: List of edge configurations containing index definitions
        """
        for edge in edges:
            index_list = (
                schema.database_features.edge_secondary_indexes(edge.edge_id)
                if schema is not None
                else []
            )
            for index_obj in index_list:
                if edge.relation is not None:
                    self._add_index(edge.relation, index_obj, is_vertex_index=False)

    def _add_index(self, obj_name: str, index: Index, is_vertex_index: bool = True):
        """Add an index to a label or relationship type.

        Args:
            obj_name: Label or relationship type name
            index: Index configuration to create
            is_vertex_index: If True, create index on nodes, otherwise on relationships
        """
        for field in index.fields:
            try:
                if is_vertex_index:
                    # FalkorDB node index syntax
                    q = f"CREATE INDEX FOR (n:{obj_name}) ON (n.{field})"
                else:
                    # FalkorDB relationship index syntax
                    q = f"CREATE INDEX FOR ()-[r:{obj_name}]-() ON (r.{field})"

                self.execute(q)
                logger.debug(f"Created index on {obj_name}.{field}")
            except Exception as e:
                # Index may already exist, log and continue
                logger.debug(f"Index creation note for {obj_name}.{field}: {e}")

    def define_schema(self, schema: Schema):
        """Define vertex and edge classes based on schema.

        Note: This is a no-op in FalkorDB as vertex/edge classes (labels/relationship types) are implicit.

        Args:
            schema: Schema containing vertex and edge class definitions
        """
        pass

    def define_vertex_classes(self, schema: Schema):
        """Define vertex classes based on schema.

        Note: This is a no-op in FalkorDB as vertex classes (labels) are implicit.

        Args:
            schema: Schema containing vertex definitions
        """
        pass

    def define_edge_classes(self, edges: list[Edge]):
        """Define edge classes based on schema.

        Note: This is a no-op in FalkorDB as edge classes (relationship types) are implicit.

        Args:
            edges: List of edge configurations
        """
        pass

    def delete_graph_structure(
        self,
        vertex_types: tuple[str, ...] | list[str] = (),
        graph_names: tuple[str, ...] | list[str] = (),
        delete_all: bool = False,
    ) -> None:
        """Delete graph structure (nodes and relationships) from FalkorDB.

        Args:
            vertex_types: Label names to delete nodes for
            graph_names: Graph names to delete entirely
            delete_all: If True, delete all nodes and relationships
        """
        if delete_all or (not vertex_types and not graph_names):
            # Delete all nodes and relationships in current graph
            try:
                self.execute("MATCH (n) DETACH DELETE n")
                logger.debug("Deleted all nodes and relationships from graph")
            except Exception as e:
                logger.debug(f"Graph may be empty or not exist: {e}")
        elif vertex_types:
            # Delete nodes with specific labels
            for label in vertex_types:
                try:
                    self.execute(f"MATCH (n:{label}) DETACH DELETE n")
                    logger.debug(f"Deleted all nodes with label '{label}'")
                except Exception as e:
                    logger.warning(f"Failed to delete nodes with label '{label}': {e}")

        # Delete specific graphs
        assert self.client is not None, "Connection is closed"
        for graph_name in graph_names:
            try:
                graph = self.client.select_graph(graph_name)
                graph.delete()
                logger.debug(f"Deleted graph '{graph_name}'")
            except Exception as e:
                logger.warning(f"Failed to delete graph '{graph_name}': {e}")

    def init_db(self, schema: Schema, recreate_schema: bool) -> None:
        """Initialize FalkorDB with the given schema.

        Uses schema.general.name if database is not set in config.

        If the graph already has nodes and recreate_schema is False, raises
        SchemaExistsError and the script halts.

        Args:
            schema: Schema containing graph structure definitions
            recreate_schema: If True, delete all existing data before initialization.
                If False and graph has nodes, raises SchemaExistsError.
        """
        # Determine graph name: use config.database if set, otherwise use schema.general.name
        graph_name = self.config.database
        if not graph_name:
            graph_name = schema.general.name
            self.config.database = graph_name

        # Select/create the graph
        assert self.client is not None, "Connection is closed"
        self.graph = self.client.select_graph(graph_name)
        self._graph_name = graph_name
        logger.info(f"Initialized FalkorDB graph '{graph_name}'")

        # Check if graph already has nodes (schema/graph exists)
        try:
            result = self.execute("MATCH (n) RETURN count(n) AS c")
            count = 0
            if hasattr(result, "data") and result.data():
                count = result.data()[0].get("c", 0) or 0
            elif result is not None and hasattr(result, "__iter__"):
                for record in result:
                    count = (
                        record.get("c", 0)
                        if hasattr(record, "get")
                        else getattr(record, "c", 0)
                    ) or 0
                    break
            if count > 0 and not recreate_schema:
                raise SchemaExistsError(
                    f"Schema/graph already exists in graph '{graph_name}' ({count} nodes). "
                    "Set recreate_schema=True to replace, or use clear_data=True before ingestion."
                )
        except SchemaExistsError:
            raise
        except Exception as e:
            logger.debug(f"Could not check graph node count: {e}")

        if recreate_schema:
            try:
                self.delete_graph_structure(delete_all=True)
                logger.debug(f"Cleaned graph '{graph_name}' for fresh start")
            except Exception as e:
                logger.debug(f"Recreate schema note for graph '{graph_name}': {e}")

        try:
            self.define_indexes(schema)
            logger.debug(f"Defined indexes for graph '{graph_name}'")
        except Exception as e:
            logger.error(
                f"Failed to define indexes for graph '{graph_name}': {e}",
                exc_info=True,
            )
            raise

    def clear_data(self, schema: Schema) -> None:
        """Remove all data from the graph without dropping the schema.

        Deletes all nodes and relationships; labels (schema) remain.
        """
        self.delete_graph_structure(delete_all=True)

    def upsert_docs_batch(
        self,
        docs: list[dict[str, Any]],
        class_name: str,
        match_keys: list[str] | tuple[str, ...],
        **kwargs: Any,
    ) -> None:
        """Upsert a batch of nodes using Cypher MERGE.

        Args:
            docs: List of node documents to upsert
            class_name: Label to upsert into
            match_keys: Keys to match for upsert operation
            **kwargs: Additional options:
                - dry (bool): If True, build query but don't execute

        Raises:
            ValueError: If any document is missing a required match_key or has None value
        """
        dry = kwargs.pop("dry", False)

        if not docs:
            return

        # Sanitize documents: filter invalid keys/values, validate match_keys
        match_keys_list = (
            list(match_keys) if isinstance(match_keys, tuple) else match_keys
        )
        sanitized_docs = self._sanitize_batch(docs, match_keys_list)

        if not sanitized_docs:
            return

        # Build the MERGE clause with match keys
        index_str = ", ".join([f"{k}: row.{k}" for k in match_keys])
        # Use 'data' instead of 'batch' as parameter name (batch might be reserved)
        # Try direct UNWIND - FalkorDB may not support WITH $param AS alias
        # Datetime objects are converted to ISO-8601 strings during sanitization
        q = f"""
            UNWIND $data AS row
            MERGE (n:{class_name} {{ {index_str} }})
            ON MATCH SET n += row
            ON CREATE SET n += row
        """
        if not dry:
            self.execute(q, data=sanitized_docs)

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
        """Create relationships between existing nodes using Cypher MERGE.

        Args:
            docs_edges: Edge specifications as list of [source, target, props] triples
            source_class: Label of source nodes
            target_class: Label of target nodes
            relation_name: Relationship type name
            match_keys_source: Properties to match source nodes
            match_keys_target: Properties to match target nodes
            filter_uniques: Unused in FalkorDB (MERGE handles uniqueness automatically)
            head: Optional limit on number of relationships to insert
            **kwargs: Additional options:
                - dry (bool): If True, build query but don't execute
        """
        dry = kwargs.pop("dry", False)
        # Extract and ignore unused parameters (kept for interface compatibility)
        kwargs.pop("collection_name", None)
        kwargs.pop("uniq_weight_fields", None)
        kwargs.pop("uniq_weight_collections", None)
        kwargs.pop("upsert_option", None)

        # Apply head limit if specified
        if head is not None and isinstance(docs_edges, list):
            docs_edges = docs_edges[:head]

        if not docs_edges:
            return

        # Note: filter_uniques is unused because FalkorDB's MERGE handles uniqueness automatically

        # Sanitize edge data: each edge is [source_dict, target_dict, props_dict]
        # We need to sanitize source, target, and props dictionaries
        sanitized_edges = []
        for edge in docs_edges:
            if len(edge) != 3:
                logger.warning(
                    f"Skipping invalid edge format: expected [source, target, props], got {edge}"
                )
                continue

            source_dict, target_dict, props_dict = edge

            # Sanitize source and target dictionaries (for match keys)
            sanitized_source = self._sanitize_document(
                source_dict if isinstance(source_dict, dict) else {},
                match_keys=list(match_keys_source),
            )
            sanitized_target = self._sanitize_document(
                target_dict if isinstance(target_dict, dict) else {},
                match_keys=list(match_keys_target),
            )

            # Sanitize props dictionary (may contain datetime objects)
            sanitized_props = self._sanitize_document(
                props_dict if isinstance(props_dict, dict) else {}
            )

            sanitized_edges.append(
                (sanitized_source, sanitized_target, sanitized_props)
            )

        if not sanitized_edges:
            return

        # Build match conditions for source and target nodes
        source_match_str = [f"source.{key} = row[0].{key}" for key in match_keys_source]
        target_match_str = [f"target.{key} = row[1].{key}" for key in match_keys_target]

        match_clause = "WHERE " + " AND ".join(source_match_str + target_match_str)

        # Datetime objects are converted to ISO-8601 strings during sanitization
        q = f"""
            UNWIND $data AS row
            MATCH (source:{source_class}),
                  (target:{target_class}) {match_clause}
            MERGE (source)-[r:{relation_name}]->(target)
            SET r += row[2]
        """
        if not dry:
            self.execute(q, data=sanitized_edges)

    def insert_return_batch(
        self, docs: list[dict[str, Any]], class_name: str
    ) -> list[dict[str, Any]] | str:
        """Insert nodes and return their properties.

        Args:
            docs: Documents to insert
            class_name: Label to insert into

        Raises:
            NotImplementedError: This method is not fully implemented for FalkorDB
        """
        raise NotImplementedError("insert_return_batch is not implemented for FalkorDB")

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
            unset_keys: Unused in FalkorDB
            **kwargs: Additional parameters

        Returns:
            List of fetched nodes as dictionaries
        """
        # Build filter clause
        if filters is not None:
            ff = FilterExpression.from_dict(filters)
            filter_clause = f"WHERE {ff(doc_name='n', kind=self.expression_flavor())}"
        else:
            filter_clause = ""

        # Build return clause
        if return_keys is not None:
            # Project specific keys
            keep_clause_ = ", ".join([f"n.{item} AS {item}" for item in return_keys])
            return_clause = f"RETURN {keep_clause_}"
        else:
            return_clause = "RETURN n"

        # Build limit clause (must be positive integer)
        if limit is not None and isinstance(limit, int) and limit > 0:
            limit_clause = f"LIMIT {limit}"
        else:
            limit_clause = ""

        q = f"""
            MATCH (n:{class_name})
            {filter_clause}
            {return_clause}
            {limit_clause}
        """

        result = self.execute(q)

        # Convert FalkorDB results to list of dictionaries
        if return_keys is not None:
            # Results are already projected
            return [dict(zip(return_keys, row)) for row in result.result_set]
        else:
            # Results contain node objects
            return [self._node_to_dict(row[0]) for row in result.result_set]

    def _node_to_dict(self, node) -> dict:
        """Convert a FalkorDB node to a dictionary.

        Args:
            node: FalkorDB node object

        Returns:
            Node properties as dictionary
        """
        if hasattr(node, "properties"):
            return dict(node.properties)
        elif isinstance(node, dict):
            return node
        else:
            # Try to convert to dict
            return dict(node) if node else {}

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
        """Fetch edges from FalkorDB using Cypher.

        Args:
            from_type: Source node label
            from_id: Source node ID (property name depends on match_keys used)
            edge_type: Optional relationship type to filter by
            to_type: Optional target node label to filter by
            to_id: Optional target node ID to filter by
            filters: Additional query filters
            limit: Maximum number of edges to return
            return_keys: Keys to return (projection)
            unset_keys: Keys to exclude (projection) - not supported in FalkorDB
            **kwargs: Additional parameters

        Returns:
            List of fetched edges as dictionaries
        """
        # Build source node match
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

        # Build WHERE clauses
        where_clauses = []
        if to_id:
            where_clauses.append(f"target.id = '{to_id}'")

        # Add additional filters if provided
        if filters is not None:
            ff = FilterExpression.from_dict(filters)
            filter_clause = ff(doc_name="r", kind=self.expression_flavor())
            where_clauses.append(filter_clause)

        where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        # Build return clause
        if return_keys is not None:
            return_parts = ", ".join([f"r.{key} AS {key}" for key in return_keys])
            return_clause = f"RETURN {return_parts}"
        else:
            return_clause = "RETURN r"

        limit_clause = f"LIMIT {limit}" if limit and limit > 0 else ""

        query = f"""
            MATCH {source_match}{rel_pattern}{target_match}
            {where_clause}
            {return_clause}
            {limit_clause}
        """

        result = self.execute(query)

        # Convert results
        if return_keys is not None:
            return [dict(zip(return_keys, row)) for row in result.result_set]
        else:
            return [self._edge_to_dict(row[0]) for row in result.result_set]

    def _edge_to_dict(self, edge) -> dict:
        """Convert a FalkorDB edge to a dictionary.

        Args:
            edge: FalkorDB edge object

        Returns:
            Edge properties as dictionary
        """
        if hasattr(edge, "properties"):
            return dict(edge.properties)
        elif isinstance(edge, dict):
            return edge
        else:
            return dict(edge) if edge else {}

    def fetch_present_documents(
        self,
        batch: list[dict[str, Any]],
        class_name: str,
        match_keys: list[str] | tuple[str, ...],
        keep_keys: list[str] | tuple[str, ...] | None = None,
        flatten: bool = False,
        filters: list[Any] | dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch nodes that exist in the database.

        Args:
            batch: Batch of documents to check
            class_name: Label to check in
            match_keys: Keys to match nodes
            keep_keys: Keys to keep in result
            flatten: Unused in FalkorDB
            filters: Additional query filters

        Returns:
            Documents that exist in the database
        """
        if not batch:
            return []

        # Build match conditions for each document in batch
        results = []
        for doc in batch:
            match_conditions = " AND ".join([f"n.{key} = ${key}" for key in match_keys])
            params = {key: doc.get(key) for key in match_keys}

            q = f"""
                MATCH (n:{class_name})
                WHERE {match_conditions}
                RETURN n
                LIMIT 1
            """

            try:
                result = self.execute(q, **params)
                if result.result_set:
                    node_dict = self._node_to_dict(result.result_set[0][0])
                    if keep_keys:
                        node_dict = {k: node_dict.get(k) for k in keep_keys}
                    results.append(node_dict)
            except Exception as e:
                logger.debug(f"Error checking document presence: {e}")

        return results

    def aggregate(
        self,
        class_name,
        aggregation_function: AggregationType,
        discriminant: str | None = None,
        aggregated_field: str | None = None,
        filters: list | dict | None = None,
    ):
        """Perform aggregation on nodes.

        Args:
            class_name: Label to aggregate
            aggregation_function: Type of aggregation to perform
            discriminant: Field to group by
            aggregated_field: Field to aggregate
            filters: Query filters

        Returns:
            Aggregation results (dict for grouped aggregations, int/float for single value)
        """
        # Build filter clause
        if filters is not None:
            ff = FilterExpression.from_dict(filters)
            filter_clause = f"WHERE {ff(doc_name='n', kind=self.expression_flavor())}"
        else:
            filter_clause = ""

        # Build aggregation query based on function type
        if aggregation_function == AggregationType.COUNT:
            if discriminant:
                q = f"""
                    MATCH (n:{class_name})
                    {filter_clause}
                    RETURN n.{discriminant} AS key, count(*) AS count
                """
                result = self.execute(q)
                return {row[0]: row[1] for row in result.result_set}
            else:
                q = f"""
                    MATCH (n:{class_name})
                    {filter_clause}
                    RETURN count(*) AS count
                """
                result = self.execute(q)
                return result.result_set[0][0] if result.result_set else 0

        elif aggregation_function == AggregationType.MAX:
            if not aggregated_field:
                raise ValueError("aggregated_field is required for MAX aggregation")
            q = f"""
                MATCH (n:{class_name})
                {filter_clause}
                RETURN max(n.{aggregated_field}) AS max_value
            """
            result = self.execute(q)
            return result.result_set[0][0] if result.result_set else None

        elif aggregation_function == AggregationType.MIN:
            if not aggregated_field:
                raise ValueError("aggregated_field is required for MIN aggregation")
            q = f"""
                MATCH (n:{class_name})
                {filter_clause}
                RETURN min(n.{aggregated_field}) AS min_value
            """
            result = self.execute(q)
            return result.result_set[0][0] if result.result_set else None

        elif aggregation_function == AggregationType.AVERAGE:
            if not aggregated_field:
                raise ValueError("aggregated_field is required for AVERAGE aggregation")
            q = f"""
                MATCH (n:{class_name})
                {filter_clause}
                RETURN avg(n.{aggregated_field}) AS avg_value
            """
            result = self.execute(q)
            return result.result_set[0][0] if result.result_set else None

        elif aggregation_function == AggregationType.SORTED_UNIQUE:
            if not aggregated_field:
                raise ValueError(
                    "aggregated_field is required for SORTED_UNIQUE aggregation"
                )
            q = f"""
                MATCH (n:{class_name})
                {filter_clause}
                RETURN DISTINCT n.{aggregated_field} AS value
                ORDER BY value
            """
            result = self.execute(q)
            return [row[0] for row in result.result_set]

        else:
            raise ValueError(
                f"Unsupported aggregation function: {aggregation_function}"
            )

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
            class_name: Label to check in
            match_keys: Keys to match nodes
            keep_keys: Keys to keep in result
            filters: Additional query filters

        Returns:
            Documents that don't exist in the database
        """
        if not batch:
            return []

        # Find documents that exist
        present_docs = self.fetch_present_documents(
            batch, class_name, match_keys, match_keys, filters=filters
        )

        # Create a set of present document keys for efficient lookup
        present_keys = set()
        for doc in present_docs:
            key_tuple = tuple(doc.get(k) for k in match_keys)
            present_keys.add(key_tuple)

        # Filter out documents that exist
        absent_docs = []
        for doc in batch:
            key_tuple = tuple(doc.get(k) for k in match_keys)
            if key_tuple not in present_keys:
                if keep_keys:
                    absent_docs.append({k: doc.get(k) for k in keep_keys})
                else:
                    absent_docs.append(doc)

        return absent_docs
