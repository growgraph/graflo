"""FalkorDB graph database connector implementation.

This module provides a production-ready connector for FalkorDB, a high-performance
graph database built on Redis. It implements the graflo Connection interface,
enabling seamless integration with the graflo ETL pipeline.

Architecture
------------
FalkorDB extends Redis with graph capabilities using the RedisGraph module.
Data is organized as:

    Redis Instance
    └── Graph (Redis key)
        ├── Nodes (with labels)
        │   └── Properties (key-value pairs)
        └── Relationships (typed, directed)
            └── Properties (key-value pairs)

Key Features
------------
- **OpenCypher Support**: Full Cypher query language for graph traversals
- **Redis Performance**: Sub-millisecond latency with Redis storage backend
- **Batch Operations**: Efficient bulk insert/upsert with UNWIND patterns
- **Input Sanitization**: Protection against Cypher injection and malformed data
- **Connection Pooling**: Automatic connection management via Redis client

Input Sanitization
------------------
The connector automatically sanitizes inputs to prevent:

- Cypher injection via property values
- Invalid property keys (non-string, reserved names)
- Unsupported values (NaN, Inf, null bytes)

Example
-------
Basic usage with ConnectionManager::

    from graflo.db import ConnectionManager

    config = FalkordbConfig(uri="redis://localhost:6379", database="mygraph")

    with ConnectionManager(connection_config=config) as db:
        # Insert nodes
        db.upsert_docs_batch(
            [{"id": "1", "name": "Alice"}],
            "Person",
            match_keys=["id"]
        )

        # Query nodes
        results = db.fetch_docs("Person", filters=["==", "Alice", "name"])

        # Create relationships
        db.insert_edges_batch(
            [[{"id": "1"}, {"id": "2"}, {"since": 2024}]],
            source_class="Person",
            target_class="Person",
            relation_name="KNOWS",
            match_keys_source=["id"],
            match_keys_target=["id"]
        )

Configuration
-------------
Connection is configured via FalkordbConfig:

    - uri: Redis connection URI (redis://host:port)
    - database: Graph name (defaults to "default")
    - password: Optional Redis authentication

See Also
--------
- FalkorDB documentation: https://docs.falkordb.com/
- OpenCypher specification: https://opencypher.org/
- graflo.db.conn.Connection: Base connection interface
"""

import logging
from urllib.parse import urlparse

from falkordb import FalkorDB
from falkordb.graph import Graph

from graflo.architecture.edge import Edge
from graflo.architecture.onto import Index
from graflo.architecture.schema import Schema
from graflo.architecture.vertex import VertexConfig
from graflo.db.conn import Connection
from graflo.filter.onto import Expression
from graflo.onto import AggregationType, DBFlavor, ExpressionFlavor

from ..connection.onto import FalkordbConfig

logger = logging.getLogger(__name__)


class FalkordbConnection(Connection):
    """FalkorDB connector implementing the graflo Connection interface.

    Provides complete graph database operations for FalkorDB including
    node/relationship CRUD, batch operations, aggregations, and raw
    Cypher query execution.

    Thread Safety
    -------------
    This class is NOT thread-safe. Each thread should use its own
    connection instance. For concurrent access, use ConnectionManager
    with separate instances per thread.

    Error Handling
    --------------
    - Connection errors raise on instantiation
    - Query errors propagate as redis.exceptions.ResponseError
    - Invalid inputs raise ValueError with descriptive messages

    Attributes
    ----------
    flavor : DBFlavor
        Database type identifier (DBFlavor.FALKORDB)
    config : FalkordbConfig
        Connection configuration (URI, database, credentials)
    client : FalkorDB
        Underlying FalkorDB client instance
    graph : Graph
        Active graph object for query execution
    _graph_name : str
        Name of the currently selected graph

    Examples
    --------
    Direct instantiation (prefer ConnectionManager for production)::

        config = FalkordbConfig(uri="redis://localhost:6379")
        conn = FalkordbConnection(config)
        try:
            result = conn.execute("MATCH (n) RETURN count(n)")
        finally:
            conn.close()
    """

    flavor = DBFlavor.FALKORDB

    # Type annotations for instance attributes
    client: FalkorDB | None
    graph: Graph | None
    _graph_name: str

    def __init__(self, config: FalkordbConfig):
        """Initialize FalkorDB connection and select graph.

        Establishes connection to the FalkorDB instance and selects
        the specified graph for subsequent operations.

        Parameters
        ----------
        config : FalkordbConfig
            Connection configuration with the following fields:
            - uri: Redis URI (redis://host:port)
            - database: Graph name (optional, defaults to "default")
            - password: Redis password (optional)

        Raises
        ------
        ValueError
            If URI is not provided in configuration
        redis.exceptions.ConnectionError
            If unable to connect to Redis instance
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

        Executes the provided Cypher query with optional parameters.
        Parameters are safely injected using FalkorDB's parameterized
        query mechanism to prevent injection attacks.

        Parameters
        ----------
        query : str
            OpenCypher query string. Can include parameter placeholders
            using $name syntax (e.g., "MATCH (n) WHERE n.id = $id")
        **kwargs
            Query parameters as keyword arguments. Values are safely
            escaped by the driver.

        Returns
        -------
        QueryResult
            FalkorDB result object containing:
            - result_set: List of result rows
            - statistics: Query execution statistics

        Examples
        --------
        Simple query::

            result = conn.execute("MATCH (n:Person) RETURN n.name")

        Parameterized query::

            result = conn.execute(
                "MATCH (n:Person) WHERE n.age > $min_age RETURN n",
                min_age=21
            )
        """
        assert self.graph is not None, "Connection is closed"
        result = self.graph.query(query, kwargs if kwargs else None)
        return result

    def close(self):
        """Close the FalkorDB connection.

        Note: FalkorDB client uses Redis connection pooling,
        so explicit close is not always necessary.
        """
        # FalkorDB client handles connection pooling internally
        # No explicit close needed, but we can delete the reference
        self.graph = None
        self.client = None

    @staticmethod
    def _is_valid_property_value(value) -> bool:
        """Validate that a value can be stored as a FalkorDB property.

        FalkorDB (like most databases) cannot store special float values.
        This method rejects values that would cause query failures.

        Parameters
        ----------
        value : Any
            Value to validate

        Returns
        -------
        bool
            True if value can be safely stored, False otherwise

        Notes
        -----
        Rejected values:
        - float('nan'): Not a Number
        - float('inf'): Positive infinity
        - float('-inf'): Negative infinity
        """
        import math

        if isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                return False
        return True

    @staticmethod
    def _sanitize_string_value(value: str) -> str:
        """Remove characters that break the Cypher parser.

        Null bytes (\\x00) cause FalkorDB's Cypher parser to fail with
        cryptic errors. This method strips them from string values.

        Parameters
        ----------
        value : str
            String value to sanitize

        Returns
        -------
        str
            Sanitized string with problematic characters removed

        Notes
        -----
        Currently handles:
        - Null bytes (\\x00): Break Cypher parser tokenization
        """
        if "\x00" in value:
            value = value.replace("\x00", "")
        return value

    def _sanitize_document(
        self, doc: dict, match_keys: list[str] | None = None
    ) -> dict:
        """Sanitize a document for safe FalkorDB insertion.

        Performs comprehensive input validation and sanitization to ensure
        documents can be safely inserted without query errors or injection.

        Sanitization Steps
        ------------------
        1. Filter non-string property keys (log warning)
        2. Remove properties with invalid float values (NaN, Inf)
        3. Strip null bytes from string values
        4. Validate presence of required match keys

        Parameters
        ----------
        doc : dict
            Document to sanitize. Modified values are logged as warnings.
        match_keys : list[str], optional
            Keys that must be present with valid (non-None) values.
            Typically the fields used for MERGE matching.

        Returns
        -------
        dict
            Sanitized copy of the document

        Raises
        ------
        ValueError
            If a required match_key is missing or has None value

        Examples
        --------
        >>> doc = {"id": "1", "name": "test\\x00", 123: "bad_key"}
        >>> sanitized = conn._sanitize_document(doc, match_keys=["id"])
        # Logs: Skipping non-string property key: 123
        # Logs: Sanitized property 'name': removed null bytes
        >>> sanitized
        {"id": "1", "name": "test"}
        """
        sanitized = {}

        for key, value in doc.items():
            # Filter non-string keys
            if not isinstance(key, str):
                logger.warning(
                    f"Skipping non-string property key: {key!r} (type: {type(key).__name__})"
                )
                continue

            # Check for invalid float values
            if not self._is_valid_property_value(value):
                logger.warning(f"Skipping property '{key}' with invalid value: {value}")
                continue

            # Sanitize string values (remove null bytes that break Cypher)
            if isinstance(value, str):
                original = value
                value = self._sanitize_string_value(value)
                if value != original:
                    logger.warning(
                        f"Sanitized property '{key}': removed null bytes from value"
                    )

            sanitized[key] = value

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
            list[dict]: List of sanitized documents
        """
        return [self._sanitize_document(doc, match_keys) for doc in docs]

    def create_database(self, name: str):
        """Create a new graph in FalkorDB.

        In FalkorDB, creating a database means selecting a new graph.
        The graph is created implicitly when data is first inserted.

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

    def define_vertex_indices(self, vertex_config: VertexConfig):
        """Define indices for vertex labels.

        Creates indices for each vertex label based on the configuration.
        FalkorDB supports range indices on node properties.

        Args:
            vertex_config: Vertex configuration containing index definitions
        """
        for c in vertex_config.vertex_set:
            for index_obj in vertex_config.indexes(c):
                self._add_index(c, index_obj)

    def define_edge_indices(self, edges: list[Edge]):
        """Define indices for relationship types.

        Creates indices for each relationship type based on the configuration.
        FalkorDB supports range indices on relationship properties.

        Args:
            edges: List of edge configurations containing index definitions
        """
        for edge in edges:
            for index_obj in edge.indexes:
                if edge.relation is not None:
                    self._add_index(edge.relation, index_obj, is_vertex_index=False)

    def _add_index(self, obj_name: str, index: Index, is_vertex_index: bool = True):
        """Add an index to a label or relationship type.

        FalkorDB uses CREATE INDEX syntax similar to Neo4j but with some differences.

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
        """Define collections based on schema.

        Note: This is a no-op in FalkorDB as collections are implicit.
        Labels and relationship types are created when data is inserted.

        Args:
            schema: Schema containing collection definitions
        """
        pass

    def define_vertex_collections(self, schema: Schema):
        """Define vertex collections based on schema.

        Note: This is a no-op in FalkorDB as vertex collections are implicit.

        Args:
            schema: Schema containing vertex definitions
        """
        pass

    def define_edge_collections(self, edges: list[Edge]):
        """Define edge collections based on schema.

        Note: This is a no-op in FalkorDB as edge collections are implicit.

        Args:
            edges: List of edge configurations
        """
        pass

    def delete_graph_structure(self, vertex_types=(), graph_names=(), delete_all=False):
        """Delete graph structure (nodes and relationships) from FalkorDB.

        In FalkorDB:
        - Labels: Categories for nodes (equivalent to vertex types)
        - Relationship Types: Types of relationships (equivalent to edge types)
        - Graph: Redis key containing all nodes and relationships

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

    def init_db(self, schema: Schema, clean_start: bool):
        """Initialize FalkorDB with the given schema.

        Uses schema.general.name if database is not set in config.

        Args:
            schema: Schema containing graph structure definitions
            clean_start: If True, delete all existing data before initialization
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

        if clean_start:
            try:
                self.delete_graph_structure(delete_all=True)
                logger.debug(f"Cleaned graph '{graph_name}' for fresh start")
            except Exception as e:
                logger.debug(f"Clean start note for graph '{graph_name}': {e}")

        try:
            self.define_indexes(schema)
            logger.debug(f"Defined indexes for graph '{graph_name}'")
        except Exception as e:
            logger.error(
                f"Failed to define indexes for graph '{graph_name}': {e}",
                exc_info=True,
            )
            raise

    def upsert_docs_batch(
        self, docs: list[dict], class_name: str, match_keys: list[str], **kwargs
    ):
        """Upsert a batch of nodes using Cypher MERGE.

        Performs atomic upsert (update-or-insert) operations on a batch of
        documents. Uses Cypher MERGE with ON MATCH/ON CREATE for efficiency.

        The operation:
        1. Sanitizes all documents (removes invalid keys/values)
        2. For each document, attempts to MERGE on match_keys
        3. If node exists: updates all properties
        4. If node doesn't exist: creates with all properties

        Parameters
        ----------
        docs : list[dict]
            Documents to upsert. Each document must contain all match_keys.
        class_name : str
            Node label (e.g., "Person", "Product")
        match_keys : list[str]
            Properties used to identify existing nodes. These form the
            MERGE pattern: ``MERGE (n:Label {key1: val1, key2: val2})``
        **kwargs
            Additional options:
            - dry (bool): If True, build query but don't execute

        Raises
        ------
        ValueError
            If any document is missing a required match_key or has None value

        Examples
        --------
        Insert or update users by email::

            docs = [
                {"email": "alice@example.com", "name": "Alice", "age": 30},
                {"email": "bob@example.com", "name": "Bob", "age": 25}
            ]
            conn.upsert_docs_batch(docs, "User", match_keys=["email"])

        Notes
        -----
        The generated Cypher query uses UNWIND for batch efficiency::

            UNWIND $batch AS row
            MERGE (n:Label {match_key: row.match_key})
            ON MATCH SET n += row
            ON CREATE SET n += row
        """
        dry = kwargs.pop("dry", False)

        if not docs:
            return

        # Sanitize documents: filter invalid keys/values, validate match_keys
        sanitized_docs = self._sanitize_batch(docs, match_keys)

        if not sanitized_docs:
            return

        # Build the MERGE clause with match keys
        index_str = ", ".join([f"{k}: row.{k}" for k in match_keys])
        q = f"""
            UNWIND $batch AS row
            MERGE (n:{class_name} {{ {index_str} }})
            ON MATCH SET n += row
            ON CREATE SET n += row
        """
        if not dry:
            self.execute(q, batch=sanitized_docs)

    def insert_edges_batch(
        self,
        docs_edges: list,
        source_class: str,
        target_class: str,
        relation_name: str,
        collection_name: str | None = None,
        match_keys_source: tuple[str, ...] = ("_key",),
        match_keys_target: tuple[str, ...] = ("_key",),
        filter_uniques: bool = True,
        uniq_weight_fields=None,
        uniq_weight_collections=None,
        upsert_option: bool = False,
        head: int | None = None,
        **kwargs,
    ):
        """Create relationships between existing nodes using Cypher MERGE.

        Efficiently creates relationships in batch by matching source and
        target nodes, then creating or updating the relationship between them.

        Parameters
        ----------
        docs_edges : list
            Edge specifications as list of [source, target, props] triples:
            ``[[{source_props}, {target_props}, {edge_props}], ...]``
        source_class : str
            Label of source nodes (e.g., "Person")
        target_class : str
            Label of target nodes (e.g., "Company")
        relation_name : str
            Relationship type name (e.g., "WORKS_AT")
        collection_name : str, optional
            Unused in FalkorDB (kept for interface compatibility)
        match_keys_source : tuple[str, ...]
            Properties to match source nodes (default: ("_key",))
        match_keys_target : tuple[str, ...]
            Properties to match target nodes (default: ("_key",))
        filter_uniques : bool
            Unused in FalkorDB (kept for interface compatibility)
        uniq_weight_fields
            Unused in FalkorDB (kept for interface compatibility)
        uniq_weight_collections
            Unused in FalkorDB (kept for interface compatibility)
        upsert_option : bool
            Unused in FalkorDB (kept for interface compatibility)
        head : int, optional
            Unused in FalkorDB (kept for interface compatibility)
        **kwargs
            Additional options:
            - dry (bool): If True, build query but don't execute

        Examples
        --------
        Create KNOWS relationships between people::

            edges = [
                [{"id": "1"}, {"id": "2"}, {"since": 2020}],
                [{"id": "1"}, {"id": "3"}, {"since": 2021}]
            ]
            conn.insert_edges_batch(
                edges,
                source_class="Person",
                target_class="Person",
                relation_name="KNOWS",
                match_keys_source=["id"],
                match_keys_target=["id"]
            )

        Notes
        -----
        Generated Cypher pattern::

            UNWIND $batch AS row
            MATCH (source:Label), (target:Label)
            WHERE source.key = row[0].key AND target.key = row[1].key
            MERGE (source)-[r:REL_TYPE]->(target)
            SET r += row[2]
        """
        dry = kwargs.pop("dry", False)

        if not docs_edges:
            return

        # Build match conditions for source and target nodes
        source_match_str = [f"source.{key} = row[0].{key}" for key in match_keys_source]
        target_match_str = [f"target.{key} = row[1].{key}" for key in match_keys_target]

        match_clause = "WHERE " + " AND ".join(source_match_str + target_match_str)

        q = f"""
            UNWIND $batch AS row
            MATCH (source:{source_class}),
                  (target:{target_class}) {match_clause}
            MERGE (source)-[r:{relation_name}]->(target)
            SET r += row[2]
        """
        if not dry:
            self.execute(q, batch=docs_edges)

    def insert_return_batch(self, docs, class_name):
        """Insert nodes and return their properties.

        Note: Limited implementation in FalkorDB.

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

        Returns:
            list: Fetched nodes as dictionaries
        """
        # Build filter clause
        if filters is not None:
            ff = Expression.from_dict(filters)
            # Use NEO4J flavor since FalkorDB uses OpenCypher
            filter_clause = f"WHERE {ff(doc_name='n', kind=DBFlavor.NEO4J)}"
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
            dict: Node properties as dictionary
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
            list: List of fetched edges as dictionaries
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
            ff = Expression.from_dict(filters)
            filter_clause = ff(doc_name="r", kind=ExpressionFlavor.NEO4J)
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
            dict: Edge properties as dictionary
        """
        if hasattr(edge, "properties"):
            return dict(edge.properties)
        elif isinstance(edge, dict):
            return edge
        else:
            return dict(edge) if edge else {}

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

        Args:
            batch: Batch of documents to check
            class_name: Label to check in
            match_keys: Keys to match nodes
            keep_keys: Keys to keep in result
            flatten: Unused in FalkorDB
            filters: Additional query filters

        Returns:
            list: Documents that exist in the database
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
            dict or int: Aggregation results
        """
        # Build filter clause
        if filters is not None:
            ff = Expression.from_dict(filters)
            filter_clause = f"WHERE {ff(doc_name='n', kind=DBFlavor.NEO4J)}"
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
        batch,
        class_name,
        match_keys,
        keep_keys,
        filters: list | dict | None = None,
    ):
        """Keep documents that don't exist in the database.

        Args:
            batch: Batch of documents to check
            class_name: Label to check in
            match_keys: Keys to match nodes
            keep_keys: Keys to keep in result
            filters: Additional query filters

        Returns:
            list: Documents that don't exist in the database
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
