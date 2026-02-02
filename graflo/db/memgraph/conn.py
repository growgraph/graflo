"""Memgraph graph database connector implementation.

This module provides a production-ready connector for Memgraph, a high-performance
in-memory graph database. It implements the graflo Connection interface,
enabling seamless integration with the graflo ETL pipeline.

Architecture
------------
Memgraph is an in-memory graph database with optional persistence.
Data is organized as:

    Memgraph Instance
    └── Database
        ├── Nodes (with labels)
        │   └── Properties (key-value pairs)
        └── Relationships (typed, directed)
            └── Properties (key-value pairs)

Key Features
------------
- **OpenCypher Support**: Full Cypher query language for graph traversals
- **In-Memory Performance**: Sub-millisecond latency with in-memory storage
- **Batch Operations**: Efficient bulk insert/upsert with UNWIND patterns
- **Input Sanitization**: Protection against Cypher injection and malformed data
- **Bolt Protocol**: Standard graph database communication protocol

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

    config = MemgraphConfig(uri="bolt://localhost:7687")

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
Connection is configured via MemgraphConfig:

    - uri: Bolt connection URI (bolt://host:port)
    - username: Optional authentication username
    - password: Optional authentication password

See Also
--------
- Memgraph documentation: https://memgraph.com/docs/
- OpenCypher specification: https://opencypher.org/
- graflo.db.conn.Connection: Base connection interface
"""

import logging
import math
from typing import Any
from urllib.parse import urlparse

import mgclient  # type: ignore[import-untyped]

from graflo.architecture.edge import Edge
from graflo.architecture.schema import Schema
from graflo.architecture.vertex import VertexConfig
from graflo.db.conn import Connection, SchemaExistsError
from graflo.filter.onto import FilterExpression
from graflo.onto import AggregationType
from graflo.onto import DBType


from ..connection.onto import MemgraphConfig

logger = logging.getLogger(__name__)


class QueryResult:
    """Wrapper class for Memgraph query results.

    Provides a consistent API with FalkorDB's QueryResult for compatibility
    with shared test utilities and consistent access patterns.

    Attributes
    ----------
    result_set : list[tuple]
        Raw result rows as tuples (for indexed access)
    columns : list[str]
        Column names from the query
    """

    def __init__(self, columns: list[str], rows: list[tuple]):
        """Initialize QueryResult with columns and rows.

        Parameters
        ----------
        columns : list[str]
            Column names from cursor.description
        rows : list[tuple]
            Raw rows from cursor.fetchall()
        """
        self.columns = columns
        self.result_set = rows

    def __len__(self) -> int:
        """Return the number of result rows."""
        return len(self.result_set)

    def __iter__(self):
        """Iterate over result rows."""
        return iter(self.result_set)


class MemgraphConnection(Connection):
    """Memgraph connector implementing the graflo Connection interface.

    Provides complete graph database operations for Memgraph including
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
    - Query errors propagate as mgclient.DatabaseError
    - Invalid inputs raise ValueError with descriptive messages

    Attributes
    ----------
    flavor : DBType
        Database type identifier (DBType.MEMGRAPH)
    config : MemgraphConfig
        Connection configuration (URI, credentials)
    conn : mgclient.Connection
        Underlying Memgraph connection instance

    Examples
    --------
    Direct instantiation (prefer ConnectionManager for production)::

        config = MemgraphConfig(uri="bolt://localhost:7687")
        conn = MemgraphConnection(config)
        try:
            result = conn.execute("MATCH (n) RETURN count(n)")
        finally:
            conn.close()
    """

    flavor = DBType.MEMGRAPH

    # Type annotations for instance attributes
    conn: mgclient.Connection | None
    _database_name: str

    def __init__(self, config: MemgraphConfig):
        """Initialize Memgraph connection.

        Establishes connection to the Memgraph instance.

        Parameters
        ----------
        config : MemgraphConfig
            Connection configuration with the following fields:
            - uri: Bolt URI (bolt://host:port)
            - username: Username (optional)
            - password: Password (optional)

        Raises
        ------
        ValueError
            If URI is not provided in configuration
        mgclient.DatabaseError
            If unable to connect to Memgraph instance
        """
        super().__init__()
        self.config = config

        if config.uri is None:
            raise ValueError("Memgraph connection requires a URI to be configured")

        # Parse URI to extract host and port
        parsed = urlparse(config.uri)
        host = parsed.hostname or "localhost"
        port = parsed.port or 7687

        # Initialize Memgraph connection
        connect_kwargs: dict[str, Any] = {
            "host": host,
            "port": port,
        }

        if config.username:
            connect_kwargs["username"] = config.username
        if config.password:
            connect_kwargs["password"] = config.password

        self.conn = mgclient.connect(**connect_kwargs)
        self.conn.autocommit = True
        self._database_name = config.database or "memgraph"

    def execute(self, query: str, **kwargs) -> QueryResult:
        """Execute a raw OpenCypher query against the database.

        Executes the provided Cypher query with optional parameters.
        Parameters are safely injected using Memgraph's parameterized
        query mechanism to prevent injection attacks.

        Parameters
        ----------
        query : str
            Cypher query string to execute
        **kwargs
            Query parameters to be safely injected

        Returns
        -------
        QueryResult
            Result object with result_set (list of tuples) and columns

        Examples
        --------
        Simple query::

            result = conn.execute("MATCH (n:Person) RETURN n.name")
            for row in result.result_set:
                print(row[0])  # Access by index

        Parameterized query::

            result = conn.execute(
                "MATCH (n:Person) WHERE n.age > $min_age RETURN n",
                min_age=21
            )
        """
        assert self.conn is not None, "Connection is closed"
        cursor = self.conn.cursor()
        try:
            if kwargs:
                cursor.execute(query, kwargs)
            else:
                cursor.execute(query)
            # mgclient uses Column objects with .name attribute, not tuples
            columns = (
                [col.name for col in cursor.description] if cursor.description else []
            )
            rows = []
            for row in cursor.fetchall():
                processed_row = []
                for value in row:
                    # Convert Memgraph Node/Relationship objects to dicts
                    if hasattr(value, "properties"):
                        processed_row.append(dict(value.properties))
                    else:
                        processed_row.append(value)
                rows.append(tuple(processed_row))
            return QueryResult(columns, rows)
        finally:
            cursor.close()

    def close(self):
        """Close the Memgraph connection."""
        if self.conn is not None:
            self.conn.close()
        self.conn = None

    def create_database(self, name: str):
        """Create a new database (no-op for Memgraph).

        Memgraph uses a single database per instance.
        This method is provided for interface compatibility.

        Args:
            name: Database name (stored for reference)
        """
        self._database_name = name
        logger.info(f"Database name set to '{name}' (Memgraph uses single database)")

    def delete_database(self, name: str):
        """Delete all data from the database.

        Since Memgraph uses a single database, this clears all data.

        Args:
            name: Database name (ignored, clears current database)
        """
        assert self.conn is not None, "Connection is closed"
        try:
            cursor = self.conn.cursor()
            cursor.execute("MATCH (n) DETACH DELETE n")
            cursor.close()
            logger.info("Successfully cleared all data from Memgraph")
        except Exception as e:
            logger.error(f"Failed to clear Memgraph data: {e}", exc_info=True)
            raise

    @staticmethod
    def _is_valid_property_value(value: Any) -> bool:
        """Validate that a value can be stored as a Memgraph property.

        Memgraph cannot store special float values (NaN, Inf) or null bytes.

        Parameters
        ----------
        value
            Value to validate

        Returns
        -------
        bool
            True if value is valid for storage
        """
        if isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                return False
        if isinstance(value, str):
            if "\x00" in value:
                return False
        return True

    @staticmethod
    def _is_valid_property_key(key: Any) -> bool:
        """Validate that a key can be used as a property name.

        Property keys must be non-empty strings that don't start with
        reserved prefixes.

        Parameters
        ----------
        key
            Key to validate

        Returns
        -------
        bool
            True if key is valid
        """
        if not isinstance(key, str):
            return False
        if not key:
            return False
        if key.startswith("_"):
            return True
        return True

    def _sanitize_doc(self, doc: dict) -> dict:
        """Sanitize a document for safe storage.

        Removes invalid keys and values, logs warnings for skipped items.

        Parameters
        ----------
        doc : dict
            Document to sanitize

        Returns
        -------
        dict
            Sanitized document
        """
        sanitized = {}
        for key, value in doc.items():
            if not self._is_valid_property_key(key):
                logger.warning(f"Skipping invalid property key: {key}")
                continue
            if not self._is_valid_property_value(value):
                logger.warning(f"Skipping property '{key}' with invalid value: {value}")
                continue
            sanitized[key] = value
        return sanitized

    def _sanitize_batch(self, docs: list[dict], match_keys: list[str]) -> list[dict]:
        """Sanitize a batch of documents.

        Parameters
        ----------
        docs : list[dict]
            Documents to sanitize
        match_keys : list[str]
            Required keys that must be present

        Returns
        -------
        list[dict]
            List of sanitized documents (invalid documents are skipped)
        """
        sanitized = []
        for doc in docs:
            clean_doc = self._sanitize_doc(doc)
            # Verify all match keys are present and valid
            valid = True
            for key in match_keys:
                if key not in clean_doc or clean_doc[key] is None:
                    logger.warning(
                        f"Document missing required match_key '{key}': {doc}"
                    )
                    valid = False
                    break
            if valid:
                sanitized.append(clean_doc)
        return sanitized

    def define_vertex_indices(self, vertex_config: VertexConfig):
        """Create indices for vertex labels based on configuration.

        Iterates through all vertices defined in the configuration and creates
        indices for each specified field. Memgraph supports indices on node
        properties for faster lookups.

        Parameters
        ----------
        vertex_config : VertexConfig
            Vertex configuration containing vertices and their index definitions.
            Each vertex may have multiple indices, each covering one or more fields.

        Notes
        -----
        - Index creation is idempotent (existing indices are skipped)
        - Uses Memgraph syntax: ``CREATE INDEX ON :Label(property)``
        - Errors are logged but don't stop processing of other indices
        """
        assert self.conn is not None, "Connection is closed"

        for label in vertex_config.vertex_set:
            for index_obj in vertex_config.indexes(label):
                for field in index_obj.fields:
                    try:
                        query = f"CREATE INDEX ON :{label}({field})"
                        cursor = self.conn.cursor()
                        cursor.execute(query)
                        cursor.close()
                        logger.debug(f"Created index on {label}.{field}")
                    except Exception as e:
                        if "already exists" in str(e).lower():
                            logger.debug(f"Index on {label}.{field} already exists")
                        else:
                            logger.warning(
                                f"Failed to create index on {label}.{field}: {e}"
                            )

    def define_edge_indices(self, edges: list[Edge]):
        """Create indices for edge types.

        Memgraph doesn't support relationship property indices in the same way,
        so this creates indices on the relationship properties if defined.

        Parameters
        ----------
        edges : list[Edge]
            List of edge configurations
        """
        assert self.conn is not None, "Connection is closed"
        for edge in edges:
            if edge.relation is None:
                continue
            for idx in edge.indexes:
                for field in idx.fields:
                    try:
                        # Create index on relationship type
                        query = f"CREATE INDEX ON :{edge.relation}({field})"
                        cursor = self.conn.cursor()
                        cursor.execute(query)
                        cursor.close()
                        logger.debug(
                            f"Created index on relationship {edge.relation}.{field}"
                        )
                    except Exception as e:
                        if "already exists" in str(e).lower():
                            logger.debug(
                                f"Index on {edge.relation}.{field} already exists"
                            )
                        else:
                            logger.debug(
                                f"Could not create index on {edge.relation}.{field}: {e}"
                            )

    def delete_graph_structure(
        self,
        vertex_types: tuple[str, ...] | list[str] = (),
        graph_names: tuple[str, ...] | list[str] = (),
        delete_all: bool = False,
    ) -> None:
        """Delete graph structure (nodes and relationships).

        Parameters
        ----------
        vertex_types : list[str], optional
            Specific node labels to delete
        edge_types : list[str], optional
            Specific relationship types to delete (not used, deletes via nodes)
        graph_names : list[str], optional
            Not applicable for Memgraph (single database)
        delete_all : bool
            If True, delete all nodes and relationships
        """
        assert self.conn is not None, "Connection is closed"

        if delete_all:
            cursor = self.conn.cursor()
            cursor.execute("MATCH (n) DETACH DELETE n")
            cursor.close()
            logger.info("Deleted all nodes and relationships")
            return

        # Convert tuple to list if needed
        vertex_types_list = (
            list(vertex_types) if isinstance(vertex_types, tuple) else vertex_types
        )
        if vertex_types_list:
            for label in vertex_types_list:
                try:
                    cursor = self.conn.cursor()
                    cursor.execute(f"MATCH (n:{label}) DETACH DELETE n")
                    cursor.close()
                    logger.debug(f"Deleted all nodes with label '{label}'")
                except Exception as e:
                    logger.warning(f"Failed to delete nodes with label '{label}': {e}")

    def init_db(self, schema: Schema, recreate_schema: bool) -> None:
        """Initialize Memgraph with the given schema.

        If the database already has nodes and recreate_schema is False, raises
        SchemaExistsError and the script halts.

        Parameters
        ----------
        schema : Schema
            Schema containing graph structure definitions
        recreate_schema : bool
            If True, delete all existing data before initialization.
            If False and database has nodes, raises SchemaExistsError.
        """
        assert self.conn is not None, "Connection is closed"

        self._database_name = schema.general.name
        logger.info(f"Initialized Memgraph with schema '{self._database_name}'")

        # Check if database already has nodes (schema/graph exists)
        cursor = self.conn.cursor()
        cursor.execute("MATCH (n) RETURN count(n) AS c")
        row = cursor.fetchone()
        cursor.close()
        count = 0
        if row is not None:
            count = (
                row[0]
                if isinstance(row, (list, tuple))
                else getattr(row, "c", row.get("c", 0) if hasattr(row, "get") else 0)
            )
        if count > 0 and not recreate_schema:
            raise SchemaExistsError(
                f"Schema/graph already exists ({count} nodes). "
                "Set recreate_schema=True to replace, or use clear_data=True before ingestion."
            )

        if recreate_schema:
            try:
                self.delete_graph_structure(delete_all=True)
            except Exception as e:
                logger.warning(f"Error clearing data on recreate_schema: {e}")

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

        Performs atomic upsert (update-or-insert) operations on a batch of
        documents. Uses Cypher MERGE with ON MATCH/ON CREATE for efficiency.
        Existing properties not in the document are preserved on update.

        Parameters
        ----------
        docs : list[dict]
            Documents to upsert. Each document must contain all match_keys
            with non-None values. Invalid documents are skipped with a warning.
        class_name : str
            Node label to create/update (e.g., "Person", "Product")
        match_keys : list[str]
            Property keys used to identify existing nodes for update.
            Supports composite keys (multiple fields).
        **kwargs
            Additional options:
            - dry (bool): If True, build query but don't execute (for debugging)

        Raises
        ------
        ValueError
            If any document is missing a required match_key or has None value.

        Notes
        -----
        - Documents are sanitized before insertion (invalid keys/values removed)
        - NaN, Inf, and null bytes are automatically filtered with warnings
        - The operation is atomic per batch (all succeed or fail together)

        Examples
        --------
        Insert or update Person nodes::

            db.upsert_docs_batch(
                [
                    {"id": "1", "name": "Alice", "age": 30},
                    {"id": "2", "name": "Bob", "age": 25},
                ],
                class_name="Person",
                match_keys=["id"]
            )

        With composite key::

            db.upsert_docs_batch(
                [{"tenant": "acme", "user_id": "u1", "email": "a@b.com"}],
                class_name="User",
                match_keys=["tenant", "user_id"]
            )
        """
        assert self.conn is not None, "Connection is closed"
        dry = kwargs.pop("dry", False)

        if not docs:
            return

        # Convert tuple to list if needed
        match_keys_list = (
            list(match_keys) if isinstance(match_keys, tuple) else match_keys
        )
        # Sanitize documents
        sanitized_docs = self._sanitize_batch(docs, match_keys_list)

        if not sanitized_docs:
            return

        # Auto-create index on match_keys for MERGE performance (idempotent)
        cursor = self.conn.cursor()
        for key in match_keys:
            try:
                cursor.execute(f"CREATE INDEX ON :{class_name}({key})")
                logger.debug(f"Created index on {class_name}.{key}")
            except Exception as e:
                if "already exists" not in str(e).lower():
                    logger.debug(f"Index on {class_name}.{key}: {e}")
        cursor.close()

        # Build the MERGE clause with match keys
        index_str = ", ".join([f"{k}: row.{k}" for k in match_keys_list])
        q = f"""
            UNWIND $batch AS row
            MERGE (n:{class_name} {{ {index_str} }})
            ON MATCH SET n += row
            ON CREATE SET n += row
        """
        if not dry:
            cursor = self.conn.cursor()
            cursor.execute(q, {"batch": sanitized_docs})
            cursor.close()

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
        """Insert a batch of edges using Cypher MERGE.

        Creates relationships between existing nodes by matching source and
        target nodes using the specified match keys, then creating or updating
        the relationship between them.

        Parameters
        ----------
        docs_edges : list[list[dict]]
            Edge specifications as list of [source_doc, target_doc, edge_props]:
            ``[[{source_props}, {target_props}, {edge_props}], ...]``
            - source_props: Properties to match the source node
            - target_props: Properties to match the target node
            - edge_props: Properties to set on the relationship (optional)
        source_class : str
            Label of source nodes (e.g., "Person")
        target_class : str
            Label of target nodes (e.g., "Company")
        relation_name : str
            Relationship type name (e.g., "WORKS_AT")
        collection_name : str, optional
            Not used for Memgraph (kept for interface compatibility)
        match_keys_source : tuple[str, ...]
            Property keys used to identify source nodes (default: ("_key",))
        match_keys_target : tuple[str, ...]
            Property keys used to identify target nodes (default: ("_key",))
        filter_uniques : bool
            Not used for Memgraph (kept for interface compatibility)
        uniq_weight_fields : list[str], optional
            Not used for Memgraph (kept for interface compatibility)
        uniq_weight_collections : list[str], optional
            Not used for Memgraph (kept for interface compatibility)
        **kwargs
            Additional options (currently unused)

        Notes
        -----
        - Edges are created with MERGE, preventing duplicates
        - If source or target node doesn't exist, the edge is silently skipped
        - Edge properties are merged on update (existing props preserved)

        Examples
        --------
        Create relationships between Person and Company nodes::

            db.insert_edges_batch(
                [
                    [{"id": "alice"}, {"id": "acme"}, {"role": "engineer"}],
                    [{"id": "bob"}, {"id": "acme"}, {"role": "manager"}],
                ],
                source_class="Person",
                target_class="Company",
                relation_name="WORKS_AT",
                match_keys_source=("id",),
                match_keys_target=("id",),
            )
        """
        assert self.conn is not None, "Connection is closed"

        if not docs_edges:
            return

        # Handle head limit if specified
        if head is not None and head > 0:
            docs_edges = docs_edges[:head]

        # Build batch data
        batch = []
        for edge_data in docs_edges:
            if len(edge_data) < 2:
                continue

            source_doc = edge_data[0]
            target_doc = edge_data[1]
            edge_props = edge_data[2] if len(edge_data) > 2 else {}

            # Sanitize
            source_doc = self._sanitize_doc(source_doc)
            target_doc = self._sanitize_doc(target_doc)
            edge_props = self._sanitize_doc(edge_props) if edge_props else {}

            batch.append(
                {
                    "source": source_doc,
                    "target": target_doc,
                    "props": edge_props,
                }
            )

        if not batch:
            return

        # Build match patterns
        source_match = ", ".join([f"{k}: row.source.{k}" for k in match_keys_source])
        target_match = ", ".join([f"{k}: row.target.{k}" for k in match_keys_target])

        q = f"""
            UNWIND $batch AS row
            MATCH (s:{source_class} {{ {source_match} }})
            MATCH (t:{target_class} {{ {target_match} }})
            MERGE (s)-[r:{relation_name}]->(t)
            ON CREATE SET r = row.props
            ON MATCH SET r += row.props
        """

        cursor = self.conn.cursor()
        cursor.execute(q, {"batch": batch})
        cursor.close()

    def fetch_docs(
        self,
        class_name: str,
        filters: list | dict | None = None,
        limit: int | None = None,
        return_keys: list[str] | None = None,
        unset_keys: list[str] | None = None,
        **kwargs,
    ) -> list[dict]:
        """Fetch nodes from the database with optional filtering and projection.

        Retrieves nodes matching the specified label and optional filter
        conditions. Supports field projection to return only specific properties.

        Parameters
        ----------
        class_name : str
            Node label to fetch (e.g., "Person", "Product")
        filters : list | dict, optional
            Query filters in graflo expression format.
            Examples: ``["==", "Alice", "name"]`` or ``["AND", [...], [...]]``
        limit : int, optional
            Maximum number of results to return. If None or <= 0, returns all.
        return_keys : list[str], optional
            Properties to include in results (projection). If None, returns
            all properties. Example: ``["id", "name"]``
        unset_keys : list[str], optional
            Not used for Memgraph (kept for interface compatibility)
        **kwargs
            Additional options (currently unused)

        Returns
        -------
        list[dict]
            List of node property dictionaries. Each dict contains the
            requested properties (or all properties if no projection).

        Examples
        --------
        Fetch all Person nodes::

            results = db.fetch_docs("Person")

        Fetch with filter and projection::

            results = db.fetch_docs(
                "Person",
                filters=["==", "Alice", "name"],
                return_keys=["id", "name", "email"],
                limit=10
            )

        Fetch with complex filter::

            results = db.fetch_docs(
                "Product",
                filters=["AND", [">", 100, "price"], ["==", "active", "status"]]
            )
        """
        assert self.conn is not None, "Connection is closed"

        q = f"MATCH (n:{class_name})"

        if filters is not None:
            ff = FilterExpression.from_dict(filters)
            filter_str = ff(doc_name="n", kind=self.expression_flavor())
            q += f" WHERE {filter_str}"

        # Handle projection
        if return_keys:
            return_clause = ", ".join([f"n.{k} AS {k}" for k in return_keys])
            q += f" RETURN {return_clause}"
        else:
            q += " RETURN n"

        if limit is not None and limit > 0:
            q += f" LIMIT {limit}"

        cursor = self.conn.cursor()
        cursor.execute(q)
        results = []

        if return_keys:
            # With projection, build dict from column values
            for row in cursor.fetchall():
                result = {return_keys[i]: row[i] for i in range(len(return_keys))}
                results.append(result)
        else:
            # Without projection, extract node properties
            for row in cursor.fetchall():
                node = row[0]
                if hasattr(node, "properties"):
                    results.append(dict(node.properties))
                else:
                    results.append(node)

        cursor.close()
        return results

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
        """Fetch edges from Memgraph using Cypher.

        Retrieves relationships starting from a specific node, optionally filtered
        by edge type, target node type, and target node ID.

        Args:
            from_type: Source node label (e.g., "Person")
            from_id: Source node ID (property value, typically "id" or "_key")
            edge_type: Optional relationship type to filter by (e.g., "WORKS_AT")
            to_type: Optional target node label to filter by
            to_id: Optional target node ID to filter by
            filters: Additional query filters applied to relationship properties
            limit: Maximum number of edges to return
            return_keys: Keys to return (projection) - not fully supported
            unset_keys: Keys to exclude (projection) - not fully supported
            **kwargs: Additional options

        Returns:
            list: List of fetched edges as dictionaries
        """
        assert self.conn is not None, "Connection is closed"

        # Build Cypher query starting from the source node
        # Use id property (common in Memgraph) or _key if needed
        q = f"MATCH (s:{from_type} {{id: $from_id}})"

        # Build relationship pattern
        if edge_type:
            rel_pattern = f"-[r:{edge_type}]->"
        else:
            rel_pattern = "-[r]->"

        # Build target node match
        if to_type:
            target_match = f"(t:{to_type})"
        else:
            target_match = "(t)"

        q += f" {rel_pattern} {target_match}"

        # Build WHERE clauses
        where_clauses = []
        if to_id:
            where_clauses.append("t.id = $to_id")

        # Add relationship property filters
        if filters is not None:
            ff = FilterExpression.from_dict(filters)
            filter_str = ff(doc_name="r", kind=self.expression_flavor())
            where_clauses.append(filter_str)

        if where_clauses:
            q += f" WHERE {' AND '.join(where_clauses)}"

        # Build RETURN clause
        # Default: return relationship properties and basic node info
        if return_keys:
            # If return_keys specified, try to return those fields
            return_fields = []
            for key in return_keys:
                if key.startswith("from_") or key.startswith("source_"):
                    return_fields.append(
                        f"s.{key.replace('from_', '').replace('source_', '')} AS {key}"
                    )
                elif key.startswith("to_") or key.startswith("target_"):
                    return_fields.append(
                        f"t.{key.replace('to_', '').replace('target_', '')} AS {key}"
                    )
                else:
                    return_fields.append(f"r.{key} AS {key}")
            q += f" RETURN {', '.join(return_fields)}"
        else:
            # Default: return relationship properties and node IDs
            q += " RETURN properties(r) AS props, s.id AS from_id, t.id AS to_id"

        if limit is not None and limit > 0:
            q += f" LIMIT {limit}"

        # Execute query with parameters
        params: dict[str, Any] = {"from_id": from_id}
        if to_id:
            params["to_id"] = to_id

        cursor = self.conn.cursor()
        cursor.execute(q, params)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        results = []
        for row in cursor.fetchall():
            result = {}
            for i, col in enumerate(columns):
                result[col] = row[i]
            # Apply unset_keys if specified
            if unset_keys:
                for key in unset_keys:
                    result.pop(key, None)
            results.append(result)
        cursor.close()
        return results

    def aggregate(
        self,
        class_name: str,
        aggregation_function: AggregationType,
        discriminant: str | None = None,
        aggregated_field: str | None = None,
        filters: list[Any] | dict[str, Any] | None = None,
    ) -> int | float | list[dict[str, Any]] | dict[str, int | float] | None:
        """Perform aggregation operations on nodes.

        Computes aggregate statistics over nodes matching the specified label
        and optional filters. Supports counting, min/max, average, and distinct
        value extraction.

        Parameters
        ----------
        class_name : str
            Node label to aggregate (e.g., "Person", "Product")
        aggregation_function : AggregationType
            Type of aggregation to perform:
            - COUNT: Count matching nodes (with optional GROUP BY)
            - MAX: Maximum value of a field
            - MIN: Minimum value of a field
            - AVERAGE: Average value of a field
            - SORTED_UNIQUE: Distinct values sorted ascending
        discriminant : str, optional
            Field to group by when using COUNT. Returns a dictionary mapping
            each distinct value to its count. Example: ``"status"``
        aggregated_field : str, optional
            Field to aggregate. Required for MAX, MIN, AVERAGE, SORTED_UNIQUE.
            Example: ``"price"``
        filters : list | dict, optional
            Query filters to apply before aggregation.
            Example: ``["==", "active", "status"]``
        **kwargs
            Additional options (currently unused)

        Returns
        -------
        int | float | list | dict[str, int] | None
            - COUNT without discriminant: int (total count)
            - COUNT with discriminant: dict mapping values to counts
            - MAX/MIN/AVERAGE: numeric value or None if no data
            - SORTED_UNIQUE: list of distinct values

        Raises
        ------
        ValueError
            If aggregated_field is missing for non-COUNT aggregations,
            or if an unsupported aggregation type is specified.

        Examples
        --------
        Count all Person nodes::

            count = db.aggregate("Person", AggregationType.COUNT)
            # Returns: 42

        Count by status (GROUP BY)::

            by_status = db.aggregate(
                "Product",
                AggregationType.COUNT,
                discriminant="status"
            )
            # Returns: {"active": 100, "inactive": 25, "draft": 5}

        Get maximum price::

            max_price = db.aggregate(
                "Product",
                AggregationType.MAX,
                aggregated_field="price"
            )
            # Returns: 999.99

        Get distinct categories::

            categories = db.aggregate(
                "Product",
                AggregationType.SORTED_UNIQUE,
                aggregated_field="category"
            )
            # Returns: ["electronics", "furniture", "toys"]
        """
        assert self.conn is not None, "Connection is closed"

        # Build filter clause
        filter_clause = ""
        if filters is not None:
            ff = FilterExpression.from_dict(filters)
            filter_str = ff(doc_name="n", kind=self.expression_flavor())
            filter_clause = f" WHERE {filter_str}"

        q = f"MATCH (n:{class_name}){filter_clause}"

        if aggregation_function == AggregationType.COUNT:
            if discriminant:
                q += f" RETURN n.{discriminant} AS key, count(*) AS count"
                cursor = self.conn.cursor()
                cursor.execute(q)
                rows = cursor.fetchall()
                cursor.close()
                return {row[0]: row[1] for row in rows}
            else:
                q += " RETURN count(n)"
        elif aggregation_function == AggregationType.MAX:
            q += f" RETURN max(n.{aggregated_field})"
        elif aggregation_function == AggregationType.MIN:
            q += f" RETURN min(n.{aggregated_field})"
        elif aggregation_function == AggregationType.AVERAGE:
            q += f" RETURN avg(n.{aggregated_field})"
        elif aggregation_function == AggregationType.SORTED_UNIQUE:
            q += f" RETURN DISTINCT n.{aggregated_field} ORDER BY n.{aggregated_field}"
        else:
            raise ValueError(f"Unsupported aggregation type: {aggregation_function}")

        cursor = self.conn.cursor()
        cursor.execute(q)
        rows = cursor.fetchall()
        cursor.close()

        if aggregation_function == AggregationType.SORTED_UNIQUE:
            return [row[0] for row in rows]
        return rows[0][0] if rows else None

    def define_schema(self, schema: Schema):
        """Define collections based on schema.

        Note: This is a no-op in Memgraph as collections are implicit.
        Labels and relationship types are created when data is inserted.

        Parameters
        ----------
        schema : Schema
            Schema containing collection definitions
        """
        pass

    def insert_return_batch(
        self, docs: list[dict[str, Any]], class_name: str
    ) -> list[dict[str, Any]] | str:
        """Insert nodes and return their properties.

        Parameters
        ----------
        docs : list[dict]
            Documents to insert
        class_name : str
            Label to insert into

        Returns
        -------
        list[dict] | str
            Inserted documents with their properties, or query string

        Raises
        ------
        NotImplementedError
            This method is not fully implemented for Memgraph
        """
        raise NotImplementedError("insert_return_batch is not implemented for Memgraph")

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

        Parameters
        ----------
        batch : list[dict]
            Batch of documents to check
        class_name : str
            Label to check in
        match_keys : list[str]
            Keys to match nodes
        keep_keys : list[str]
            Keys to keep in result
        flatten : bool
            Unused in Memgraph
        filters : list | dict, optional
            Additional query filters

        Returns
        -------
        list[dict]
            Documents that exist in the database
        """
        if not batch:
            return []

        assert self.conn is not None, "Connection is closed"
        results = []

        for doc in batch:
            # Build match conditions
            match_conditions = " AND ".join([f"n.{key} = ${key}" for key in match_keys])
            params = {key: doc.get(key) for key in match_keys}

            # Build return clause with keep_keys
            if keep_keys:
                return_clause = ", ".join([f"n.{k} AS {k}" for k in keep_keys])
            else:
                return_clause = "n"

            q = f"MATCH (n:{class_name}) WHERE {match_conditions} RETURN {return_clause} LIMIT 1"

            cursor = self.conn.cursor()
            cursor.execute(q, params)
            rows = cursor.fetchall()
            cursor.close()

            if rows:
                if keep_keys:
                    result = {keep_keys[i]: rows[0][i] for i in range(len(keep_keys))}
                else:
                    node = rows[0][0]
                    if hasattr(node, "properties"):
                        result = dict(node.properties)
                    else:
                        result = node
                results.append(result)

        return results

    def keep_absent_documents(
        self,
        batch: list[dict[str, Any]],
        class_name: str,
        match_keys: list[str] | tuple[str, ...],
        keep_keys: list[str] | tuple[str, ...] | None = None,
        filters: list[Any] | dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Keep documents that don't exist in the database.

        Parameters
        ----------
        batch : list[dict]
            Batch of documents to check
        class_name : str
            Label to check in
        match_keys : list[str]
            Keys to match nodes
        keep_keys : list[str]
            Keys to keep in result
        filters : list | dict, optional
            Additional query filters

        Returns
        -------
        list[dict]
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

        # Keep documents that don't exist
        absent = []
        for doc in batch:
            key_tuple = tuple(doc.get(k) for k in match_keys)
            if key_tuple not in present_keys:
                if keep_keys:
                    absent.append({k: doc.get(k) for k in keep_keys})
                else:
                    absent.append(doc)

        return absent
