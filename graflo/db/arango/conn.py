"""ArangoDB connection implementation for graph database operations.

This module implements the Connection interface for ArangoDB, providing
specific functionality for graph operations in ArangoDB. It handles:

- Graph and vertex/edge class management (ArangoDB uses collections internally)
- Document and edge operations
- Index creation and management
- AQL query execution
- Batch operations with upsert support

Key Features:

    - Graph-based document organization
    - Vertex and edge class management
    - Persistent, hash, skiplist, and fulltext indices
    - Batch document and edge operations
    - AQL query generation and execution

Example:
    >>> conn = ArangoConnection(config)
    >>> conn.init_db(schema, clean_start=True)
    >>> conn.upsert_docs_batch(docs, "users", match_keys=["email"])
"""

import json
import logging
from typing import Any, cast

from arango import ArangoClient
from arango.graph import Graph

from graflo.architecture.edge import Edge
from graflo.architecture.onto import (
    Index,
    IndexType,
)
from graflo.architecture.schema import Schema
from graflo.architecture.vertex import VertexConfig
from graflo.db.arango.query import fetch_fields_query
from graflo.db.arango.util import render_filters
from graflo.db.conn import Connection
from graflo.db.util import get_data_from_cursor, json_serializer
from graflo.filter.onto import Clause
from graflo.onto import AggregationType
from graflo.util.transform import pick_unique_dict
from graflo.onto import DBType


from ..connection.onto import ArangoConfig

logger = logging.getLogger(__name__)

# Alias for backward compatibility
_json_serializer = json_serializer


class ArangoConnection(Connection):
    """ArangoDB-specific implementation of the Connection interface.

    This class provides ArangoDB-specific implementations for all database
    operations, including graph management, document operations, and query
    execution. It uses the ArangoDB Python driver for all operations.

    Attributes:
        conn: ArangoDB database connection instance
    """

    def __init__(self, config: ArangoConfig):
        """Initialize ArangoDB connection.

        Args:
            config: ArangoDB connection configuration containing URL, credentials,
                and database name
        """
        super().__init__()
        # Store config for later use
        self.config = config
        # Validate required config values
        if config.url is None:
            raise ValueError("ArangoDB connection requires a URL to be configured")
        if config.database is None:
            raise ValueError(
                "ArangoDB connection requires a database name to be configured"
            )

        # ArangoDB accepts empty string for password if None
        password = config.password if config.password is not None else ""
        # ArangoDB has default username "root" if None
        username = config.username if config.username is not None else "root"

        # Store client for system operations
        self.client = ArangoClient(
            hosts=config.url, request_timeout=config.request_timeout
        )
        # Connect to the configured database for regular operations
        self.conn = self.client.db(
            config.database,
            username=username,
            password=password,
        )
        # Store credentials for system operations
        self._username = username
        self._password = password

    def create_database(self, name: str) -> None:
        """Create a new ArangoDB database.

        Database creation/deletion operations must be performed from the _system database.

        Args:
            name: Name of the database to create
        """
        try:
            # Connect to _system database for system operations
            system_db = self.client.db(
                "_system", username=self._username, password=self._password
            )
            if not system_db.has_database(name):
                try:
                    system_db.create_database(name)
                    logger.info(f"Successfully created ArangoDB database '{name}'")
                except Exception as create_error:
                    logger.error(
                        f"Failed to create ArangoDB database '{name}': {create_error}",
                        exc_info=True,
                    )
                    raise
            else:
                logger.debug(f"ArangoDB database '{name}' already exists")
        except Exception as e:
            logger.error(
                f"Error creating ArangoDB database '{name}': {e}",
                exc_info=True,
            )
            raise

    def delete_database(self, name: str) -> None:
        """Delete an ArangoDB database.

        Database creation/deletion operations must be performed from the _system database.

        Args:
            name: Name of the database to delete
        """
        try:
            # Connect to _system database for system operations
            system_db = self.client.db(
                "_system", username=self._username, password=self._password
            )
            if system_db.has_database(name):
                try:
                    system_db.delete_database(name)
                    logger.info(f"Successfully deleted ArangoDB database '{name}'")
                except Exception as delete_error:
                    logger.error(
                        f"Failed to delete ArangoDB database '{name}': {delete_error}",
                        exc_info=True,
                    )
                    raise
            else:
                logger.debug(
                    f"ArangoDB database '{name}' does not exist, skipping deletion"
                )
        except Exception as e:
            logger.error(
                f"Error deleting ArangoDB database '{name}': {e}",
                exc_info=True,
            )
            raise

    def execute(self, query: str, **kwargs: Any) -> Any:
        """Execute an AQL query.

        Args:
            query: AQL query string to execute
            **kwargs: Additional query parameters

        Returns:
            Cursor: ArangoDB cursor for the query results
        """
        cursor = self.conn.aql.execute(query)
        return cursor

    def close(self) -> None:
        """Close the ArangoDB connection."""
        # self.conn.close()
        pass

    def init_db(self, schema: Schema, clean_start: bool) -> None:
        """Initialize ArangoDB with the given schema.

        Checks if the database exists and creates it if it doesn't.
        Uses schema.general.name if database is not set in config.

        Args:
            schema: Schema containing graph structure definitions
            clean_start: If True, delete all existing vertex and edge classes before initialization
        """
        # Determine database name: use config.database if set, otherwise use schema.general.name
        db_name = self.config.database
        if not db_name:
            db_name = schema.general.name
            # Update config for subsequent operations
            self.config.database = db_name

        # Check if database exists and create it if it doesn't
        # Use context manager pattern for system database operations
        try:
            system_db = self.client.db(
                "_system", username=self._username, password=self._password
            )
            if not system_db.has_database(db_name):
                logger.info(f"Database '{db_name}' does not exist, creating it...")
                try:
                    system_db.create_database(db_name)
                    logger.info(f"Successfully created database '{db_name}'")
                except Exception as create_error:
                    logger.error(
                        f"Failed to create database '{db_name}': {create_error}",
                        exc_info=True,
                    )
                    raise

            # Reconnect to the target database (newly created or existing)
            if (
                self.config.database != db_name
                or not hasattr(self, "_db_connected")
                or self._db_connected != db_name
            ):
                try:
                    self.conn = self.client.db(
                        db_name, username=self._username, password=self._password
                    )
                    self._db_connected = db_name
                    logger.debug(f"Connected to database '{db_name}'")
                except Exception as conn_error:
                    logger.error(
                        f"Failed to connect to database '{db_name}': {conn_error}",
                        exc_info=True,
                    )
                    raise
        except Exception as e:
            logger.error(
                f"Error during database initialization for '{db_name}': {e}",
                exc_info=True,
            )
            raise

        try:
            if clean_start:
                try:
                    self.delete_graph_structure((), (), delete_all=True)
                    logger.debug(f"Cleaned database '{db_name}' for fresh start")
                except Exception as clean_error:
                    logger.warning(
                        f"Error during clean_start for database '{db_name}': {clean_error}",
                        exc_info=True,
                    )
                    # Continue - may be first run or already clean

            try:
                self.define_schema(schema)
                logger.debug(f"Defined schema for database '{db_name}'")
            except Exception as schema_error:
                logger.error(
                    f"Failed to define schema for database '{db_name}': {schema_error}",
                    exc_info=True,
                )
                raise

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

    def define_schema(self, schema: Schema) -> None:
        """Define ArangoDB collections based on schema.

        Args:
            schema: Schema containing collection definitions
        """
        self.define_vertex_classes(schema)
        self.define_edge_classes(schema.edge_config.edges_list(include_aux=True))

    def define_vertex_classes(self, schema: Schema) -> None:
        """Define vertex collections in ArangoDB.

        Creates vertex collections for both connected and disconnected vertices,
        organizing them into appropriate graphs.

        Args:
            schema: Schema containing vertex definitions
        """
        vertex_config = schema.vertex_config
        disconnected_vertex_collections = (
            set(vertex_config.vertex_set) - schema.edge_config.vertices
        )
        for item in schema.edge_config.edges_list():
            u, v = item.source, item.target
            gname = item.graph_name
            if not gname:
                logger.warning(
                    f"Edge {item.source} -> {item.target} has no graph_name, skipping"
                )
                continue
            logger.info(f"{item.source}, {item.target}, {gname}")
            if self.conn.has_graph(gname):
                g_result = self.conn.graph(gname)
            else:
                g_result = self.conn.create_graph(gname)  # type: ignore

            # Type narrowing: ensure g is a Graph instance
            g: Graph | None = None
            if isinstance(g_result, Graph):
                g = g_result
            elif g_result is not None:
                # If it's not a Graph, log warning and skip
                logger.warning(f"Graph {gname} is not a Graph instance, skipping")
                continue

            _ = self.create_collection(
                vertex_config.vertex_dbname(u), vertex_config.index(u), g
            )

            _ = self.create_collection(
                vertex_config.vertex_dbname(v), vertex_config.index(v), g
            )
        for v in disconnected_vertex_collections:
            _ = self.create_collection(
                vertex_config.vertex_dbname(v), vertex_config.index(v), None
            )

    def define_edge_classes(self, edges: list[Edge]) -> None:
        """Define edge classes in ArangoDB.

        Creates edge collections and their definitions in the appropriate graphs.

        Args:
            edges: List of edge configurations to create
        """
        for item in edges:
            gname = item.graph_name
            if not gname:
                logger.warning("Edge has no graph_name, skipping")
                continue
            if self.conn.has_graph(gname):
                g_result = self.conn.graph(gname)
            else:
                g_result = self.conn.create_graph(gname)
            # Type guard: ensure g is a Graph instance
            if not isinstance(g_result, Graph):
                logger.warning(f"Graph {gname} is not a Graph instance, skipping")
                continue
            g = g_result
            collection_name = item.database_name
            if not collection_name:
                logger.warning("Edge has no database_name, skipping")
                continue
            if not g.has_edge_definition(collection_name):
                _ = g.create_edge_definition(
                    edge_collection=collection_name,
                    from_vertex_collections=[item._source],
                    to_vertex_collections=[item._target],
                )

    def _add_index(self, general_collection: Any, index: Index) -> Any | None:
        """Add an index to an ArangoDB collection.

        Supports persistent, hash, skiplist, and fulltext indices.

        Args:
            general_collection: ArangoDB collection to add index to
            index: Index configuration to create

        Returns:
            IndexHandle: Handle to the created index, or None if index type is not supported
        """
        data = index.db_form(DBType.ARANGO)
        ih: Any | None = None
        if index.type == IndexType.PERSISTENT:
            ih = general_collection.add_index(data)
        elif index.type == IndexType.HASH:
            ih = general_collection.add_index(data)
        elif index.type == IndexType.SKIPLIST:
            ih = general_collection.add_skiplist_index(
                fields=index.fields, unique=index.unique
            )
        elif index.type == IndexType.FULLTEXT:
            ih = general_collection.add_index(
                data={"fields": index.fields, "type": "fulltext"}
            )
        return ih

    def define_vertex_indices(self, vertex_config: VertexConfig) -> None:
        """Define indices for vertex collections.

        Creates indices for each vertex collection based on the configuration.

        Args:
            vertex_config: Vertex configuration containing index definitions
        """
        for c in vertex_config.vertex_set:
            general_collection = self.conn.collection(vertex_config.vertex_dbname(c))
            ixs = general_collection.indexes()
            field_combinations: list[tuple[Any, ...]] = []
            if isinstance(ixs, list):
                for ix in ixs:
                    if isinstance(ix, dict):
                        ix_dict = cast(dict[str, Any], ix)
                        fields_value = ix_dict.get("fields")
                        if isinstance(fields_value, (list, tuple)):
                            field_combinations.append(tuple(fields_value))
            for index_obj in vertex_config.indexes(c):
                if tuple(index_obj.fields) not in field_combinations:
                    self._add_index(general_collection, index_obj)

    def define_edge_indices(self, edges: list[Edge]) -> None:
        """Define indices for edge collections.

        Creates indices for each edge collection based on the configuration.

        Args:
            edges: List of edge configurations containing index definitions
        """
        for edge in edges:
            collection_name = edge.database_name
            if not collection_name:
                logger.warning("Edge has no database_name, skipping index creation")
                continue
            general_collection = self.conn.collection(collection_name)
            for index_obj in edge.indexes:
                self._add_index(general_collection, index_obj)

    def fetch_indexes(self, db_class_name: str | None = None) -> dict[str, Any]:
        """Fetch all indices from the database.

        Args:
            db_class_name: Optional collection name to fetch indices for

        Returns:
            dict: Mapping of collection names to their indices
        """
        classes: list[Any] = []
        if db_class_name is None:
            classes_result = self.conn.collections()
            if isinstance(classes_result, list):
                classes = classes_result
        elif self.conn.has_collection(db_class_name):
            classes = [self.conn.collection(db_class_name)]

        r: dict[str, Any] = {}
        for cname in classes:
            if isinstance(cname, dict):
                cname_dict = cast(dict[str, Any], cname)
                name_value = cname_dict.get("name")
                if isinstance(name_value, str):
                    c = self.conn.collection(name_value)
                    r[name_value] = c.indexes()
        return r

    def create_collection(
        self,
        db_class_name: str,
        index: None | Index = None,
        g: Graph | None = None,
    ) -> Any | None:
        """Create a new vertex or edge class (ArangoDB uses collections internally).

        Args:
            db_class_name: Name of the vertex/edge class to create (ArangoDB collection name)
            index: Optional index to create on the class
            g: Optional graph to create the class in

        Returns:
            IndexHandle: Handle to the created index if one was created, None otherwise
        """
        if not self.conn.has_collection(db_class_name):
            if g is not None:
                _ = g.create_vertex_collection(db_class_name)
            else:
                self.conn.create_collection(db_class_name)
            general_collection = self.conn.collection(db_class_name)
            if index is not None and index.fields != ["_key"]:
                ih = self._add_index(general_collection, index)
                return ih
            else:
                return None

    def delete_graph_structure(
        self,
        vertex_types: tuple[str, ...] | list[str] = (),
        graph_names: tuple[str, ...] | list[str] = (),
        delete_all: bool = False,
    ) -> None:
        """Delete graph structure (vertex/edge classes and graphs) from ArangoDB.

        In ArangoDB:
        - Collections (internal): Container for vertices (vertex collections) and edges (edge collections)
        - Graphs: Named graphs that connect vertex and edge collections

        Args:
            vertex_types: Vertex/edge class names to delete (ArangoDB collection names)
            graph_names: Graph names to delete
            delete_all: If True, delete all non-system vertex/edge classes and graphs
        """
        cnames: list[str] = list(vertex_types)
        gnames: list[str] = list(graph_names)
        logger.info("vertex/edge classes (non system, ArangoDB collections):")
        collections_result = self.conn.collections()
        if isinstance(collections_result, list):
            filtered_collections: list[dict[str, Any]] = []
            for c in collections_result:
                if isinstance(c, dict):
                    c_dict = cast(dict[str, Any], c)
                    name_value = c_dict.get("name")
                    if isinstance(name_value, str) and name_value[0] != "_":
                        filtered_collections.append(c_dict)
            logger.info(filtered_collections)
        else:
            logger.info([])

        if delete_all:
            collections_result = self.conn.collections()
            graphs_result = self.conn.graphs()
            cnames = []
            if isinstance(collections_result, list):
                for c in collections_result:
                    if isinstance(c, dict):
                        c_dict = cast(dict[str, Any], c)
                        name_value = c_dict.get("name")
                        if isinstance(name_value, str) and name_value[0] != "_":
                            cnames.append(name_value)
            gnames = []
            if isinstance(graphs_result, list):
                for g in graphs_result:
                    if isinstance(g, dict):
                        g_dict = cast(dict[str, Any], g)
                        name_value = g_dict.get("name")
                        if isinstance(name_value, str):
                            gnames.append(name_value)

        for gn in gnames:
            if self.conn.has_graph(gn):
                self.conn.delete_graph(gn)

        logger.info("graphs (after delete operation):")
        logger.info(self.conn.graphs())

        for cn in cnames:
            if self.conn.has_collection(cn):
                self.conn.delete_collection(cn)

        logger.info(
            "vertex/edge classes (after delete operation, ArangoDB collections):"
        )
        collections_result = self.conn.collections()
        if isinstance(collections_result, list):
            collection_names: list[str] = []
            for c in collections_result:
                if isinstance(c, dict):
                    c_dict = cast(dict[str, Any], c)
                    name_value = c_dict.get("name")
                    if isinstance(name_value, str) and name_value[0] != "_":
                        collection_names.append(name_value)
            logger.info(collection_names)
        else:
            logger.info([])

        logger.info("graphs:")
        logger.info(self.conn.graphs())

    def get_collections(self) -> list[dict[str, Any]]:
        """Get all vertex and edge classes in the database (ArangoDB collections).

        Returns:
            list: List of class information dictionaries (ArangoDB collection info)
        """
        result = self.conn.collections()
        if isinstance(result, list):
            return [
                cast(dict[str, Any], item) if isinstance(item, dict) else {}
                for item in result
            ]
        return []

    def upsert_docs_batch(
        self,
        docs: list[dict[str, Any]],
        class_name: str,
        match_keys: list[str] | tuple[str, ...] = (),
        **kwargs: Any,
    ) -> None:
        """Upsert a batch of documents using AQL.

        Performs an upsert operation on a batch of documents, using the specified
        match keys to determine whether to update existing documents or insert new ones.

        Args:
            docs: List of documents to upsert
            class_name: Collection name to upsert into
            match_keys: Keys to match for upsert operation
            **kwargs: Additional options:
                - dry: If True, don't execute the query
                - update_keys: Keys to update on match
                - filter_uniques: If True, filter duplicate documents
        """
        dry = kwargs.pop("dry", False)
        update_keys = kwargs.pop("update_keys", None)
        filter_uniques = kwargs.pop("filter_uniques", True)

        if not docs:
            return
        if filter_uniques:
            docs = pick_unique_dict(docs)
        docs_json = json.dumps(docs, default=json_serializer)
        if not match_keys:
            upsert_clause = ""
            update_clause = ""
        else:
            upsert_clause = ", ".join([f'"{k}": doc.{k}' for k in match_keys])
            upsert_clause = f"UPSERT {{{upsert_clause}}}"

            if isinstance(update_keys, list):
                update_clause = ", ".join([f'"{k}": doc.{k}' for k in update_keys])
                update_clause = f"{{{update_clause}}}"
            elif update_keys == "doc":
                update_clause = "doc"
            else:
                update_clause = "{}"
            update_clause = f"UPDATE {update_clause}"

        options = "OPTIONS {exclusive: true, ignoreErrors: true}"

        q_update = f"""FOR doc in {docs_json}
                            {upsert_clause}
                            INSERT doc
                            {update_clause} 
                                IN {class_name} {options}"""
        if not dry:
            self.execute(q_update)

    def insert_edges_batch(
        self,
        docs_edges: list[list[dict[str, Any]]] | list[Any] | None,
        source_class: str,
        target_class: str,
        relation_name: str,
        match_keys_source: tuple[str, ...] = ("_key",),
        match_keys_target: tuple[str, ...] = ("_key",),
        filter_uniques: bool = True,
        head: int | None = None,
        **kwargs: Any,
    ) -> None:
        """Insert a batch of edges using AQL.

        Creates edges between source and target vertices, with support for
        weight fields and unique constraints.

        Args:
            docs_edges: List of edge documents in format [{_source_aux: source_doc, _target_aux: target_doc}]
            source_class: Source vertex class name
            target_class: Target vertex class name
            relation_name: Optional relation name for the edges
            match_keys_source: Keys to match source vertices
            match_keys_target: Keys to match target vertices
            filter_uniques: If True, filter duplicate edges
            head: Optional limit on number of edges to insert
            **kwargs: Additional options:
                - dry: If True, don't execute the query
                - collection_name: Edge collection name (defaults to {source_class}_{target_class}_edges if not provided)
                - uniq_weight_fields: Fields to consider for uniqueness
                - uniq_weight_collections: Classes to consider for uniqueness
                - upsert_option: If True, use upsert instead of insert
        """
        dry = kwargs.pop("dry", False)

        # Extract collection_name from kwargs, with default generation
        collection_name = kwargs.pop("collection_name", None)
        if collection_name is None:
            collection_name = f"{source_class}_{target_class}_edges"

        # Extract ArangoDB-specific parameters from kwargs
        uniq_weight_fields = kwargs.pop("uniq_weight_fields", None)
        uniq_weight_collections = kwargs.pop("uniq_weight_collections", None)
        upsert_option = kwargs.pop("upsert_option", False)

        if isinstance(docs_edges, list):
            if docs_edges:
                logger.debug(f" docs_edges[0] = {docs_edges[0]}")
            if head is not None:
                docs_edges = docs_edges[:head]
            if filter_uniques:
                docs_edges = pick_unique_dict(docs_edges)
            docs_edges_str = json.dumps(docs_edges)
        else:
            return

        if match_keys_source[0] == "_key":
            result_from = f'CONCAT("{source_class}/", edge[0]._key)'
            source_filter = ""
        else:
            result_from = "sources[0]._id"
            filter_source = " && ".join(
                [f"v.{k} == edge[0].{k}" for k in match_keys_source]
            )
            source_filter = (
                f"LET sources = (FOR v IN {source_class} FILTER"
                f" {filter_source} LIMIT 1 RETURN v)"
            )

        if match_keys_target[0] == "_key":
            result_to = f'CONCAT("{target_class}/", edge[1]._key)'
            target_filter = ""
        else:
            result_to = "targets[0]._id"
            filter_target = " && ".join(
                [f"v.{k} == edge[1].{k}" for k in match_keys_target]
            )
            target_filter = (
                f"LET targets = (FOR v IN {target_class} FILTER"
                f" {filter_target} LIMIT 1 RETURN v)"
            )

        doc_definition = f"MERGE({{_from : {result_from}, _to : {result_to}}}, edge[2])"

        logger.debug(f" source_filter = {source_filter}")
        logger.debug(f" target_filter = {target_filter}")
        logger.debug(f" doc = {doc_definition}")

        if upsert_option:
            ups_from = result_from if source_filter else "doc._from"
            ups_to = result_to if target_filter else "doc._to"

            weight_fs = []
            if uniq_weight_fields is not None:
                weight_fs += uniq_weight_fields
            if uniq_weight_collections is not None:
                weight_fs += uniq_weight_collections
            if relation_name is not None:
                weight_fs += ["relation"]

            if weight_fs:
                weights_clause = ", " + ", ".join(
                    [f"'{x}' : edge.{x}" for x in weight_fs]
                )
            else:
                weights_clause = ""

            upsert = f"{{'_from': {ups_from}, '_to': {ups_to}" + weights_clause + "}"
            logger.debug(f" upsert clause: {upsert}")
            clauses = f"UPSERT {upsert} INSERT doc UPDATE {{}}"
            options = "OPTIONS {exclusive: true}"
        else:
            if relation_name is None:
                doc_clause = "doc"
            else:
                doc_clause = f"MERGE(doc, {{'relation': '{relation_name}' }})"
            clauses = f"INSERT {doc_clause}"
            options = "OPTIONS {exclusive: true, ignoreErrors: true}"

        q_update = f"""
            FOR edge in {docs_edges_str} {source_filter} {target_filter}
                LET doc = {doc_definition}
                {clauses}
                in {collection_name} {options}"""
        if not dry:
            self.execute(q_update)

    def insert_return_batch(self, docs: list[dict[str, Any]], class_name: str) -> str:
        """Insert documents and return the AQL query string.

        Note: ArangoDB-specific behavior - returns query string instead of executing.
        This allows for deferred execution and is used by caster.py and tests.

        Args:
            docs: Documents to insert
            class_name: Collection to insert into

        Returns:
            str: AQL query string for the operation (can be executed with execute())
        """
        docs_str = json.dumps(docs, default=json_serializer)
        query0 = f"""FOR doc in {docs_str}
              INSERT doc
              INTO {class_name}
              LET inserted = NEW
              RETURN {{_key: inserted._key}}
        """
        return query0

    def fetch_present_documents(
        self,
        batch: list[dict[str, Any]],
        class_name: str,
        match_keys: list[str] | tuple[str, ...],
        keep_keys: list[str] | tuple[str, ...] | None = None,
        flatten: bool = False,
        filters: None | Clause | list[Any] | dict[str, Any] = None,
    ) -> list[dict[str, Any]] | dict[int, list[dict[str, Any]]]:
        """Fetch documents that exist in the database.

        Args:
            batch: Batch of documents to check
            class_name: Collection to check in
            match_keys: Keys to match documents
            keep_keys: Keys to keep in result
            flatten: If True, flatten the result into a list
            filters: Additional query filters

        Returns:
            list | dict: Documents that exist in the database, either as a
                flat list or a dictionary mapping batch indices to documents
        """
        q0 = fetch_fields_query(
            collection_name=class_name,
            docs=batch,
            match_keys=match_keys,
            keep_keys=keep_keys,
            filters=filters,
        )
        # {"__i": i, "_group": [doc]}
        cursor = self.execute(q0)

        if flatten:
            rdata = []
            for item in get_data_from_cursor(cursor):
                group = item.pop("_group", [])
                rdata += [sub_item for sub_item in group]
            return rdata
        else:
            rdata_dict: dict[int, list[dict[str, Any]]] = {}
            for item in get_data_from_cursor(cursor):
                __i = item.pop("__i")
                group = item.pop("_group", [])
                rdata_dict[__i] = group
            return rdata_dict

    def fetch_docs(
        self,
        class_name: str,
        filters: None | Clause | list[Any] | dict[str, Any] = None,
        limit: int | None = None,
        return_keys: list[str] | None = None,
        unset_keys: list[str] | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Fetch documents from a collection.

        Args:
            class_name: Collection to fetch from
            filters: Query filters
            limit: Maximum number of documents to return
            return_keys: Keys to return
            unset_keys: Keys to unset

        Returns:
            list: Fetched documents
        """
        filter_clause = render_filters(filters, doc_name="d")

        if return_keys is None:
            if unset_keys is None:
                return_clause = "d"
            else:
                tmp_clause = ", ".join([f'"{item}"' for item in unset_keys])
                return_clause = f"UNSET(d, {tmp_clause})"
        else:
            if unset_keys is None:
                tmp_clause = ", ".join([f'"{item}"' for item in return_keys])
                return_clause = f"KEEP(d, {tmp_clause})"
            else:
                raise ValueError("both return_keys and unset_keys are set")

        if limit is not None and isinstance(limit, int):
            limit_clause = f"LIMIT {limit}"
        else:
            limit_clause = ""

        q = (
            f"FOR d in {class_name}"
            f"  {filter_clause}"
            f"  {limit_clause}"
            f"  RETURN {return_clause}"
        )
        cursor = self.execute(q)
        return get_data_from_cursor(cursor)

    # TODO test
    def fetch_edges(
        self,
        from_type: str,
        from_id: str,
        edge_type: str | None = None,
        to_type: str | None = None,
        to_id: str | None = None,
        filters: list[Any] | dict[str, Any] | Clause | None = None,
        limit: int | None = None,
        return_keys: list[str] | None = None,
        unset_keys: list[str] | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Fetch edges from ArangoDB using AQL.

        Args:
            from_type: Source vertex collection name
            from_id: Source vertex ID (can be _key or _id)
            edge_type: Optional edge collection name to filter by
            to_type: Optional target vertex collection name to filter by
            to_id: Optional target vertex ID to filter by
            filters: Additional query filters
            limit: Maximum number of edges to return
            return_keys: Keys to return (projection)
            unset_keys: Keys to exclude (projection)
            **kwargs: Additional parameters

        Returns:
            list: List of fetched edges
        """
        # Convert from_id to _id format if needed
        if not from_id.startswith(from_type):
            # Assume it's a _key, convert to _id
            from_vertex_id = f"{from_type}/{from_id}"
        else:
            from_vertex_id = from_id

        # Build AQL query to fetch edges
        # Start with basic edge traversal
        if edge_type:
            edge_collection = edge_type
        else:
            # If no edge_type specified, we need to search all edge collections
            # This is a simplified version - in practice you might want to list all edge collections
            raise ValueError("edge_type is required for ArangoDB edge fetching")

        filter_clause = render_filters(filters, doc_name="e")
        filter_parts = []

        if to_type:
            filter_parts.append(f"e._to LIKE '{to_type}/%'")
        if to_id and to_type:
            if not to_id.startswith(to_type):
                to_vertex_id = f"{to_type}/{to_id}"
            else:
                to_vertex_id = to_id
            filter_parts.append(f"e._to == '{to_vertex_id}'")

        additional_filters = " && ".join(filter_parts)
        if filter_clause and additional_filters:
            filter_clause = f"{filter_clause} && {additional_filters}"
        elif additional_filters:
            filter_clause = additional_filters

        query = f"""
            FOR e IN {edge_collection}
                FILTER e._from == '{from_vertex_id}'
                {f"FILTER {filter_clause}" if filter_clause else ""}
                {f"LIMIT {limit}" if limit else ""}
                RETURN e
        """

        cursor = self.execute(query)
        result = list(get_data_from_cursor(cursor))

        # Apply projection
        if return_keys is not None:
            result = [
                {k: doc.get(k) for k in return_keys if k in doc} for doc in result
            ]
        elif unset_keys is not None:
            result = [
                {k: v for k, v in doc.items() if k not in unset_keys} for doc in result
            ]

        return result

    def aggregate(
        self,
        class_name: str,
        aggregation_function: AggregationType,
        discriminant: str | None = None,
        aggregated_field: str | None = None,
        filters: None | Clause | list[Any] | dict[str, Any] = None,
    ) -> list[dict[str, Any]]:
        """Perform aggregation on a collection.

        Args:
            class_name: Collection to aggregate
            aggregation_function: Type of aggregation to perform
            discriminant: Field to group by
            aggregated_field: Field to aggregate
            filters: Query filters

        Returns:
            list: Aggregation results
        """
        filter_clause = render_filters(filters, doc_name="doc")

        if (
            aggregated_field is not None
            and aggregation_function != AggregationType.COUNT
        ):
            group_unit = f"g[*].doc.{aggregated_field}"
        else:
            group_unit = "g"

        if discriminant is not None:
            collect_clause = f"COLLECT value = doc['{discriminant}'] INTO g"
            return_clause = f"""{{ '{discriminant}' : value, '_value': {aggregation_function}({group_unit})}}"""
        else:
            if (
                aggregated_field is None
                and aggregation_function == AggregationType.COUNT
            ):
                collect_clause = (
                    f"COLLECT AGGREGATE value =  {aggregation_function} (doc)"
                )
            else:
                collect_clause = (
                    "COLLECT AGGREGATE value ="
                    f" {aggregation_function}(doc['{aggregated_field}'])"
                )
            return_clause = """{ '_value' : value }"""

        q = f"""FOR doc IN {class_name} 
                    {filter_clause}
                    {collect_clause}
                    RETURN {return_clause}"""

        cursor = self.execute(q)
        data = get_data_from_cursor(cursor)
        return data

    def keep_absent_documents(
        self,
        batch: list[dict[str, Any]],
        class_name: str,
        match_keys: list[str] | tuple[str, ...],
        keep_keys: list[str] | tuple[str, ...] | None = None,
        filters: None | Clause | list[Any] | dict[str, Any] = None,
    ) -> list[dict[str, Any]]:
        """Keep documents that don't exist in the database.

        Args:
            batch: Batch of documents to check
            class_name: Collection to check in
            match_keys: Keys to match documents
            keep_keys: Keys to keep in result
            filters: Additional query filters

        Returns:
            list: Documents that don't exist in the database
        """
        present_docs_keys = self.fetch_present_documents(
            batch=batch,
            class_name=class_name,
            match_keys=match_keys,
            keep_keys=keep_keys,
            flatten=False,
            filters=filters,
        )

        assert isinstance(present_docs_keys, dict)

        if any([len(v) > 1 for v in present_docs_keys.values()]):
            logger.warning(
                "fetch_present_documents returned multiple docs per filtering condition"
            )

        absent_indices = sorted(set(range(len(batch))) - set(present_docs_keys.keys()))
        batch_absent = [batch[j] for j in absent_indices]
        return batch_absent

    def update_to_numeric(self, collection_name: str, field: str) -> str:
        """Update a field to numeric type in all documents.

        Args:
            collection_name: Vertex/edge class name to update (ArangoDB collection name)
            field: Field to convert to numeric

        Returns:
            str: AQL query string for the operation
        """
        s1 = f"FOR p IN {collection_name} FILTER p.{field} update p with {{"
        s2 = f"{field}: TO_NUMBER(p.{field}) "
        s3 = f"}} in {collection_name}"
        q0 = s1 + s2 + s3
        return q0
