"""Grafeo embedded graph database connection implementation.

This module implements the Connection interface for Grafeo, a high-performance
embeddable graph database with a Rust core. Unlike server-based backends,
Grafeo runs in-process: no network, no Docker, no external dependencies.

Key Features:

    - In-memory or file-backed persistent storage
    - GQL query execution via Grafeo's multi-language engine
    - Label-based node organization
    - MERGE-based upsert semantics
    - Zero-copy embedded operation (no serialization overhead)

Example:
    >>> from graflo.db import GrafeoConfig, ConnectionManager
    >>> config = GrafeoConfig.in_memory(database="test")
    >>> with ConnectionManager(connection_config=config) as conn:
    ...     conn.init_db(schema, recreate_schema=True)
    ...     conn.upsert_docs_batch(docs, "Person", match_keys=["id"])
"""

import logging
from typing import Any

from graflo.architecture.graph_types import Index
from graflo.architecture.schema import Schema
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.vertex import VertexConfig
from graflo.db.conn import Connection, SchemaExistsError, consume_insert_edges_kwargs
from graflo.db.util import serialize_value
from graflo.filter.onto import FilterExpression
from graflo.onto import AggregationType, DBType

from ..connection.onto import GrafeoConfig

logger = logging.getLogger(__name__)


def _gql_literal(value: Any) -> str:
    """Render a Python value as a GQL literal."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        # Clamp to i64 range; values outside are stored as float
        if -(2**63) <= value <= 2**63 - 1:
            return str(value)
        return repr(float(value))
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, str):
        escaped = value.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{escaped}'"
    if isinstance(value, list):
        items = ", ".join(_gql_literal(v) for v in value)
        return f"[{items}]"
    if isinstance(value, dict):
        pairs = ", ".join(
            f"{k}: {_gql_literal(v)}" for k, v in value.items()
        )
        return f"{{{pairs}}}"
    # Fallback: stringify
    return _gql_literal(str(value))


def _props_map(doc: dict[str, Any]) -> str:
    """Build a GQL map literal ``{k1: v1, k2: v2}`` from a dict."""
    if not doc:
        return "{}"
    pairs = ", ".join(
        f"`{k}`: {_gql_literal(v)}" for k, v in doc.items()
    )
    return f"{{{pairs}}}"


def _match_clause(keys: list[str] | tuple[str, ...], doc: dict[str, Any]) -> str:
    """Build ``key1: val1, key2: val2`` for a MERGE match pattern."""
    return ", ".join(f"`{k}`: {_gql_literal(doc[k])}" for k in keys)


class GrafeoConnection(Connection):
    """Grafeo embedded graph database connection.

    Wraps a ``grafeo.GrafeoDB`` instance and exposes it through the graflo
    Connection interface using GQL queries.

    Attributes:
        flavor: Database flavor identifier (GRAFEO).
        config: Grafeo connection configuration.
        db: Underlying GrafeoDB instance.
    """

    flavor = DBType.GRAFEO

    def __init__(self, config: GrafeoConfig):
        super().__init__()
        self.config = config

        try:
            from grafeo import GrafeoDB
        except ImportError as exc:
            raise ImportError(
                "The 'grafeo' package is required for the Grafeo backend. "
                "Install it with: pip install grafeo"
            ) from exc

        # Create in-memory or persistent database
        if config.path:
            self.db = GrafeoDB(config.path)
        else:
            self.db = GrafeoDB()

        logger.info(
            "Opened Grafeo database (%s)",
            config.path or "in-memory",
        )

    # ------------------------------------------------------------------
    # Query execution
    # ------------------------------------------------------------------

    def execute(self, query: str, **kwargs: Any) -> Any:
        """Execute a GQL query against the embedded Grafeo database.

        Grafeo's GQL engine supports Cypher-compatible syntax
        (MERGE, SET +=, MATCH, DETACH DELETE, etc.) so all standard
        property-graph patterns work out of the box.

        Args:
            query: GQL query string.
            **kwargs: Query parameters (passed to ``execute``).

        Returns:
            ``QueryResult`` from the Grafeo Python bindings.
        """
        params = kwargs if kwargs else None
        return self.db.execute(query, params)

    def close(self):
        """Close the Grafeo database."""
        if self.db is not None:
            self.db.close()
            self.db = None

    # ------------------------------------------------------------------
    # Database / graph lifecycle
    # ------------------------------------------------------------------

    def create_database(self, name: str):
        """Create a new database (no-op for Grafeo).

        Grafeo is a single embedded graph with no multi-database concept.

        Args:
            name: Name of the database to create.
        """
        logger.debug("create_database('%s') is a no-op for Grafeo", name)

    def delete_database(self, name: str):
        """Delete a database (no-op for Grafeo).

        Grafeo is a single embedded graph with no multi-database concept.

        Args:
            name: Name of the database to delete.
        """
        logger.debug("delete_database('%s') is a no-op for Grafeo", name)

    def define_schema(self, schema: Schema):
        """Define vertex and edge classes based on schema.

        Note: This is a no-op in Grafeo as labels and relationship types
        are created implicitly on first use.

        Args:
            schema: Schema containing vertex and edge class definitions.
        """
        pass

    def define_vertex_classes(self, schema: Schema) -> None:
        """Define vertex classes based on schema.

        Note: This is a no-op in Grafeo as vertex labels are implicit.

        Args:
            schema: Schema containing vertex definitions.
        """
        pass

    def define_edge_classes(self, edges: list[Edge]) -> None:
        """Define edge classes based on schema.

        Note: This is a no-op in Grafeo as relationship types are implicit.

        Args:
            edges: List of edge configurations.
        """
        pass

    def delete_graph_structure(
        self,
        vertex_types: tuple[str, ...] | list[str] = (),
        graph_names: tuple[str, ...] | list[str] = (),
        delete_all: bool = False,
    ) -> None:
        """Delete nodes and relationships from the Grafeo graph."""
        if delete_all:
            try:
                self.execute("MATCH (n) DETACH DELETE n")
            except Exception as exc:
                logger.debug("Graph may be empty: %s", exc)
        elif vertex_types:
            for label in vertex_types:
                try:
                    self.execute(f"MATCH (n:`{label}`) DETACH DELETE n")
                except Exception as exc:
                    logger.warning(
                        "Failed to delete nodes with label '%s': %s", label, exc
                    )

    def init_db(self, schema: Schema, recreate_schema: bool) -> None:
        """Initialize the Grafeo database with the given schema.

        Args:
            schema: Schema containing graph structure definitions.
            recreate_schema: If True, wipe all data first.
        """
        # Check existing data
        if not recreate_schema:
            try:
                result = self.execute(
                    "MATCH (n) RETURN count(n) AS c"
                )
                rows = result.to_list()
                count = rows[0]["c"] if rows else 0
                if count > 0:
                    raise SchemaExistsError(
                        f"Graph already has {count} nodes. "
                        "Set recreate_schema=True to replace."
                    )
            except SchemaExistsError:
                raise
            except Exception as exc:
                logger.debug("Could not check node count: %s", exc)

        if recreate_schema:
            self.delete_graph_structure(delete_all=True)

        self.define_indexes(schema)
        logger.info("Grafeo database initialized")

    def clear_data(self, schema: Schema) -> None:
        """Remove all data without dropping the schema."""
        vc = schema.resolve_db_aware(DBType.GRAFEO).vertex_config
        vertex_types = tuple(vc.vertex_dbname(v) for v in vc.vertex_set)
        if vertex_types:
            self.delete_graph_structure(vertex_types=vertex_types)

    # ------------------------------------------------------------------
    # Indexes
    # ------------------------------------------------------------------

    def define_vertex_indexes(
        self, vertex_config: VertexConfig, schema: Schema | None = None
    ):
        """Create property indexes for vertex labels."""
        if schema is None:
            return
        db_vc = schema.resolve_db_aware(DBType.GRAFEO).vertex_config
        for vertex_name in vertex_config.vertex_set:
            index_list = list(schema.db_profile.vertex_secondary_indexes(vertex_name))
            identity_fields = db_vc.identity_fields(vertex_name)
            if identity_fields:
                identity_idx = Index(fields=identity_fields)
                seen = {tuple(ix.fields) for ix in index_list}
                if tuple(identity_idx.fields) not in seen:
                    index_list = [identity_idx, *index_list]
            label = db_vc.vertex_dbname(vertex_name)
            for index_obj in index_list:
                self._add_index(label, index_obj)

    def define_edge_indexes(self, edges: list[Edge], schema: Schema | None = None):
        """Create property indexes for relationship types."""
        if schema is None:
            return
        for edge in edges:
            index_list = schema.db_profile.edge_secondary_indexes(edge.edge_id)
            for index_obj in index_list:
                if edge.relation is not None:
                    self._add_index(edge.relation, index_obj, is_vertex=False)

    def _add_index(self, name: str, index: Index, is_vertex: bool = True):
        """Create an index on a label or relationship type."""
        for field in index.fields:
            try:
                if is_vertex:
                    q = f"CREATE INDEX FOR (n:`{name}`) ON (n.`{field}`)"
                else:
                    q = f"CREATE INDEX FOR ()-[r:`{name}`]-() ON (r.`{field}`)"
                self.execute(q)
                logger.debug("Created index on %s.%s", name, field)
            except Exception as exc:
                logger.debug("Index creation note for %s.%s: %s", name, field, exc)

    # ------------------------------------------------------------------
    # Value helpers
    # ------------------------------------------------------------------

    def _sanitize_value(self, value: Any) -> Any:
        """Sanitize a value for storage in Grafeo."""
        from datetime import date, datetime, time

        if isinstance(value, (datetime, date, time)):
            return serialize_value(value)
        if isinstance(value, str):
            # Strip null bytes for cross-backend compatibility
            return value.replace("\x00", "") if "\x00" in value else value
        if isinstance(value, dict):
            return {
                k: self._sanitize_value(v)
                for k, v in value.items()
                if isinstance(k, str)
            }
        if isinstance(value, list):
            return [self._sanitize_value(v) for v in value]
        return serialize_value(value)

    def _sanitize_doc(
        self,
        doc: dict[str, Any],
        match_keys: list[str] | None = None,
    ) -> dict[str, Any]:
        """Sanitize a document, filtering bad keys and serializing values."""
        sanitized: dict[str, Any] = {}
        for key, value in doc.items():
            if not isinstance(key, str):
                continue
            sanitized[key] = self._sanitize_value(value)
        if match_keys:
            for key in match_keys:
                if key not in sanitized:
                    # Key absent entirely: add as null so MERGE can still match
                    sanitized[key] = None
        return sanitized

    # ------------------------------------------------------------------
    # Vertex operations
    # ------------------------------------------------------------------

    def upsert_docs_batch(
        self,
        docs: list[dict[str, Any]],
        class_name: str,
        match_keys: list[str] | tuple[str, ...],
        **kwargs: Any,
    ) -> None:
        """Upsert a batch of nodes using individual MERGE statements.

        Args:
            docs: Node documents to upsert.
            class_name: Label for the nodes.
            match_keys: Properties used to identify existing nodes.
        """
        dry = kwargs.pop("dry", False)
        if not docs:
            return

        match_keys_list = list(match_keys)

        for doc in docs:
            sanitized = self._sanitize_doc(doc, match_keys_list)
            match_map = _match_clause(match_keys_list, sanitized)
            props = _props_map(sanitized)
            query = (
                f"MERGE (n:`{class_name}` {{{match_map}}})\n"
                f"SET n += {props}"
            )
            if not dry:
                self.execute(query)

    def insert_return_batch(
        self, docs: list[dict[str, Any]], class_name: str
    ) -> list[dict[str, Any]]:
        """Insert nodes and return their properties."""
        results = []
        for doc in docs:
            sanitized = self._sanitize_doc(doc)
            props = _props_map(sanitized)
            query = (
                f"CREATE (n:`{class_name}` {props}) RETURN n"
            )
            result = self.execute(query)
            rows = result.to_list()
            if rows:
                results.append(self._extract_node_props(rows[0].get("n", rows[0])))
        return results

    # ------------------------------------------------------------------
    # Edge operations
    # ------------------------------------------------------------------

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
        """Create relationships between existing nodes using MERGE.

        Each element of *docs_edges* is ``[source_dict, target_dict, props_dict]``.
        """
        opts = consume_insert_edges_kwargs(kwargs)
        dry = opts.dry
        relationship_merge_properties = opts.relationship_merge_properties

        if head is not None and isinstance(docs_edges, list):
            docs_edges = docs_edges[:head]
        if not docs_edges:
            return

        merge_props: tuple[str, ...] | None = None
        if relationship_merge_properties:
            merge_props = tuple(relationship_merge_properties)

        for edge in docs_edges:
            if len(edge) != 3:
                logger.warning("Skipping invalid edge format: %s", edge)
                continue

            src_raw, tgt_raw, props_raw = edge
            src = self._sanitize_doc(
                src_raw if isinstance(src_raw, dict) else {},
                list(match_keys_source),
            )
            tgt = self._sanitize_doc(
                tgt_raw if isinstance(tgt_raw, dict) else {},
                list(match_keys_target),
            )
            props = self._sanitize_doc(
                props_raw if isinstance(props_raw, dict) else {}
            )

            src_match = _match_clause(match_keys_source, src)
            tgt_match = _match_clause(match_keys_target, tgt)

            if merge_props:
                merge_map = ", ".join(
                    f"`{p}`: {_gql_literal(props.get(p))}" for p in merge_props
                )
                rel_pattern = f"[r:`{relation_name}` {{{merge_map}}}]"
            else:
                rel_pattern = f"[r:`{relation_name}`]"

            props_literal = _props_map(props)
            query = (
                f"MATCH (a:`{source_class}` {{{src_match}}}), "
                f"(b:`{target_class}` {{{tgt_match}}})\n"
                f"MERGE (a)-{rel_pattern}->(b)\n"
                f"SET r += {props_literal}"
            )
            if not dry:
                self.execute(query)

    # ------------------------------------------------------------------
    # Fetch operations
    # ------------------------------------------------------------------

    def fetch_docs(
        self,
        class_name: str,
        filters: list[Any] | dict[str, Any] | None = None,
        limit: int | None = None,
        return_keys: list[str] | None = None,
        unset_keys: list[str] | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Fetch nodes from a label."""
        if filters is not None:
            ff = FilterExpression.from_dict(filters)
            filter_clause = f"WHERE {ff(doc_name='n', kind=self.expression_flavor())}"
        else:
            filter_clause = ""

        if return_keys is not None:
            projection = ", ".join(f"n.`{k}` AS `{k}`" for k in return_keys)
            return_clause = f"RETURN {projection}"
        else:
            return_clause = "RETURN n"

        limit_clause = f"LIMIT {int(limit)}" if limit and limit > 0 else ""

        query = f"MATCH (n:`{class_name}`) {filter_clause} {return_clause} {limit_clause}"
        result = self.execute(query)
        rows = result.to_list()

        if return_keys is not None:
            return rows
        # Rows contain node objects keyed as "n"; extract properties
        return [self._extract_node_props(row.get("n", row)) for row in rows]

    def _extract_node_props(self, node: Any) -> dict[str, Any]:
        """Extract a property dict from a Grafeo node object or dict."""
        if hasattr(node, "properties"):
            return dict(node.properties())
        if isinstance(node, dict):
            return node
        return {}

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
        """Fetch edges from the Grafeo graph."""
        source = f"(source:`{from_type}` {{id: {_gql_literal(from_id)}}})"
        rel = f"-[r:`{edge_type}`]->" if edge_type else "-[r]->"
        target = f"(target:`{to_type}`)" if to_type else "(target)"

        where_parts: list[str] = []
        if to_id:
            where_parts.append(f"target.id = {_gql_literal(to_id)}")
        if filters is not None:
            ff = FilterExpression.from_dict(filters)
            where_parts.append(str(ff(doc_name="r", kind=self.expression_flavor())))
        where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

        if return_keys is not None:
            projection = ", ".join(f"r.`{k}` AS `{k}`" for k in return_keys)
            return_clause = f"RETURN {projection}"
        else:
            return_clause = "RETURN r"

        limit_clause = f"LIMIT {limit}" if limit and limit > 0 else ""

        query = (
            f"MATCH {source}{rel}{target} {where_clause} "
            f"{return_clause} {limit_clause}"
        )
        result = self.execute(query)
        rows = result.to_list()

        if return_keys is not None:
            return rows
        return [self._extract_edge_props(row.get("r", row)) for row in rows]

    def _extract_edge_props(self, edge: Any) -> dict[str, Any]:
        """Extract properties from a Grafeo edge object or dict."""
        if hasattr(edge, "properties"):
            return dict(edge.properties())
        if isinstance(edge, dict):
            return edge
        return {}

    # ------------------------------------------------------------------
    # Presence / absence checks
    # ------------------------------------------------------------------

    def fetch_present_documents(
        self,
        batch: list[dict[str, Any]],
        class_name: str,
        match_keys: list[str] | tuple[str, ...],
        keep_keys: list[str] | tuple[str, ...] | None = None,
        flatten: bool = False,
        filters: list[Any] | dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Return documents from *batch* that already exist in the graph."""
        if not batch:
            return []

        results: list[dict[str, Any]] = []
        for doc in batch:
            conditions = " AND ".join(
                f"n.`{k}` = {_gql_literal(doc.get(k))}" for k in match_keys
            )
            query = (
                f"MATCH (n:`{class_name}`) WHERE {conditions} RETURN n LIMIT 1"
            )
            try:
                result = self.execute(query)
                rows = result.to_list()
                if rows:
                    node_dict = self._extract_node_props(rows[0].get("n", rows[0]))
                    if keep_keys:
                        node_dict = {k: node_dict.get(k) for k in keep_keys}
                    results.append(node_dict)
            except Exception as exc:
                logger.debug("Error checking document presence: %s", exc)

        return results

    def keep_absent_documents(
        self,
        batch: list[dict[str, Any]],
        class_name: str,
        match_keys: list[str] | tuple[str, ...],
        keep_keys: list[str] | tuple[str, ...] | None = None,
        filters: list[Any] | dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Return documents from *batch* that do NOT exist in the graph."""
        if not batch:
            return []

        present = self.fetch_present_documents(
            batch, class_name, match_keys, match_keys, filters=filters
        )
        present_keys = {
            tuple(doc.get(k) for k in match_keys) for doc in present
        }

        absent: list[dict[str, Any]] = []
        for doc in batch:
            key_tuple = tuple(doc.get(k) for k in match_keys)
            if key_tuple not in present_keys:
                if keep_keys:
                    absent.append({k: doc.get(k) for k in keep_keys})
                else:
                    absent.append(doc)
        return absent

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------

    def aggregate(
        self,
        class_name: str,
        aggregation_function: AggregationType,
        discriminant: str | None = None,
        aggregated_field: str | None = None,
        filters: list[Any] | dict[str, Any] | None = None,
    ) -> int | float | list[dict[str, Any]] | dict[str, int | float] | None:
        """Perform aggregation on a label."""
        if filters is not None:
            ff = FilterExpression.from_dict(filters)
            filter_clause = f"WHERE {ff(doc_name='n', kind=self.expression_flavor())}"
        else:
            filter_clause = ""

        if aggregation_function == AggregationType.COUNT:
            if discriminant:
                q = (
                    f"MATCH (n:`{class_name}`) {filter_clause} "
                    f"RETURN n.`{discriminant}` AS key, count(*) AS count"
                )
                rows = self.execute(q).to_list()
                return {row["key"]: row["count"] for row in rows}
            q = (
                f"MATCH (n:`{class_name}`) {filter_clause} "
                f"RETURN count(*) AS count"
            )
            rows = self.execute(q).to_list()
            return rows[0]["count"] if rows else 0

        _simple_agg = {
            AggregationType.MAX: "max",
            AggregationType.MIN: "min",
            AggregationType.AVERAGE: "avg",
        }
        if aggregation_function in _simple_agg:
            if not aggregated_field:
                raise ValueError(
                    f"aggregated_field required for {aggregation_function.name}"
                )
            fn = _simple_agg[aggregation_function]
            q = (
                f"MATCH (n:`{class_name}`) {filter_clause} "
                f"RETURN {fn}(n.`{aggregated_field}`) AS v"
            )
            rows = self.execute(q).to_list()
            return rows[0]["v"] if rows else None

        if aggregation_function == AggregationType.SORTED_UNIQUE:
            if not aggregated_field:
                raise ValueError("aggregated_field required for SORTED_UNIQUE")
            q = (
                f"MATCH (n:`{class_name}`) {filter_clause} "
                f"RETURN DISTINCT n.`{aggregated_field}` AS v ORDER BY v"
            )
            rows = self.execute(q).to_list()
            return [row["v"] for row in rows]

        raise ValueError(f"Unsupported aggregation: {aggregation_function}")
