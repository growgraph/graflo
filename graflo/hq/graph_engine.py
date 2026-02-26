"""Graph engine for orchestrating schema inference, pattern creation, and ingestion.

This module provides the GraphEngine class which serves as the main orchestrator
for graph database operations, coordinating between inference, pattern mapping,
and data ingestion.
"""

import logging

from graflo.architecture.schema import Schema
from graflo.onto import DBType
from graflo.architecture.onto_sql import SchemaIntrospectionResult
from graflo.db import ConnectionManager, PostgresConnection
from graflo.db import DBConfig, PostgresConfig, SparqlEndpointConfig
from graflo.hq.caster import Caster, IngestionParams
from graflo.hq.inferencer import InferenceManager
from graflo.hq.resource_mapper import ResourceMapper
from graflo.util.onto import Patterns

from pathlib import Path

logger = logging.getLogger(__name__)


class GraphEngine:
    """Orchestrator for graph database operations.

    GraphEngine coordinates schema inference, pattern creation, schema definition,
    and data ingestion, providing a unified interface for working with graph databases.

    The typical workflow is:
    1. infer_schema() - Infer schema from source database (if possible)
    2. create_patterns() - Create patterns mapping resources to data sources (if possible)
    3. define_schema() - Define schema in target database (if possible and necessary)
    4. ingest() - Ingest data into the target database

    Attributes:
        target_db_flavor: Target database flavor for schema sanitization
        resource_mapper: ResourceMapper instance for pattern creation
    """

    def __init__(
        self,
        target_db_flavor: DBType = DBType.ARANGO,
    ):
        """Initialize the GraphEngine.

        Args:
            target_db_flavor: Target database flavor for schema sanitization
        """
        self.target_db_flavor = target_db_flavor
        self.resource_mapper = ResourceMapper()

    def introspect(
        self,
        postgres_config: PostgresConfig,
        schema_name: str | None = None,
    ) -> SchemaIntrospectionResult:
        """Introspect PostgreSQL schema and return a serializable result.

        Args:
            postgres_config: PostgresConfig instance
            schema_name: Schema name to introspect (defaults to config schema_name or 'public')

        Returns:
            SchemaIntrospectionResult: Introspection result (vertex_tables, edge_tables,
                raw_tables, schema_name) suitable for serialization.
        """
        with PostgresConnection(postgres_config) as postgres_conn:
            inferencer = InferenceManager(
                conn=postgres_conn,
                target_db_flavor=self.target_db_flavor,
            )
            return inferencer.introspect(schema_name=schema_name)

    def infer_schema(
        self,
        postgres_config: PostgresConfig,
        schema_name: str | None = None,
        fuzzy_threshold: float = 0.8,
        discard_disconnected_vertices: bool = False,
    ) -> Schema:
        """Infer a graflo Schema from PostgreSQL database.

        Args:
            postgres_config: PostgresConfig instance
            schema_name: Schema name to introspect (defaults to config schema_name or 'public')
            fuzzy_threshold: Similarity threshold for fuzzy matching (0.0 to 1.0, default 0.8)
            discard_disconnected_vertices: If True, remove vertices that do not take part in
                any relation (and resources/actors that reference them). Default False.

        Returns:
            Schema: Inferred schema with vertices, edges, and resources
        """
        with PostgresConnection(postgres_config) as postgres_conn:
            inferencer = InferenceManager(
                conn=postgres_conn,
                target_db_flavor=self.target_db_flavor,
                fuzzy_threshold=fuzzy_threshold,
            )
            schema = inferencer.infer_complete_schema(schema_name=schema_name)
        if discard_disconnected_vertices:
            schema.remove_disconnected_vertices()
        return schema

    def create_patterns(
        self,
        postgres_config: PostgresConfig,
        schema_name: str | None = None,
        datetime_columns: dict[str, str] | None = None,
    ) -> Patterns:
        """Create Patterns from PostgreSQL tables.

        Args:
            postgres_config: PostgresConfig instance
            schema_name: Schema name to introspect
            datetime_columns: Optional mapping of resource/table name to datetime
                column name for date-range filtering (sets date_field per
                TablePattern). Use with IngestionParams.datetime_after /
                datetime_before.

        Returns:
            Patterns: Patterns object with TablePattern instances for all tables
        """
        with PostgresConnection(postgres_config) as postgres_conn:
            return self.resource_mapper.create_patterns_from_postgres(
                conn=postgres_conn,
                schema_name=schema_name,
                datetime_columns=datetime_columns,
            )

    def define_schema(
        self,
        schema: Schema,
        target_db_config: DBConfig,
        recreate_schema: bool = False,
    ) -> None:
        """Define schema in the target database.

        This method handles database/schema creation and initialization.
        Some databases don't require explicit schema definition (e.g., Neo4j),
        but this method ensures the database is properly initialized.

        If the schema/graph already exists and recreate_schema is False (default),
        init_db raises SchemaExistsError and the script halts.

        Args:
            schema: Schema configuration for the graph
            target_db_config: Target database connection configuration
            recreate_schema: If True, drop existing schema and define new one.
                If False and schema/graph already exists, raises SchemaExistsError.
        """
        # If effective_schema is not set, use schema.general.name as fallback
        if (
            target_db_config.can_be_target()
            and target_db_config.effective_schema is None
        ):
            schema_name = schema.general.name
            # Map to the appropriate field based on DB type
            if target_db_config.connection_type == DBType.TIGERGRAPH:
                # TigerGraph uses 'schema_name' field
                target_db_config.schema_name = schema_name
            else:
                # ArangoDB, Neo4j use 'database' field (which maps to effective_schema)
                target_db_config.database = schema_name

        # Ensure schema reflects target DB so finish_init applies DB-specific defaults.
        schema.database_features.db_flavor = target_db_config.connection_type
        schema.finish_init()

        # Initialize database with schema definition
        # init_db() handles database/schema creation automatically
        # It checks if the database exists and creates it if needed
        with ConnectionManager(connection_config=target_db_config) as db_client:
            db_client.init_db(schema, recreate_schema)

    def define_and_ingest(
        self,
        schema: Schema,
        target_db_config: DBConfig,
        patterns: "Patterns | None" = None,
        ingestion_params: IngestionParams | None = None,
        recreate_schema: bool | None = None,
        clear_data: bool | None = None,
    ) -> None:
        """Define schema and ingest data into the graph database in one operation.

        This is a convenience method that chains define_schema() and ingest().
        It's the recommended way to set up and populate a graph database.

        Args:
            schema: Schema configuration for the graph
            target_db_config: Target database connection configuration
            patterns: Patterns instance mapping resources to data sources.
                If None, defaults to empty Patterns()
            ingestion_params: IngestionParams instance with ingestion configuration.
                If None, uses default IngestionParams()
            recreate_schema: If True, drop existing schema and define new one.
                If None, defaults to False. When False and schema already exists,
                define_schema raises SchemaExistsError and the script halts.
            clear_data: If True, remove existing data before ingestion (schema unchanged).
                If None, uses ingestion_params.clear_data.
        """
        ingestion_params = ingestion_params or IngestionParams()
        if clear_data is None:
            clear_data = ingestion_params.clear_data
        if recreate_schema is None:
            recreate_schema = False

        # Define schema first (halts with SchemaExistsError if schema exists and recreate_schema is False)
        self.define_schema(
            schema=schema,
            target_db_config=target_db_config,
            recreate_schema=recreate_schema,
        )

        # Then ingest data (clear_data is applied inside ingest() when ingestion_params.clear_data)
        ingestion_params = ingestion_params.model_copy(
            update={"clear_data": clear_data}
        )
        self.ingest(
            schema=schema,
            target_db_config=target_db_config,
            patterns=patterns,
            ingestion_params=ingestion_params,
        )

    def ingest(
        self,
        schema: Schema,
        target_db_config: DBConfig,
        patterns: "Patterns | None" = None,
        ingestion_params: IngestionParams | None = None,
    ) -> None:
        """Ingest data into the graph database.

        If ingestion_params.clear_data is True, removes all existing data
        (without touching the schema) before ingestion.

        Args:
            schema: Schema configuration for the graph
            target_db_config: Target database connection configuration
            patterns: Patterns instance mapping resources to data sources.
                If None, defaults to empty Patterns()
            ingestion_params: IngestionParams instance with ingestion configuration.
                If None, uses default IngestionParams()
        """
        ingestion_params = ingestion_params or IngestionParams()
        if ingestion_params.clear_data:
            with ConnectionManager(connection_config=target_db_config) as db_client:
                db_client.clear_data(schema)
        caster = Caster(schema=schema, ingestion_params=ingestion_params)
        caster.ingest(
            target_db_config=target_db_config,
            patterns=patterns or Patterns(),
            ingestion_params=ingestion_params,
        )

    # ------------------------------------------------------------------
    # RDF / SPARQL inference
    # ------------------------------------------------------------------

    def infer_schema_from_rdf(
        self,
        source: str | Path,
        *,
        endpoint_url: str | None = None,
        graph_uri: str | None = None,
        schema_name: str | None = None,
    ) -> Schema:
        """Infer a graflo Schema from an RDF / OWL ontology.

        Reads the TBox (class and property declarations) and produces
        vertices (from ``owl:Class``), fields (from ``owl:DatatypeProperty``),
        and edges (from ``owl:ObjectProperty`` with domain/range).

        Args:
            source: Path to an RDF file (e.g. ``ontology.ttl``) or a base
                URL when using *endpoint_url*.
            endpoint_url: Optional SPARQL endpoint to CONSTRUCT the
                ontology from.
            graph_uri: Named graph containing the ontology.
            schema_name: Name for the resulting schema.

        Returns:
            A fully initialised :class:`Schema`.
        """
        from graflo.hq.rdf_inferencer import RdfInferenceManager

        mgr = RdfInferenceManager(target_db_flavor=self.target_db_flavor)
        return mgr.infer_schema(
            source,
            endpoint_url=endpoint_url,
            graph_uri=graph_uri,
            schema_name=schema_name,
        )

    def create_patterns_from_rdf(
        self,
        source: str | Path,
        *,
        endpoint_url: str | None = None,
        graph_uri: str | None = None,
        sparql_config: SparqlEndpointConfig | None = None,
    ) -> Patterns:
        """Create :class:`Patterns` from an RDF ontology.

        One :class:`SparqlPattern` is created per ``owl:Class`` found in the
        ontology.

        Args:
            source: Path to an RDF file or base URL.
            endpoint_url: SPARQL endpoint for the *data* (ABox).
            graph_uri: Named graph containing the data.
            sparql_config: Optional :class:`SparqlEndpointConfig` to attach
                to the resulting patterns for authentication.

        Returns:
            Patterns with SPARQL patterns for each class.
        """
        from graflo.hq.rdf_inferencer import RdfInferenceManager

        mgr = RdfInferenceManager(target_db_flavor=self.target_db_flavor)
        patterns = mgr.create_patterns(
            source,
            endpoint_url=endpoint_url,
            graph_uri=graph_uri,
        )

        if sparql_config:
            patterns.sparql_configs["default"] = sparql_config

        return patterns
