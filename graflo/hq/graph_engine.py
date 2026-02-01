"""Graph engine for orchestrating schema inference, pattern creation, and ingestion.

This module provides the GraphEngine class which serves as the main orchestrator
for graph database operations, coordinating between inference, pattern mapping,
and data ingestion.
"""

import logging

from graflo import Schema
from graflo.onto import DBType
from graflo.db import ConnectionManager, PostgresConnection
from graflo.db.connection.onto import DBConfig, PostgresConfig
from graflo.hq.caster import Caster, IngestionParams
from graflo.hq.inferencer import InferenceManager
from graflo.hq.resource_mapper import ResourceMapper
from graflo.util.onto import Patterns

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
    ) -> Patterns:
        """Create Patterns from PostgreSQL tables.

        Args:
            postgres_config: PostgresConfig instance
            schema_name: Schema name to introspect

        Returns:
            Patterns: Patterns object with TablePattern instances for all tables
        """
        with PostgresConnection(postgres_config) as postgres_conn:
            return self.resource_mapper.create_patterns_from_postgres(
                conn=postgres_conn, schema_name=schema_name
            )

    def define_schema(
        self,
        schema: Schema,
        output_config: DBConfig,
        clean_start: bool = False,
    ) -> None:
        """Define schema in the target database.

        This method handles database/schema creation and initialization.
        Some databases don't require explicit schema definition (e.g., Neo4j),
        but this method ensures the database is properly initialized.

        Args:
            schema: Schema configuration for the graph
            output_config: Target database connection configuration
            clean_start: Whether to clean the database before defining schema
        """
        # If effective_schema is not set, use schema.general.name as fallback
        if output_config.can_be_target() and output_config.effective_schema is None:
            schema_name = schema.general.name
            # Map to the appropriate field based on DB type
            if output_config.connection_type == DBType.TIGERGRAPH:
                # TigerGraph uses 'schema_name' field
                output_config.schema_name = schema_name
            else:
                # ArangoDB, Neo4j use 'database' field (which maps to effective_schema)
                output_config.database = schema_name

        # Initialize database with schema definition
        # init_db() handles database/schema creation automatically
        # It checks if the database exists and creates it if needed
        with ConnectionManager(connection_config=output_config) as db_client:
            db_client.init_db(schema, clean_start)

    def define_and_ingest(
        self,
        schema: Schema,
        output_config: DBConfig,
        patterns: "Patterns | None" = None,
        ingestion_params: IngestionParams | None = None,
        clean_start: bool | None = None,
    ) -> None:
        """Define schema and ingest data into the graph database in one operation.

        This is a convenience method that chains define_schema() and ingest().
        It's the recommended way to set up and populate a graph database.

        Args:
            schema: Schema configuration for the graph
            output_config: Target database connection configuration
            patterns: Patterns instance mapping resources to data sources.
                If None, defaults to empty Patterns()
            ingestion_params: IngestionParams instance with ingestion configuration.
                If None, uses default IngestionParams()
            clean_start: Whether to clean the database before defining schema.
                If None, uses ingestion_params.clean_start if provided, otherwise False.
                Note: If clean_start is True, ingestion_params.clean_start will be
                set to False to avoid double-cleaning.
        """
        ingestion_params = ingestion_params or IngestionParams()

        # Determine clean_start value: explicit parameter > ingestion_params > False
        if clean_start is None:
            clean_start = ingestion_params.clean_start

        # Define schema first
        self.define_schema(
            schema=schema,
            output_config=output_config,
            clean_start=clean_start,
        )

        # If we cleaned during schema definition, don't clean again during ingestion
        if clean_start:
            ingestion_params = IngestionParams(
                **{**ingestion_params.model_dump(), "clean_start": False}
            )

        # Then ingest data
        self.ingest(
            schema=schema,
            output_config=output_config,
            patterns=patterns,
            ingestion_params=ingestion_params,
        )

    def ingest(
        self,
        schema: Schema,
        output_config: DBConfig,
        patterns: "Patterns | None" = None,
        ingestion_params: IngestionParams | None = None,
    ) -> None:
        """Ingest data into the graph database.

        Args:
            schema: Schema configuration for the graph
            output_config: Target database connection configuration
            patterns: Patterns instance mapping resources to data sources.
                If None, defaults to empty Patterns()
            ingestion_params: IngestionParams instance with ingestion configuration.
                If None, uses default IngestionParams()
        """
        ingestion_params = ingestion_params or IngestionParams()
        caster = Caster(schema=schema, ingestion_params=ingestion_params)
        caster.ingest(
            output_config=output_config,
            patterns=patterns or Patterns(),
            ingestion_params=ingestion_params,
        )
