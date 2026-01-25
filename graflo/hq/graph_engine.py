"""Graph engine for orchestrating schema inference, pattern creation, and ingestion.

This module provides the GraphEngine class which serves as the main orchestrator
for graph database operations, coordinating between inference, pattern mapping,
and data ingestion.
"""

import logging

from graflo import Schema
from graflo.db import PostgresConnection
from graflo.db.connection.onto import DBConfig, PostgresConfig
from graflo.hq.caster import Caster, IngestionParams
from graflo.hq.inferencer import InferenceManager
from graflo.hq.resource_mapper import ResourceMapper
from graflo.onto import DBFlavor
from graflo.util.onto import Patterns

logger = logging.getLogger(__name__)


class GraphEngine:
    """Orchestrator for graph database operations.

    GraphEngine coordinates schema inference, pattern creation, and data ingestion,
    providing a unified interface for working with graph databases.

    Attributes:
        inferencer: InferenceManager instance for schema inference
        caster: Caster instance for data ingestion
        resource_mapper: ResourceMapper instance for pattern creation
    """

    def __init__(
        self,
        target_db_flavor: DBFlavor = DBFlavor.ARANGO,
        ingestion_params: IngestionParams | None = None,
    ):
        """Initialize the GraphEngine.

        Args:
            target_db_flavor: Target database flavor for schema sanitization
            ingestion_params: IngestionParams instance for controlling ingestion behavior
        """
        self.target_db_flavor = target_db_flavor
        self.ingestion_params = ingestion_params or IngestionParams()
        self.resource_mapper = ResourceMapper()

    def infer_schema(
        self,
        postgres_config: PostgresConfig,
        schema_name: str | None = None,
        fuzzy_threshold: float = 0.8,
    ) -> Schema:
        """Infer a graflo Schema from PostgreSQL database.

        Args:
            postgres_config: PostgresConfig instance
            schema_name: Schema name to introspect (defaults to config schema_name or 'public')
            fuzzy_threshold: Similarity threshold for fuzzy matching (0.0 to 1.0, default 0.8)

        Returns:
            Schema: Inferred schema with vertices, edges, and resources
        """
        with PostgresConnection(postgres_config) as postgres_conn:
            inferencer = InferenceManager(
                conn=postgres_conn,
                target_db_flavor=self.target_db_flavor,
                fuzzy_threshold=fuzzy_threshold,
            )
            return inferencer.infer_complete_schema(schema_name=schema_name)

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
                If None, uses the instance's default ingestion_params
        """
        caster = Caster(
            schema=schema, ingestion_params=ingestion_params or self.ingestion_params
        )
        caster.ingest(
            output_config=output_config,
            patterns=patterns or Patterns(),
            ingestion_params=ingestion_params or self.ingestion_params,
        )
