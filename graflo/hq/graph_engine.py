"""Graph engine for orchestrating schema inference, connector creation, and ingestion.

This module provides the GraphEngine class which serves as the main orchestrator
for graph database operations, coordinating between inference, connector mapping,
and data ingestion.
"""

import logging

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.contract.declarations.ingestion_model import IngestionModel
from graflo.architecture.schema import Schema
from graflo.onto import DBType
from graflo.architecture.onto_sql import SchemaIntrospectionResult
from graflo.db import ConnectionManager, PostgresConnection
from graflo.db import DBConfig, PostgresConfig, SparqlEndpointConfig
from graflo.hq.caster import Caster, IngestionParams
from graflo.hq.inferencer import InferenceManager
from graflo.hq.resource_mapper import ResourceMapper
from graflo.architecture.contract.bindings import Bindings

from pathlib import Path

logger = logging.getLogger(__name__)


class GraphEngine:
    """Orchestrator for graph database operations.

    GraphEngine coordinates schema inference, connector creation, schema definition,
    and data ingestion, providing a unified interface for working with graph databases.

    The typical workflow is:
    1. infer_schema() - Infer schema from source database (if possible)
    2. create_bindings() - Create bindings mapping resources to data sources (if possible)
    3. define_schema() - Define schema in target database (if possible and necessary)
    4. ingest() - Ingest data into the target database

    Attributes:
        target_db_flavor: Target database flavor for schema sanitization
        resource_mapper: ResourceMapper instance for connector creation
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

    def infer_manifest(
        self,
        postgres_config: PostgresConfig,
        schema_name: str | None = None,
        fuzzy_threshold: float = 0.8,
        discard_disconnected_vertices: bool = False,
    ) -> GraphManifest:
        """Infer a GraphManifest from PostgreSQL database.

        Args:
            postgres_config: PostgresConfig instance
            schema_name: Schema name to introspect (defaults to config schema_name or 'public')
            fuzzy_threshold: Similarity threshold for fuzzy matching (0.0 to 1.0, default 0.8)
            discard_disconnected_vertices: If True, remove vertices that do not take part in
                any relation (and resources/actors that reference them). Default False.

        Returns:
            GraphManifest: Inferred manifest with schema and ingestion model.
        """
        with PostgresConnection(postgres_config) as postgres_conn:
            inferencer = InferenceManager(
                conn=postgres_conn,
                target_db_flavor=self.target_db_flavor,
                fuzzy_threshold=fuzzy_threshold,
            )
            schema, ingestion_model = inferencer.infer_complete_schema(
                schema_name=schema_name
            )
        if discard_disconnected_vertices:
            disconnected = schema.remove_disconnected_vertices()
            ingestion_model.prune_to_graph(
                schema.core_schema, disconnected=disconnected
            )
        return GraphManifest(graph_schema=schema, ingestion_model=ingestion_model)

    def create_bindings(
        self,
        postgres_config: PostgresConfig,
        schema_name: str | None = None,
        datetime_columns: dict[str, str] | None = None,
        type_lookup_overrides: dict[str, dict] | None = None,
    ) -> Bindings:
        """Create Bindings from PostgreSQL tables.

        Args:
            postgres_config: PostgresConfig instance
            schema_name: Schema name to introspect
            datetime_columns: Optional mapping of resource/table name to datetime
                column name for date-range filtering (sets date_field per
                TableConnector). Use with IngestionParams.datetime_after /
                datetime_before.
            type_lookup_overrides: Optional mapping of table name to type_lookup
                spec for edge tables where source/target types come from a
                lookup table. Each value: {table, identity, type_column,
                source, target, relation?}.

        Returns:
            Bindings: Bindings object with TableConnector instances for all tables
        """
        with PostgresConnection(postgres_config) as postgres_conn:
            return self.resource_mapper.create_bindings_from_postgres(
                conn=postgres_conn,
                schema_name=schema_name,
                datetime_columns=datetime_columns,
                type_lookup_overrides=type_lookup_overrides,
            )

    def define_schema(
        self,
        manifest: GraphManifest,
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
            manifest: GraphManifest with schema block.
            target_db_config: Target database connection configuration
            recreate_schema: If True, drop existing schema and define new one.
                If False and schema/graph already exists, raises SchemaExistsError.
        """
        schema = manifest.require_schema()

        # If effective_schema is not set, use schema.metadata.name as fallback
        if (
            target_db_config.can_be_target()
            and target_db_config.effective_schema is None
        ):
            schema_name = schema.metadata.name
            # Map to the appropriate field based on DB type
            if target_db_config.connection_type == DBType.TIGERGRAPH:
                # TigerGraph uses 'schema_name' field
                target_db_config.schema_name = schema_name
            else:
                # ArangoDB, Neo4j use 'database' field (which maps to effective_schema)
                target_db_config.database = schema_name

        # Ensure schema reflects target DB so finish_init applies DB-specific defaults.
        schema.db_profile.db_flavor = target_db_config.connection_type
        schema.finish_init()

        # Initialize database with schema definition
        # init_db() handles database/schema creation automatically
        # It checks if the database exists and creates it if needed
        with ConnectionManager(connection_config=target_db_config) as db_client:
            db_client.init_db(schema, recreate_schema)

    def define_and_ingest(
        self,
        manifest: GraphManifest,
        target_db_config: DBConfig,
        ingestion_params: IngestionParams | None = None,
        recreate_schema: bool | None = None,
        clear_data: bool | None = None,
    ) -> None:
        """Define schema and ingest data into the graph database in one operation.

        This is a convenience method that chains define_schema() and ingest().
        It's the recommended way to set up and populate a graph database.

        Args:
            manifest: GraphManifest with schema/ingestion/bindings blocks.
            target_db_config: Target database connection configuration
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
            manifest=manifest,
            target_db_config=target_db_config,
            recreate_schema=recreate_schema,
        )

        # Then ingest data (clear_data is applied inside ingest() when ingestion_params.clear_data)
        ingestion_params = ingestion_params.model_copy(
            update={"clear_data": clear_data}
        )
        self.ingest(
            manifest=manifest,
            target_db_config=target_db_config,
            ingestion_params=ingestion_params,
        )

    def ingest(
        self,
        manifest: GraphManifest,
        target_db_config: DBConfig,
        ingestion_params: IngestionParams | None = None,
    ) -> None:
        """Ingest data into the graph database.

        If ingestion_params.clear_data is True, removes all existing data
        (without touching the schema) before ingestion.

        Args:
            manifest: GraphManifest with schema/ingestion/bindings blocks.
            target_db_config: Target database connection configuration
            ingestion_params: IngestionParams instance with ingestion configuration.
                If None, uses default IngestionParams()
        """
        schema = manifest.require_schema()
        ingestion_model = manifest.require_ingestion_model()
        bindings = manifest.bindings

        ingestion_params = ingestion_params or IngestionParams()
        if ingestion_params.clear_data:
            with ConnectionManager(connection_config=target_db_config) as db_client:
                db_client.clear_data(schema)
        caster = Caster(
            schema=schema,
            ingestion_model=ingestion_model,
            ingestion_params=ingestion_params,
        )
        caster.ingest(
            target_db_config=target_db_config,
            bindings=bindings or Bindings(),
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
    ) -> tuple[Schema, IngestionModel]:
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
            tuple[Schema, IngestionModel]: fully initialised schema and ingestion model.
        """
        from graflo.hq.rdf_inferencer import RdfInferenceManager

        mgr = RdfInferenceManager(target_db_flavor=self.target_db_flavor)
        return mgr.infer_schema(
            source,
            endpoint_url=endpoint_url,
            graph_uri=graph_uri,
            schema_name=schema_name,
        )

    def create_bindings_from_rdf(
        self,
        source: str | Path,
        *,
        endpoint_url: str | None = None,
        graph_uri: str | None = None,
        sparql_config: SparqlEndpointConfig | None = None,
    ) -> Bindings:
        """Create :class:`Bindings` from an RDF ontology.

        One :class:`SparqlConnector` is created per ``owl:Class`` found in the
        ontology.

        Args:
            source: Path to an RDF file or base URL.
            endpoint_url: SPARQL endpoint for the *data* (ABox).
            graph_uri: Named graph containing the data.
            sparql_config: Optional :class:`SparqlEndpointConfig` to attach
                to the resulting connectors for authentication.

        Returns:
            Bindings with SPARQL connectors for each class.
        """
        from graflo.hq.rdf_inferencer import RdfInferenceManager

        mgr = RdfInferenceManager(target_db_flavor=self.target_db_flavor)
        bindings = mgr.create_bindings(
            source,
            endpoint_url=endpoint_url,
            graph_uri=graph_uri,
        )

        if sparql_config:
            bindings.sparql_configs["default"] = sparql_config

        return bindings
