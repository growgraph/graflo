"""PostgreSQL schema inference and resource mapping (high level).

Schema sanitization for the target DB flavor is intentionally not performed
here. Use :class:`graflo.hq.sanitizer.Sanitizer` (or
:func:`graflo.architecture.evolution.apply_sanitize`) on the produced
:class:`GraphManifest` a posteriori.
"""

import logging
from dataclasses import dataclass

from graflo.architecture import Resource
from graflo.architecture.contract.declarations.ingestion_model import IngestionModel
from graflo.architecture.onto_sql import SchemaIntrospectionResult
from graflo.architecture.schema import Schema
from graflo.db import PostgresConnection
from graflo.db.postgres import PostgresResourceMapper, PostgresSchemaInferencer
from graflo.onto import DBType

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SQLInferenceArtifacts:
    """Shared inference artifacts derived from one PostgreSQL introspection snapshot."""

    introspection_result: SchemaIntrospectionResult
    schema: Schema
    ingestion_model: IngestionModel


class SQLInferenceManager:
    """Inference manager for PostgreSQL sources.

    This class only performs introspection / schema inference / resource
    creation. Sanitization for a target DB flavor (reserved words, TigerGraph
    identity normalization, etc.) is the caller's responsibility and should be
    applied a posteriori via
    :class:`graflo.hq.sanitizer.Sanitizer`.
    """

    def __init__(
        self,
        conn: PostgresConnection,
        target_db_flavor: DBType = DBType.ARANGO,
        fuzzy_threshold: float = 0.8,
    ):
        """Initialize the PostgreSQL inference manager.

        Args:
            conn: PostgresConnection instance
            target_db_flavor: Target database flavor (used for type mapping
                during inference; does NOT trigger sanitization).
            fuzzy_threshold: Similarity threshold for fuzzy matching (0.0 to 1.0, default 0.8)
        """
        self.target_db_flavor = target_db_flavor
        self.conn = conn
        self.inferencer = PostgresSchemaInferencer(
            db_flavor=target_db_flavor, conn=conn
        )
        self.mapper = PostgresResourceMapper(fuzzy_threshold=fuzzy_threshold)

    def introspect(
        self,
        schema_name: str | None = None,
        include_raw_tables: bool = False,
    ) -> SchemaIntrospectionResult:
        """Introspect PostgreSQL schema.

        Args:
            schema_name: Schema name to introspect
            include_raw_tables: Whether to build sampled per-column raw table metadata.
                Defaults to False for performance (binding/schema inference does not require it).

        Returns:
            SchemaIntrospectionResult: PostgreSQL schema introspection result
        """
        return self.conn.introspect_schema(
            schema_name=schema_name,
            include_raw_tables=include_raw_tables,
        )

    def infer_schema(
        self, introspection_result, schema_name: str | None = None
    ) -> Schema:
        """Infer graflo Schema from PostgreSQL introspection result.

        Args:
            introspection_result: SchemaIntrospectionResult from PostgreSQL
            schema_name: Schema name (optional, may be inferred from result)

        Returns:
            Schema: Inferred schema with vertices and edges
        """
        return self.inferencer.infer_schema(
            introspection_result, schema_name=schema_name
        )

    def create_resources(
        self, introspection_result, schema: Schema
    ) -> list["Resource"]:
        """Create Resources from PostgreSQL introspection result.

        Args:
            introspection_result: SchemaIntrospectionResult from PostgreSQL
            schema: Existing Schema object

        Returns:
            list[Resource]: List of Resources for PostgreSQL tables
        """
        return self.mapper.create_resources_from_tables(
            introspection_result,
            schema.core_schema.vertex_config,
            schema.core_schema.edge_config,
            fuzzy_threshold=self.mapper.fuzzy_threshold,
        )

    def infer_complete_schema(
        self, schema_name: str | None = None
    ) -> tuple[Schema, IngestionModel]:
        """Infer a complete schema and ingestion model from source.

        This is a convenience method that:
        1. Introspects the source schema
        2. Infers the graflo Schema
        3. Creates resources and finishes ingestion init

        No sanitization is performed; apply :class:`graflo.hq.sanitizer.Sanitizer`
        a posteriori to the returned manifest if you need it.

        Args:
            schema_name: Schema name to introspect (source-specific)

        Returns:
            tuple[Schema, IngestionModel]: Complete schema and ingestion model
        """
        artifacts = self.infer_artifacts(schema_name=schema_name)
        return artifacts.schema, artifacts.ingestion_model

    def infer_artifacts(self, schema_name: str | None = None) -> SQLInferenceArtifacts:
        """Infer schema/resources from a single introspection pass.

        Returns:
            SQLInferenceArtifacts: introspection + schema + ingestion model tuple.
                The output is NOT sanitized for the target DB flavor.
        """
        introspection_result = self.introspect(schema_name=schema_name)
        schema = self.infer_schema(introspection_result, schema_name=schema_name)
        resources = self.create_resources(introspection_result, schema)
        ingestion_model = IngestionModel(resources=resources)
        ingestion_model.finish_init(schema.core_schema)
        return SQLInferenceArtifacts(
            introspection_result=introspection_result,
            schema=schema,
            ingestion_model=ingestion_model,
        )

    def create_resources_for_schema(
        self, schema: Schema, schema_name: str | None = None
    ) -> list["Resource"]:
        """Create Resources from source for an existing schema.

        Args:
            schema: Existing Schema object
            schema_name: Schema name to introspect (source-specific)

        Returns:
            list[Resource]: List of Resources for the source
        """
        # Introspect the schema
        introspection_result = self.introspect(schema_name=schema_name)

        # Create resources
        return self.create_resources(introspection_result, schema)
