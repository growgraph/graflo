from graflo import Schema
from graflo.architecture import Resource
from graflo.db import PostgresConnection
from graflo.db.postgres import PostgresSchemaInferencer, PostgresResourceMapper
from graflo.hq.sanitizer import SchemaSanitizer
from graflo.onto import DBFlavor
import logging

logger = logging.getLogger(__name__)


class InferenceManager:
    """Inference manager for PostgreSQL sources."""

    def __init__(
        self,
        conn: PostgresConnection,
        target_db_flavor: DBFlavor = DBFlavor.ARANGO,
    ):
        """Initialize the PostgreSQL inference manager.

        Args:
            conn: PostgresConnection instance
            target_db_flavor: Target database flavor for schema sanitization
        """
        self.target_db_flavor = target_db_flavor
        self.sanitizer = SchemaSanitizer(target_db_flavor)
        self.conn = conn
        self.inferencer = PostgresSchemaInferencer(
            db_flavor=target_db_flavor, conn=conn
        )
        self.mapper = PostgresResourceMapper()

    def introspect(self, schema_name: str | None = None):
        """Introspect PostgreSQL schema.

        Args:
            schema_name: Schema name to introspect

        Returns:
            SchemaIntrospectionResult: PostgreSQL schema introspection result
        """
        return self.conn.introspect_schema(schema_name=schema_name)

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
        return self.mapper.map_tables_to_resources(
            introspection_result, schema.vertex_config, self.sanitizer
        )

    def infer_complete_schema(self, schema_name: str | None = None) -> Schema:
        """Infer a complete Schema from source and sanitize for target.

        This is a convenience method that:
        1. Introspects the source schema
        2. Infers the graflo Schema
        3. Sanitizes for the target database flavor
        4. Creates and adds resources
        5. Re-initializes the schema

        Args:
            schema_name: Schema name to introspect (source-specific)

        Returns:
            Schema: Complete inferred schema with vertices, edges, and resources
        """
        # Introspect the schema
        introspection_result = self.introspect(schema_name=schema_name)

        # Infer schema
        schema = self.infer_schema(introspection_result, schema_name=schema_name)

        # Sanitize for target database flavor
        schema = self.sanitizer.sanitize(schema)

        # Create and add resources
        resources = self.create_resources(introspection_result, schema)
        schema.resources = resources

        # Re-initialize to set up resource mappings
        schema.__post_init__()

        return schema

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
