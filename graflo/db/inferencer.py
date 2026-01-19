from graflo import Schema
from graflo.util.onto import Patterns, TablePattern
from graflo.architecture import Resource
from graflo.db import PostgresConnection
from graflo.db.postgres import PostgresSchemaInferencer, PostgresResourceMapper
from graflo.db.sanitizer import SchemaSanitizer
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


def infer_schema_from_postgres(
    conn: PostgresConnection,
    schema_name: str | None = None,
    db_flavor: DBFlavor = DBFlavor.ARANGO,
) -> Schema:
    """Convenience function to infer a graflo Schema from PostgreSQL database.

    Args:
        conn: PostgresConnection instance
        schema_name: Schema name to introspect (defaults to config schema_name or 'public')
        db_flavor: Target database flavor (defaults to ARANGO)

    Returns:
        Schema: Inferred schema with vertices, edges, and resources
    """
    manager = InferenceManager(conn, target_db_flavor=db_flavor)
    return manager.infer_complete_schema(schema_name=schema_name)


def create_patterns_from_postgres(
    conn: PostgresConnection, schema_name: str | None = None
) -> Patterns:
    """Create Patterns from PostgreSQL tables.

    Args:
        conn: PostgresConnection instance
        schema_name: Schema name to introspect

    Returns:
        Patterns: Patterns object with TablePattern instances for all tables
    """

    # Introspect the schema
    introspection_result = conn.introspect_schema(schema_name=schema_name)

    # Create patterns
    patterns = Patterns()

    # Get schema name
    effective_schema = schema_name or introspection_result.schema_name

    # Store the connection config
    config_key = "default"
    patterns.postgres_configs[(config_key, effective_schema)] = conn.config

    # Add patterns for vertex tables
    for table_info in introspection_result.vertex_tables:
        table_name = table_info.name
        table_pattern = TablePattern(
            table_name=table_name,
            schema_name=effective_schema,
            resource_name=table_name,
        )
        patterns.table_patterns[table_name] = table_pattern
        patterns.postgres_table_configs[table_name] = (
            config_key,
            effective_schema,
            table_name,
        )

    # Add patterns for edge tables
    for table_info in introspection_result.edge_tables:
        table_name = table_info.name
        table_pattern = TablePattern(
            table_name=table_name,
            schema_name=effective_schema,
            resource_name=table_name,
        )
        patterns.table_patterns[table_name] = table_pattern
        patterns.postgres_table_configs[table_name] = (
            config_key,
            effective_schema,
            table_name,
        )

    return patterns
