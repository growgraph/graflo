"""Database connection manager for graph and source databases.

This module provides a connection manager for handling database connections
to different database implementations (ArangoDB, Neo4j, PostgreSQL, etc.).
It manages connection lifecycle and configuration.

Key Components:
    - ConnectionManager: Main class for managing database connections
    - DBType: Enum for supported database types

The manager supports:
    - Target databases (ArangoDB, Neo4j, TigerGraph) - OUTPUT
    - Source databases (PostgreSQL, MySQL, MongoDB, etc.) - INPUT
    - Connection configuration
    - Context manager interface
    - Automatic connection cleanup

Example:
    >>> from graflo.connections.onto import ArangoConfig
    >>> config = ArangoConfig.from_env()
    >>> with ConnectionManager(connection_config=config) as conn:
    ...     # ArangoDB-specific AQL query (collection is ArangoDB terminology)
    ...     conn.execute("FOR doc IN vertex_class RETURN doc")
"""

from typing import Any, cast

from graflo.connections.onto import TARGET_DATABASES, DBConfig
from graflo.db.arango.conn import ArangoConnection
from graflo.db.conn import ConnectionCapability
from graflo.db.falkordb.conn import FalkordbConnection
from graflo.db.graflo_backend.connection import GraFloBackendConnection
from graflo.db.memgraph.conn import MemgraphConnection
from graflo.db.nebula.conn import NebulaConnection
from graflo.db.neo4j.conn import Neo4jConnection
from graflo.db.postgres.conn import PostgresConnection
from graflo.db.tigergraph.conn import TigerGraphConnection
from graflo.onto import DBType


class ConnectionManager:
    """Manager for database connections (both graph and source databases).

    This class manages database connections to different database
    implementations. It provides a context manager interface for safe
    connection handling and automatic cleanup.

    Supports:
    - Target databases (OUTPUT): ArangoDB, Neo4j, TigerGraph
    - Source databases (INPUT): PostgreSQL, MySQL, MongoDB, etc.

    Attributes:
        target_conn_mapping: Mapping of target database types to connection classes
        config: Connection configuration
        working_db: Current working database name
        conn: Active database connection
    """

    # Target database connections (OUTPUT)
    target_conn_mapping = {
        DBType.ARANGO: ArangoConnection,
        DBType.NEO4J: Neo4jConnection,
        DBType.TIGERGRAPH: TigerGraphConnection,
        DBType.FALKORDB: FalkordbConnection,
        DBType.MEMGRAPH: MemgraphConnection,
        DBType.NEBULA: NebulaConnection,
        DBType.POSTGRES: PostgresConnection,
        DBType.GRAFLO_BACKEND: GraFloBackendConnection,
    }

    @classmethod
    def flavors_supporting(cls, capability: ConnectionCapability) -> list[DBType]:
        """Database types whose connection class declares *capability*."""
        return [
            db_type
            for db_type, conn_cls in cls.target_conn_mapping.items()
            if getattr(conn_cls, capability.value, False)
        ]

    @classmethod
    def graph_export_flavors(cls) -> list[DBType]:
        """Database types that support bulk graph export."""
        return cls.flavors_supporting(ConnectionCapability.GRAPH_EXPORT)

    @classmethod
    def open_read_connection(
        cls,
        connection_config: DBConfig,
        *,
        require: ConnectionCapability,
    ):
        """Open a connection for reading, gated on the capability actually needed.

        Replaces ``open_graph_connection``, which gated every read path on
        ``supports_graph_export`` -- "can dump the whole graph". That is a far
        stronger claim than "can describe itself" or "can answer a bounded
        read", and asking for it is why introspection reached three of eight
        backends. Callers now name the capability they use, so a backend that
        can introspect but not export is no longer turned away.

        The caller is responsible for closing the connection.

        Args:
            connection_config: Which database to open.
            require: The capability the caller is about to exercise.

        Raises:
            ValueError: If the flavor has no implementation, or does not declare
                *require*.
        """
        db_type = connection_config.connection_type
        conn_cls = cls.target_conn_mapping.get(db_type)
        if conn_cls is None:
            raise ValueError(
                f"No graph connection implementation for database type {db_type!r}"
            )
        if not getattr(conn_cls, require.value, False):
            supported = [t.value for t in cls.flavors_supporting(require)]
            raise ValueError(
                f"Database type {db_type!r} does not support {require.label}. "
                f"Supported types: {supported}"
            )
        return conn_cls(config=cast(Any, connection_config))

    def __init__(
        self,
        connection_config: DBConfig,
        **kwargs,
    ):
        """Initialize the connection manager.

        Args:
            connection_config: Database connection configuration
            **kwargs: Additional configuration parameters
        """
        self.config: DBConfig = connection_config
        self.working_db = kwargs.pop("working_db", None)
        self.conn = None

    def __enter__(self):
        """Enter the context manager.

        Creates and returns a new database connection.

        Returns:
            Connection: Database connection instance
        """
        # Check if database can be used as target
        if not self.config.can_be_target():
            raise ValueError(
                f"Database type '{self.config.connection_type}' cannot be used as a target. "
                f"Only these types can be targets: {[t.value for t in TARGET_DATABASES]}"
            )

        db_type = self.config.connection_type
        cls = self.target_conn_mapping[db_type]

        config = self.config
        if self.working_db is not None:
            # Work on a copy: the caller's config may be shared across concurrent
            # connection managers, and mutating it here would leak working_db.
            config = config.model_copy(update={"database": self.working_db})
        self.conn = cls(config=cast(Any, config))
        return self.conn

    def close(self):
        """Close the database connection.

        Closes the active connection and performs any necessary cleanup.
        """
        if self.conn is not None:
            self.conn.close()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Exit the context manager.

        Ensures the connection is properly closed when exiting the context.

        Args:
            exc_type: Exception type if an exception occurred
            exc_value: Exception value if an exception occurred
            exc_traceback: Exception traceback if an exception occurred
        """
        self.close()
