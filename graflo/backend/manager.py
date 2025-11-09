"""Database connection manager for graph databases.

This module provides a connection manager for handling database connections
to different graph database implementations (ArangoDB, Neo4j). It manages
connection lifecycle and configuration.

Key Components:
    - ConnectionManager: Main class for managing database connections
    - ConnectionKind: Enum for supported database types

The manager supports:
    - Multiple database types (ArangoDB, Neo4j)
    - Connection configuration
    - Context manager interface
    - Automatic connection cleanup

Example:
    >>> with ConnectionManager(secret_path="config.json") as conn:
    ...     conn.execute("FOR doc IN collection RETURN doc")
"""

from graflo.backend.arango.conn import ArangoConnection
from graflo.backend.connection.onto import ConnectionKind, ProtoConnectionConfig
from graflo.backend.neo4j.conn import Neo4jConnection
from graflo.backend.tigergraph.conn import TigerGraphConnection


class ConnectionManager:
    """Manager for database connections.

    This class manages database connections to different graph database
    implementations. It provides a context manager interface for safe
    connection handling and automatic cleanup.

    Attributes:
        conn_class_mapping: Mapping of connection types to connection classes
        config: Connection configuration
        working_db: Current working database name
        conn: Active database connection
    """

    conn_class_mapping = {
        ConnectionKind.ARANGO: ArangoConnection,
        ConnectionKind.NEO4J: Neo4jConnection,
        ConnectionKind.TIGERGRAPH: TigerGraphConnection,
    }

    def __init__(
        self,
        connection_config: ProtoConnectionConfig,
        **kwargs,
    ):
        """Initialize the connection manager.

        Args:
            secret_path: Path to configuration file
            args: Command line arguments
            connection_config: Optional connection configuration
            **kwargs: Additional configuration parameters
        """
        self.config: ProtoConnectionConfig = connection_config
        self.working_db = kwargs.pop("working_db", None)
        self.conn = None

    def __enter__(self):
        """Enter the context manager.

        Creates and returns a new database connection.

        Returns:
            Connection: Database connection instance
        """
        cls = self.conn_class_mapping[self.config.connection_type]
        if self.working_db is not None:
            self.config.database = self.working_db
        self.conn = cls(config=self.config)
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
