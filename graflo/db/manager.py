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
    >>> from graflo.db.connection.onto import ArangoConfig
    >>> config = ArangoConfig.from_env()
    >>> with ConnectionManager(connection_config=config) as conn:
    ...     conn.execute("FOR doc IN collection RETURN doc")
"""

from graflo.db.arango.conn import ArangoConnection
from graflo.db.connection.onto import DBConfig, DBType, TARGET_DATABASES
from graflo.db.neo4j.conn import Neo4jConnection
from graflo.db.tigergraph.conn import TigerGraphConnection


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
    }

    # Source database connections (INPUT) - to be implemented
    # source_conn_mapping = {
    #     DBType.POSTGRES: PostgresConnection,
    #     DBType.MYSQL: MySQLConnection,
    #     DBType.MONGODB: MongoDBConnection,
    # }

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
