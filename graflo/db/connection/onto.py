import abc
from enum import EnumMeta
from pathlib import Path
from strenum import StrEnum
from typing import Any, Dict, Type, TypeVar
from urllib.parse import urlparse

from pydantic import Field, model_validator
from pydantic import AliasChoices
from pydantic_settings import BaseSettings, SettingsConfigDict


class EnumMetaWithContains(EnumMeta):
    """Enhanced EnumMeta that supports 'in' operator checks."""

    def __contains__(cls, item, **kwargs):
        try:
            cls(item, **kwargs)
        except ValueError:
            return False
        return True


# Type variable for DBConfig subclasses
T = TypeVar("T", bound="DBConfig")


class DBType(StrEnum, metaclass=EnumMetaWithContains):
    """Enum representing different types of databases.

    Includes both graph databases and source databases (SQL, NoSQL, etc.).
    """

    # Graph databases
    ARANGO = "arango"
    NEO4J = "neo4j"
    TIGERGRAPH = "tigergraph"

    # Source databases (SQL, NoSQL)
    POSTGRES = "postgres"
    MYSQL = "mysql"
    MONGODB = "mongodb"
    SQLITE = "sqlite"

    @property
    def config_class(self) -> Type["DBConfig"]:
        """Get the appropriate config class for this database type."""
        from .config_mapping import DB_TYPE_MAPPING

        return DB_TYPE_MAPPING[self]


# Databases that can be used as sources (INPUT)
SOURCE_DATABASES: set[DBType] = {
    DBType.ARANGO,  # Graph DBs can be sources
    DBType.NEO4J,  # Graph DBs can be sources
    DBType.TIGERGRAPH,  # Graph DBs can be sources
    DBType.POSTGRES,  # SQL DBs
    DBType.MYSQL,
    DBType.MONGODB,
    DBType.SQLITE,
}

# Databases that can be used as targets (OUTPUT)
TARGET_DATABASES: set[DBType] = {
    DBType.ARANGO,
    DBType.NEO4J,
    DBType.TIGERGRAPH,
}


class DBConfig(BaseSettings, abc.ABC):
    """Abstract base class for all database connection configurations using Pydantic BaseSettings."""

    uri: str | None = Field(default=None, description="Backend URI")
    username: str | None = Field(default=None, description="Authentication username")
    password: str | None = Field(default=None, description="Authentication Password")
    database: str | None = Field(
        default=None,
        description="Database name (backward compatibility, DB-specific mapping)",
    )
    schema_name: str | None = Field(
        default=None,
        validation_alias=AliasChoices("schema", "schema_name"),
        description="Schema/graph name (unified internal structure)",
    )
    request_timeout: float = Field(
        default=60.0, description="Request timeout in seconds"
    )

    @abc.abstractmethod
    def _get_default_port(self) -> int:
        """Get the default port for this db type."""
        pass

    @abc.abstractmethod
    def _get_effective_database(self) -> str | None:
        """Get the effective database name based on DB type.

        For SQL databases: returns the database name
        For graph databases: returns None (they don't have a database level)

        Returns:
            Database name or None
        """
        pass

    @abc.abstractmethod
    def _get_effective_schema(self) -> str | None:
        """Get the effective schema/graph name based on DB type.

        For SQL databases: returns the schema name
        For graph databases: returns the graph/database name (mapped from user-facing field)

        Returns:
            Schema/graph name or None
        """
        pass

    @property
    def effective_database(self) -> str | None:
        """Get the effective database name (delegates to concrete class)."""
        return self._get_effective_database()

    @property
    def effective_schema(self) -> str | None:
        """Get the effective schema/graph name (delegates to concrete class)."""
        return self._get_effective_schema()

    @model_validator(mode="after")
    def _add_default_port_to_uri(self):
        """Add default port to URI if missing."""
        if self.uri is None:
            return self

        parsed = urlparse(self.uri)
        if parsed.port is not None:
            return self

        # Add default port
        default_port = self._get_default_port()
        if parsed.scheme and parsed.hostname:
            # Reconstruct URI with port
            port_part = f":{default_port}" if default_port else ""
            path_part = parsed.path or ""
            query_part = f"?{parsed.query}" if parsed.query else ""
            fragment_part = f"#{parsed.fragment}" if parsed.fragment else ""
            self.uri = f"{parsed.scheme}://{parsed.hostname}{port_part}{path_part}{query_part}{fragment_part}"

        return self

    @property
    def url(self) -> str | None:
        """Backward compatibility property: alias for uri."""
        return self.uri

    @property
    def url_without_port(self) -> str:
        """Get URL without port."""
        if self.uri is None:
            raise ValueError("URI is not set")
        parsed = urlparse(self.uri)
        return f"{parsed.scheme}://{parsed.hostname}"

    @property
    def port(self) -> str | None:
        """Get port from URI."""
        if self.uri is None:
            return None
        parsed = urlparse(self.uri)
        return str(parsed.port) if parsed.port else None

    @property
    def protocol(self) -> str:
        """Get protocol/scheme from URI."""
        if self.uri is None:
            return "http"
        parsed = urlparse(self.uri)
        return parsed.scheme or "http"

    @property
    def hostname(self) -> str | None:
        """Get hostname from URI."""
        if self.uri is None:
            return None
        parsed = urlparse(self.uri)
        return parsed.hostname

    @property
    def connection_type(self) -> "DBType":
        """Get database type from class."""
        # Map class to DBType - need to import here to avoid circular import
        from .config_mapping import DB_TYPE_MAPPING

        # Reverse lookup: find DBType for this class
        for db_type, config_class in DB_TYPE_MAPPING.items():
            if type(self) is config_class:
                return db_type

        # Fallback (shouldn't happen)
        return DBType.ARANGO

    def can_be_source(self) -> bool:
        """Check if this database type can be used as a source."""
        return self.connection_type in SOURCE_DATABASES

    def can_be_target(self) -> bool:
        """Check if this database type can be used as a target."""
        return self.connection_type in TARGET_DATABASES

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DBConfig":
        """Create a connection config from a dictionary."""
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")

        # Copy the data to avoid modifying the original
        config_data = data.copy()

        db_type = config_data.pop("db_type", None) or config_data.pop(
            "connection_type", None
        )
        if not db_type:
            raise ValueError("Missing 'db_type' or 'connection_type' in configuration")

        try:
            conn_type = DBType(db_type)
        except ValueError:
            raise ValueError(
                f"Database type '{db_type}' not supported. "
                f"Should be one of: {list(DBType)}"
            )

        # Map old 'url' field to 'uri' for backward compatibility
        if "url" in config_data and "uri" not in config_data:
            config_data["uri"] = config_data.pop("url")

        # Map old credential fields
        if "cred_name" in config_data and "username" not in config_data:
            config_data["username"] = config_data.pop("cred_name")
        if "cred_pass" in config_data and "password" not in config_data:
            config_data["password"] = config_data.pop("cred_pass")

        # Construct URI from protocol/hostname/port if uri is not provided
        if "uri" not in config_data:
            protocol = config_data.pop("protocol", "http")
            hostname = config_data.pop("hostname", None)
            port = config_data.pop("port", None)
            hosts = config_data.pop("hosts", None)

            if hosts:
                # Use hosts as URI
                config_data["uri"] = hosts
            elif hostname:
                # Construct URI from components
                if port:
                    config_data["uri"] = f"{protocol}://{hostname}:{port}"
                else:
                    config_data["uri"] = f"{protocol}://{hostname}"

        # Get the appropriate config class and initialize it
        config_class = conn_type.config_class
        return config_class(**config_data)

    @classmethod
    def from_docker_env(cls, docker_dir: str | Path | None = None) -> "DBConfig":
        """Load config from docker .env file.

        Args:
            docker_dir: Path to docker directory. If None, uses default based on db type.

        Returns:
            DBConfig instance loaded from .env file
        """
        raise NotImplementedError("Subclasses must implement from_docker_env")

    @classmethod
    def from_env(cls: Type[T], prefix: str | None = None) -> T:
        """Load config from environment variables using Pydantic BaseSettings.

        Supports custom prefixes for multiple configs:
        - Default (prefix=None): Uses {BASE_PREFIX}URI, {BASE_PREFIX}USERNAME, etc.
        - With prefix (prefix="USER"): Uses USER_{BASE_PREFIX}URI, USER_{BASE_PREFIX}USERNAME, etc.

        Args:
            prefix: Optional prefix for environment variables (e.g., "USER", "LAKE", "KG").
                   If None, uses default {BASE_PREFIX}* variables.

        Returns:
            DBConfig instance loaded from environment variables using Pydantic BaseSettings

        Examples:
            # Load default config (ARANGO_URI, ARANGO_USERNAME, etc.)
            config = ArangoConfig.from_env()

            # Load config with prefix (USER_ARANGO_URI, USER_ARANGO_USERNAME, etc.)
            user_config = ArangoConfig.from_env(prefix="USER")
        """
        if prefix:
            # Get the base prefix from the class's model_config
            base_prefix = cls.model_config.get("env_prefix")
            if not base_prefix:
                raise ValueError(
                    f"Class {cls.__name__} does not have env_prefix configured in model_config"
                )
            # Create a new model class with modified env_prefix
            new_prefix = f"{prefix.upper()}_{base_prefix}"
            case_sensitive = cls.model_config.get("case_sensitive", False)
            model_config = SettingsConfigDict(
                env_prefix=new_prefix,
                case_sensitive=case_sensitive,
            )
            # Create a new class dynamically with the modified prefix
            temp_class = type(
                f"{cls.__name__}WithPrefix", (cls,), {"model_config": model_config}
            )
            return temp_class()
        else:
            # Use default prefix - Pydantic will read from environment automatically
            return cls()


class ArangoConfig(DBConfig):
    """Configuration for ArangoDB connections."""

    model_config = SettingsConfigDict(
        env_prefix="ARANGO_",
        case_sensitive=False,
    )

    def _get_default_port(self) -> int:
        """Get default ArangoDB port."""
        return 8529

    def _get_effective_database(self) -> str | None:
        """ArangoDB doesn't have a database level (connection -> database/graph -> collections)."""
        return None

    def _get_effective_schema(self) -> str | None:
        """For ArangoDB, 'database' field maps to schema (graph) in unified model.

        ArangoDB structure: connection -> database (graph) -> collections
        Unified model: connection -> schema -> entities
        """
        return self.database

    @classmethod
    def from_docker_env(cls, docker_dir: str | Path | None = None) -> "ArangoConfig":
        """Load ArangoDB config from docker/arango/.env file.

        The .env file structure is minimal and may contain:
        - ARANGO_PORT: Port number (defaults hostname to localhost, protocol to http)
        - ARANGO_URI: Full URI (alternative to ARANGO_PORT)
        - ARANGO_HOSTNAME: Hostname (defaults to localhost)
        - ARANGO_PROTOCOL: Protocol (defaults to http)
        - ARANGO_USERNAME: Username (defaults to root)
        - ARANGO_PASSWORD: Password (or read from secret file if PATH_TO_SECRET is set)
        - ARANGO_DATABASE: Database name (optional, can be set later)
        - PATH_TO_SECRET: Path to secret file containing password (relative to docker_dir)
        """
        if docker_dir is None:
            docker_dir = (
                Path(__file__).parent.parent.parent.parent / "docker" / "arango"
            )
        else:
            docker_dir = Path(docker_dir)

        env_file = docker_dir / ".env"
        if not env_file.exists():
            raise FileNotFoundError(f"Environment file not found: {env_file}")

        # Load .env file manually with simple variable expansion
        env_vars: Dict[str, str] = {}
        with open(env_file, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")
                    env_vars[key] = value

        # Expand variables (simple single-pass expansion)
        # First pass: expand ${SPEC} references
        for key, value in env_vars.items():
            if "${SPEC}" in value and "SPEC" in env_vars:
                env_vars[key] = value.replace("${SPEC}", env_vars["SPEC"])

        # Second pass: expand other variables (like ${CONTAINER_NAME})
        for key, value in env_vars.items():
            for var_name, var_value in env_vars.items():
                var_ref = f"${{{var_name}}}"
                if var_ref in value:
                    env_vars[key] = value.replace(var_ref, var_value)

        # Map environment variables to config
        config_data: Dict[str, Any] = {}

        # URI construction
        if "ARANGO_URI" in env_vars:
            config_data["uri"] = env_vars["ARANGO_URI"]
        elif "ARANGO_PORT" in env_vars:
            port = env_vars["ARANGO_PORT"]
            hostname = env_vars.get("ARANGO_HOSTNAME", "localhost")
            protocol = env_vars.get("ARANGO_PROTOCOL", "http")
            config_data["uri"] = f"{protocol}://{hostname}:{port}"
        else:
            # Default to localhost:8529 if nothing is specified
            config_data["uri"] = "http://localhost:8529"

        # Username (defaults to root for ArangoDB)
        if "ARANGO_USERNAME" in env_vars:
            config_data["username"] = env_vars["ARANGO_USERNAME"]
        else:
            config_data["username"] = "root"

        # Password: check ARANGO_PASSWORD first, then try secret file
        if "ARANGO_PASSWORD" in env_vars:
            config_data["password"] = env_vars["ARANGO_PASSWORD"]
        elif "PATH_TO_SECRET" in env_vars:
            # Read password from secret file
            secret_path_str = env_vars["PATH_TO_SECRET"]
            # Handle relative paths (relative to docker_dir)
            if secret_path_str.startswith("./"):
                secret_path = docker_dir / secret_path_str[2:]
            else:
                secret_path = Path(secret_path_str)

            if secret_path.exists():
                with open(secret_path, "r") as f:
                    config_data["password"] = f.read().strip()
            else:
                # Secret file not found, password will be None (ArangoDB accepts empty string)
                config_data["password"] = None

        # Database (optional, can be set later or use Schema.general.name)
        if "ARANGO_DATABASE" in env_vars:
            config_data["database"] = env_vars["ARANGO_DATABASE"]

        return cls(**config_data)


class Neo4jConfig(DBConfig):
    """Configuration for Neo4j connections."""

    model_config = SettingsConfigDict(
        env_prefix="NEO4J_",
        case_sensitive=False,
    )

    bolt_port: int | None = Field(default=None, description="Neo4j bolt protocol port")

    def _get_default_port(self) -> int:
        """Get default Neo4j HTTP port."""
        return 7474

    def _get_effective_database(self) -> str | None:
        """Neo4j doesn't have a database level (connection -> database -> nodes/relationships)."""
        return None

    def _get_effective_schema(self) -> str | None:
        """For Neo4j, 'database' field maps to schema (database) in unified model.

        Neo4j structure: connection -> database -> nodes/relationships
        Unified model: connection -> schema -> entities
        """
        return self.database

    def __init__(self, **data):
        """Initialize Neo4j config."""
        super().__init__(**data)
        # Set default bolt_port if not provided
        if self.bolt_port is None:
            self.bolt_port = 7687

    @classmethod
    def from_docker_env(cls, docker_dir: str | Path | None = None) -> "Neo4jConfig":
        """Load Neo4j config from docker/neo4j/.env file."""
        if docker_dir is None:
            docker_dir = Path(__file__).parent.parent.parent.parent / "docker" / "neo4j"
        else:
            docker_dir = Path(docker_dir)

        env_file = docker_dir / ".env"
        if not env_file.exists():
            raise FileNotFoundError(f"Environment file not found: {env_file}")

        # Load .env file manually
        env_vars: Dict[str, str] = {}
        with open(env_file, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    env_vars[key.strip()] = value.strip().strip('"').strip("'")

        # Map environment variables to config
        config_data: Dict[str, Any] = {}
        # Neo4j typically uses bolt protocol
        if "NEO4J_BOLT_PORT" in env_vars:
            port = env_vars["NEO4J_BOLT_PORT"]
            hostname = env_vars.get("NEO4J_HOSTNAME", "localhost")
            config_data["uri"] = f"bolt://{hostname}:{port}"
            config_data["bolt_port"] = int(port)
        elif "NEO4J_URI" in env_vars:
            config_data["uri"] = env_vars["NEO4J_URI"]

        if "NEO4J_USERNAME" in env_vars:
            config_data["username"] = env_vars["NEO4J_USERNAME"]
        elif "NEO4J_AUTH" in env_vars:
            # Parse NEO4J_AUTH format: username/password
            auth = env_vars["NEO4J_AUTH"].split("/")
            if len(auth) == 2:
                config_data["username"] = auth[0]
                config_data["password"] = auth[1]

        if "NEO4J_PASSWORD" in env_vars:
            config_data["password"] = env_vars["NEO4J_PASSWORD"]
        if "NEO4J_DATABASE" in env_vars:
            config_data["database"] = env_vars["NEO4J_DATABASE"]

        return cls(**config_data)


class TigergraphConfig(DBConfig):
    """Configuration for TigerGraph connections."""

    model_config = SettingsConfigDict(
        env_prefix="TIGERGRAPH_",
        case_sensitive=False,
    )

    gs_port: int | None = Field(default=None, description="TigerGraph GSQL port")
    secret: str | None = Field(
        default=None, description="TigerGraph secret for token authentication"
    )

    def _get_default_port(self) -> int:
        """Get default TigerGraph REST++ port."""
        return 9000

    def _get_effective_database(self) -> str | None:
        """TigerGraph doesn't have a database level (connection -> schema -> vertices/edges)."""
        return None

    def _get_effective_schema(self) -> str | None:
        """For TigerGraph, 'schema_name' field maps to schema (graph) in unified model.

        TigerGraph structure: connection -> schema -> vertices/edges
        Unified model: connection -> schema -> entities
        """
        return self.schema_name

    def __init__(self, **data):
        """Initialize TigerGraph config."""
        super().__init__(**data)
        # Set default gs_port if not provided
        if self.gs_port is None:
            self.gs_port = 14240

    @classmethod
    def from_docker_env(
        cls, docker_dir: str | Path | None = None
    ) -> "TigergraphConfig":
        """Load TigerGraph config from docker/tigergraph/.env file."""
        if docker_dir is None:
            docker_dir = (
                Path(__file__).parent.parent.parent.parent / "docker" / "tigergraph"
            )
        else:
            docker_dir = Path(docker_dir)

        env_file = docker_dir / ".env"
        if not env_file.exists():
            raise FileNotFoundError(f"Environment file not found: {env_file}")

        # Load .env file manually
        env_vars: Dict[str, str] = {}
        with open(env_file, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    env_vars[key.strip()] = value.strip().strip('"').strip("'")

        # Map environment variables to config
        config_data: Dict[str, Any] = {}
        if "TG_REST" in env_vars or "TIGERGRAPH_PORT" in env_vars:
            port = env_vars.get("TG_REST") or env_vars.get("TIGERGRAPH_PORT")
            hostname = env_vars.get("TIGERGRAPH_HOSTNAME", "localhost")
            protocol = env_vars.get("TIGERGRAPH_PROTOCOL", "http")
            config_data["uri"] = f"{protocol}://{hostname}:{port}"

        if "TG_WEB" in env_vars or "TIGERGRAPH_GS_PORT" in env_vars:
            gs_port = env_vars.get("TG_WEB") or env_vars.get("TIGERGRAPH_GS_PORT")
            config_data["gs_port"] = int(gs_port) if gs_port else None

        if "TIGERGRAPH_USERNAME" in env_vars:
            config_data["username"] = env_vars["TIGERGRAPH_USERNAME"]
        if "TIGERGRAPH_PASSWORD" in env_vars or "GSQL_PASSWORD" in env_vars:
            config_data["password"] = env_vars.get(
                "TIGERGRAPH_PASSWORD"
            ) or env_vars.get("GSQL_PASSWORD")
        if "TIGERGRAPH_DATABASE" in env_vars:
            config_data["database"] = env_vars["TIGERGRAPH_DATABASE"]

        return cls(**config_data)


class PostgresConfig(DBConfig):
    """Configuration for PostgreSQL connections."""

    model_config = SettingsConfigDict(
        env_prefix="POSTGRES_",
        case_sensitive=False,
    )

    def _get_default_port(self) -> int:
        """Get default PostgreSQL port."""
        return 5432

    def _get_effective_database(self) -> str | None:
        """For PostgreSQL, 'database' field is the actual database name.

        PostgreSQL structure: connection -> database -> schema -> table
        Unified model: connection -> database -> schema -> entity
        """
        return self.database

    def _get_effective_schema(self) -> str | None:
        """For PostgreSQL, 'schema_name' field is the schema name.

        PostgreSQL structure: connection -> database -> schema -> table
        Unified model: connection -> database -> schema -> entity
        """
        return self.schema_name

    @classmethod
    def from_docker_env(cls, docker_dir: str | Path | None = None) -> "PostgresConfig":
        """Load PostgreSQL config from docker/postgres/.env file."""
        if docker_dir is None:
            docker_dir = (
                Path(__file__).parent.parent.parent.parent / "docker" / "postgres"
            )
        else:
            docker_dir = Path(docker_dir)

        env_file = docker_dir / ".env"
        if not env_file.exists():
            raise FileNotFoundError(f"Environment file not found: {env_file}")

        # Load .env file manually
        env_vars: Dict[str, str] = {}
        with open(env_file, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    env_vars[key.strip()] = value.strip().strip('"').strip("'")

        # Map environment variables to config
        config_data: Dict[str, Any] = {}
        if "POSTGRES_URI" in env_vars:
            config_data["uri"] = env_vars["POSTGRES_URI"]
        elif "POSTGRES_PORT" in env_vars:
            port = env_vars["POSTGRES_PORT"]
            hostname = env_vars.get("POSTGRES_HOSTNAME", "localhost")
            protocol = env_vars.get("POSTGRES_PROTOCOL", "postgresql")
            config_data["uri"] = f"{protocol}://{hostname}:{port}"
        elif "POSTGRES_HOST" in env_vars:
            # PostgreSQL often uses POSTGRES_HOST instead of POSTGRES_HOSTNAME
            port = env_vars.get("POSTGRES_PORT", "5432")
            hostname = env_vars["POSTGRES_HOST"]
            protocol = env_vars.get("POSTGRES_PROTOCOL", "postgresql")
            config_data["uri"] = f"{protocol}://{hostname}:{port}"

        if "POSTGRES_USER" in env_vars or "POSTGRES_USERNAME" in env_vars:
            config_data["username"] = env_vars.get("POSTGRES_USER") or env_vars.get(
                "POSTGRES_USERNAME"
            )
        if "POSTGRES_PASSWORD" in env_vars:
            config_data["password"] = env_vars["POSTGRES_PASSWORD"]
        if "POSTGRES_DB" in env_vars or "POSTGRES_DATABASE" in env_vars:
            config_data["database"] = env_vars.get("POSTGRES_DB") or env_vars.get(
                "POSTGRES_DATABASE"
            )

        return cls(**config_data)
