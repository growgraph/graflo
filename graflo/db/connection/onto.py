import abc
import logging
import warnings
from pathlib import Path
from strenum import StrEnum
from typing import Any, Dict, Type, TypeVar
from urllib.parse import urlparse

from pydantic import Field, model_validator
from pydantic import AliasChoices
from pydantic_settings import BaseSettings, SettingsConfigDict

from graflo.onto import MetaEnum

logger = logging.getLogger(__name__)

# Type variable for DBConfig subclasses
T = TypeVar("T", bound="DBConfig")


class DBType(StrEnum, metaclass=MetaEnum):
    """Enum representing different types of databases.

    Includes both graph databases and source databases (SQL, NoSQL, etc.).
    """

    # Graph databases
    ARANGO = "arango"
    NEO4J = "neo4j"
    TIGERGRAPH = "tigergraph"
    FALKORDB = "falkordb"
    MEMGRAPH = "memgraph"
    NEBULA = "nebula"

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
    DBType.FALKORDB,  # Graph DBs can be sources
    DBType.MEMGRAPH,  # Graph DBs can be sources
    DBType.NEBULA,  # Graph DBs can be sources
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
    DBType.FALKORDB,
    DBType.MEMGRAPH,
    DBType.NEBULA,
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
    def _normalize_uri(self):
        """Normalize URI: handle URIs without scheme and add default port if missing."""
        if self.uri is None:
            return self

        # Valid URL schemes (common database protocols)
        valid_schemes = {
            "http",
            "https",
            "bolt",
            "bolt+s",
            "bolt+ssc",
            "neo4j",
            "neo4j+s",
            "neo4j+ssc",
            "mongodb",
            "postgresql",
            "postgres",
            "mysql",
            "nebula",
            "redis",  # FalkorDB uses redis:// protocol
            "rediss",  # Redis with SSL
        }

        # Try to parse as-is first
        parsed = urlparse(self.uri)

        # Check if parsed scheme is actually a valid scheme or if it's a hostname
        # urlparse treats "localhost:14240" as scheme="localhost", path="14240"
        # We need to detect this case
        has_valid_scheme = parsed.scheme.lower() in valid_schemes
        has_netloc = bool(parsed.netloc)

        # If scheme doesn't look like a valid scheme and we have a colon, treat as host:port
        if not has_valid_scheme and ":" in self.uri and not self.uri.startswith("//"):
            # Check if it looks like host:port format
            parts = self.uri.split(":", 1)
            if len(parts) == 2:
                potential_host = parts[0]
                port_and_rest = parts[1]
                # Extract port (may have path/query after it)
                port_part = port_and_rest.split("/")[0].split("?")[0].split("#")[0]
                try:
                    # Validate port is numeric
                    int(port_part)
                    # If hostname doesn't look like a scheme (contains dots, is localhost, etc.)
                    # or if the parsed scheme is not in valid schemes, treat as host:port
                    if (
                        "." in potential_host
                        or potential_host.lower() in {"localhost", "127.0.0.1"}
                        or not has_valid_scheme
                    ):
                        # Reconstruct as proper URI with default scheme
                        default_scheme = "http"  # Default to http for most DBs
                        rest = port_and_rest[len(port_part) :]  # Everything after port
                        self.uri = (
                            f"{default_scheme}://{potential_host}:{port_part}{rest}"
                        )
                        parsed = urlparse(self.uri)
                except ValueError:
                    # Not a valid port, treat as regular URI - add scheme if needed
                    if not has_valid_scheme:
                        default_scheme = "http"
                        self.uri = f"{default_scheme}://{self.uri}"
                        parsed = urlparse(self.uri)
        elif not has_valid_scheme and not has_netloc:
            # No valid scheme and no netloc - add default scheme
            default_scheme = "http"
            self.uri = f"{default_scheme}://{self.uri}"
            parsed = urlparse(self.uri)

        # Add default port if missing
        if parsed.port is None:
            default_port = self._get_default_port()
            if parsed.scheme and parsed.hostname:
                # Reconstruct URI with port
                port_part = f":{default_port}" if default_port else ""
                path_part = parsed.path or ""
                query_part = f"?{parsed.query}" if parsed.query else ""
                fragment_part = f"#{parsed.fragment}" if parsed.fragment else ""
                self.uri = f"{parsed.scheme}://{parsed.hostname}{port_part}{path_part}{query_part}{fragment_part}"

        return self

    @model_validator(mode="after")
    def _extract_port_from_uri(self):
        """Extract port from URI and set it as gs_port for TigerGraph (if applicable).

        For TigerGraph 4+, gs_port is the primary port. If URI has a port but gs_port
        is not set, automatically extract and set gs_port from URI port.
        This simplifies configuration - users can just provide URI with port.
        """
        # Only apply to configs that have gs_port field (TigerGraph)
        if not hasattr(self, "gs_port"):
            return self

        if self.uri and self.gs_port is None:
            uri_port = self.port  # Get port from URI (property from base class)
            if uri_port:
                try:
                    self.gs_port = int(uri_port)
                    logger.debug(
                        f"Automatically set gs_port={self.gs_port} from URI port"
                    )
                except (ValueError, TypeError):
                    # Port couldn't be converted to int, skip auto-setting
                    pass

        return self

    @model_validator(mode="after")
    def _check_port_conflicts(self):
        """Check for port conflicts between URI and separate port fields.

        If port is provided both in URI and as a separate field, warn and prefer URI port.
        This ensures consistency and avoids confusion.
        """
        if self.uri is None:
            return self

        uri_port = self.port  # Get port from URI
        if uri_port is None:
            return self

        # Check for port fields in subclasses
        # Get model fields to check for port-related fields
        port_fields = []

        # Check for specific port fields that might exist in subclasses
        # Use getattr with None default to avoid AttributeError
        if hasattr(self, "gs_port"):
            gs_port_val = getattr(self, "gs_port", None)
            if gs_port_val is not None:
                port_fields.append(("gs_port", gs_port_val))

        if hasattr(self, "bolt_port"):
            bolt_port_val = getattr(self, "bolt_port", None)
            if bolt_port_val is not None:
                port_fields.append(("bolt_port", bolt_port_val))

        # Check each port field for conflicts
        port_conflicts = []
        for field_name, field_port in port_fields:
            # Compare as strings to handle int vs str differences
            if str(field_port) != str(uri_port):
                port_conflicts.append((field_name, field_port, uri_port))

        # Warn about conflicts and prefer URI port
        if port_conflicts:
            conflict_msgs = [
                f"{field_name}={field_port} (URI has port={uri_port})"
                for field_name, field_port, _ in port_conflicts
            ]
            warning_msg = (
                f"Port conflict detected: Port specified both in URI ({uri_port}) "
                f"and as separate field(s): {', '.join(conflict_msgs)}. "
                f"Using port from URI ({uri_port}). Consider removing the separate port field(s)."
            )
            warnings.warn(warning_msg, UserWarning, stacklevel=2)
            logger.warning(warning_msg)

            # Update port fields to match URI port (prefer URI)
            for field_name, _, _ in port_conflicts:
                try:
                    setattr(self, field_name, int(uri_port))
                except (ValueError, AttributeError):
                    # Field might be read-only or not settable, that's okay
                    pass

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
    """Configuration for TigerGraph connections.

    Authentication (Recommended for TigerGraph 4+):
        Token-based authentication using secrets is the most robust and recommended
        approach for TigerGraph 4+. This provides better security than username/password
        authentication and is the officially recommended method.

        To use token authentication:
        1. Create a secret in TigerGraph: CREATE SECRET mysecret
        2. Provide the secret in this config
        3. The connection will automatically generate and use tokens

        Example:
            >>> config = TigergraphConfig(
            ...     uri="http://localhost:14240",
            ...     username="tigergraph",
            ...     password="tigergraph",
            ...     secret="mysecret",  # Recommended!
            ...     database="my_graph"
            ... )

    Port Configuration for TigerGraph 4+:
        TigerGraph 4.1+ uses port 14240 (GSQL server) as the primary interface.
        Port 9000 (REST++) is for internal use only in TG 4.1+.

        Standard ports:
        - Port 14240: GSQL server (primary interface for all API requests)
        - Port 9000: REST++ (internal-only in TG 4.1+)

        For custom Docker deployments with port mapping, ports are configured via
        environment variables (e.g., TG_WEB, TG_REST) and loaded automatically
        when using TigergraphConfig.from_docker_env().
    """

    model_config = SettingsConfigDict(
        env_prefix="TIGERGRAPH_",
        case_sensitive=False,
    )

    gs_port: int | None = Field(
        default=None, description="TigerGraph GSQL port (standard: 14240 for TG 4+)"
    )
    secret: str | None = Field(
        default=None,
        description="TigerGraph secret for token authentication (RECOMMENDED for TG 4+). "
        "Enables secure token-based authentication instead of basic username/password.",
    )
    version: str | None = Field(
        default=None,
        description="TigerGraph version (e.g., '4.2.1'). If not provided, will be auto-detected. "
        "Versions < 4.2.2 use /restpp prefix in REST API URLs",
    )
    ssl_verify: bool = Field(
        default=True,
        description="Whether to verify SSL certificates. Set to False to disable SSL verification "
        "for cases where certificate hostname doesn't match (e.g., internal deployments with self-signed certs). "
        "WARNING: Disabling SSL verification reduces security and should only be used in trusted environments.",
    )

    def _get_default_port(self) -> int:
        """Get default TigerGraph REST++ port.

        Note: TigerGraph 4.1+ uses port 14240 (GSQL server) as the primary interface.
        Port 9000 (REST++) is for internal use only in TG 4.1+.

        Standard ports:
        - Port 14240: GSQL server (primary interface)
        - Port 9000: REST++ (internal-only in TG 4.1+)

        This method is kept for backward compatibility but should not be relied upon.
        Ports should be explicitly configured in TigergraphConfig.
        """
        return 14240  # Standard GSQL port for TG 4+

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
        """Initialize TigerGraph config.

        Note: For TigerGraph 4+, gs_port is the primary port (14240).
        If URI is provided with a port, it will be automatically set as gs_port
        by the _extract_port_from_uri validator.
        Standard ports:
        - 14240: GSQL server (primary interface)
        - 9000: REST++ (internal-only in TG 4.1+)

        If port is provided both in URI and as gs_port, the port from URI will be used
        and a warning will be issued.
        """
        super().__init__(**data)
        # Port extraction from URI is handled by _extract_port_from_uri validator
        # Port conflicts are handled by _check_port_conflicts validator in base class

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

        # For TigerGraph 4+, use GSQL port (TG_WEB) for both REST++ and GSQL
        # TG_REST (port 9000) is internal-only in TG 4.1+
        gs_port = env_vars.get("TG_WEB") or env_vars.get("TIGERGRAPH_GS_PORT")
        rest_port = env_vars.get("TG_REST") or env_vars.get("TIGERGRAPH_PORT")

        # Prefer GSQL port for TigerGraph 4+ compatibility
        # Standard ports: 14240 (GSQL), 9000 (REST++)
        # Docker may map these to different external ports (e.g., 14241, 9001)
        if gs_port:
            port = gs_port
            config_data["gs_port"] = int(gs_port)
        elif rest_port:
            port = rest_port
            # If only REST port is provided, use it for both (Docker mapping scenario)
            config_data["gs_port"] = int(rest_port)
        else:
            raise ValueError(
                "Either TG_WEB or TG_REST must be set in .env file. "
                "Standard ports: 14240 (GSQL), 9000 (REST++)."
            )

        hostname = env_vars.get("TIGERGRAPH_HOSTNAME", "localhost")
        protocol = env_vars.get("TIGERGRAPH_PROTOCOL", "http")
        config_data["uri"] = f"{protocol}://{hostname}:{port}"

        # Set default username if not provided
        if "TIGERGRAPH_USERNAME" in env_vars:
            config_data["username"] = env_vars["TIGERGRAPH_USERNAME"]
        else:
            config_data["username"] = "tigergraph"  # Default username

        # Set password from env vars or use default
        if "TIGERGRAPH_PASSWORD" in env_vars or "GSQL_PASSWORD" in env_vars:
            config_data["password"] = env_vars.get(
                "TIGERGRAPH_PASSWORD"
            ) or env_vars.get("GSQL_PASSWORD")
        else:
            # Check environment variable as fallback, default to "tigergraph"
            import os

            config_data["password"] = (
                os.environ.get("GSQL_PASSWORD")
                or os.environ.get("TIGERGRAPH_PASSWORD")
                or "tigergraph"
            )
        if "TIGERGRAPH_DATABASE" in env_vars:
            config_data["database"] = env_vars["TIGERGRAPH_DATABASE"]

        return cls(**config_data)


class FalkordbConfig(DBConfig):
    """Configuration for FalkorDB connections.

    FalkorDB is a Redis-based graph database that supports OpenCypher.
    It stores graphs as Redis keys where each graph is a separate namespace.

    FalkorDB structure: connection -> graph (Redis key) -> nodes/relationships
    Unified model: connection -> schema -> entities
    """

    model_config = SettingsConfigDict(
        env_prefix="FALKORDB_",
        case_sensitive=False,
    )

    def _get_default_port(self) -> int:
        """Get default FalkorDB/Redis port."""
        return 6379

    def _get_effective_database(self) -> str | None:
        """FalkorDB doesn't have a database level (connection -> graph -> nodes/relationships)."""
        return None

    def _get_effective_schema(self) -> str | None:
        """For FalkorDB, 'database' field maps to schema (graph name) in unified model.

        FalkorDB structure: connection -> graph (Redis key) -> nodes/relationships
        Unified model: connection -> schema -> entities
        """
        return self.database

    @classmethod
    def from_docker_env(cls, docker_dir: str | Path | None = None) -> "FalkordbConfig":
        """Load FalkorDB config from docker/falkordb/.env file.

        The .env file structure may contain:
        - FALKORDB_HOST: Hostname (defaults to localhost)
        - FALKORDB_PORT: Port number (defaults to 6379)
        - FALKORDB_PASSWORD: Redis AUTH password (optional)
        - FALKORDB_DATABASE: Graph name (optional, can be set later)
        """
        if docker_dir is None:
            docker_dir = (
                Path(__file__).parent.parent.parent.parent / "docker" / "falkordb"
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

        # URI construction (FalkorDB uses redis:// protocol)
        if "FALKORDB_URI" in env_vars:
            config_data["uri"] = env_vars["FALKORDB_URI"]
        else:
            port = env_vars.get("FALKORDB_PORT", "6379")
            hostname = env_vars.get("FALKORDB_HOST", "localhost")
            config_data["uri"] = f"redis://{hostname}:{port}"

        # Password (Redis AUTH)
        if "FALKORDB_PASSWORD" in env_vars and env_vars["FALKORDB_PASSWORD"]:
            config_data["password"] = env_vars["FALKORDB_PASSWORD"]

        # Graph name (database in unified model)
        if "FALKORDB_DATABASE" in env_vars:
            config_data["database"] = env_vars["FALKORDB_DATABASE"]

        return cls(**config_data)


class MemgraphConfig(DBConfig):
    """Configuration for Memgraph connections.

    Memgraph is a high-performance, in-memory graph database that supports
    OpenCypher query language. It uses the Bolt protocol for connections.

    Memgraph structure: connection -> database -> nodes/relationships
    Unified model: connection -> schema -> entities
    """

    model_config = SettingsConfigDict(
        env_prefix="MEMGRAPH_",
        case_sensitive=False,
    )

    def _get_default_port(self) -> int:
        """Get default Memgraph Bolt port."""
        return 7687

    def _get_effective_database(self) -> str | None:
        """Memgraph uses a single database per instance."""
        return self.database

    def _get_effective_schema(self) -> str | None:
        """Memgraph doesn't have a schema level (connection -> database -> nodes/relationships)."""
        return None

    @classmethod
    def from_docker_env(cls, docker_dir: str | Path | None = None) -> "MemgraphConfig":
        """Load Memgraph config from docker/memgraph/.env file.

        The .env file structure may contain:
        - MEMGRAPH_HOST: Hostname (defaults to localhost)
        - MEMGRAPH_PORT: Port number (defaults to 7687)
        - MEMGRAPH_USER: Username (optional)
        - MEMGRAPH_PASSWORD: Password (optional)
        - MEMGRAPH_DATABASE: Database name (optional)
        """
        if docker_dir is None:
            docker_dir = (
                Path(__file__).parent.parent.parent.parent / "docker" / "memgraph"
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

        # URI construction (Memgraph uses bolt:// protocol)
        if "MEMGRAPH_URI" in env_vars:
            config_data["uri"] = env_vars["MEMGRAPH_URI"]
        else:
            port = env_vars.get("MEMGRAPH_PORT", "7687")
            hostname = env_vars.get("MEMGRAPH_HOST", "localhost")
            config_data["uri"] = f"bolt://{hostname}:{port}"

        # Authentication
        if "MEMGRAPH_USER" in env_vars and env_vars["MEMGRAPH_USER"]:
            config_data["username"] = env_vars["MEMGRAPH_USER"]
        if "MEMGRAPH_PASSWORD" in env_vars and env_vars["MEMGRAPH_PASSWORD"]:
            config_data["password"] = env_vars["MEMGRAPH_PASSWORD"]

        # Database name
        if "MEMGRAPH_DATABASE" in env_vars:
            config_data["database"] = env_vars["MEMGRAPH_DATABASE"]

        return cls(**config_data)


class NebulaConfig(DBConfig):
    """Configuration for NebulaGraph connections."""

    model_config = SettingsConfigDict(
        env_prefix="NEBULA_",
        case_sensitive=False,
    )

    def _get_default_port(self) -> int:
        """Get default NebulaGraph GraphD port."""
        return 9669

    def _get_effective_database(self) -> str | None:
        """NebulaGraph doesn't have a database level (connection -> space -> vertices/edges)."""
        return None

    def _get_effective_schema(self) -> str | None:
        """For NebulaGraph, 'schema_name' field maps to schema (space) in unified model.

        NebulaGraph structure: connection -> space -> vertices/edges
        Unified model: connection -> schema -> entities
        """
        return self.schema_name

    @classmethod
    def from_docker_env(cls, docker_dir: str | Path | None = None) -> "NebulaConfig":
        """Load NebulaGraph config from docker/nebula/.env file."""
        if docker_dir is None:
            docker_dir = (
                Path(__file__).parent.parent.parent.parent / "docker" / "nebula"
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
        if "NEBULA_URI" in env_vars:
            config_data["uri"] = env_vars["NEBULA_URI"]
        elif "NEBULA_PORT" in env_vars:
            port = env_vars["NEBULA_PORT"]
            hostname = env_vars.get("NEBULA_ADDRESS", "localhost")
            protocol = env_vars.get("NEBULA_PROTOCOL", "nebula")
            config_data["uri"] = f"{protocol}://{hostname}:{port}"
        elif "NEBULA_ADDRESS" in env_vars:
            # NebulaGraph often uses NEBULA_ADDRESS instead of NEBULA_HOSTNAME
            port = env_vars.get("NEBULA_PORT", "9669")
            hostname = env_vars["NEBULA_ADDRESS"]
            protocol = env_vars.get("NEBULA_PROTOCOL", "nebula")
            config_data["uri"] = f"{protocol}://{hostname}:{port}"

        if "NEBULA_USER" in env_vars or "NEBULA_USERNAME" in env_vars:
            config_data["username"] = env_vars.get("NEBULA_USER") or env_vars.get(
                "NEBULA_USERNAME"
            )
        if "NEBULA_PASSWORD" in env_vars:
            config_data["password"] = env_vars["NEBULA_PASSWORD"]
        if "NEBULA_SPACE" in env_vars or "NEBULA_SCHEMA_NAME" in env_vars:
            config_data["schema_name"] = env_vars.get("NEBULA_SPACE") or env_vars.get(
                "NEBULA_SCHEMA_NAME"
            )

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

    def to_sqlalchemy_connection_string(self) -> str:
        """Convert PostgresConfig to SQLAlchemy connection string.

        Returns:
            SQLAlchemy connection string (e.g., 'postgresql://user:pass@host:port/dbname')
        """
        from urllib.parse import quote_plus

        host = self.hostname or "localhost"
        port = int(self.port) if self.port else 5432
        database = self.database
        if database is None:
            raise ValueError(
                "PostgreSQL database name is required for connection string"
            )
        user = self.username or "postgres"
        password = self.password or ""

        # URL-encode user, password, and database name to handle special characters
        user_encoded = quote_plus(user)
        password_encoded = quote_plus(password) if password else ""
        database_encoded = quote_plus(database)

        # Build connection string
        if password_encoded:
            return f"postgresql://{user_encoded}:{password_encoded}@{host}:{port}/{database_encoded}"
        else:
            return f"postgresql://{user_encoded}@{host}:{port}/{database_encoded}"

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
