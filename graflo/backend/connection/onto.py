import abc
from enum import EnumMeta
from pathlib import Path
from strenum import StrEnum
from typing import Any, Dict, Type
from urllib.parse import urlparse

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class EnumMetaWithContains(EnumMeta):
    """Enhanced EnumMeta that supports 'in' operator checks."""

    def __contains__(cls, item, **kwargs):
        try:
            cls(item, **kwargs)
        except ValueError:
            return False
        return True


class BackendType(StrEnum, metaclass=EnumMetaWithContains):
    """Enum representing different types of database backends."""

    ARANGO = "arango"
    NEO4J = "neo4j"
    TIGERGRAPH = "tigergraph"

    @property
    def config_class(self) -> Type["DBConfig"]:
        """Get the appropriate config class for this backend type."""
        from .config_mapping import BACKEND_TYPE_MAPPING

        return BACKEND_TYPE_MAPPING[self]


class DBConfig(BaseSettings, abc.ABC):
    """Abstract base class for all database connection configurations using Pydantic BaseSettings."""

    uri: str | None = Field(default=None, description="Backend URI")
    username: str | None = Field(default=None, description="Authentication username")
    password: str | None = Field(default=None, description="Authentication Password")
    database: str | None = Field(default=None, description="Database name")
    request_timeout: float = Field(
        default=60.0, description="Request timeout in seconds"
    )

    @abc.abstractmethod
    def _get_default_port(self) -> int:
        """Get the default port for this backend type."""
        pass

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
    def connection_type(self) -> "BackendType":
        """Get backend type from class."""
        # Map class to BackendType - need to import here to avoid circular import
        from .config_mapping import BACKEND_TYPE_MAPPING

        # Reverse lookup: find BackendType for this class
        for backend_type, config_class in BACKEND_TYPE_MAPPING.items():
            if type(self) is config_class:
                return backend_type

        # Fallback (shouldn't happen)
        return BackendType.ARANGO

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
            conn_type = BackendType(db_type)
        except ValueError:
            raise ValueError(
                f"Backend type '{db_type}' not supported. "
                f"Should be one of: {list(BackendType)}"
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
            docker_dir: Path to docker directory. If None, uses default based on backend type.

        Returns:
            DBConfig instance loaded from .env file
        """
        raise NotImplementedError("Subclasses must implement from_docker_env")


class ArangoConfig(DBConfig):
    """Configuration for ArangoDB connections."""

    model_config = SettingsConfigDict(
        env_prefix="ARANGO_",
        case_sensitive=False,
    )

    def _get_default_port(self) -> int:
        """Get default ArangoDB port."""
        return 8529

    @classmethod
    def from_docker_env(cls, docker_dir: str | Path | None = None) -> "ArangoConfig":
        """Load ArangoDB config from docker/arango/.env file."""
        if docker_dir is None:
            docker_dir = (
                Path(__file__).parent.parent.parent.parent / "docker" / "arango"
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
        if "ARANGO_URI" in env_vars:
            config_data["uri"] = env_vars["ARANGO_URI"]
        elif "ARANGO_PORT" in env_vars:
            port = env_vars["ARANGO_PORT"]
            hostname = env_vars.get("ARANGO_HOSTNAME", "localhost")
            protocol = env_vars.get("ARANGO_PROTOCOL", "http")
            config_data["uri"] = f"{protocol}://{hostname}:{port}"

        if "ARANGO_USERNAME" in env_vars:
            config_data["username"] = env_vars["ARANGO_USERNAME"]
        if "ARANGO_PASSWORD" in env_vars:
            config_data["password"] = env_vars["ARANGO_PASSWORD"]
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
