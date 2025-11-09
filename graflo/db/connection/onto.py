import dataclasses
from enum import EnumMeta
from strenum import StrEnum
from typing import Any, Dict, Optional, Type
from urllib.parse import urlparse

from dataclass_wizard import JSONWizard


class EnumMetaWithContains(EnumMeta):
    """Enhanced EnumMeta that supports 'in' operator checks."""

    def __contains__(cls, item, **kwargs):
        try:
            cls(item, **kwargs)
        except ValueError:
            return False
        return True


class ConnectionKind(StrEnum, metaclass=EnumMetaWithContains):
    """Enum representing different types of connections."""

    ARANGO = "arango"
    NEO4J = "neo4j"
    WSGI = "wsgi"
    TIGERGRAPH = "tigergraph"

    @property
    def config_class(self) -> Type["ConnectionConfig"]:
        """Get the appropriate config class for this connection type."""
        from .config_mapping import CONNECTION_TYPE_MAPPING

        return CONNECTION_TYPE_MAPPING[self]


@dataclasses.dataclass
class ConnectionConfig(JSONWizard, JSONWizard.Meta):
    """Base class for all connection configurations."""

    connection_type: Optional[ConnectionKind] = None
    comment: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ConnectionConfig":
        """Create a connection config from a dictionary."""
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")

        db_type = data.pop("db_type", None) or data.pop("connection_type", None)
        if not db_type:
            raise ValueError("Missing 'db_type' or 'connection_type' in configuration")

        try:
            conn_type = ConnectionKind(db_type)
        except ValueError:
            raise ValueError(
                f"Connection type '{db_type}' not supported. "
                f"Should be one of: {list(ConnectionKind)}"
            )

        # Copy the data to avoid modifying the original
        config_data = data.copy()
        # Get the appropriate config class and initialize it
        config_class = conn_type.config_class
        return config_class(**config_data)


@dataclasses.dataclass
class ProtoConnectionConfig(ConnectionConfig):
    """Configuration for protocol-based connections."""

    url: Optional[str] = None
    hosts: Optional[str] = None
    hostname: Optional[str] = None
    port: Optional[str] = None
    protocol: str = "http"
    request_timeout: float = 60

    def __post_init__(self):
        """Process the connection information after initialization."""
        # Process URL if provided, otherwise build from components
        if self.hosts and not self.url:
            self.url = self.hosts
        if self.url:
            self._parse_url()
        elif self.hostname is not None:
            self.url = f"{self.protocol}://{self.hostname}"
        if self.port is not None:
            self.url = f"{self.url}:{self.port}"

    @property
    def url_without_port(self):
        return f"{self.protocol}://{self.hostname}"

    def _parse_url(self):
        """Parse the URL into components."""
        if not self.url:
            return

        parsed = urlparse(self.url)

        # Extract protocol
        if parsed.scheme:
            self.protocol = parsed.scheme

        # Extract port
        if parsed.port:
            self.port = str(parsed.port)

        return parsed


@dataclasses.dataclass
class DBConnectionConfig(ProtoConnectionConfig):
    """Configuration for database connections."""

    username: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None

    # Backward compatibility
    cred_name: Optional[str] = None
    cred_pass: Optional[str] = None

    def __post_init__(self):
        """Process after initialization and handle credential aliases."""
        super().__post_init__()

        # Handle credential aliases for backward compatibility
        if not self.username and self.cred_name:
            self.username = self.cred_name

        if not self.password and self.cred_pass:
            self.password = self.cred_pass


@dataclasses.dataclass
class WSGIConfig(ProtoConnectionConfig):
    """Configuration for WSGI connections."""

    path: str = "/"
    paths: Dict[str, str] = dataclasses.field(default_factory=dict)
    listen_addr: str = "0.0.0.0"

    def __post_init__(self):
        """Process after initialization."""
        self.connection_type = ConnectionKind.WSGI

        # Handle path from URL if available
        if self.url is None:
            super().__post_init__()
            # After super().__post_init__(), self.url might still be None
            # Initialize it to empty string if still None, then append path
            if self.url is None:
                self.url = ""
            self.url += f"{self.path}"
        else:
            parsed = urlparse(self.url)
            if parsed.path:
                self.path = parsed.path

    def __parse_path(self, h):
        self.path = "/" + "/".join(h[1:])


@dataclasses.dataclass
class ArangoConnectionConfig(DBConnectionConfig):
    """Configuration for ArangoDB connections."""

    def __post_init__(self):
        """Set connection type and process parent class initialization."""
        self.connection_type = ConnectionKind.ARANGO
        super().__post_init__()


@dataclasses.dataclass
class Neo4jConnectionConfig(DBConnectionConfig):
    """Configuration for Neo4j connections."""

    def __post_init__(self):
        """Set connection type and process parent class initialization."""
        self.connection_type = ConnectionKind.NEO4J
        super().__post_init__()


@dataclasses.dataclass
class TigergraphConnectionConfig(DBConnectionConfig):
    """Configuration for TigerGraph connections."""

    gs_port: int | None = None

    def __post_init__(self):
        """Set connection type and process parent class initialization."""
        self.connection_type = ConnectionKind.TIGERGRAPH
        super().__post_init__()
