from typing import Dict

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class WSGIConfig(BaseSettings):
    """Configuration for WSGI connections.

    Note: WSGI is not a database db, so it doesn't inherit from DBConfig.
    This is kept separate from database db configurations.
    """

    model_config = SettingsConfigDict(
        env_prefix="WSGI_",
        case_sensitive=False,
    )

    uri: str | None = Field(default=None, description="WSGI URI")
    path: str = Field(default="/", description="WSGI path")
    paths: Dict[str, str] = Field(
        default_factory=dict, description="WSGI paths mapping"
    )
    listen_addr: str = Field(default="0.0.0.0", description="Listen address")

    @classmethod
    def from_docker_env(cls, docker_dir: str | None = None) -> "WSGIConfig":
        """WSGI config doesn't typically use docker env files."""
        raise NotImplementedError("WSGI config doesn't support from_docker_env")
