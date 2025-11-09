import os
import re
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from suthing import FileHandle

from .onto import ConnectionConfig, ConnectionKind


def is_filepath(input_string) -> bool:
    """
    Determine if a string is a URL or a Linux filepath with extension.

    Args:
        input_string (str): The string to check

    Returns:
        str: "url", "filepath", or "unknown"
    """
    # Check if it's a linux filepath with extension
    if "." in os.path.basename(input_string):
        file_ext = os.path.splitext(input_string)[1]
        if file_ext and len(file_ext) > 1:
            return True

    url_pattern = re.compile(r"^(?:http|https|ftp|sftp|file)://", re.IGNORECASE)
    if url_pattern.match(input_string):
        return False

    # Check for URL with hostname but missing scheme (www.example.com)
    if re.match(r"^www\.", input_string) or (
        "." in input_string and "/" in input_string and not input_string.startswith("/")
    ):
        return False

    return False


class ConfigFactory:
    """Factory for creating connection configurations from various sources."""

    @staticmethod
    def _guess_connection_kind_from_port(port: int) -> Optional[ConnectionKind]:
        """Guess the connection kind based on port number.

        Args:
            port: Port number to check

        Returns:
            Guessed ConnectionKind or None if no match
        """
        if 8529 <= port <= 8559:
            return ConnectionKind.ARANGO
        elif 7400 <= port <= 7699:
            return ConnectionKind.NEO4J
        elif 9000 <= port <= 9100:
            return ConnectionKind.TIGERGRAPH
        return None

    @staticmethod
    def _extract_port_from_config(config: Dict[str, Any]) -> Optional[int]:
        """Extract port number from configuration dictionary.

        Args:
            config: Configuration dictionary

        Returns:
            Port number if found, None otherwise
        """
        # Check direct port field
        if "port" in config:
            try:
                return int(config["port"])
            except (ValueError, TypeError):
                pass

        # Check URL if present
        if "url" in config:
            parsed_url = urlparse(config["url"])
            if parsed_url.port:
                return parsed_url.port
            elif parsed_url.scheme == "http":
                return 80
            elif parsed_url.scheme == "https":
                return 443

        return None

    @classmethod
    def create_config(
        cls,
        *args,
        **kwargs,
    ) -> ConnectionConfig:
        """
        Create a connection configuration from a file path, dictionary, or URL.

        Args:
            path: Path to a JSON or YAML configuration file
            dict_like: Dictionary containing configuration parameters
            url: URL string to create a connection directly

        Returns:
            A properly typed connection configuration object

        Raises:
            ValueError: If no valid input source is provided or if connection type is invalid
        """
        # Handle URL-based configuration
        url: Optional[str] = kwargs.get("url", None)
        path: Optional[str | Path] = kwargs.get("path", None)
        dict_like: Optional[Dict[str, Any]] = kwargs.get("dict_like", None)
        if len(args) > 0:
            arg = args[0]
            if isinstance(arg, dict):
                dict_like = arg
            elif isinstance(arg, Path):
                path = arg
            elif isinstance(arg, str):
                if is_filepath(arg):
                    path = arg
                else:
                    url = arg

        if url:
            # Parse URL to get port
            parsed_url = urlparse(url)
            port = None
            if parsed_url.port:
                port = parsed_url.port
            elif parsed_url.scheme == "http":
                port = 80
            elif parsed_url.scheme == "https":
                port = 443

            # Start with an empty config and add the URL
            config = {"url": url}

        # Handle file-based configuration
        elif path is not None:
            config = FileHandle.load(path)
        # Handle dictionary-based configuration
        elif dict_like is not None and isinstance(dict_like, dict):
            config = deepcopy(dict_like)
        else:
            raise ValueError(
                "At least one of the arguments must be provided: path, dict_like, or url"
            )

        # Try to guess connection kind from port in config if not already set
        if "connection_type" not in config:
            port = cls._extract_port_from_config(config)
            if port:
                guessed_kind = cls._guess_connection_kind_from_port(port)
                if guessed_kind:
                    config["connection_type"] = guessed_kind.value

        return ConnectionConfig.from_dict(config)
