"""Test fixtures for data source tests."""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest


class MockAPIHandler(BaseHTTPRequestHandler):
    """Mock API server handler for testing."""

    def do_GET(self):
        """Handle GET requests."""
        parsed_path = urlparse(self.path)
        query_params = parse_qs(parsed_path.query)

        # Default response data
        data = [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
            {"id": 3, "name": "Charlie", "age": 35},
        ]

        # Handle pagination
        offset = int(query_params.get("offset", [0])[0])
        limit = int(query_params.get("limit", [100])[0])

        # Slice data based on pagination
        paginated_data = data[offset : offset + limit]
        has_more = offset + limit < len(data)

        response = {
            "data": paginated_data,
            "has_more": has_more,
            "offset": offset,
            "limit": limit,
            "total": len(data),
        }

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(response).encode("utf-8"))

    def log_message(self, format: str, *args: Any) -> None:
        """Suppress log messages."""
        pass


class MockAPIServer:
    """Mock API server for testing."""

    def __init__(self, port: int = 0):
        """Initialize the mock server.

        Args:
            port: Port to bind to (0 for auto-assign)
        """
        self.port = port
        self.server: HTTPServer | None = None
        self.thread: threading.Thread | None = None

    def start(self) -> int:
        """Start the mock server.

        Returns:
            The port the server is running on
        """
        self.server = HTTPServer(("localhost", self.port), MockAPIHandler)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.daemon = True
        self.thread.start()
        return self.port

    def stop(self):
        """Stop the mock server."""
        if self.server:
            self.server.shutdown()
            self.server.server_close()
            if self.thread:
                self.thread.join(timeout=1)


@pytest.fixture(scope="function")
def mock_api_server():
    """Fixture providing a mock API server."""
    server = MockAPIServer()
    port = server.start()
    yield server, port
    server.stop()
