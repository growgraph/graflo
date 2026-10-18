"""Dependent requests: one child request per record of a parent API connector."""

from __future__ import annotations

import json
import threading
from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest

from graflo.architecture.contract.bindings import (
    APIConnector,
    Bindings,
    FileConnector,
)
from graflo.connections.provider import (
    ApiGeneralizedConnConfig,
    InMemoryConnectionProvider,
)
from graflo.connections.sources import RestApiConnConfig
from graflo.hq.registry_builder import RegistryBuilder
from graflo.hq.sampler import ResourceSampler

#: path, query -> JSON body
Route = Callable[[str, dict[str, list[str]]], Any]

CHANNELS = [{"id": "C1", "name": "support"}, {"id": "C2", "name": "ops"}]
HISTORY = {
    "C1": [
        {"ts": "1.0", "text": "pump down", "thread_ts": "1.0"},
        {"ts": "2.0", "text": "no thread"},
    ],
    "C2": [{"ts": "3.0", "text": "deploy done"}],
}
REPLIES = {("C1", "1.0"): [{"ts": "1.0"}, {"ts": "1.5", "text": "on it"}]}


def slack_route(path: str, query: dict[str, list[str]]) -> Any:
    if path == "/conversations.list":
        return {"ok": True, "channels": CHANNELS}
    if path == "/conversations.history":
        channel = query["channel"][0]
        return {"ok": True, "messages": HISTORY[channel]}
    if path == "/conversations.replies":
        key = (query["channel"][0], query["ts"][0])
        return {"ok": True, "messages": REPLIES[key]}
    if path.startswith("/channels/") and path.endswith("/pins"):
        channel = path.split("/")[2]
        return {"ok": True, "items": [{"pin": f"{channel}-p"}]}
    return {"ok": False, "error": "unknown_method"}


class _Server:
    def __init__(self, route: Route) -> None:
        self.paths: list[str] = []
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                parsed = urlparse(self.path)
                owner.paths.append(parsed.path)
                payload = json.dumps(route(parsed.path, parse_qs(parsed.query)))
                body = payload.encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: Any) -> None:
                return

        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.base_url = f"http://127.0.0.1:{self._httpd.server_address[1]}"
        self._thread = threading.Thread(
            target=self._httpd.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()


@pytest.fixture
def slack() -> Iterator[_Server]:
    server = _Server(slack_route)
    yield server
    server.stop()


def _response(records: str) -> dict[str, Any]:
    return {
        "request": {"strategy": "cursor"},
        "response": {"records_path": records, "ok_path": "ok", "error_path": "error"},
    }


CHANNELS_CONNECTOR = {
    "name": "channels",
    "path": "/conversations.list",
    "pagination": _response("channels"),
}
HISTORY_CONNECTOR = {
    "name": "history",
    "path": "/conversations.history",
    "params": {"channel": "{channel}"},
    "parent": {"connector": "channels", "fields": {"channel": "id"}},
    "pagination": _response("messages"),
}
REPLIES_CONNECTOR = {
    "name": "replies",
    "path": "/conversations.replies",
    "params": {"channel": "{channel}", "ts": "{thread}"},
    "parent": {
        "connector": "history",
        "fields": {"channel": "channel", "thread": "thread_ts"},
    },
    "pagination": _response("messages"),
}


def _bindings(*connectors: dict[str, Any]) -> Bindings:
    return Bindings.model_validate(
        {
            "connectors": list(connectors),
            "connector_connection": [
                {"connector": "channels", "conn_proxy": "slack"},
            ],
        }
    )


def _provider(base_url: str, bindings: Bindings) -> InMemoryConnectionProvider:
    provider = InMemoryConnectionProvider()
    provider.register_generalized_config(
        conn_proxy="slack",
        config=ApiGeneralizedConnConfig(config=RestApiConnConfig(base_url=base_url)),
    )
    provider.bind_from_bindings(bindings=bindings)
    return provider


def _connector(bindings: Bindings, name: str) -> APIConnector:
    found = next(c for c in bindings.connectors if c.name == name)
    assert isinstance(found, APIConnector)
    return found


def _rows(source: Any) -> list[dict]:
    return [row for batch in source.iter_batches(batch_size=10) for row in batch]


class TestContract:
    def test_a_template_variable_must_be_declared(self) -> None:
        with pytest.raises(ValueError, match="thread"):
            APIConnector.model_validate(
                {
                    "path": "/conversations.replies",
                    "params": {"ts": "{thread}"},
                    "parent": {"connector": "history", "fields": {"channel": "id"}},
                }
            )

    def test_a_parent_needs_at_least_one_field(self) -> None:
        with pytest.raises(ValueError):
            APIConnector.model_validate(
                {"path": "/x", "parent": {"connector": "channels", "fields": {}}}
            )

    def test_an_unset_parent_stays_out_of_the_hash(self) -> None:
        connector = APIConnector(path="/conversations.list")
        assert "parent" not in connector._hash_payload()

    def test_a_parent_moves_the_hash(self) -> None:
        plain = APIConnector(path="/conversations.history")
        child = APIConnector.model_validate(
            {
                "path": "/conversations.history",
                "parent": {"connector": "channels", "fields": {"channel": "id"}},
            }
        )
        assert plain.hash != child.hash


class TestBindings:
    def test_an_unknown_parent_is_refused(self) -> None:
        with pytest.raises(ValueError, match="unknown parent"):
            Bindings.model_validate({"connectors": [HISTORY_CONNECTOR]})

    def test_a_parent_that_is_not_an_api_connector_is_refused(self) -> None:
        files = FileConnector(name="channels", regex=r"\.json$", sub_path=".")
        with pytest.raises(ValueError, match="not an API connector"):
            Bindings.model_validate(
                {"connectors": [files.model_dump(), HISTORY_CONNECTOR]}
            )

    def test_a_cycle_is_refused(self) -> None:
        a = {
            "name": "a",
            "path": "/a",
            "params": {"x": "{x}"},
            "parent": {"connector": "b", "fields": {"x": "id"}},
        }
        b = {
            "name": "b",
            "path": "/b",
            "params": {"x": "{x}"},
            "parent": {"connector": "a", "fields": {"x": "id"}},
        }
        with pytest.raises(ValueError, match="cycle"):
            Bindings.model_validate({"connectors": [a, b]})

    def test_the_parent_is_resolved_by_name(self) -> None:
        bindings = _bindings(CHANNELS_CONNECTOR, HISTORY_CONNECTOR)
        parent = bindings.get_parent_connector(_connector(bindings, "history"))
        assert parent is not None and parent.name == "channels"


class TestRuntime:
    def test_one_child_request_per_parent_record(self, slack: _Server) -> None:
        bindings = _bindings(CHANNELS_CONNECTOR, HISTORY_CONNECTOR)
        source = RegistryBuilder.api_source(
            _connector(bindings, "history"),
            bindings,
            _provider(slack.base_url, bindings),
        )
        rows = _rows(source)
        assert [row["ts"] for row in rows] == ["1.0", "2.0", "3.0"]
        assert slack.paths.count("/conversations.history") == 2

    def test_child_rows_carry_the_variables_they_were_fetched_with(
        self, slack: _Server
    ) -> None:
        bindings = _bindings(CHANNELS_CONNECTOR, HISTORY_CONNECTOR)
        source = RegistryBuilder.api_source(
            _connector(bindings, "history"),
            bindings,
            _provider(slack.base_url, bindings),
        )
        assert [row["channel"] for row in _rows(source)] == ["C1", "C1", "C2"]

    def test_a_parent_record_without_a_field_is_skipped(self, slack: _Server) -> None:
        # Only the message with a thread_ts gets a replies request.
        bindings = _bindings(CHANNELS_CONNECTOR, HISTORY_CONNECTOR, REPLIES_CONNECTOR)
        source = RegistryBuilder.api_source(
            _connector(bindings, "replies"),
            bindings,
            _provider(slack.base_url, bindings),
        )
        rows = _rows(source)
        assert [row["ts"] for row in rows] == ["1.0", "1.5"]
        assert slack.paths.count("/conversations.replies") == 1
        assert {row["thread"] for row in rows} == {"1.0"}

    def test_the_path_is_templated_too(self, slack: _Server) -> None:
        pins = {
            "name": "pins",
            "path": "/channels/{channel}/pins",
            "parent": {"connector": "channels", "fields": {"channel": "id"}},
            "pagination": _response("items"),
        }
        bindings = _bindings(CHANNELS_CONNECTOR, pins)
        source = RegistryBuilder.api_source(
            _connector(bindings, "pins"),
            bindings,
            _provider(slack.base_url, bindings),
        )
        assert [row["pin"] for row in _rows(source)] == ["C1-p", "C2-p"]

    def test_the_limit_stops_parent_iteration(self, slack: _Server) -> None:
        bindings = _bindings(CHANNELS_CONNECTOR, HISTORY_CONNECTOR)
        source = RegistryBuilder.api_source(
            _connector(bindings, "history"),
            bindings,
            _provider(slack.base_url, bindings),
        )
        rows = [row for batch in source.iter_batches(limit=2) for row in batch]
        assert len(rows) == 2
        assert slack.paths.count("/conversations.history") == 1

    def test_a_child_with_its_own_connection_uses_it(self, slack: _Server) -> None:
        other = _Server(slack_route)
        try:
            bindings = Bindings.model_validate(
                {
                    "connectors": [CHANNELS_CONNECTOR, HISTORY_CONNECTOR],
                    "connector_connection": [
                        {"connector": "channels", "conn_proxy": "slack"},
                        {"connector": "history", "conn_proxy": "mirror"},
                    ],
                }
            )
            provider = _provider(slack.base_url, bindings)
            provider.register_generalized_config(
                conn_proxy="mirror",
                config=ApiGeneralizedConnConfig(
                    config=RestApiConnConfig(base_url=other.base_url)
                ),
            )
            source = RegistryBuilder.api_source(
                _connector(bindings, "history"), bindings, provider
            )
            _rows(source)
        finally:
            other.stop()
        assert "/conversations.history" in other.paths
        assert "/conversations.history" not in slack.paths


def test_a_child_connector_is_sampled_through_its_parent(slack: _Server) -> None:
    bindings = _bindings(CHANNELS_CONNECTOR, HISTORY_CONNECTOR)
    sample = ResourceSampler(max_docs=2).sample_connector(
        _connector(bindings, "history"),
        resource_name="messages",
        connection_provider=_provider(slack.base_url, bindings),
        bindings=bindings,
    )
    assert [doc["ts"] for doc in sample.docs] == ["1.0", "2.0"]
    assert sample.truncated is True
