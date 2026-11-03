"""Enumerated connectors: one connector, one request per value of its variables."""

from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest
from pydantic import ValidationError
from rdflib import Graph

from graflo.architecture.contract.bindings import (
    APIConnector,
    Bindings,
    FileConnector,
    SparqlConnector,
)
from graflo.connections.provider import (
    InMemoryConnectionProvider,
)
from graflo.connections.sources import RestApiConnConfig
from graflo.data_source.enumerated import EnumeratedDataSource
from graflo.data_source.rdf import SparqlEndpointDataSource
from graflo.hq.registry_builder import RegistryBuilder
from graflo.hq.sampler import ResourceSampler

OBJECTS = {
    "person": [{"id": "p1", "ts": 3}, {"id": "p2", "ts": 9}],
    "org": [{"id": "o1", "ts": 5}],
    "place": [{"id": "x1", "ts": 1, "cls": "venue"}],
}
GROUPS = [{"id": "g1"}, {"id": "g2"}]


class _Server:
    """Answers GET and POST; records (method, path, query, body) per request."""

    def __init__(self) -> None:
        self.requests: list[tuple[str, str, dict[str, list[str]], Any]] = []
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def _answer(self, method: str) -> None:
                parsed = urlparse(self.path)
                query = parse_qs(parsed.query)
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length).decode() if length else None
                is_json = self.headers.get("Content-Type") == "application/json"
                body = json.loads(raw) if raw and is_json else raw
                owner.requests.append((method, parsed.path, query, body))
                payload = json.dumps({"items": owner.route(parsed.path, query, body)})
                data = payload.encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self) -> None:
                self._answer("GET")

            def do_POST(self) -> None:
                self._answer("POST")

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

    @staticmethod
    def route(path: str, query: dict[str, list[str]], body: Any) -> list[dict]:
        if path == "/groups":
            return GROUPS
        if path == "/objects":
            rows = OBJECTS.get(query["class"][0], [])
            group = query.get("group")
            return [{**row, "seen_in": group[0]} for row in rows] if group else rows
        if path.startswith("/v1/") and path.endswith("/list"):
            return OBJECTS.get(path.split("/")[2], [])
        if path == "/search" and isinstance(body, dict):
            return OBJECTS.get(body["filter"]["class"], [])[: body["filter"]["top"]]
        return []

    def stop(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()


@pytest.fixture
def api() -> Iterator[_Server]:
    server = _Server()
    yield server
    server.stop()


#: The fake server wraps records in ``{"items": [...]}``.
_ITEMS = {"request": {"strategy": "cursor"}, "response": {"records_path": "items"}}


def _source(server: _Server, *connectors: dict[str, Any], name: str = "objects"):
    bindings = Bindings.model_validate(
        {
            "connectors": [{"pagination": _ITEMS, **c} for c in connectors],
            "connector_connection": [
                {"connector": c["name"], "conn_proxy": "api"} for c in connectors
            ],
        }
    )
    provider = InMemoryConnectionProvider()
    provider.register(
        "api",
        RestApiConnConfig(base_url=server.base_url),
    )
    connector = next(c for c in bindings.connectors if c.name == name)
    assert isinstance(connector, APIConnector)
    return RegistryBuilder.api_source(connector, bindings, provider), (
        connector,
        bindings,
        provider,
    )


def _rows(source: Any, **kwargs: Any) -> list[dict]:
    return [row for batch in source.iter_batches(**kwargs) for row in batch]


OBJECTS_CONNECTOR: dict[str, Any] = {
    "name": "objects",
    "path": "/objects",
    "params": {"class": "{cls}"},
    "values": {"cls": ["person", "org", "place"]},
}


class TestContract:
    def test_unset_values_and_body_stay_out_of_the_hash(self) -> None:
        payload = APIConnector(path="/objects")._hash_payload()
        assert "values" not in payload
        assert "body" not in payload

    def test_values_move_the_hash(self) -> None:
        plain = APIConnector.model_validate(
            {**OBJECTS_CONNECTOR, "params": {"class": "person"}, "values": None}
        )
        with_one = APIConnector.model_validate(
            {**OBJECTS_CONNECTOR, "values": {"cls": ["person"]}}
        )
        with_all = APIConnector.model_validate(OBJECTS_CONNECTOR)
        assert len({plain.hash, with_one.hash, with_all.hash}) == 3

    def test_an_undeclared_variable_in_the_path_is_refused(self) -> None:
        with pytest.raises(ValueError, match="cls"):
            APIConnector(path="/v1/{cls}/list")

    def test_an_undeclared_variable_in_a_param_is_refused(self) -> None:
        with pytest.raises(ValueError, match="kind"):
            APIConnector.model_validate(
                {**OBJECTS_CONNECTOR, "params": {"class": "{cls}", "k": "{kind}"}}
            )

    def test_a_variable_no_template_uses_is_refused(self) -> None:
        with pytest.raises(ValueError, match="no template field uses"):
            APIConnector.model_validate(
                {**OBJECTS_CONNECTOR, "values": {"cls": ["a"], "region": ["eu"]}}
            )

    def test_a_variable_both_enumerated_and_a_parent_field_is_refused(self) -> None:
        with pytest.raises(ValueError, match="both in values and as a parent field"):
            APIConnector.model_validate(
                {
                    **OBJECTS_CONNECTOR,
                    "parent": {"connector": "groups", "fields": {"cls": "id"}},
                }
            )

    def test_a_variable_also_set_by_row_annotations_is_refused(self) -> None:
        with pytest.raises(ValueError, match="row_annotations"):
            APIConnector.model_validate(
                {**OBJECTS_CONNECTOR, "row_annotations": {"cls": "person"}}
            )

    @pytest.mark.parametrize(
        "values, message",
        [
            ({}, "declares no variable"),
            ({"cls": []}, "lists no value"),
            ({"cls": ["a", "a"]}, "twice"),
            ({"1cls": ["a"]}, "not a variable name"),
        ],
    )
    def test_malformed_values_are_refused(self, values, message) -> None:
        with pytest.raises(ValueError, match=message):
            APIConnector.model_validate({**OBJECTS_CONNECTOR, "values": values})

    def test_values_of_different_types_are_different_values(self) -> None:
        connector = APIConnector.model_validate(
            {**OBJECTS_CONNECTOR, "values": {"cls": [1, True, "1"]}}
        )
        assert connector.values == {"cls": [1, True, "1"]}

    def test_a_get_request_carries_no_body(self) -> None:
        with pytest.raises(ValueError, match="GET"):
            APIConnector(path="/search", body={"q": 1})

    def test_braces_in_a_body_that_are_not_variables_are_literal(self) -> None:
        connector = APIConnector.model_validate(
            {
                "path": "/graphql",
                "method": "POST",
                "body": '{objects(cls:"{cls}"){id}}',
                "values": {"cls": ["person"]},
            }
        )
        assert connector.rendered({"cls": "person"}).body == (
            '{objects(cls:"person"){id}}'
        )

    def test_values_are_refused_where_nothing_is_templated(self) -> None:
        with pytest.raises(ValidationError):
            FileConnector.model_validate({"regex": "x", "values": {"cls": ["a"]}})

    def test_row_constants_cover_every_combination_in_request_order(self) -> None:
        connector = APIConnector.model_validate(
            {
                "path": "/objects",
                "params": {"class": "{cls}", "region": "{region}"},
                "values": {"cls": ["person", "org"], "region": ["eu", "us"]},
                "row_annotations": {"source": "crm"},
            }
        )
        assert connector.row_constants() == [
            {"source": "crm", "cls": "person", "region": "eu"},
            {"source": "crm", "cls": "person", "region": "us"},
            {"source": "crm", "cls": "org", "region": "eu"},
            {"source": "crm", "cls": "org", "region": "us"},
        ]

    def test_a_rendered_copy_keeps_the_hash_its_connection_is_bound_by(self) -> None:
        connector = APIConnector.model_validate(OBJECTS_CONNECTOR)
        one = connector.rendered({"cls": "org"})
        assert one.hash == connector.hash
        assert one.params == {"class": "org"}
        assert one.values is None


class TestApiRuntime:
    def test_one_request_per_value_in_order(self, api: _Server) -> None:
        source, _ = _source(api, OBJECTS_CONNECTOR)
        assert isinstance(source, EnumeratedDataSource)
        rows = _rows(source)
        assert [row["id"] for row in rows] == ["p1", "p2", "o1", "x1"]
        assert [q["class"] for _, _, q, _ in api.requests] == [
            ["person"],
            ["org"],
            ["place"],
        ]

    def test_rows_carry_the_value_and_a_row_field_wins(self, api: _Server) -> None:
        source, _ = _source(api, OBJECTS_CONNECTOR)
        assert [row["cls"] for row in _rows(source)] == [
            "person",
            "person",
            "org",
            "venue",
        ]

    def test_the_limit_counts_rows_over_every_request(self, api: _Server) -> None:
        source, _ = _source(api, OBJECTS_CONNECTOR)
        assert [row["id"] for row in _rows(source, limit=3)] == ["p1", "p2", "o1"]
        assert len(api.requests) == 2

    def test_a_path_value_is_percent_encoded(self, api: _Server) -> None:
        source, _ = _source(
            api,
            {"name": "objects", "path": "/v1/{cls}/list", "values": {"cls": ["a b"]}},
        )
        _rows(source)
        assert api.requests[0][1] == "/v1/a%20b/list"

    def test_a_json_body_is_filled_at_its_leaves_and_keeps_types(
        self, api: _Server
    ) -> None:
        source, _ = _source(
            api,
            {
                "name": "objects",
                "path": "/search",
                "method": "POST",
                "body": {"filter": {"class": "{cls}", "top": "{top}"}},
                "values": {"cls": ["person", "org"], "top": [1]},
            },
        )
        rows = _rows(source)
        assert [row["id"] for row in rows] == ["p1", "o1"]
        assert [body for _, _, _, body in api.requests] == [
            {"filter": {"class": "person", "top": 1}},
            {"filter": {"class": "org", "top": 1}},
        ]
        assert rows[0]["top"] == 1

    def test_a_string_body_is_sent_as_written_with_its_variables_filled(
        self, api: _Server
    ) -> None:
        source, _ = _source(
            api,
            {
                "name": "objects",
                "path": "/graphql",
                "method": "POST",
                "headers": {"Content-Type": "application/graphql"},
                "body": '{objects(cls:"{cls}"){id}}',
                "values": {"cls": ["person"]},
            },
        )
        _rows(source)
        assert api.requests[0][3] == '{objects(cls:"person"){id}}'

    def test_values_compose_with_a_parent(self, api: _Server) -> None:
        source, _ = _source(
            api,
            {"name": "groups", "path": "/groups"},
            {
                **OBJECTS_CONNECTOR,
                "values": {"cls": ["person", "org"]},
                "params": {"class": "{cls}", "group": "{group}"},
                "parent": {"connector": "groups", "fields": {"group": "id"}},
            },
        )
        rows = _rows(source)
        assert [(row["cls"], row["group"], row["id"]) for row in rows] == [
            ("person", "g1", "p1"),
            ("person", "g1", "p2"),
            ("person", "g2", "p1"),
            ("person", "g2", "p2"),
            ("org", "g1", "o1"),
            ("org", "g2", "o1"),
        ]

    def test_the_high_water_is_the_largest_over_every_request(
        self, api: _Server
    ) -> None:
        source, _ = _source(
            api, {**OBJECTS_CONNECTOR, "watermark": {"field": "ts", "param": "since"}}
        )
        _rows(source)
        assert source.high_water == 9

    def test_the_high_water_counts_only_requests_made_this_iteration(
        self, api: _Server
    ) -> None:
        source, _ = _source(
            api, {**OBJECTS_CONNECTOR, "watermark": {"field": "ts", "param": "since"}}
        )
        _rows(source)
        _rows(source, limit=1)
        assert source.high_water == 3

    def test_an_enumerated_connector_is_sampled_over_its_values(
        self, api: _Server
    ) -> None:
        _, (connector, bindings, provider) = _source(api, OBJECTS_CONNECTOR)
        sample = ResourceSampler(max_docs=3).sample_connector(
            connector,
            resource_name="objects",
            connection_provider=provider,
            bindings=bindings,
        )
        assert [doc["cls"] for doc in sample.docs] == ["person", "person", "org"]

    def test_sampling_resolves_the_connection_through_the_bindings(
        self, api: _Server
    ) -> None:
        _, (connector, bindings, provider) = _source(api, OBJECTS_CONNECTOR)
        sample = ResourceSampler(max_docs=10).sample_connector(
            connector,
            resource_name="objects",
            connection_provider=provider,
            bindings=bindings,
        )
        assert len(sample.docs) == 4

    def test_sampling_without_bindings_says_so(self, api: _Server) -> None:
        _, (connector, _, provider) = _source(api, OBJECTS_CONNECTOR)
        with pytest.raises(ValueError, match="the bindings that name it"):
            ResourceSampler(max_docs=10).sample_connector(
                connector, resource_name="objects", connection_provider=provider
            )


EX = "http://example.org/"
_TURTLE = """
@prefix ex: <http://example.org/> .
ex:alice a ex:Person ; ex:name "Alice" .
ex:acme a ex:Org ; ex:name "Acme" .
ex:paris a ex:Place ; ex:name "Paris" .
"""


class _Endpoint:
    def __init__(self, graph: Graph) -> None:
        self.graph = graph
        self.queries: list[str] = []

    def setQuery(self, query: str) -> None:
        self.queries.append(query)

    def queryAndConvert(self) -> dict[str, Any]:
        result = self.graph.query(self.queries[-1])
        return json.loads(result.serialize(format="json"))


@pytest.fixture
def endpoint(monkeypatch: pytest.MonkeyPatch) -> _Endpoint:
    graph = Graph()
    graph.parse(data=_TURTLE, format="turtle")
    fake = _Endpoint(graph)
    monkeypatch.setattr(SparqlEndpointDataSource, "_create_wrapper", lambda self: fake)
    return fake


def _sparql_rows(connector: SparqlConnector) -> list[dict]:
    source = RegistryBuilder.sparql_source(
        connector,
        Bindings(connectors=[connector]),
        resource_name="things",
        page_size=100,
        connection_provider=InMemoryConnectionProvider(),
    )
    return _rows(source)


class TestSparql:
    def test_one_class_per_value(self, endpoint: _Endpoint) -> None:
        connector = SparqlConnector.model_validate(
            {
                "rdf_class": EX + "{cls}",
                "endpoint_url": "http://sparql.invalid/ds",
                "values": {"cls": ["Person", "Org"]},
            }
        )
        rows = _sparql_rows(connector)
        assert [(row["cls"], row["name"]) for row in rows] == [
            ("Person", "Alice"),
            ("Org", "Acme"),
        ]

    def test_a_query_text_is_filled_and_keeps_its_braces(
        self, endpoint: _Endpoint
    ) -> None:
        query = "SELECT ?s ?p ?o WHERE { ?s a <http://example.org/{cls}> . ?s ?p ?o . }"
        connector = SparqlConnector.model_validate(
            {
                "rdf_class": EX + "Thing",
                "endpoint_url": "http://sparql.invalid/ds",
                "sparql_query": query,
                "values": {"cls": ["Place"]},
            }
        )
        rows = _sparql_rows(connector)
        assert [row["name"] for row in rows] == ["Paris"]
        (sent,) = [q for q in endpoint.queries if "example.org/" in q]
        assert "<http://example.org/Place>" in sent
        assert "WHERE {" in sent

    def test_an_undeclared_variable_in_the_class_is_refused(self) -> None:
        with pytest.raises(ValueError, match="cls"):
            SparqlConnector(rdf_class=EX + "{cls}", endpoint_url="http://x")


def _routed_manifest(base_url: str):
    from graflo.architecture.contract.manifest import GraphManifest

    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "objects"},
                "core_schema": {
                    "vertex_config": {
                        "vertices": [
                            {"name": c, "properties": ["id"], "identity": ["id"]}
                            for c in ("person", "org", "venue")
                        ]
                    },
                    "edge_config": {
                        "edges": [
                            {"source": "person", "target": "org", "relation": "in"},
                            {"source": "org", "target": "venue", "relation": "at"},
                        ]
                    },
                },
            },
            "ingestion_model": {
                "resources": [
                    {
                        "name": "objects",
                        "pipeline": [
                            {
                                "vertex_router": {
                                    "type_field": "cls",
                                    "from": {"id": "id"},
                                }
                            }
                        ],
                    }
                ]
            },
            "bindings": {
                "connectors": [
                    {
                        **OBJECTS_CONNECTOR,
                        "pagination": _ITEMS,
                        "resource_name": "objects",
                    }
                ],
                "connector_connection": [{"connector": "objects", "conn_proxy": "api"}],
            },
        }
    )
    manifest.finish_init()
    provider = InMemoryConnectionProvider()
    provider.register(
        "api",
        RestApiConnConfig(base_url=base_url),
    )
    return manifest, provider


def test_an_enumerated_connector_feeds_one_router_end_to_end(api: _Server, tmp_path):
    from graflo.connections.graflo_backend import GraFloBackendConfig
    from graflo.db.manager import ConnectionManager
    from graflo.hq.graph_engine import GraphEngine
    from graflo.hq.ingestion_parameters import IngestionParams
    from graflo.onto import DBType

    manifest, provider = _routed_manifest(api.base_url)
    params = IngestionParams()
    registry = RegistryBuilder(
        manifest.require_schema(), manifest.require_ingestion_model()
    ).build(manifest.require_bindings(), params, provider, strict=True)
    backend = GraFloBackendConfig(output_dir=tmp_path)
    engine = GraphEngine(target_db_flavor=DBType.GRAFLO_BACKEND)
    engine.define_schema(manifest=manifest, target_db_config=backend)
    engine.ingest(
        manifest=manifest,
        target_db_config=backend,
        ingestion_params=params,
        data_source_registry=registry,
    )

    with ConnectionManager(connection_config=backend) as conn:
        landed = {
            name: sorted(doc["id"] for doc in conn.fetch_all_docs(name))
            for name in ("person", "org", "venue")
        }
    # `place` names no class; the record that says `venue` itself is routed there.
    assert landed == {"person": ["p1", "p2"], "org": ["o1"], "venue": ["x1"]}


def test_a_projected_manifest_asks_only_for_what_it_may_still_route(
    api: _Server,
) -> None:
    from graflo.architecture.evolution import ProjectManifestOp, apply_evolution
    from graflo.hq.ingestion_parameters import IngestionParams

    manifest, provider = _routed_manifest(api.base_url)
    projected = apply_evolution(
        manifest,
        [ProjectManifestOp(keep_vertices=["person", "org"])],
        bump_version=False,
    )
    registry = RegistryBuilder(
        projected.require_schema(), projected.require_ingestion_model()
    ).build(projected.require_bindings(), IngestionParams(), provider, strict=True)

    rows = [
        row for source in registry.get_data_sources("objects") for row in _rows(source)
    ]
    # `place` named no class before the projection, so it is not the projection's
    # to drop; its one record says `venue`, a class the projection removed.
    assert [q["class"] for _, _, q, _ in api.requests] == [
        ["person"],
        ["org"],
        ["place"],
    ]
    assert [row["id"] for row in rows] == ["p1", "p2", "o1", "x1"]
