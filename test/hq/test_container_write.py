"""Writing a graph container by identity upsert, without an ingestion resource."""

import asyncio
import copy
from typing import Any

import pytest

from graflo.architecture.graph_types.container import GraphContainer
from graflo.architecture.graph_types.pending import EdgeEndpoint, PendingEdge
from graflo.architecture.schema.document import Schema
from graflo.connections.graflo_backend import GraFloBackendConfig
from graflo.db.manager import ConnectionManager
from graflo.hq.container_write import write_container

SCHEMA = {
    "metadata": {"name": "plant", "version": "1.0.0"},
    "core_schema": {
        "vertex_config": {
            "vertices": [
                {
                    "name": "Machine",
                    "properties": [{"name": "serial", "type": "STRING"}],
                    "identity": ["serial"],
                },
                {
                    "name": "Person",
                    "properties": [
                        {"name": "email", "type": "STRING"},
                        {"name": "phone", "type": "STRING"},
                    ],
                    "identity": ["email"],
                    "secondary_identities": [{"name": "by_phone", "fields": ["phone"]}],
                },
            ]
        },
        "edge_config": {
            "edges": [{"source": "Person", "target": "Machine", "relation": "OPERATES"}]
        },
    },
}
OPERATES = ("Person", "Machine", "OPERATES")


def _schema() -> Schema:
    return Schema.model_validate(SCHEMA)


def _container() -> GraphContainer:
    return GraphContainer(
        vertices={
            "Person": [{"email": "ann@x.com", "phone": "+1 555"}],
            "Machine": [{"serial": "S-7"}, {"serial": "S-9"}],
        },
        edges={OPERATES: [({"email": "ann@x.com"}, {"serial": "S-7"}, {})]},
    )


def test_a_container_is_defined_and_written(tmp_path):
    conf = GraFloBackendConfig(output_dir=tmp_path)
    schema, container = _schema(), _container()
    before = (schema.to_dict(), container.vertices, container.edges)

    report = asyncio.run(write_container(schema, container, conf))

    assert report.schema_defined
    assert report.vertices == {"Person": 1, "Machine": 2}
    assert report.edges == {"Person-OPERATES-Machine": 1}
    assert (schema.to_dict(), container.vertices, container.edges) == before
    with ConnectionManager(connection_config=conf) as conn:
        assert {d["serial"] for d in conn.fetch_all_docs("Machine")} == {"S-7", "S-9"}
        assert len(conn.fetch_all_edges("Person", "Machine", "OPERATES")) == 1


def test_a_second_write_reuses_the_definition(tmp_path):
    conf = GraFloBackendConfig(output_dir=tmp_path)
    asyncio.run(write_container(_schema(), _container(), conf))
    report = asyncio.run(write_container(_schema(), _container(), conf))
    assert not report.schema_defined


def test_a_pending_edge_is_looked_up_by_its_secondary_identity(tmp_path):
    conf = GraFloBackendConfig(output_dir=tmp_path)
    asyncio.run(write_container(_schema(), _container(), conf))
    pending = [
        PendingEdge(
            edge_id=OPERATES,
            source=EdgeEndpoint(vertex="Person", match={"phone": "+1 555"}),
            target=EdgeEndpoint(vertex="Machine", identity={"serial": "S-9"}),
        ),
        PendingEdge(
            edge_id=OPERATES,
            source=EdgeEndpoint(vertex="Person", match={"phone": "+1 999"}),
            target=EdgeEndpoint(vertex="Machine", identity={"serial": "S-9"}),
        ),
    ]
    report = asyncio.run(
        write_container(_schema(), GraphContainer(), conf, pending=pending)
    )
    assert (report.lookups_resolved, report.lookups_unresolved) == (1, 1)
    with ConnectionManager(connection_config=conf) as conn:
        edges = conn.fetch_all_edges("Person", "Machine", "OPERATES")
    assert len(edges) == 2


def test_a_pending_edge_finds_a_vertex_of_the_same_container(tmp_path):
    conf = GraFloBackendConfig(output_dir=tmp_path)
    pending = [
        PendingEdge(
            edge_id=OPERATES,
            source=EdgeEndpoint(vertex="Person", match={"phone": "+1 555"}),
            target=EdgeEndpoint(vertex="Machine", identity={"serial": "S-9"}),
        )
    ]
    report = asyncio.run(
        write_container(_schema(), _container(), conf, pending=pending)
    )
    assert (report.lookups_resolved, report.lookups_unresolved) == (1, 0)
    assert report.edges == {"Person-OPERATES-Machine": 2}


def test_a_guard_on_a_backend_without_read_guards_is_refused(tmp_path):
    from graflo.filter.onto import FilterExpression

    conf = GraFloBackendConfig(output_dir=tmp_path)
    pending = [
        PendingEdge(
            edge_id=OPERATES,
            source=EdgeEndpoint(vertex="Person", match={"phone": "+1 555"}),
            target=EdgeEndpoint(vertex="Machine", identity={"serial": "S-9"}),
        )
    ]
    guard = FilterExpression.from_dict(["==", "x", "email"])

    with pytest.raises(NotImplementedError):
        asyncio.run(
            write_container(_schema(), _container(), conf, pending=pending, guard=guard)
        )


def test_a_stamped_write_leaves_the_callers_edges_as_they_were(tmp_path):
    raw: dict[str, Any] = copy.deepcopy(SCHEMA)
    zones = {
        "name": "access_zones",
        "type": "LIST",
        "item_type": "INT",
        "reserved": True,
    }
    raw["core_schema"]["edge_config"]["edges"][0]["properties"] = [zones]
    conf = GraFloBackendConfig(output_dir=tmp_path)
    container = _container()
    before = copy.deepcopy(container.edges)

    asyncio.run(
        write_container(
            Schema.model_validate(raw), container, conf, stamp={"access_zones": [7]}
        )
    )

    assert container.edges == before
