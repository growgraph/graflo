"""Removing a class takes the connector requests that fetched only that class.

One generic resource ``objects`` routes each record by its ``cls`` field; the
connectors that feed it fix ``cls`` per request, by ``values`` or by
``row_annotations``. A second resource ``accounts`` keeps the manifest whole
when every routed class goes.
"""

from __future__ import annotations

from typing import Any

import pytest

from graflo.architecture.contract.bindings import APIConnector
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    ProjectManifestOp,
    RemoveVerticesOp,
    RenameVerticesOp,
    apply_evolution,
)
from graflo.architecture.evolution.autogenerate import diff_manifests_verified
from graflo.migrate.io import manifest_hash

CLASSES = ("person", "org", "place")
#: Projection keeps a class only while an edge to it survives.
EDGES = [
    ("person", "org", "member_of"),
    ("org", "place", "located_in"),
    ("account", "org", "held_by"),
]
ROUTER: dict[str, Any] = {"vertex_router": {"type_field": "cls"}}
ENUMERATED: dict[str, Any] = {
    "name": "objects_api",
    "path": "/objects",
    "params": {"class": "{cls}"},
    "values": {"cls": list(CLASSES)},
}


def _manifest(
    *connectors: dict[str, Any],
    pipeline: list[dict[str, Any]] | None = None,
    bound_by: str = "resource_name",
) -> GraphManifest:
    connectors_out: list[dict[str, Any]] = [
        {
            "name": "accounts_table",
            "table_name": "accounts",
            "resource_name": "accounts",
        }
    ]
    rows: list[dict[str, str]] = []
    for connector in connectors:
        if bound_by == "resource_name":
            connectors_out.append({**connector, "resource_name": "objects"})
        else:
            connectors_out.append(connector)
            rows.append({"resource": "objects", "connector": connector["name"]})
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "objects", "version": "1.0.0"},
                "core_schema": {
                    "vertex_config": {
                        "vertices": [
                            {"name": name, "properties": ["id"], "identity": ["id"]}
                            for name in (*CLASSES, "account")
                        ]
                    },
                    "edge_config": {
                        "edges": [
                            {"source": s, "target": t, "relation": r}
                            for s, t, r in EDGES
                        ]
                    },
                },
            },
            "ingestion_model": {
                "resources": [
                    {"name": "objects", "pipeline": pipeline or [ROUTER]},
                    {"name": "accounts", "pipeline": [{"vertex": "account"}]},
                ]
            },
            "bindings": {
                "connectors": connectors_out,
                "resource_connector": rows,
                "connector_connection": [
                    {"connector": c["name"], "conn_proxy": "api"} for c in connectors
                ]
                + [{"connector": "accounts_table", "conn_proxy": "db"}],
            },
        }
    )
    manifest.finish_init()
    return manifest


def _connector(manifest: GraphManifest, name: str) -> APIConnector | None:
    bindings = manifest.bindings
    assert bindings is not None
    found = [c for c in bindings.connectors if c.name == name]
    if not found:
        return None
    (connector,) = found
    assert isinstance(connector, APIConnector)
    return connector


def _names(manifest: GraphManifest) -> set[str]:
    bindings = manifest.bindings
    assert bindings is not None
    return {c.name or "" for c in bindings.connectors}


def _proxies(manifest: GraphManifest) -> set[str]:
    bindings = manifest.bindings
    assert bindings is not None
    return {row.conn_proxy for row in bindings.connector_connection_bindings}


def _remove(manifest: GraphManifest, *names: str) -> GraphManifest:
    return apply_evolution(
        manifest, [RemoveVerticesOp(names=list(names))], bump_version=False
    )


def _per_class(field: str = "cls") -> list[dict[str, Any]]:
    return [
        {"name": f"{c}_api", "path": f"/{c}", "row_annotations": {field: c}}
        for c in CLASSES
    ]


# -- an enumerated connector loses the values of removed classes --------------


@pytest.mark.parametrize("bound_by", ["resource_name", "resource_connector"])
def test_removing_a_class_drops_its_value(bound_by) -> None:
    out = _remove(_manifest(ENUMERATED, bound_by=bound_by), "person")

    connector = _connector(out, "objects_api")
    assert connector is not None
    assert connector.values == {"cls": ["org", "place"]}
    assert "api" in _proxies(out)


def test_removing_every_class_it_fetches_drops_the_connector_and_its_proxy() -> None:
    out = _remove(_manifest(ENUMERATED), *CLASSES)

    assert _names(out) == {"accounts_table"}
    assert _proxies(out) == {"db"}


def test_a_value_is_routed_through_the_type_map() -> None:
    connector = {**ENUMERATED, "values": {"cls": ["P", "O", "X"]}}
    pipeline = [
        {
            "vertex_router": {
                "type_field": "cls",
                "type_map": {"P": "person", "O": "org", "X": "place"},
            }
        }
    ]
    out = _remove(_manifest(connector, pipeline=pipeline), "person")

    found = _connector(out, "objects_api")
    assert found is not None and found.values == {"cls": ["O", "X"]}


def test_a_value_that_routed_nowhere_is_left_alone() -> None:
    connector = {**ENUMERATED, "values": {"cls": ["person", "ghost"]}}
    out = _remove(_manifest(connector), "person")

    found = _connector(out, "objects_api")
    assert found is not None and found.values == {"cls": ["ghost"]}


def test_only_the_variable_that_decides_the_class_is_trimmed() -> None:
    connector = {
        **ENUMERATED,
        "params": {"class": "{cls}", "region": "{region}"},
        "values": {"cls": ["person", "org"], "region": ["eu", "us"]},
    }
    out = _remove(_manifest(connector), "person")

    found = _connector(out, "objects_api")
    assert found is not None
    assert found.values == {"cls": ["org"], "region": ["eu", "us"]}


def test_a_connector_referred_to_by_hash_keeps_its_wiring() -> None:
    master = _manifest(ENUMERATED, bound_by="resource_connector")
    bindings = master.bindings
    assert bindings is not None
    old = _connector(master, "objects_api")
    assert old is not None
    payload = master.to_dict(skip_defaults=False)
    payload["bindings"]["resource_connector"] = [
        {"resource": "objects", "connector": old.hash}
    ]
    payload["bindings"]["connector_connection"] = [
        row if row["connector"] != "objects_api" else {**row, "connector": old.hash}
        for row in payload["bindings"]["connector_connection"]
    ]
    by_hash = GraphManifest.from_config(payload)
    by_hash.finish_init()

    out = _remove(by_hash, "person")

    new = _connector(out, "objects_api")
    assert new is not None and new.hash != old.hash
    out_bindings = out.bindings
    assert out_bindings is not None
    assert [c.name for c in out_bindings.get_connectors_for_resource("objects")] == [
        "objects_api"
    ]
    assert "api" in _proxies(out)


# -- a connector per class, told apart by row_annotations ---------------------


def test_the_connector_of_a_removed_class_goes_and_the_others_stay() -> None:
    out = _remove(_manifest(*_per_class()), "person")

    assert _names(out) == {"accounts_table", "org_api", "place_api"}


def test_a_connector_that_fixes_no_router_field_stays() -> None:
    out = _remove(_manifest(*_per_class(field="kind")), "person")

    assert _names(out) == {"accounts_table", "person_api", "org_api", "place_api"}


def test_a_connector_feeding_a_surviving_class_through_a_second_router_stays() -> None:
    pipeline = [
        {"vertex_router": {"type_field": "_src_type", "role": "src"}},
        {"vertex_router": {"type_field": "_tgt_type", "role": "tgt"}},
    ]
    link = {
        "name": "links_api",
        "path": "/links",
        "row_annotations": {"_src_type": "person", "_tgt_type": "org"},
    }
    out = _remove(_manifest(link, pipeline=pipeline), "person")

    assert "links_api" in _names(out)


def test_a_transform_that_may_rewrite_the_field_keeps_every_connector() -> None:
    pipeline = [
        {"transform": {"call": {"use": "str.lower", "input": ["raw"]}}},
        ROUTER,
    ]
    out = _remove(_manifest(*_per_class(), pipeline=pipeline), "person")

    assert "person_api" in _names(out)


def test_a_transform_writing_other_fields_does_not_hide_the_router() -> None:
    pipeline = [{"transform": {"rename": {"name": "label"}}}, ROUTER]
    out = _remove(_manifest(*_per_class(), pipeline=pipeline), "person")

    assert "person_api" not in _names(out)


def test_a_router_under_a_descend_counts_with_all_it_can_produce() -> None:
    pipeline = [{"descend": {"key": "items", "pipeline": [ROUTER]}}]
    out = _remove(_manifest(*_per_class(), pipeline=pipeline), "person")

    assert "person_api" in _names(out)


# -- projection, renames, the differ ------------------------------------------


def test_a_projection_trims_values_without_dropping_the_resource() -> None:
    out = apply_evolution(
        _manifest(ENUMERATED),
        [ProjectManifestOp(keep_vertices=["org", "place", "account"])],
        bump_version=False,
    )

    assert {r.name for r in out.require_ingestion_model().resources} == {
        "objects",
        "accounts",
    }
    found = _connector(out, "objects_api")
    assert found is not None and found.values == {"cls": ["org", "place"]}


def test_a_rename_leaves_the_values_the_source_is_asked_with() -> None:
    out = apply_evolution(
        _manifest(ENUMERATED),
        [RenameVerticesOp(renames={"person": "individual"})],
        bump_version=False,
    )

    found = _connector(out, "objects_api")
    assert found is not None and found.values == {"cls": list(CLASSES)}
    (router,) = out.require_ingestion_model().resources[0].pipeline
    assert router["vertex_router"]["type_map"] == {"person": "individual"}


@pytest.mark.parametrize(
    "op",
    [
        RemoveVerticesOp(names=["person"]),
        RemoveVerticesOp(names=list(CLASSES)),
        ProjectManifestOp(keep_vertices=["org", "account"]),
    ],
    ids=["one", "all", "project"],
)
def test_a_diff_to_the_removal_replays(op) -> None:
    master = _manifest(ENUMERATED, *_per_class(field="kind"))
    target = apply_evolution(master, [op], bump_version=False)

    ops, warnings = diff_manifests_verified(master, target)

    assert warnings == []
    replayed = apply_evolution(master, ops, bump_version=False)
    assert manifest_hash(replayed) == manifest_hash(target)
