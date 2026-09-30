"""Projection, resource removal and the differ over a manifest with bindings.

One master: a customer side (``crm``, ``registry``) and an HR side (``hr``,
``rm_assignments``), each resource read by its own table connector, the two HR
connectors sharing one connection proxy.
"""

from __future__ import annotations

import logging

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    EdgeSelector,
    ProjectManifestOp,
    RemoveEdgesOp,
    RemoveResourcesOp,
    RemoveVerticesOp,
    apply_evolution,
)
from graflo.architecture.evolution.autogenerate import (
    diff_manifests,
    diff_manifests_verified,
)
from graflo.migrate.io import manifest_hash


def _vertex(name: str) -> dict:
    return {"name": name, "properties": ["id"], "identity": ["id"]}


def _edge(source: str, target: str, relation: str) -> dict:
    return {"source": source, "target": target, "relation": relation}


def _pipeline(*vertices: str, edge: tuple[str, str, str] | None = None) -> list:
    steps: list[dict] = [{"vertex": name} for name in vertices]
    if edge is not None:
        source, target, relation = edge
        steps.append({"edge": {"from": source, "to": target, "relation": relation}})
    return steps


def _master(*, bound_by: str = "resource_name") -> GraphManifest:
    resources = {
        "crm": _pipeline("customer", "account", edge=("customer", "account", "owns")),
        "registry": _pipeline(
            "customer", "legal_entity", edge=("legal_entity", "customer", "controls")
        ),
        "hr": _pipeline(
            "employee", "department", edge=("employee", "department", "member_of")
        ),
        "rm_assignments": _pipeline(
            "employee", "customer", edge=("employee", "customer", "manages")
        ),
    }
    proxies = {
        "crm": "crm_db",
        "registry": "registry_db",
        "hr": "hr_db",
        "rm_assignments": "hr_db",
    }
    connectors = []
    for name in resources:
        connector = {"name": f"{name}_table", "table_name": name}
        if bound_by == "resource_name":
            connector["resource_name"] = name
        connectors.append(connector)
    bindings: dict = {
        "connectors": connectors,
        "connector_connection": [
            {"connector": f"{name}_table", "conn_proxy": proxy}
            for name, proxy in proxies.items()
        ],
    }
    if bound_by == "resource_connector":
        bindings["resource_connector"] = [
            {"resource": name, "connector": f"{name}_table"} for name in resources
        ]
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "bank", "version": "1.0.0"},
                "core_schema": {
                    "vertex_config": {
                        "vertices": [
                            _vertex(name)
                            for name in (
                                "customer",
                                "account",
                                "legal_entity",
                                "employee",
                                "department",
                            )
                        ]
                    },
                    "edge_config": {
                        "edges": [
                            _edge("customer", "account", "owns"),
                            _edge("legal_entity", "customer", "controls"),
                            _edge("employee", "department", "member_of"),
                            _edge("employee", "customer", "manages"),
                        ]
                    },
                },
            },
            "ingestion_model": {
                "resources": [
                    {"name": name, "pipeline": pipeline}
                    for name, pipeline in resources.items()
                ]
            },
            "bindings": bindings,
        }
    )
    manifest.finish_init()
    return manifest


def _customer_side(**kwargs) -> ProjectManifestOp:
    return ProjectManifestOp(keep_vertices=["customer", "account"], **kwargs)


def _connector_names(manifest: GraphManifest) -> set[str]:
    bindings = manifest.bindings
    assert bindings is not None
    return {connector.name or "" for connector in bindings.connectors}


def _proxies(manifest: GraphManifest) -> set[str]:
    bindings = manifest.bindings
    assert bindings is not None
    return {row.conn_proxy for row in bindings.connector_connection_bindings}


def _resources(manifest: GraphManifest) -> set[str]:
    return {r.name for r in manifest.require_ingestion_model().resources}


# -- bindings follow the resources they serve --------------------------------


@pytest.mark.parametrize("bound_by", ["resource_name", "resource_connector"])
def test_a_projection_keeps_only_the_connectors_of_its_resources(bound_by) -> None:
    out = apply_evolution(
        _master(bound_by=bound_by),
        [_customer_side(keep_resources=["crm"])],
        bump_version=False,
    )

    assert _resources(out) == {"crm"}
    assert _connector_names(out) == {"crm_table"}
    assert _proxies(out) == {"crm_db"}


def test_vertex_removal_takes_the_connector_of_a_resource_it_empties() -> None:
    out = apply_evolution(
        _master(),
        [RemoveVerticesOp(names=["department"])],
        bump_version=False,
    )
    # `hr` still casts `employee`, so it and its connector stay.
    assert "hr_table" in _connector_names(out)

    out = apply_evolution(
        _master(),
        [RemoveVerticesOp(names=["employee", "department"])],
        bump_version=False,
    )
    assert "hr" not in _resources(out)
    assert "hr_table" not in _connector_names(out)
    # `rm_assignments` survives on `customer`, so the shared proxy stays.
    assert "hr_db" in _proxies(out)


def test_removing_a_resource_takes_its_connector_and_an_unused_proxy() -> None:
    out = apply_evolution(
        _master(),
        [RemoveResourcesOp(names=["hr", "rm_assignments"])],
        bump_version=False,
    )
    assert _connector_names(out) == {"crm_table", "registry_table"}
    assert _proxies(out) == {"crm_db", "registry_db"}


def test_a_connector_shared_with_a_surviving_resource_stays() -> None:
    manifest = _master(bound_by="resource_connector")
    payload = manifest.to_dict(skip_defaults=False)
    payload["bindings"]["resource_connector"].append(
        {"resource": "crm", "connector": "hr_table"}
    )
    shared = GraphManifest.from_config(payload)
    shared.finish_init()

    out = apply_evolution(shared, [RemoveResourcesOp(names=["hr"])], bump_version=False)
    assert "hr_table" in _connector_names(out)
    assert "hr_db" in _proxies(out)


def test_removing_a_relation_drops_its_edge_step_whole() -> None:
    out = apply_evolution(
        _master(), [RemoveEdgesOp(relations=["manages"])], bump_version=False
    )
    (resource,) = [
        r for r in out.require_ingestion_model().resources if r.name == "rm_assignments"
    ]
    assert resource.to_dict()["pipeline"] == [
        {"vertex": "employee"},
        {"vertex": "customer"},
    ]


# -- the differ replays a removal that empties a resource ---------------------


@pytest.mark.parametrize(
    "op",
    [
        _customer_side(keep_resources=["crm", "registry"]),
        _customer_side(keep_resources=["crm"]),
        RemoveVerticesOp(names=["employee", "department"]),
    ],
    ids=["keep-two", "keep-one", "remove-vertices"],
)
def test_a_diff_to_a_removal_replays(op) -> None:
    master = _master()
    target = apply_evolution(master, [op], bump_version=False)

    ops, warnings = diff_manifests_verified(master, target)

    assert warnings == []
    replayed = apply_evolution(master, ops, bump_version=False)
    assert manifest_hash(replayed) == manifest_hash(target)


def test_bindings_are_set_only_where_the_cascade_does_not_reach_them() -> None:
    master = _master()
    target = apply_evolution(
        master, [RemoveResourcesOp(names=["hr"])], bump_version=False
    )

    ops, _ = diff_manifests(master, target)

    assert [op.op for op in ops] == ["remove_resources"]


# -- a projection names the resources it trimmed ------------------------------


def test_a_default_projection_trims_and_names_what_it_trimmed(caplog) -> None:
    with caplog.at_level(logging.WARNING):
        out = apply_evolution(_master(), [_customer_side()], bump_version=False)

    # `registry` and `rm_assignments` survive as a lone `customer` step.
    assert _resources(out) == {"crm", "registry", "rm_assignments"}
    (record,) = [r for r in caplog.records if "trimmed" in r.getMessage()]
    assert "registry" in record.getMessage()
    assert "rm_assignments" in record.getMessage()


def test_a_dropping_projection_keeps_only_whole_resources() -> None:
    out = apply_evolution(
        _master(), [_customer_side(partial_resources="drop")], bump_version=False
    )
    assert _resources(out) == {"crm"}
    assert _connector_names(out) == {"crm_table"}


def test_a_dropping_projection_that_leaves_nothing_is_refused() -> None:
    with pytest.raises(ValueError, match="empty"):
        apply_evolution(
            _master(),
            [_customer_side(keep_resources=["registry"], partial_resources="drop")],
            bump_version=False,
        )


# -- an edge step is found in every spelling -----------------------------------

_SPELLINGS = {
    "nested": lambda s, t, r: {"edge": {"source": s, "target": t, "relation": r}},
    "flat-typed": lambda s, t, r: {
        "source": s,
        "target": t,
        "relation": r,
        "type": "edge",
    },
    "flat": lambda s, t, r: {"from": s, "to": t, "relation": r},
}


@pytest.mark.parametrize("spelling", sorted(_SPELLINGS))
@pytest.mark.parametrize("by", ["triple", "relation"])
def test_an_edge_removal_takes_its_step_and_only_its_step(spelling, by) -> None:
    step = _SPELLINGS[spelling]
    payload = _master().to_dict(skip_defaults=False)
    for resource in payload["ingestion_model"]["resources"]:
        if resource["name"] == "rm_assignments":
            resource["pipeline"] = [
                {"vertex": "employee"},
                {"vertex": "customer"},
                step("employee", "customer", "manages"),
                step("employee", "department", "member_of"),
            ]
    manifest = GraphManifest.from_config(payload)
    manifest.finish_init()
    op = (
        RemoveEdgesOp(
            edges=[
                EdgeSelector(source="employee", target="customer", relation="manages")
            ]
        )
        if by == "triple"
        else RemoveEdgesOp(relations=["manages"])
    )

    out = apply_evolution(manifest, [op], bump_version=False)

    (resource,) = [
        r for r in out.require_ingestion_model().resources if r.name == "rm_assignments"
    ]
    assert resource.to_dict()["pipeline"] == [
        {"vertex": "employee"},
        {"vertex": "customer"},
        step("employee", "department", "member_of"),
    ]
