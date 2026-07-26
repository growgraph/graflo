"""Tests for :func:`~graflo.architecture.evolution.compose_manifests`."""

from __future__ import annotations

import pytest

from graflo.architecture.contract.bindings import FileConnector
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    ComposeManifestsOp,
    PropertyEquivalence,
    RelationEquivalence,
    VertexEquivalence,
    apply_evolution,
    compose_manifests,
)
from graflo.architecture.schema.core import CoreSchema
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.metadata import GraphMetadata
from graflo.architecture.schema.vertex import Field, FieldType, Vertex, VertexConfig
from graflo.migrate.io import manifest_hash


def _manifest(
    *,
    name: str,
    vertices: list[Vertex],
    edges: list[Edge],
    resources: list[dict],
    bindings: dict | None = None,
) -> GraphManifest:
    schema = Schema(
        metadata=GraphMetadata(name=name, version="1.0.0"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(vertices=vertices, force_types={}),
            edge_config=EdgeConfig(edges=edges),
        ),
    )
    payload: dict = {
        "schema": schema.to_dict(skip_defaults=False),
        "ingestion_model": {"resources": resources, "transforms": []},
    }
    if bindings is not None:
        payload["bindings"] = bindings
    m = GraphManifest.from_config(payload)
    m.finish_init()
    return m


def _left_client_manifest() -> GraphManifest:
    return _manifest(
        name="left",
        vertices=[
            Vertex(
                name="Client",
                properties=[
                    Field(name="client_id", type=FieldType.STRING),
                    Field(name="email", type=FieldType.STRING),
                ],
                identity=["client_id"],
            ),
            Vertex(
                name="Order",
                properties=[Field(name="id", type=FieldType.STRING)],
                identity=["id"],
            ),
        ],
        edges=[Edge(source="Client", target="Order", relation="places")],
        resources=[{"name": "r_clients", "apply": [{"vertex": "Client"}]}],
        bindings={
            "connectors": [
                FileConnector(
                    name="c_clients", regex="clients.*", resource_name="r_clients"
                ).to_dict(skip_defaults=False)
            ],
            "resource_connector": [{"resource": "r_clients", "connector": "c_clients"}],
        },
    )


def _right_customer_manifest() -> GraphManifest:
    return _manifest(
        name="right",
        vertices=[
            Vertex(
                name="Customer",
                properties=[
                    Field(name="customer_id", type=FieldType.STRING),
                    Field(name="email_addr", type=FieldType.STRING),
                ],
                identity=["customer_id"],
            ),
            Vertex(
                name="Invoice",
                properties=[Field(name="id", type=FieldType.STRING)],
                identity=["id"],
            ),
        ],
        edges=[Edge(source="Customer", target="Invoice", relation="billed")],
        resources=[{"name": "r_customers", "apply": [{"vertex": "Customer"}]}],
        bindings={
            "connectors": [
                FileConnector(
                    name="c_customers", regex="customers.*", resource_name="r_customers"
                ).to_dict(skip_defaults=False)
            ],
            "resource_connector": [
                {"resource": "r_customers", "connector": "c_customers"}
            ],
        },
    )


def test_disjoint_union_keeps_resources_and_bindings() -> None:
    left = _manifest(
        name="a",
        vertices=[
            Vertex(name="A", properties=[Field(name="id")], identity=["id"]),
        ],
        edges=[],
        resources=[{"name": "r_a", "apply": [{"vertex": "A"}]}],
        bindings={
            "connectors": [
                FileConnector(name="c_a", regex="a.*", resource_name="r_a").to_dict(
                    skip_defaults=False
                )
            ],
            "resource_connector": [{"resource": "r_a", "connector": "c_a"}],
        },
    )
    right = _manifest(
        name="b",
        vertices=[
            Vertex(name="B", properties=[Field(name="id")], identity=["id"]),
        ],
        edges=[],
        resources=[{"name": "r_b", "apply": [{"vertex": "B"}]}],
        bindings={
            "connectors": [
                FileConnector(name="c_b", regex="b.*", resource_name="r_b").to_dict(
                    skip_defaults=False
                )
            ],
            "resource_connector": [{"resource": "r_b", "connector": "c_b"}],
        },
    )
    h_left = manifest_hash(left)
    out = compose_manifests(left, right, ComposeManifestsOp(), bump_version=False)
    assert out.graph_schema is not None
    assert out.graph_schema.core_schema.vertex_config.vertex_set == {"A", "B"}
    assert out.ingestion_model is not None
    assert {r.name for r in out.ingestion_model.resources} == {"r_a", "r_b"}
    assert out.bindings is not None
    assert {c.name for c in out.bindings.connectors} == {"c_a", "c_b"}
    assert len(out.bindings.resource_connector) == 2
    assert manifest_hash(out) != h_left


def test_boundary_client_customer_with_explicit_identity() -> None:
    left = _left_client_manifest()
    right = _right_customer_manifest()
    op = ComposeManifestsOp(
        vertices=[
            VertexEquivalence(
                left="Client",
                right="Customer",
                into="Person",
                properties=[
                    PropertyEquivalence(
                        left="client_id", right="customer_id", into="id"
                    ),
                    PropertyEquivalence(left="email", right="email_addr", into="email"),
                ],
                identity=["email"],
            )
        ],
        relations=[
            RelationEquivalence(left="places", right="billed", into="activity"),
        ],
    )
    out = compose_manifests(left, right, op, bump_version=False)
    assert out.graph_schema is not None
    vc = out.graph_schema.core_schema.vertex_config
    assert vc.vertex_set == {"Person", "Order", "Invoice"}
    person = next(v for v in vc.vertices if v.name == "Person")
    assert person.identity == ["email"]
    prop_names = {f.name for f in person.properties}
    assert "id" in prop_names
    assert "email" in prop_names
    assert "client_id" not in prop_names
    assert "customer_id" not in prop_names

    relations = {e.relation for e in out.graph_schema.core_schema.edge_config.edges}
    assert "activity" in relations
    assert out.ingestion_model is not None
    assert {r.name for r in out.ingestion_model.resources} == {
        "r_clients",
        "r_customers",
    }


def test_property_identity_flags_derive_identity() -> None:
    left = _left_client_manifest()
    right = _right_customer_manifest()
    op = ComposeManifestsOp(
        vertices=[
            VertexEquivalence(
                left="Client",
                right="Customer",
                into="Person",
                properties=[
                    PropertyEquivalence(
                        left="email",
                        right="email_addr",
                        into="email",
                        identity=True,
                    ),
                    PropertyEquivalence(
                        left="client_id", right="customer_id", into="id"
                    ),
                ],
            )
        ],
    )
    out = compose_manifests(left, right, op, bump_version=False)
    person = next(
        v
        for v in out.graph_schema.core_schema.vertex_config.vertices  # type: ignore[union-attr]
        if v.name == "Person"
    )
    # Merged identity union (client_id→id, customer_id→id) plus flagged email.
    assert "email" in person.identity
    assert "id" in person.identity


def test_incompatible_property_types_raise() -> None:
    left = _manifest(
        name="l",
        vertices=[
            Vertex(
                name="A",
                properties=[Field(name="x", type=FieldType.STRING)],
                identity=["x"],
            )
        ],
        edges=[],
        resources=[{"name": "r_l", "apply": [{"vertex": "A"}]}],
    )
    right = _manifest(
        name="r",
        vertices=[
            Vertex(
                name="B",
                properties=[Field(name="y", type=FieldType.INT)],
                identity=["y"],
            )
        ],
        edges=[],
        resources=[{"name": "r_r", "apply": [{"vertex": "B"}]}],
    )
    op = ComposeManifestsOp(
        vertices=[
            VertexEquivalence(
                left="A",
                right="B",
                into="C",
                properties=[
                    PropertyEquivalence(left="x", right="y", into="z"),
                ],
            )
        ]
    )
    with pytest.raises(ValueError, match="incompatible types"):
        compose_manifests(left, right, op, bump_version=False)


def test_resource_name_collision_error_and_rename() -> None:
    left = _manifest(
        name="l",
        vertices=[
            Vertex(name="A", properties=[Field(name="id")], identity=["id"]),
        ],
        edges=[],
        resources=[{"name": "shared", "apply": [{"vertex": "A"}]}],
    )
    right = _manifest(
        name="r",
        vertices=[
            Vertex(name="B", properties=[Field(name="id")], identity=["id"]),
        ],
        edges=[],
        resources=[{"name": "shared", "apply": [{"vertex": "B"}]}],
    )
    with pytest.raises(ValueError, match="resource name collision"):
        compose_manifests(left, right, ComposeManifestsOp(), bump_version=False)

    out = compose_manifests(
        left,
        right,
        ComposeManifestsOp(resource_renames={"shared": "shared_right"}),
        bump_version=False,
    )
    assert {r.name for r in out.ingestion_model.resources} == {  # type: ignore[union-attr]
        "shared",
        "shared_right",
    }

    out_prefix = compose_manifests(
        left,
        right,
        ComposeManifestsOp(name_conflict="prefix_right"),
        bump_version=False,
    )
    assert {r.name for r in out_prefix.ingestion_model.resources} == {  # type: ignore[union-attr]
        "shared",
        "r_shared",
    }


def test_relation_equivalence_and_self_loop_after_boundary() -> None:
    left = _manifest(
        name="l",
        vertices=[
            Vertex(name="A", properties=[Field(name="id")], identity=["id"]),
            Vertex(name="B", properties=[Field(name="id")], identity=["id"]),
        ],
        edges=[Edge(source="A", target="B", relation="link")],
        resources=[
            {"name": "r_a", "apply": [{"vertex": "A"}]},
            {"name": "r_b", "apply": [{"vertex": "B"}]},
        ],
    )
    right = _manifest(
        name="r",
        vertices=[
            Vertex(name="X", properties=[Field(name="id")], identity=["id"]),
            Vertex(name="Y", properties=[Field(name="id")], identity=["id"]),
        ],
        edges=[Edge(source="X", target="Y", relation="link")],
        resources=[
            {"name": "r_x", "apply": [{"vertex": "X"}]},
            {"name": "r_y", "apply": [{"vertex": "Y"}]},
        ],
    )
    op = ComposeManifestsOp(
        vertices=[
            VertexEquivalence(left="A", right="X", into="AB"),
            VertexEquivalence(left="B", right="Y", into="AB"),
        ],
        relations=[RelationEquivalence(left="link", right="link", into="link")],
    )
    out = compose_manifests(left, right, op, bump_version=False)
    assert out.graph_schema is not None
    assert out.graph_schema.core_schema.vertex_config.vertex_set == {"AB"}
    edges = out.graph_schema.core_schema.edge_config.edges
    assert len(edges) == 1
    assert edges[0].source == "AB"
    assert edges[0].target == "AB"
    assert edges[0].relation == "link"


def test_apply_evolution_rejects_compose_op() -> None:
    m = _manifest(
        name="only",
        vertices=[
            Vertex(name="A", properties=[Field(name="id")], identity=["id"]),
        ],
        edges=[],
        resources=[{"name": "r", "apply": [{"vertex": "A"}]}],
    )
    with pytest.raises(ValueError, match="compose_manifests is binary"):
        apply_evolution(m, [ComposeManifestsOp()], bump_version=False)
