"""Tests for :func:`~graflo.architecture.evolution.compose_manifests`."""

from __future__ import annotations

import pytest

from graflo.architecture.contract.bindings import FileConnector
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    ClusterConflictError,
    ComposeIdentityError,
    ComposeManifestsOp,
    ComposeNameConflictError,
    PropertyEquivalence,
    RelationEquivalence,
    SideIdentity,
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
        vertices=[VertexEquivalence(left=["A", "B"], right=["X", "Y"], into="AB")],
        relations=[RelationEquivalence(left="link", right="link", into="link")],
        allow_merges=True,
        allow_self_relations=True,
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


# ── canonical near-collisions (CORE-MERGE-001) ──────────────────────────────
#
# Compose matched vertices and edges by raw name, so an overlay authored as
# `order_line` beside a core `OrderLine` composed into two unrelated types with
# the source data split between them and nothing raising. Names now collide
# both exactly and when they key alike under `canonical_key`.


def _named(
    name: str, *, relation: str | None = None, resource: str | None = None
) -> GraphManifest:
    """A one-vertex manifest, optionally with a self-edge under *relation*."""
    vertex = Vertex(
        name=name,
        properties=[Field(name="id", type=FieldType.STRING)],
        identity=["id"],
    )
    edges = (
        [Edge(source=name, target=name, relation=relation)]
        if relation is not None
        else []
    )
    return _manifest(
        name=f"m-{name}",
        vertices=[vertex],
        edges=edges,
        resources=[
            {
                "name": resource or f"r_{name.lower()}",
                "apply": [{"vertex": name}],
            }
        ],
    )


def _vertex_names(manifest: GraphManifest) -> set[str]:
    schema = manifest.graph_schema
    assert schema is not None
    return {v.name for v in schema.core_schema.vertex_config.vertices}


def test_a_canonical_near_collision_raises_by_default() -> None:
    """The defect itself: two spellings, no equivalence, previously silent."""
    with pytest.raises(ComposeNameConflictError, match="same concept"):
        compose_manifests(
            _named("OrderLine"), _named("order_line"), ComposeManifestsOp()
        )


def test_a_trailing_plural_is_a_collision_too() -> None:
    """`canonical_key` folds the plural, and so must the check."""
    with pytest.raises(ComposeNameConflictError):
        compose_manifests(_named("Customer"), _named("Customers"), ComposeManifestsOp())


def test_the_message_names_both_spellings_and_the_ways_out() -> None:
    with pytest.raises(ComposeNameConflictError) as excinfo:
        compose_manifests(
            _named("OrderLine"), _named("order_line"), ComposeManifestsOp()
        )
    message = str(excinfo.value)
    assert "'OrderLine' / 'order_line'" in message
    assert "VertexEquivalence" in message
    assert "fuse_right" in message and "prefix_right" in message


def test_a_declared_equivalence_exempts_a_near_collision() -> None:
    """Raising is a prompt, not a wall.

    This is the whole point: the author is told to say what they mean, and
    saying it composes cleanly under the unchanged default policy.
    """
    composed = compose_manifests(
        _named("OrderLine"),
        _named("order_line"),
        ComposeManifestsOp(
            vertices=[
                VertexEquivalence(
                    left="OrderLine", right="order_line", into="OrderLine"
                )
            ]
        ),
    )
    assert _vertex_names(composed) == {"OrderLine"}


def test_prefix_right_keeps_a_near_collision_apart() -> None:
    composed = compose_manifests(
        _named("OrderLine"),
        _named("order_line"),
        ComposeManifestsOp(name_conflict="prefix_right"),
    )
    assert _vertex_names(composed) == {"OrderLine", "r_order_line"}


def test_fuse_right_adopts_the_left_spelling() -> None:
    composed = compose_manifests(
        _named("OrderLine"),
        _named("order_line"),
        ComposeManifestsOp(name_conflict="fuse_right"),
    )
    assert _vertex_names(composed) == {"OrderLine"}


def test_fuse_right_rewrites_ingestion_too() -> None:
    """The rename has to reach the pipelines, not just the schema.

    A rename that lands on the schema alone leaves every resource step pointing
    at a vertex that no longer exists -- the same shape of silent breakage the
    check exists to prevent.
    """
    composed = compose_manifests(
        _named("OrderLine"),
        _named("order_line"),
        ComposeManifestsOp(name_conflict="fuse_right"),
    )
    assert composed.ingestion_model is not None
    targets = {
        step.get("vertex")
        for resource in composed.ingestion_model.resources
        for step in resource.pipeline
        if isinstance(step, dict)
    }
    assert targets == {"OrderLine"}


def test_relations_that_key_alike_collide() -> None:
    with pytest.raises(ComposeNameConflictError, match="relation"):
        compose_manifests(
            _named("A", relation="placedBy"),
            _named("B", relation="placed_by"),
            ComposeManifestsOp(),
        )


def test_an_exact_collision_still_reports_the_old_way() -> None:
    """Exact equality was already refused; that message must not change."""
    with pytest.raises(ValueError, match="vertex name collision") as excinfo:
        # Distinct resource names, so the vertex check is what fires: resources
        # are checked first and would otherwise mask it.
        compose_manifests(
            _named("Order", resource="r_left"),
            _named("Order", resource="r_right"),
            ComposeManifestsOp(),
        )
    assert not isinstance(excinfo.value, ComposeNameConflictError)


def test_resources_that_key_alike_still_compose() -> None:
    """Deliberately exempt: a resource is an address, not a concept.

    Two resources whose names key alike split nothing -- each keeps its name,
    each is looked up by it, each targets its own vertex.
    """
    left = _manifest(
        name="l",
        vertices=[Vertex(name="A", properties=[Field(name="id")], identity=["id"])],
        edges=[],
        resources=[{"name": "orders", "apply": [{"vertex": "A"}]}],
    )
    right = _manifest(
        name="r",
        vertices=[Vertex(name="B", properties=[Field(name="id")], identity=["id"])],
        edges=[],
        resources=[{"name": "order", "apply": [{"vertex": "B"}]}],
    )
    composed = compose_manifests(left, right, ComposeManifestsOp())
    assert composed.ingestion_model is not None
    assert {r.name for r in composed.ingestion_model.resources} == {"orders", "order"}


def test_properties_that_key_alike_are_not_fused() -> None:
    """A field name binds to a key in the source document.

    Fusing `customer_email` with `customerEmail` would fuse two columns fed by
    two different document keys, so property matching stays exact.
    """
    left = _manifest(
        name="l",
        vertices=[
            Vertex(
                name="Party",
                properties=[Field(name="id"), Field(name="customer_email")],
                identity=["id"],
            )
        ],
        edges=[],
        resources=[{"name": "r_l", "apply": [{"vertex": "Party"}]}],
    )
    right = _manifest(
        name="r",
        vertices=[
            Vertex(
                name="Party",
                properties=[Field(name="id"), Field(name="customerEmail")],
                identity=["id"],
            )
        ],
        edges=[],
        resources=[{"name": "r_r", "apply": [{"vertex": "Party"}]}],
    )
    composed = compose_manifests(
        left,
        right,
        ComposeManifestsOp(
            vertices=[VertexEquivalence(left="Party", right="Party", into="Party")]
        ),
    )
    schema = composed.graph_schema
    assert schema is not None
    names = {f.name for f in schema.core_schema.vertex_config.vertices[0].properties}
    assert {"customer_email", "customerEmail"} <= names


def test_exact_name_properties_fuse_without_equivalence() -> None:
    """Same spelling on both sides merges for free — no PropertyEquivalence."""
    left = _manifest(
        name="l",
        vertices=[
            Vertex(
                name="A",
                properties=[
                    Field(name="id", type=FieldType.STRING),
                    Field(name="email", type=FieldType.STRING),
                ],
                identity=["id"],
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
                properties=[
                    Field(name="id", type=FieldType.STRING),
                    Field(name="email", type=FieldType.STRING),
                ],
                identity=["id"],
            )
        ],
        edges=[],
        resources=[{"name": "r_r", "apply": [{"vertex": "B"}]}],
    )
    composed = compose_manifests(
        left,
        right,
        ComposeManifestsOp(
            vertices=[VertexEquivalence(left="A", right="B", into="Person")]
        ),
        bump_version=False,
    )
    schema = composed.graph_schema
    assert schema is not None
    person = next(
        v for v in schema.core_schema.vertex_config.vertices if v.name == "Person"
    )
    names = [f.name for f in person.properties]
    assert names.count("email") == 1
    assert names.count("id") == 1


def test_disagreeing_into_on_shared_node_raises() -> None:
    """Overlapping equivalences with different into must not last-write-wins."""
    left = _manifest(
        name="l",
        vertices=[
            Vertex(name="CA1", properties=[Field(name="id")], identity=["id"]),
            Vertex(name="CA2", properties=[Field(name="id")], identity=["id"]),
        ],
        edges=[],
        resources=[
            {"name": "r_a1", "apply": [{"vertex": "CA1"}]},
            {"name": "r_a2", "apply": [{"vertex": "CA2"}]},
        ],
    )
    right = _manifest(
        name="r",
        vertices=[
            Vertex(name="CB1", properties=[Field(name="id")], identity=["id"]),
            Vertex(name="CB2", properties=[Field(name="id")], identity=["id"]),
        ],
        edges=[],
        resources=[
            {"name": "r_b1", "apply": [{"vertex": "CB1"}]},
            {"name": "r_b2", "apply": [{"vertex": "CB2"}]},
        ],
    )
    from graflo.architecture.evolution import ClusterConflictError

    with pytest.raises(ClusterConflictError, match="claimed"):
        compose_manifests(
            left,
            right,
            ComposeManifestsOp(
                vertices=[
                    VertexEquivalence(left="CA1", right="CB1", into="X"),
                    VertexEquivalence(left="CA1", right="CB2", into="X"),
                    VertexEquivalence(left="CA2", right="CB1", into="Y"),
                ]
            ),
        )


def test_identity_alignments_apply_inside_compose() -> None:
    from graflo.architecture.evolution import (
        AlignmentRow,
        DerivationSpec,
        IdentityAlignment,
        LocalKeySource,
        LocalKeySpec,
    )

    left = _manifest(
        name="l",
        vertices=[
            Vertex(
                name="Company",
                properties=[
                    Field(name="company_id", type=FieldType.STRING),
                    Field(name="shared_raw", type=FieldType.STRING),
                ],
                identity=["company_id"],
            )
        ],
        edges=[],
        resources=[{"name": "r_a", "apply": [{"vertex": "Company"}]}],
    )
    right = _manifest(
        name="r",
        vertices=[
            Vertex(
                name="Org",
                properties=[
                    Field(name="org_id", type=FieldType.STRING),
                    Field(name="shared_raw", type=FieldType.STRING),
                ],
                identity=["org_id"],
            )
        ],
        edges=[],
        resources=[{"name": "r_b", "apply": [{"vertex": "Org"}]}],
    )
    alignment = IdentityAlignment(
        vertex="Company",
        rows=[
            AlignmentRow(
                into="match_key",
                sources={
                    "r_a": DerivationSpec(input=["shared_raw"]),
                    "r_b": DerivationSpec(input=["shared_raw"]),
                },
            )
        ],
        local_key=LocalKeySpec(
            sources={
                "r_a": LocalKeySource(field="company_id", tag="a"),
                "r_b": LocalKeySource(field="org_id", tag="b"),
            }
        ),
        secondary_identities={
            "by_company_id": ["company_id"],
            "by_org_id": ["org_id"],
        },
    )
    composed = compose_manifests(
        left,
        right,
        ComposeManifestsOp(
            vertices=[VertexEquivalence(left="Company", right="Org", into="Company")],
            identity_alignments=[alignment],
        ),
        bump_version=False,
    )
    assert composed.graph_schema is not None
    vc = composed.graph_schema.core_schema.vertex_config
    assert {"match_key", "local_key"} <= set(vc.property_names("Company"))
    assert vc.identity_fields("Company") == ["id"]
    assert {s.name for s in vc.secondary_identities("Company")} == {
        "by_company_id",
        "by_org_id",
    }


def test_an_equivalence_in_the_wrong_convention_says_what_to_use() -> None:
    """The likeliest authoring mistake gets a way out, not a dead end."""
    with pytest.raises(ValueError, match="denotes the same concept"):
        compose_manifests(
            _named("OrderLine"),
            _named("order_line"),
            ComposeManifestsOp(
                vertices=[
                    VertexEquivalence(
                        left="order_line", right="order_line", into="order_line"
                    )
                ]
            ),
        )


def test_nary_cluster_composes_schema_and_ingestion() -> None:
    """The example-19 shape: {Company, Shop} ~ {Org, Branch} -> Company."""
    left = _manifest(
        name="l",
        vertices=[
            Vertex(
                name="Company",
                properties=[Field(name="company_id"), Field(name="shared")],
                identity=["company_id"],
            ),
            Vertex(
                name="Shop",
                properties=[Field(name="shop_id"), Field(name="shared")],
                identity=["shop_id"],
            ),
        ],
        edges=[],
        resources=[
            {"name": "r_company", "apply": [{"vertex": "Company"}]},
            {"name": "r_shop", "apply": [{"vertex": "Shop"}]},
        ],
    )
    right = _manifest(
        name="r",
        vertices=[
            Vertex(
                name="Org",
                properties=[Field(name="org_id"), Field(name="shared")],
                identity=["org_id"],
            ),
            Vertex(
                name="Branch",
                properties=[Field(name="branch_id"), Field(name="shared")],
                identity=["branch_id"],
            ),
        ],
        edges=[],
        resources=[
            {"name": "r_org", "apply": [{"vertex": "Org"}]},
            {"name": "r_branch", "apply": [{"vertex": "Branch"}]},
        ],
    )
    op = ComposeManifestsOp(
        vertices=[
            VertexEquivalence(
                left=["Company", "Shop"],
                right=["Org", "Branch"],
                into="Company",
                identity=["company_id"],
            )
        ],
        allow_merges=True,
    )
    out = compose_manifests(left, right, op, bump_version=False)
    schema = out.graph_schema
    assert schema is not None
    assert schema.core_schema.vertex_config.vertex_set == {"Company"}
    company = next(
        v for v in schema.core_schema.vertex_config.vertices if v.name == "Company"
    )
    assert company.identity == ["company_id"]
    assert {f.name for f in company.properties} == {
        "company_id",
        "shop_id",
        "org_id",
        "branch_id",
        "shared",
    }
    # `by_company_id` is skipped: it would restate the new primary as a secondary.
    assert {s.name for s in company.secondary_identities} == {
        "by_shop_id",
        "by_org_id",
        "by_branch_id",
    }
    assert out.ingestion_model is not None
    assert {r.name for r in out.ingestion_model.resources} == {
        "r_company",
        "r_shop",
        "r_org",
        "r_branch",
    }


def test_per_member_property_equivalence_maps() -> None:
    """E1 regression: two members on one side need different old-field maps."""
    left = _manifest(
        name="l",
        vertices=[
            Vertex(
                name="Company",
                properties=[Field(name="company_key")],
                identity=["company_key"],
            ),
            Vertex(
                name="Shop", properties=[Field(name="shop_key")], identity=["shop_key"]
            ),
        ],
        edges=[],
        resources=[
            {"name": "r_company", "apply": [{"vertex": "Company"}]},
            {"name": "r_shop", "apply": [{"vertex": "Shop"}]},
        ],
    )
    right = _manifest(
        name="r",
        vertices=[
            Vertex(name="Org", properties=[Field(name="org_key")], identity=["org_key"])
        ],
        edges=[],
        resources=[{"name": "r_org", "apply": [{"vertex": "Org"}]}],
    )
    op = ComposeManifestsOp(
        vertices=[
            VertexEquivalence(
                left=["Company", "Shop"],
                right="Org",
                into="Company",
                properties=[
                    PropertyEquivalence(
                        left={"Company": "company_key", "Shop": "shop_key"},
                        right="org_key",
                        into="uid",
                    )
                ],
                identity=["uid"],
            )
        ],
        allow_merges=True,
    )
    out = compose_manifests(left, right, op, bump_version=False)
    company = next(
        v
        for v in out.graph_schema.core_schema.vertex_config.vertices  # type: ignore[union-attr]
        if v.name == "Company"
    )
    prop_names = {f.name for f in company.properties}
    assert "uid" in prop_names
    assert not {"company_key", "shop_key", "org_key"} & prop_names
    assert company.identity == ["uid"]


def test_relation_nary_collapse() -> None:
    left = _manifest(
        name="l",
        vertices=[
            Vertex(name="P", properties=[Field(name="id")], identity=["id"]),
            Vertex(name="Q", properties=[Field(name="id")], identity=["id"]),
        ],
        edges=[
            Edge(source="P", target="Q", relation="signs"),
            Edge(source="P", target="Q", relation="owns"),
        ],
        resources=[
            {"name": "r_p", "apply": [{"vertex": "P"}]},
            {"name": "r_q", "apply": [{"vertex": "Q"}]},
        ],
    )
    right = _manifest(
        name="r",
        vertices=[
            Vertex(name="X", properties=[Field(name="id")], identity=["id"]),
            Vertex(name="Y", properties=[Field(name="id")], identity=["id"]),
        ],
        edges=[Edge(source="X", target="Y", relation="has")],
        resources=[
            {"name": "r_x", "apply": [{"vertex": "X"}]},
            {"name": "r_y", "apply": [{"vertex": "Y"}]},
        ],
    )
    op = ComposeManifestsOp(
        vertices=[
            VertexEquivalence(left="P", right="X", into="P"),
            VertexEquivalence(left="Q", right="Y", into="Q"),
        ],
        relations=[
            RelationEquivalence(left=["signs", "owns"], right="has", into="signs")
        ],
        allow_merges=True,
    )
    out = compose_manifests(left, right, op, bump_version=False)
    relations = {
        e.relation
        for e in out.graph_schema.core_schema.edge_config.edges  # type: ignore[union-attr]
        if e.relation is not None
    }
    assert relations == {"signs"}


def test_fuse_right_adopts_left_spelling_for_relations() -> None:
    left = _manifest(
        name="l",
        vertices=[
            Vertex(name="A", properties=[Field(name="id")], identity=["id"]),
            Vertex(name="B", properties=[Field(name="id")], identity=["id"]),
        ],
        edges=[Edge(source="A", target="B", relation="placedBy")],
        resources=[
            {"name": "r_a", "apply": [{"vertex": "A"}]},
            {"name": "r_b", "apply": [{"vertex": "B"}]},
        ],
    )
    right = _manifest(
        name="r",
        vertices=[
            Vertex(name="AR", properties=[Field(name="id")], identity=["id"]),
            Vertex(name="BR", properties=[Field(name="id")], identity=["id"]),
        ],
        edges=[Edge(source="AR", target="BR", relation="placed_by")],
        resources=[
            {"name": "r_ar", "apply": [{"vertex": "AR"}]},
            {"name": "r_br", "apply": [{"vertex": "BR"}]},
        ],
    )
    op = ComposeManifestsOp(
        vertices=[
            VertexEquivalence(left="A", right="AR", into="A"),
            VertexEquivalence(left="B", right="BR", into="B"),
        ],
        name_conflict="fuse_right",
    )
    out = compose_manifests(left, right, op, bump_version=False)
    relations = {
        e.relation
        for e in out.graph_schema.core_schema.edge_config.edges  # type: ignore[union-attr]
        if e.relation is not None
    }
    assert relations == {"placedBy"}


def test_undeclared_identity_disagreement_raises() -> None:
    left = _manifest(
        name="l",
        vertices=[
            Vertex(
                name="Company",
                properties=[Field(name="company_id")],
                identity=["company_id"],
            )
        ],
        edges=[],
        resources=[{"name": "r_l", "apply": [{"vertex": "Company"}]}],
    )
    right = _manifest(
        name="r",
        vertices=[
            Vertex(name="Org", properties=[Field(name="org_id")], identity=["org_id"])
        ],
        edges=[],
        resources=[{"name": "r_r", "apply": [{"vertex": "Org"}]}],
    )
    op = ComposeManifestsOp(
        vertices=[VertexEquivalence(left="Company", right="Org", into="Company")]
    )
    with pytest.raises(ComposeIdentityError, match="disagree"):
        compose_manifests(left, right, op, bump_version=False)


def test_side_identity_shorthand_lowers_to_one_funnel() -> None:
    left = _manifest(
        name="l",
        vertices=[
            Vertex(
                name="Company",
                properties=[Field(name="tax_id"), Field(name="company_id")],
                identity=["company_id"],
            )
        ],
        edges=[],
        resources=[{"name": "r_l", "apply": [{"vertex": "Company"}]}],
    )
    right = _manifest(
        name="r",
        vertices=[
            Vertex(
                name="Org",
                properties=[Field(name="tax_id"), Field(name="org_id")],
                identity=["org_id"],
            )
        ],
        edges=[],
        resources=[{"name": "r_r", "apply": [{"vertex": "Org"}]}],
    )
    op = ComposeManifestsOp(
        vertices=[
            VertexEquivalence(
                left="Company",
                right="Org",
                into="Company",
                identity=SideIdentity(
                    left=[["tax_id"], ["company_id"]],
                    right=[["tax_id"], ["org_id"]],
                ),
            )
        ]
    )
    out = compose_manifests(left, right, op, bump_version=False)
    company = next(
        v
        for v in out.graph_schema.core_schema.vertex_config.vertices  # type: ignore[union-attr]
        if v.name == "Company"
    )
    assert company.identity == ["id"]
    assert company.identity_funnel is not None
    assert [b.fields for b in company.identity_funnel.branches] == [
        ["tax_id"],
        ["company_id"],
        ["org_id"],
    ]
    assert {s.name for s in company.secondary_identities} == {
        "by_company_id",
        "by_org_id",
    }


def test_side_identity_order_inversion_raises() -> None:
    left = _manifest(
        name="l",
        vertices=[
            Vertex(
                name="Company",
                properties=[Field(name="a"), Field(name="b")],
                identity=["a"],
            )
        ],
        edges=[],
        resources=[{"name": "r_l", "apply": [{"vertex": "Company"}]}],
    )
    right = _manifest(
        name="r",
        vertices=[
            Vertex(
                name="Org",
                properties=[Field(name="a"), Field(name="b")],
                identity=["b"],
            )
        ],
        edges=[],
        resources=[{"name": "r_r", "apply": [{"vertex": "Org"}]}],
    )
    op = ComposeManifestsOp(
        vertices=[
            VertexEquivalence(
                left="Company",
                right="Org",
                into="Company",
                identity=SideIdentity(left=[["a"], ["b"]], right=[["b"], ["a"]]),
            )
        ]
    )
    with pytest.raises(ComposeIdentityError, match="inconsistent"):
        compose_manifests(left, right, op, bump_version=False)


def test_occupied_into_raises_through_compose() -> None:
    """E6 regression: a single source renaming onto an unrelated existing name."""
    left = _manifest(
        name="l",
        vertices=[
            Vertex(name="A", properties=[Field(name="id")], identity=["id"]),
            Vertex(name="Person", properties=[Field(name="id")], identity=["id"]),
        ],
        edges=[],
        resources=[
            {"name": "r_a", "apply": [{"vertex": "A"}]},
            {"name": "r_person", "apply": [{"vertex": "Person"}]},
        ],
    )
    right = _manifest(
        name="r",
        vertices=[Vertex(name="B", properties=[Field(name="id")], identity=["id"])],
        edges=[],
        resources=[{"name": "r_b", "apply": [{"vertex": "B"}]}],
    )
    op = ComposeManifestsOp(
        vertices=[VertexEquivalence(left="A", right="B", into="Person")]
    )
    with pytest.raises(ClusterConflictError, match="not a member"):
        compose_manifests(left, right, op, bump_version=False)
