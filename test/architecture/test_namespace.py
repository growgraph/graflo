"""Resolving the physical namespace a schema deploys into.

``Schema.metadata.name`` is a label -- merges fold it into ``left+right`` -- so
the database / graph / space name is derived from it per flavor, and an
explicit ``db_profile.target_namespace`` is validated rather than rewritten.
"""

from __future__ import annotations

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import MergeManifestsOp, merge_manifests
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.namespace import (
    InvalidNamespaceError,
    namespace_problem,
    resolve_namespace,
    sanitize_namespace,
    validate_namespace,
)
from graflo.onto import DBType

FLAVORS = [
    DBType.ARANGO,
    DBType.NEO4J,
    DBType.TIGERGRAPH,
    DBType.NEBULA,
    DBType.FALKORDB,
    DBType.MEMGRAPH,
    DBType.GRAFLO_BACKEND,
]

LABELS = [
    "a+b",
    "a b",
    "1graph",
    "Grüne Graph",
    "select",
    "gsql_sys_x",
    "x" * 100,
    "",
    "ab",
    "system",
    "my__graph",
    "kg-v1",
    "__x",
    "+++",
    "日本",
]


def _schema(
    name: str,
    *,
    db_flavor: str = "arango",
    target_namespace: str | None = None,
    vertex: str = "V",
) -> Schema:
    profile: dict[str, object] = {"db_flavor": db_flavor}
    if target_namespace is not None:
        profile["target_namespace"] = target_namespace
    return Schema.from_dict(
        {
            "metadata": {"name": name},
            "core_schema": {
                "vertex_config": {
                    "vertices": [
                        {"name": vertex, "properties": ["id"], "identity": ["id"]}
                    ]
                },
                "edge_config": {"edges": []},
            },
            "db_profile": profile,
        }
    )


@pytest.mark.parametrize("flavor", FLAVORS)
@pytest.mark.parametrize("label", LABELS)
def test_sanitized_namespace_is_valid_and_idempotent(
    flavor: DBType, label: str
) -> None:
    once = sanitize_namespace(label, flavor)
    assert namespace_problem(once, flavor) is None
    assert sanitize_namespace(once, flavor) == once


@pytest.mark.parametrize(
    ("flavor", "label", "expected"),
    [
        (DBType.ARANGO, "a+b", "a_b"),
        (DBType.ARANGO, "kg-v1", "kg-v1"),
        (DBType.ARANGO, "1graph", "g_1graph"),
        (DBType.NEO4J, "a+b", "a-b"),
        (DBType.NEO4J, "academic_kg", "academic-kg"),
        (DBType.NEO4J, "kg", "kg-db"),
        (DBType.NEO4J, "system", "g-system"),
        (DBType.TIGERGRAPH, "kg-v1", "kg_v1"),
        (DBType.TIGERGRAPH, "select", "select_graph"),
        (DBType.TIGERGRAPH, "__x", "__x"),
        (DBType.NEBULA, "Grüne Graph", "Grune_Graph"),
        (DBType.FALKORDB, "a+b", "a_b"),
    ],
)
def test_sanitize_rewrites_only_what_the_flavor_rejects(
    flavor: DBType, label: str, expected: str
) -> None:
    assert sanitize_namespace(label, flavor) == expected


@pytest.mark.parametrize("flavor", [DBType.ARANGO, DBType.NEO4J])
def test_long_labels_sharing_a_prefix_do_not_collide(flavor: DBType) -> None:
    left = sanitize_namespace("a" * 80 + "left", flavor)
    right = sanitize_namespace("a" * 80 + "right", flavor)
    assert left != right
    assert namespace_problem(left, flavor) is None


def test_explicit_namespace_is_validated_not_rewritten() -> None:
    validate_namespace("estate", DBType.NEO4J)
    with pytest.raises(InvalidNamespaceError, match="'estate-db'|valid spelling"):
        validate_namespace("es", DBType.NEO4J)
    with pytest.raises(InvalidNamespaceError, match="a_b"):
        validate_namespace("a+b", DBType.TIGERGRAPH)


def test_resolve_precedence_override_then_profile_then_label() -> None:
    schema = _schema("cmdb+discovery", db_flavor="neo4j")
    assert resolve_namespace(schema) == "cmdb-discovery"
    assert schema.effective_namespace(DBType.ARANGO) == "cmdb_discovery"
    schema.db_profile.target_namespace = "estate"
    assert schema.effective_namespace() == "estate"
    assert schema.effective_namespace(override="other") == "other"


def test_resolve_refuses_an_invalid_profile_namespace() -> None:
    schema = _schema("s", db_flavor="tigergraph", target_namespace="bad name")
    with pytest.raises(InvalidNamespaceError):
        schema.effective_namespace()


def test_merge_op_target_namespace_resolves_a_side_disagreement() -> None:
    left = GraphManifest(
        graph_schema=_schema("l", db_flavor="neo4j", target_namespace="one", vertex="A")
    )
    right = GraphManifest(
        graph_schema=_schema("r", db_flavor="neo4j", target_namespace="two", vertex="B")
    )
    with pytest.raises(ValueError, match="MergeManifestsOp.target_namespace"):
        merge_manifests(left, right, MergeManifestsOp(), bump_version=False)
    out = merge_manifests(
        left, right, MergeManifestsOp(target_namespace="estate"), bump_version=False
    )
    assert out.require_schema().db_profile.target_namespace == "estate"


def test_merge_op_target_namespace_is_validated_against_the_merged_flavor() -> None:
    left = GraphManifest(graph_schema=_schema("l", db_flavor="neo4j", vertex="A"))
    right = GraphManifest(graph_schema=_schema("r", db_flavor="neo4j", vertex="B"))
    with pytest.raises(InvalidNamespaceError):
        merge_manifests(
            left, right, MergeManifestsOp(target_namespace="a_b"), bump_version=False
        )
