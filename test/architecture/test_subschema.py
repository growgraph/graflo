"""Bounded seeded slicing: budgets, elision, and the round-trip contract."""

import pytest

from graflo.architecture.schema.context import Budget, SchemaGraph, subschema
from graflo.architecture.schema.context.budget import estimate_tokens
from graflo.architecture.schema.context.subschema import protected_property_names
from graflo.architecture.schema.core import CoreSchema
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.metadata import GraphMetadata
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig

UNBOUNDED = Budget(max_elements=None, max_tokens=None)


@pytest.fixture()
def wide_schema():
    """60 vertex types in a ring plus a hub — large enough for budgets to bite."""
    size = 60
    vertices = [
        Vertex(
            name=f"v{i:02d}",
            properties=[Field(name=f"p{j}", type="string") for j in range(8)],
            identity=["p0"],
        )
        for i in range(size)
    ]
    edges = [
        Edge(source=f"v{i:02d}", target=f"v{(i + 1) % size:02d}", relation=f"r{i}")
        for i in range(size)
    ]
    edges += [
        Edge(source="v00", target=f"v{i:02d}", relation="hub") for i in range(2, 20)
    ]
    return Schema(
        metadata=GraphMetadata(name="wide", version="1.0.0"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(vertices=vertices),
            edge_config=EdgeConfig(edges=edges),
        ),
    )


def test_requires_a_seed(context_schema):
    with pytest.raises(ValueError, match="at least one seed"):
        subschema(context_schema, [])


def test_rejects_unknown_seed(context_schema):
    with pytest.raises(KeyError, match="nope"):
        subschema(context_schema, ["nope"])


def test_slice_round_trips(context_schema):
    sliced, _ = subschema(context_schema, ["person"], budget=UNBOUNDED)
    assert Schema.model_validate(sliced.to_dict()) == sliced


def test_slice_is_endpoint_closed(wide_schema):
    sliced, _ = subschema(wide_schema, ["v00"], budget=Budget(max_elements=8))
    declared = sliced.core_schema.vertex_config.vertex_set
    for source, target, _relation in (
        e.edge_id for e in sliced.core_schema.edge_config.edges
    ):
        assert source in declared and target in declared


def test_seed_always_survives_even_when_isolated(context_schema):
    """``orphan`` has no edges; a seeded query must still answer with it."""
    sliced, _ = subschema(context_schema, ["orphan"], budget=UNBOUNDED)
    assert sliced.core_schema.vertex_config.vertex_set == {"orphan"}


def test_does_not_mutate_source(context_schema):
    before = context_schema.to_dict()
    subschema(context_schema, ["person"], budget=Budget(max_elements=3))
    assert context_schema.to_dict() == before


def test_element_budget_is_respected(wide_schema):
    _, report = subschema(
        wide_schema, ["v00"], budget=Budget(max_elements=10, max_tokens=None)
    )
    assert report.budget.elements_used <= 10
    assert report.budget.exhausted_by == "elements"


def test_token_budget_is_respected(wide_schema):
    cap = 800
    sliced, report = subschema(
        wide_schema, ["v00"], budget=Budget(max_elements=None, max_tokens=cap)
    )
    assert report.budget.exhausted_by == "tokens"
    assert report.budget.estimated_tokens <= cap
    # the promise is about the actual payload, not the running estimate
    assert estimate_tokens(sliced.to_minimal_canonical_dict()) <= cap


def test_generous_budget_is_not_marked_exhausted(context_schema):
    _, report = subschema(context_schema, ["person"], budget=UNBOUNDED)
    assert report.budget.exhausted_by == "none"


def test_seeds_may_exceed_the_budget_rather_than_be_dropped(wide_schema):
    """Seeds are never trimmed, so a tiny ceiling overruns instead of lying."""
    sliced, report = subschema(wide_schema, ["v00"], budget=Budget(max_tokens=10))
    assert sliced.core_schema.vertex_config.vertex_set == {"v00"}
    assert report.budget.estimated_tokens > 10


def test_serialized_chars_is_exact(context_schema):
    sliced, report = subschema(context_schema, ["person"], budget=UNBOUNDED)
    import json

    exact = len(
        json.dumps(
            sliced.to_minimal_canonical_dict(),
            separators=(",", ":"),
            sort_keys=True,
            default=str,
        )
    )
    assert report.budget.serialized_chars == exact


def test_identity_fields_survive_property_elision(context_schema):
    sliced, report = subschema(
        context_schema,
        ["person"],
        budget=Budget(max_properties_per_vertex=1, max_elements=None, max_tokens=None),
    )
    person = sliced.core_schema.vertex_config["person"]
    assert "email" in person.property_names
    assert set(report.elided_properties["person"]).isdisjoint({"email"})


def test_protected_names_cover_every_identity_flavour():
    vertex = Vertex(
        name="thing",
        properties=[Field(name=n, type="string") for n in ("a", "b", "c", "d")],
        identity=["a"],
        secondary_identities=[{"name": "alt", "fields": ["b"]}],
        hash_identity_properties=["c"],
    )
    assert protected_property_names(vertex) == {"a", "b", "c"}


def test_elision_report_names_what_was_dropped(wide_schema):
    _, report = subschema(wide_schema, ["v00"], budget=Budget(max_elements=6))
    assert report.truncated
    assert report.elided_vertices
    dropped = {item.name for item in report.elided_vertices}
    assert dropped
    assert all(
        item.drill_in.startswith("subschema(") for item in report.elided_vertices
    )


def test_unreachable_types_are_reported_as_unreachable(context_schema):
    _, report = subschema(context_schema, ["orphan"], budget=UNBOUNDED)
    reasons = {item.name: item.reason for item in report.elided_vertices}
    assert reasons["person"] == "unreachable"


def test_edges_dropped_with_their_endpoint_say_so(context_schema):
    _, report = subschema(context_schema, ["person"], budget=Budget(max_elements=1))
    reasons = {item.edge_id: item.reason for item in report.elided_edges}
    assert reasons[("company", "city", "hq_in")] == "endpoint_elided"


def test_full_slice_reports_nothing_elided(context_schema):
    _, report = subschema(
        context_schema,
        sorted(context_schema.core_schema.vertex_config.vertex_set),
        budget=UNBOUNDED,
    )
    assert not report.truncated


def test_is_deterministic(wide_schema):
    budget = Budget(max_tokens=1000)
    expected = subschema(wide_schema, ["v00"], budget=budget)[0].to_dict()
    for _ in range(20):
        assert subschema(wide_schema, ["v00"], budget=budget)[0].to_dict() == expected


def test_accepts_a_prebuilt_graph(context_schema):
    graph = SchemaGraph.from_schema(context_schema)
    a, _ = subschema(context_schema, ["person"], budget=UNBOUNDED, graph=graph)
    b, _ = subschema(context_schema, ["person"], budget=UNBOUNDED)
    assert a.to_dict() == b.to_dict()


def test_max_hops_bounds_candidates(wide_schema):
    _, near = subschema(wide_schema, ["v30"], budget=UNBOUNDED, max_hops=1)
    _, far = subschema(wide_schema, ["v30"], budget=UNBOUNDED, max_hops=3)
    assert near.budget.elements_used < far.budget.elements_used
