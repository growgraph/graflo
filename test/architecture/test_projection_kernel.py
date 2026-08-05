"""The L2 selection kernel shared by manifest projection and schema context."""

import pytest

from graflo.architecture.graph_types import Index
from graflo.architecture.schema.database_features import (
    DefaultPropertyValues,
    EdgePhysicalSpec,
)
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.projection import (
    build_subschema,
    project_db_profile,
    select_induced,
)


def test_select_all_by_default(context_schema):
    selection = select_induced(context_schema.core_schema)
    assert (
        selection.surviving_vertices
        == context_schema.core_schema.vertex_config.vertex_set
    )
    assert not selection.removed_vertices
    assert not selection.removed_edge_ids


def test_edges_survive_only_with_both_endpoints(context_schema):
    selection = select_induced(
        context_schema.core_schema, keep_vertices={"person", "company"}
    )
    assert selection.surviving_vertices == {"person", "company"}
    assert all(
        source in {"person", "company"} and target in {"person", "company"}
        for source, target, _ in selection.surviving_edge_ids
    )
    assert ("person", "city", "lives_in") in selection.removed_edge_ids


def test_induced_keeps_isolated_requested_vertex(context_schema):
    """A seed with no surviving edge is still the answer to a seeded query."""
    selection = select_induced(
        context_schema.core_schema, keep_vertices={"orphan"}, connectivity="induced"
    )
    assert selection.surviving_vertices == {"orphan"}


def test_induced_prune_drops_isolated_requested_vertex(context_schema):
    """Manifest projection wants the opposite: prune what ends up unconnected."""
    selection = select_induced(
        context_schema.core_schema,
        keep_vertices={"orphan"},
        connectivity="induced_prune",
    )
    assert selection.surviving_vertices == set()


def test_unknown_keep_vertices_are_ignored_not_fatal(context_schema):
    """Strictness is the caller's policy; the kernel simply intersects."""
    selection = select_induced(context_schema.core_schema, keep_vertices={"nope"})
    assert selection.surviving_vertices == set()


def test_keep_edge_ids_filters_parallel_edges(context_schema):
    selection = select_induced(
        context_schema.core_schema,
        keep_edge_ids={("person", "company", "works_at")},
    )
    assert selection.surviving_edge_ids == {("person", "company", "works_at")}
    assert ("person", "company", "founded") in selection.removed_edge_ids


def test_build_subschema_round_trips(context_schema):
    selection = select_induced(
        context_schema.core_schema, keep_vertices={"person", "company"}
    )
    sliced = build_subschema(context_schema, selection)
    assert Schema.model_validate(sliced.to_dict()) == sliced


def test_build_subschema_does_not_mutate_source(context_schema):
    before = context_schema.to_dict()
    selection = select_induced(context_schema.core_schema, keep_vertices={"person"})
    sliced = build_subschema(context_schema, selection)
    sliced.core_schema.vertex_config["person"].properties.clear()
    assert context_schema.to_dict() == before


def test_build_subschema_drops_requested_properties(context_schema):
    selection = select_induced(context_schema.core_schema, keep_vertices={"person"})
    sliced = build_subschema(
        context_schema, selection, drop_properties={"person": {"bio", "age"}}
    )
    assert sliced.core_schema.vertex_config["person"].property_names == [
        "email",
        "name",
    ]


def test_stale_edge_spec_would_break_construction_without_projection(context_schema):
    """The failure mode ``project_db_profile`` exists to prevent.

    ``Schema.finish_init`` validates edge specs against declared edges, so a spec
    outliving its edge makes the slice unconstructible — and the error reads like
    a validation bug rather than a missing projection step.
    """
    context_schema.db_profile.edge_specs = [
        EdgePhysicalSpec(source="person", target="city", relation="lives_in")
    ]
    selection = select_induced(
        context_schema.core_schema, keep_vertices={"person", "company"}
    )

    with pytest.raises(ValueError, match="references undeclared edge"):
        Schema(
            metadata=context_schema.metadata,
            core_schema=build_subschema(context_schema, selection).core_schema,
            db_profile=context_schema.db_profile,
        )

    # ...and the projected profile makes it construct.
    assert build_subschema(context_schema, selection).db_profile.edge_specs == []


def test_project_db_profile_prunes_every_keyed_entry(context_schema):
    profile = context_schema.db_profile
    profile.vertex_storage_names = {"person": "people", "city": "cities"}
    profile.vertex_indexes = {
        "person": [Index(fields=["name"])],
        "city": [Index(fields=["code"])],
    }
    profile.edge_specs = [
        EdgePhysicalSpec(source="person", target="company", relation="works_at"),
        EdgePhysicalSpec(source="person", target="city", relation="lives_in"),
    ]
    profile.default_property_values = DefaultPropertyValues(
        vertices={"person": {"age": 0}, "city": {"code": "??"}}, edges=[]
    )

    selection = select_induced(
        context_schema.core_schema, keep_vertices={"person", "company"}
    )
    projected = project_db_profile(profile, selection)

    assert set(projected.vertex_storage_names) == {"person"}
    assert set(projected.vertex_indexes) == {"person"}
    assert [spec.edge_id for spec in projected.edge_specs] == [
        ("person", "company", "works_at")
    ]
    assert projected.default_property_values is not None
    assert set(projected.default_property_values.vertices) == {"person"}
    # source profile untouched
    assert set(profile.vertex_storage_names) == {"person", "city"}


def test_project_db_profile_clears_empty_defaults(context_schema):
    context_schema.db_profile.default_property_values = DefaultPropertyValues(
        vertices={"city": {"code": "??"}}, edges=[]
    )
    selection = select_induced(context_schema.core_schema, keep_vertices={"person"})
    projected = project_db_profile(context_schema.db_profile, selection)
    assert projected.default_property_values is None
