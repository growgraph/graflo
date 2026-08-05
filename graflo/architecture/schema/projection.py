"""Pure schema-slicing kernel: induced selection and standalone slice assembly.

This is the layer-2 half of manifest projection. It knows how to answer *which*
vertices and edges survive a selection and how to assemble the survivors into a
valid standalone :class:`~graflo.architecture.schema.document.Schema`, without
knowing anything about manifests, ingestion, bindings or ops.

:mod:`graflo.architecture.evolution.project` (L4) calls down into
:func:`select_induced` so manifest projection and schema-context projection
share one definition of induced connectivity rather than two.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Literal

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.graph_types import EdgeId
from graflo.architecture.schema.core import CoreSchema
from graflo.architecture.schema.database_features import (
    DatabaseProfile,
    DefaultPropertyValues,
)
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import EdgeConfig
from graflo.architecture.schema.vertex import VertexConfig

Connectivity = Literal["induced", "induced_prune"]


class SubschemaSelection(ConfigBaseModel):
    """Survivor and removal sets computed from a selection over a core schema."""

    surviving_vertices: set[str]
    surviving_edge_ids: set[EdgeId]
    removed_vertices: set[str]
    removed_edge_ids: set[EdgeId]


def select_induced(
    core: CoreSchema,
    *,
    keep_vertices: Iterable[str] | None = None,
    keep_edge_ids: Iterable[EdgeId] | None = None,
    connectivity: Connectivity = "induced",
) -> SubschemaSelection:
    """Compute survivors for a vertex/edge selection over *core*.

    An edge survives only when both endpoints survive, so the result is always
    endpoint-closed and therefore constructible as a standalone ``CoreSchema``
    (see :meth:`CoreSchema._validate_edge_vertices_defined`).

    Args:
        core: Logical model to slice. Not mutated.
        keep_vertices: Vertex names to retain, or None for all.
        keep_edge_ids: Edge ids to retain, or None for all.
        connectivity: ``"induced"`` keeps every requested vertex, including ones
            left with no surviving edge. ``"induced_prune"`` additionally drops
            requested vertices that end up isolated — correct for manifest
            projection, wrong for a seeded context query, where a seed with no
            edges is still the answer to the question asked.

    Returns:
        SubschemaSelection: surviving and removed vertices/edges.
    """
    all_vertices = core.vertex_config.vertex_set
    all_edge_ids = {edge.edge_id for edge in core.edge_config.edges}

    if keep_edge_ids is not None:
        surviving_edge_ids = set(keep_edge_ids) & all_edge_ids
    else:
        surviving_edge_ids = set(all_edge_ids)

    keep_vertex_set = set(keep_vertices) if keep_vertices is not None else None

    if keep_vertex_set is not None:
        surviving_edge_ids = {
            edge_id
            for edge_id in surviving_edge_ids
            if edge_id[0] in keep_vertex_set and edge_id[1] in keep_vertex_set
        }

    connected: set[str] = set()
    for source, target, _relation in surviving_edge_ids:
        connected.add(source)
        connected.add(target)

    requested = (
        all_vertices if keep_vertex_set is None else keep_vertex_set & all_vertices
    )
    surviving_vertices = (
        connected & requested if connectivity == "induced_prune" else requested
    )

    return SubschemaSelection(
        surviving_vertices=surviving_vertices,
        surviving_edge_ids=surviving_edge_ids,
        removed_vertices=all_vertices - surviving_vertices,
        removed_edge_ids=all_edge_ids - surviving_edge_ids,
    )


def project_db_profile(
    profile: DatabaseProfile, selection: SubschemaSelection
) -> DatabaseProfile:
    """Return a copy of *profile* pruned to *selection*.

    Mandatory before assembling a slice: :meth:`Schema.finish_init` calls
    :meth:`DatabaseProfile.validate_against_schema`, which raises when an
    ``EdgePhysicalSpec`` outlives the edge it references. Every physical entry
    keyed by a logical vertex or edge is pruned here.
    """
    vertices = selection.surviving_vertices
    edge_ids = selection.surviving_edge_ids

    projected = profile.model_copy(deep=True)
    projected.vertex_storage_names = {
        name: storage
        for name, storage in projected.vertex_storage_names.items()
        if name in vertices
    }
    projected.vertex_indexes = {
        name: indexes
        for name, indexes in projected.vertex_indexes.items()
        if name in vertices
    }
    projected.edge_specs = [
        spec for spec in projected.edge_specs if spec.edge_id in edge_ids
    ]

    defaults = projected.default_property_values
    if defaults is not None:
        kept_vertex_defaults = {
            name: values
            for name, values in defaults.vertices.items()
            if name in vertices
        }
        kept_edge_defaults = [
            entry for entry in defaults.edges if entry.edge_id in edge_ids
        ]
        if kept_vertex_defaults or kept_edge_defaults:
            projected.default_property_values = DefaultPropertyValues(
                vertices=kept_vertex_defaults,
                edges=kept_edge_defaults,
            )
        else:
            projected.default_property_values = None

    return projected


def build_subschema(
    schema: Schema,
    selection: SubschemaSelection,
    *,
    drop_properties: Mapping[str, set[str]] | None = None,
) -> Schema:
    """Assemble a valid standalone ``Schema`` from *selection*.

    Args:
        schema: Source schema. Not mutated — every surviving element is deep-copied.
        selection: Survivors, as produced by :func:`select_induced`.
        drop_properties: Vertex name -> property names to omit. Callers are
            responsible for never listing an identity-bearing field; dropping one
            yields a schema that validates and is semantically a lie.

    Returns:
        Schema: a slice that round-trips through ``Schema.model_validate``.
    """
    source_core = schema.core_schema
    dropped = dict(drop_properties or {})

    vertices = []
    for vertex in source_core.vertex_config.vertices:
        if vertex.name not in selection.surviving_vertices:
            continue
        copied = vertex.model_copy(deep=True)
        omit = dropped.get(vertex.name)
        if omit:
            copied.properties = [
                field for field in copied.properties if field.name not in omit
            ]
        vertices.append(copied)

    vertex_config = VertexConfig(
        vertices=vertices,
        force_types={
            name: types
            for name, types in source_core.vertex_config.force_types.items()
            if name in selection.surviving_vertices
        },
        identity_from_all_properties=source_core.vertex_config.identity_from_all_properties,
    )
    edge_config = EdgeConfig(
        edges=[
            edge.model_copy(deep=True)
            for edge in source_core.edge_config.edges
            if edge.edge_id in selection.surviving_edge_ids
        ]
    )

    return Schema(
        metadata=schema.metadata.model_copy(deep=True),
        core_schema=CoreSchema(vertex_config=vertex_config, edge_config=edge_config),
        db_profile=project_db_profile(schema.db_profile, selection),
    )
