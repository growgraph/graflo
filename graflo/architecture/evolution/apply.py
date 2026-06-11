"""Apply manifest evolution operations to a copy of a :class:`~graflo.architecture.contract.manifest.GraphManifest`."""

from __future__ import annotations

import logging
from typing import Any, Literal, Sequence

from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.graph_types import EdgeId
from graflo.architecture.pipeline.runtime.actor import ActorWrapper
from graflo.architecture.database_features import DatabaseProfile
from graflo.architecture.schema import Schema
from graflo.architecture.schema.core import CoreSchema
from graflo.architecture.schema.edge import EdgeConfig
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig

from .db_profile import (
    apply_edge_id_removal_to_db_profile,
    apply_edge_property_removal_to_db_profile,
    apply_edge_property_rename_to_db_profile,
    apply_field_rename_to_db_profile,
    apply_relation_removal_to_db_profile,
    apply_relation_rename_to_db_profile,
    apply_storage_name_sanitization_to_db_profile,
    apply_vertex_merge_to_db_profile,
    apply_vertex_removal_to_db_profile,
    apply_vertex_rename_to_db_profile,
    apply_inverse_edges_to_db_profile,
    merge_relation_entries_in_db_profile,
)
from .inverse_edges import (
    _append_inverse_flat_specs,
    _append_inverses_for_nested_edges,
    _as_dict_list,
    _schema_edges_with_inverses,
    append_inverses_to_pipeline,
)
from .merge_core import (
    edge_config_from_edges,
    merge_vertex_models,
    remap_relation_and_merge_edges,
    redirect_and_merge_edges,
)
from .ops import (
    AddEdgePropertiesOp,
    AddInverseEdgesOp,
    AddVertexPropertiesOp,
    MergeEdgesOp,
    MergeVerticesOp,
    ProjectManifestOp,
    RemoveEdgePropertiesOp,
    RemoveEdgesOp,
    RemoveVertexPropertiesOp,
    RemoveVerticesOp,
    RenameEdgePropertiesOp,
    RenameRelationsOp,
    RenameResourcesOp,
    RenameVertexPropertiesOp,
    RenameVerticesOp,
    SanitizeOp,
)
from .rewrite import (
    pipeline_mentions_any_vertex,
    rewrite_edge_properties_in_pipeline,
    rewrite_entity_names_in_pipeline,
    rewrite_extra_weights_vertex_field_names,
    rewrite_remove_relations_in_pipeline,
    rewrite_remove_edge_ids_in_pipeline,
    rewrite_remove_vertex_properties_in_pipeline,
    rewrite_vertex_field_names_in_pipeline,
    rewrite_vertex_names_in_pipeline,
    rewrite_vertex_names_in_value,
)
from .project import compute_projection
from .sanitize import (
    compute_vertex_field_renames,
    normalize_relation_identity,
)
from .version import bump_semver_minor

logger = logging.getLogger(__name__)


def _revalidate_db_profile(profile: DatabaseProfile) -> DatabaseProfile:
    """Re-run :class:`DatabaseProfile` validators after in-place edge_spec edits."""
    return DatabaseProfile.model_validate(profile.to_dict(skip_defaults=False))


def _actor_wrapper_mentions_removed(wrapper: Any, removed: set[str]) -> bool:
    return bool(wrapper.actor.references_vertices() & removed)


def _prune_ingestion_for_removed_vertices(
    im: IngestionModel, removed: set[str]
) -> None:
    """Drop or trim resources that reference removed vertex names."""
    to_drop: list[Any] = []
    for resource in list(im.resources):
        if pipeline_mentions_any_vertex(resource.pipeline, removed):
            to_drop.append(resource)
            continue
        root = ActorWrapper(*resource.pipeline)
        if _actor_wrapper_mentions_removed(root, removed):
            to_drop.append(resource)
            continue
        root.remove_descendants_if(
            lambda w: _actor_wrapper_mentions_removed(w, removed)
        )
        if not any(a.references_vertices() for a in root.collect_actors()):
            to_drop.append(resource)

    for r in to_drop:
        im.resources.remove(r)

    for i, r in enumerate(list(im.resources)):
        new_mc = [c for c in r.merge_collections if c not in removed]
        if new_mc != list(r.merge_collections):
            im.resources[i] = r.model_copy(
                update={"merge_collections": new_mc}, deep=True
            )

    if not im.resources:
        raise ValueError(
            "remove_vertices would leave ingestion_model.resources empty; aborting."
        )


def _filter_bindings_for_resources(
    manifest: GraphManifest, surviving: set[str]
) -> None:
    if manifest.bindings is None:
        return
    data = manifest.bindings.to_dict(skip_defaults=False)
    rc = data.get("resource_connector") or []
    filtered = []
    for entry in rc:
        if isinstance(entry, dict):
            name = entry.get("resource")
        else:
            name = getattr(entry, "resource", None)
        if name in surviving:
            filtered.append(entry)
    data["resource_connector"] = filtered
    from graflo.architecture.contract.bindings import Bindings

    manifest.bindings = Bindings.model_validate(data)


def _edge_id_from_resource_spec(spec: Any) -> EdgeId | None:
    """Extract logical edge id from infer/extra_weights dict payloads."""
    if not isinstance(spec, dict):
        return None
    edge_payload = spec.get("edge")
    if isinstance(edge_payload, dict):
        source = edge_payload.get("source") or edge_payload.get("from")
        target = edge_payload.get("target") or edge_payload.get("to")
        relation = edge_payload.get("relation")
    else:
        source = spec.get("source")
        target = spec.get("target")
        relation = spec.get("relation")
    if not isinstance(source, str) or not isinstance(target, str):
        return None
    rel = relation if isinstance(relation, str) else None
    return source, target, rel


def _apply_keep_resources(manifest: GraphManifest, allowed: set[str]) -> None:
    """Retain only ingestion resources (and bindings rows) in *allowed*."""
    if manifest.ingestion_model is None:
        return
    present = {resource.name for resource in manifest.ingestion_model.resources}
    missing = allowed - present
    if missing:
        raise ValueError(
            f"keep_resources not found on ingestion_model: {sorted(missing)}"
        )
    manifest.ingestion_model.resources = [
        resource
        for resource in manifest.ingestion_model.resources
        if resource.name in allowed
    ]
    manifest.ingestion_model = IngestionModel.model_validate(
        manifest.ingestion_model.to_dict(skip_defaults=False)
    )
    _filter_bindings_for_resources(manifest, allowed)
    if not manifest.ingestion_model.resources:
        raise ValueError(
            "project_manifest would leave ingestion_model.resources empty; aborting."
        )


def _bump_schema_version(
    manifest: GraphManifest, mode: bool | Literal["minor"]
) -> None:
    if manifest.graph_schema is None:
        return
    if mode is False:
        return
    if mode is True or mode == "minor":
        meta = manifest.graph_schema.metadata
        meta.version = bump_semver_minor(meta.version)


def apply_remove_vertices(manifest: GraphManifest, op: RemoveVerticesOp) -> None:
    """Mutate *manifest* in place: cascade-remove vertices (schema, ingestion, bindings)."""
    removed = set(op.names)
    schema = manifest.graph_schema
    if schema is None:
        raise ValueError("remove_vertices requires graph_schema")

    core = schema.core_schema
    missing = removed - core.vertex_config.vertex_set
    if missing:
        raise ValueError(f"Unknown vertices to remove: {sorted(missing)}")

    core.vertex_config.remove_vertices(removed)
    filtered_edges = [
        e
        for e in core.edge_config.edges
        if e.source not in removed and e.target not in removed
    ]
    schema.core_schema = CoreSchema(
        vertex_config=core.vertex_config,
        edge_config=EdgeConfig(edges=filtered_edges),
    )

    apply_vertex_removal_to_db_profile(schema.db_profile, removed)
    schema.db_profile = _revalidate_db_profile(schema.db_profile)

    if manifest.ingestion_model is not None:
        _prune_ingestion_for_removed_vertices(manifest.ingestion_model, removed)
        manifest.ingestion_model = IngestionModel.model_validate(
            manifest.ingestion_model.to_dict(skip_defaults=False)
        )
        surviving = {r.name for r in manifest.ingestion_model.resources}
        _filter_bindings_for_resources(manifest, surviving)


def _build_merged_vertex_config(
    vc: VertexConfig, sources: list[str], into: str
) -> VertexConfig:
    sset = set(sources)
    if into in sset:
        raise ValueError("merge_vertices: `sources` must not include `into`")
    missing = sset - vc.vertex_set
    if missing:
        raise ValueError(f"Unknown source vertices: {sorted(missing)}")

    into_exists = into in vc.vertex_set

    if into_exists:
        merged = merge_vertex_models([vc[into]] + [vc[s] for s in sources], into)
        new_vertices: list[Vertex] = []
        for v in vc.vertices:
            if v.name in sset:
                continue
            if v.name == into:
                new_vertices.append(merged)
            else:
                new_vertices.append(v)
    else:
        merged = merge_vertex_models([vc[s] for s in sources], into)
        new_vertices = [v for v in vc.vertices if v.name not in sset] + [merged]

    keys_to_merge = set(sources)
    if into_exists:
        keys_to_merge.add(into)
    accumulated: list[Any] = []
    for k in sorted(keys_to_merge):
        if k in vc.force_types:
            accumulated.extend(vc.force_types[k])
    seen_ft: set[Any] = set()
    deduped_ft: list[Any] = []
    for x in accumulated:
        if x not in seen_ft:
            seen_ft.add(x)
            deduped_ft.append(x)

    new_force = {k: v for k, v in vc.force_types.items() if k not in sset and k != into}
    if deduped_ft:
        new_force[into] = deduped_ft

    return VertexConfig(
        vertices=new_vertices,
        force_types=new_force,
    )


def _rewrite_ingestion_for_merge(im: IngestionModel, mapping: dict[str, str]) -> None:
    from graflo.architecture.contract.ingestion.resource import Resource

    new_resources: list[Resource] = []
    for r in im.resources:
        d = r.to_dict(skip_defaults=False)
        d["pipeline"] = rewrite_vertex_names_in_pipeline(r.pipeline, mapping)
        d["merge_collections"] = [mapping.get(c, c) for c in r.merge_collections]
        if d.get("infer_edge_only"):
            d["infer_edge_only"] = rewrite_vertex_names_in_value(
                d["infer_edge_only"], mapping
            )
        if d.get("infer_edge_except"):
            d["infer_edge_except"] = rewrite_vertex_names_in_value(
                d["infer_edge_except"], mapping
            )
        if d.get("extra_weights"):
            d["extra_weights"] = rewrite_vertex_names_in_value(
                d["extra_weights"], mapping
            )
        new_resources.append(Resource.model_validate(d))
    im.resources = new_resources


def apply_merge_vertices(manifest: GraphManifest, op: MergeVerticesOp) -> None:
    """Mutate *manifest* in place: merge source vertices into ``into``."""
    schema = manifest.graph_schema
    if schema is None:
        raise ValueError("merge_vertices requires graph_schema")

    sources = list(op.sources)
    into = op.into
    sset = set(sources)
    if into in sset:
        raise ValueError("merge_vertices: `into` must not appear in `sources`")

    core = schema.core_schema
    new_vc = _build_merged_vertex_config(core.vertex_config, sources, into)
    m = {s: into for s in sources}
    merged_edges = redirect_and_merge_edges(core.edge_config.edges, m)

    schema.core_schema = CoreSchema(
        vertex_config=new_vc,
        edge_config=edge_config_from_edges(merged_edges),
    )
    apply_vertex_merge_to_db_profile(schema.db_profile, sset, into)
    schema.db_profile = _revalidate_db_profile(schema.db_profile)

    if manifest.ingestion_model is not None:
        _rewrite_ingestion_for_merge(manifest.ingestion_model, m)
        manifest.ingestion_model = IngestionModel.model_validate(
            manifest.ingestion_model.to_dict(skip_defaults=False)
        )


def _rename_fields_in_schema(
    schema: Schema, renames: dict[str, dict[str, str]]
) -> None:
    """Mutate vertex properties + identity in place per the rename map."""
    if not renames:
        return
    for vertex in schema.core_schema.vertex_config.vertices:
        per_vertex = renames.get(vertex.name)
        if not per_vertex:
            continue
        # Update identity first so Vertex.validate_assignment doesn't re-add
        # stale pre-rename identity names into properties as type=None ghosts.
        renamed_identity: list[str] = []
        seen_identity: set[str] = set()
        for name in vertex.identity:
            new_name = per_vertex.get(name, name)
            if new_name in seen_identity:
                continue
            seen_identity.add(new_name)
            renamed_identity.append(new_name)
        vertex.identity = renamed_identity

        new_properties: list[Field] = []
        seen_names: set[str] = set()
        for field in vertex.properties:
            new_name = per_vertex.get(field.name, field.name)
            if new_name in seen_names:
                continue
            seen_names.add(new_name)
            if new_name == field.name:
                new_properties.append(field)
            else:
                new_properties.append(field.model_copy(update={"name": new_name}))
        vertex.properties = new_properties


def _rebuild_ingestion_with_pipeline_rewrite(
    manifest: GraphManifest,
    rewriter,  # callable: pipeline -> new pipeline
    *,
    vertex_field_renames: dict[str, dict[str, str]] | None = None,
) -> None:
    """Rebuild ``manifest.ingestion_model`` after applying *rewriter* to each
    resource's pipeline.

    When *vertex_field_renames* is non-empty, ``Resource.extra_weights`` vertex
    weight rules are updated to reference renamed vertex fields (``fields``, and
    ``map`` / ``filter`` keys that address vertex observation columns).
    """
    if manifest.ingestion_model is None:
        return
    from graflo.architecture.contract.ingestion.resource import Resource

    renames_ctx = vertex_field_renames if vertex_field_renames else {}

    new_resources: list[Resource] = []
    for resource in manifest.ingestion_model.resources:
        d = resource.to_dict(skip_defaults=False)
        d["pipeline"] = rewriter(resource.pipeline)
        ew = d.get("extra_weights")
        if isinstance(ew, list) and renames_ctx:
            d["extra_weights"] = rewrite_extra_weights_vertex_field_names(
                ew, renames_ctx
            )
        new_resources.append(Resource.model_validate(d))
    manifest.ingestion_model.resources = new_resources
    manifest.ingestion_model = IngestionModel.model_validate(
        manifest.ingestion_model.to_dict(skip_defaults=False)
    )


def apply_rename_vertex_properties(
    manifest: GraphManifest, op: RenameVertexPropertiesOp
) -> None:
    """Rename vertex properties (and their references) across the manifest.

    Mutates *manifest* in place:

    - Rewrites schema ``Field.name`` and ``vertex.identity``.
    - Rewrites :class:`DatabaseProfile` field references (vertex_indexes,
      edge_specs.indexes, default_property_values).
    - Rewrites resource pipelines so that ``VertexActor.from`` covers the
      rename and ``TransformActor.rename`` produces the renamed property
      (see :func:`rewrite_vertex_field_names_in_pipeline`).
    - Rewrites ``Resource.extra_weights`` / ``vertex_weights`` (and any
      ``vertex_weights`` embedded in ``edge`` pipeline steps).
    """
    if not op.renames:
        return
    schema = manifest.graph_schema
    if schema is None:
        raise ValueError("rename_vertex_properties requires graph_schema")

    unknown = sorted(set(op.renames) - schema.core_schema.vertex_config.vertex_set)
    if unknown:
        raise ValueError(
            f"rename_vertex_properties: unknown vertices in renames: {unknown}"
        )

    _rename_fields_in_schema(schema, op.renames)
    apply_field_rename_to_db_profile(schema.db_profile, op.renames)
    schema.db_profile = _revalidate_db_profile(schema.db_profile)
    schema.finish_init()

    _rebuild_ingestion_with_pipeline_rewrite(
        manifest,
        lambda pipeline: rewrite_vertex_field_names_in_pipeline(pipeline, op.renames),
        vertex_field_renames=op.renames,
    )


def apply_remove_vertex_properties(
    manifest: GraphManifest, op: RemoveVertexPropertiesOp
) -> None:
    """Remove vertex properties and clean up ingestion/db profile references."""
    if not op.removals:
        return
    schema = manifest.graph_schema
    if schema is None:
        raise ValueError("remove_vertex_properties requires graph_schema")

    unknown_vertices = sorted(
        set(op.removals) - schema.core_schema.vertex_config.vertex_set
    )
    if unknown_vertices:
        raise ValueError(
            f"remove_vertex_properties: unknown vertices in removals: {unknown_vertices}"
        )

    removals = {
        vertex_name: {field for field in fields if isinstance(field, str)}
        for vertex_name, fields in op.removals.items()
    }
    if not removals:
        return

    for vertex in schema.core_schema.vertex_config.vertices:
        remove_fields = removals.get(vertex.name, set())
        if not remove_fields:
            continue
        identity_overlap = sorted(set(vertex.identity) & remove_fields)
        if identity_overlap:
            raise ValueError(
                "remove_vertex_properties cannot remove identity fields "
                f"for vertex {vertex.name}: {identity_overlap}"
            )
        vertex.properties = [
            field for field in vertex.properties if field.name not in remove_fields
        ]

    for vertex_name, indexes in list(schema.db_profile.vertex_indexes.items()):
        remove_fields = removals.get(vertex_name, set())
        if not remove_fields:
            continue
        updated_indexes = []
        for index in indexes:
            fields = [field for field in index.fields if field not in remove_fields]
            if fields:
                updated_indexes.append(index.model_copy(update={"fields": fields}))
        schema.db_profile.vertex_indexes[vertex_name] = updated_indexes

    for edge_spec in schema.db_profile.edge_specs:
        updated_indexes = []
        for index in edge_spec.indexes:
            fields = list(index.fields)
            source_removals = removals.get(edge_spec.source, set())
            target_removals = removals.get(edge_spec.target, set())
            if source_removals:
                fields = [field for field in fields if field not in source_removals]
            if target_removals:
                fields = [field for field in fields if field not in target_removals]
            if fields:
                updated_indexes.append(index.model_copy(update={"fields": fields}))
        edge_spec.indexes = updated_indexes

    schema.db_profile = _revalidate_db_profile(schema.db_profile)
    schema.finish_init()

    if manifest.ingestion_model is not None:
        _rebuild_ingestion_with_pipeline_rewrite(
            manifest,
            lambda pipeline: rewrite_remove_vertex_properties_in_pipeline(
                pipeline, removals
            ),
        )
        for resource in manifest.ingestion_model.resources:
            if resource.extra_weights:
                for entry in resource.extra_weights:
                    for weight in entry.vertex_weights:
                        if not isinstance(weight.name, str):
                            continue
                        remove_fields = removals.get(weight.name, set())
                        if not remove_fields:
                            continue
                        weight.fields = [
                            field
                            for field in weight.fields
                            if field not in remove_fields
                        ]
                        weight.map = {
                            key: value
                            for key, value in weight.map.items()
                            if key not in remove_fields
                        }
                        weight.filter = {
                            key: value
                            for key, value in weight.filter.items()
                            if key not in remove_fields
                        }


def _apply_rename_entities(
    manifest: GraphManifest,
    *,
    vertex_map: dict[str, str] | None = None,
    edge_map: dict[str, str] | None = None,
    resource_map: dict[str, str] | None = None,
) -> None:
    """Rename logical vertices/relations/resources across manifest payload blocks."""
    payload = manifest.to_dict(skip_defaults=False)

    vertex_map = vertex_map or {}
    edge_map = edge_map or {}
    resource_map = resource_map or {}

    schema_payload = payload.get("schema")
    if isinstance(schema_payload, dict):
        graph_payload = schema_payload.get("core_schema")
        if isinstance(graph_payload, dict):
            vertex_config = graph_payload.get("vertex_config")
            if isinstance(vertex_config, dict):
                vertices_payload = vertex_config.get("vertices")
                if isinstance(vertices_payload, list):
                    for vertex in vertices_payload:
                        if isinstance(vertex, dict) and isinstance(
                            vertex.get("name"), str
                        ):
                            vertex["name"] = vertex_map.get(
                                vertex["name"], vertex["name"]
                            )

                force_types = vertex_config.get("force_types")
                if isinstance(force_types, dict):
                    vertex_config["force_types"] = {
                        vertex_map.get(name, name): value
                        for name, value in force_types.items()
                    }

            edge_config = graph_payload.get("edge_config")
            if isinstance(edge_config, dict):
                edges_payload = edge_config.get("edges")
                if isinstance(edges_payload, list):
                    for edge in edges_payload:
                        if not isinstance(edge, dict):
                            continue
                        if isinstance(edge.get("source"), str):
                            edge["source"] = vertex_map.get(
                                edge["source"], edge["source"]
                            )
                        if isinstance(edge.get("target"), str):
                            edge["target"] = vertex_map.get(
                                edge["target"], edge["target"]
                            )
                        if isinstance(edge.get("relation"), str):
                            edge["relation"] = edge_map.get(
                                edge["relation"], edge["relation"]
                            )

    ingestion_payload = payload.get("ingestion_model")
    if isinstance(ingestion_payload, dict):
        resources_payload = ingestion_payload.get("resources")
        if isinstance(resources_payload, list):
            for resource in resources_payload:
                if not isinstance(resource, dict):
                    continue
                if isinstance(resource.get("name"), str):
                    resource["name"] = resource_map.get(
                        resource["name"], resource["name"]
                    )

                pipeline = resource.get("pipeline")
                if isinstance(pipeline, list):
                    rewrite_entity_names_in_pipeline(
                        pipeline,
                        vertices=vertex_map,
                        edges=edge_map,
                    )

                for spec_key in ("infer_edge_only", "infer_edge_except"):
                    specs = resource.get(spec_key)
                    if not isinstance(specs, list):
                        continue
                    for spec in specs:
                        if not isinstance(spec, dict):
                            continue
                        if isinstance(spec.get("source"), str):
                            spec["source"] = vertex_map.get(
                                spec["source"], spec["source"]
                            )
                        if isinstance(spec.get("target"), str):
                            spec["target"] = vertex_map.get(
                                spec["target"], spec["target"]
                            )
                        if isinstance(spec.get("relation"), str):
                            spec["relation"] = edge_map.get(
                                spec["relation"], spec["relation"]
                            )

                extra_weights = resource.get("extra_weights")
                if isinstance(extra_weights, list):
                    for entry in extra_weights:
                        if not isinstance(entry, dict):
                            continue
                        edge = entry.get("edge")
                        if isinstance(edge, dict):
                            if isinstance(edge.get("source"), str):
                                edge["source"] = vertex_map.get(
                                    edge["source"], edge["source"]
                                )
                            if isinstance(edge.get("target"), str):
                                edge["target"] = vertex_map.get(
                                    edge["target"], edge["target"]
                                )
                            if isinstance(edge.get("relation"), str):
                                edge["relation"] = edge_map.get(
                                    edge["relation"], edge["relation"]
                                )

    bindings_payload = payload.get("bindings")
    if isinstance(bindings_payload, dict):
        connectors = bindings_payload.get("connectors")
        if isinstance(connectors, list):
            for connector in connectors:
                if isinstance(connector, dict) and isinstance(
                    connector.get("resource_name"), str
                ):
                    connector["resource_name"] = resource_map.get(
                        connector["resource_name"],
                        connector["resource_name"],
                    )

        resource_connector = bindings_payload.get("resource_connector")
        if isinstance(resource_connector, list):
            for mapping in resource_connector:
                if isinstance(mapping, dict) and isinstance(
                    mapping.get("resource"), str
                ):
                    mapping["resource"] = resource_map.get(
                        mapping["resource"],
                        mapping["resource"],
                    )

    schema = manifest.graph_schema
    if schema is not None and (vertex_map or edge_map):
        if vertex_map:
            apply_vertex_rename_to_db_profile(schema.db_profile, vertex_map)
        if edge_map:
            apply_relation_rename_to_db_profile(schema.db_profile, edge_map)
        if isinstance(schema_payload, dict):
            schema_payload["db_profile"] = schema.db_profile.to_dict(
                skip_defaults=False
            )

    updated = GraphManifest.from_dict(payload)
    manifest.graph_schema = updated.graph_schema
    manifest.ingestion_model = updated.ingestion_model
    manifest.bindings = updated.bindings


def apply_rename_vertices(manifest: GraphManifest, op: RenameVerticesOp) -> None:
    """Rename logical vertex names across schema/ingestion/bindings."""
    _apply_rename_entities(manifest, vertex_map=op.vertices)


def apply_rename_relations(manifest: GraphManifest, op: RenameRelationsOp) -> None:
    """Rename logical relation names across schema/ingestion/db profile."""
    _apply_rename_entities(manifest, edge_map=op.relations)
    schema = manifest.graph_schema
    if schema is None:
        return
    apply_relation_rename_to_db_profile(schema.db_profile, op.relations)
    merge_relation_entries_in_db_profile(schema.db_profile)
    schema.db_profile = _revalidate_db_profile(schema.db_profile)
    schema.finish_init()


def apply_rename_resources(manifest: GraphManifest, op: RenameResourcesOp) -> None:
    """Rename ingestion resources and bindings references."""
    _apply_rename_entities(manifest, resource_map=op.resources)


def apply_remove_edges(manifest: GraphManifest, op: RemoveEdgesOp) -> None:
    """Remove edges by relation name and prune related references."""
    removed = set(op.relations)
    if not removed:
        return
    schema = manifest.graph_schema
    if schema is None:
        raise ValueError("remove_edges requires graph_schema")
    apply_relation_removal_to_db_profile(schema.db_profile, removed)
    schema.db_profile = _revalidate_db_profile(schema.db_profile)
    schema.core_schema = CoreSchema(
        vertex_config=schema.core_schema.vertex_config,
        edge_config=EdgeConfig(
            edges=[
                edge
                for edge in schema.core_schema.edge_config.edges
                if edge.relation not in removed
            ]
        ),
    )
    schema.finish_init()

    if manifest.ingestion_model is not None:
        from graflo.architecture.contract.ingestion.resource import Resource

        resources: list[Resource] = []
        for resource in manifest.ingestion_model.resources:
            payload = resource.to_dict(skip_defaults=False)
            pipeline = payload.get("pipeline")
            if isinstance(pipeline, list):
                payload["pipeline"] = rewrite_remove_relations_in_pipeline(
                    pipeline, removed
                )
            for key in ("infer_edge_only", "infer_edge_except"):
                specs = payload.get(key)
                if isinstance(specs, list):
                    payload[key] = [
                        spec
                        for spec in specs
                        if not (
                            isinstance(spec, dict) and spec.get("relation") in removed
                        )
                    ]
            extra_weights = payload.get("extra_weights")
            if isinstance(extra_weights, list):
                payload["extra_weights"] = [
                    entry
                    for entry in extra_weights
                    if not (
                        isinstance(entry, dict)
                        and isinstance(entry.get("edge"), dict)
                        and entry["edge"].get("relation") in removed
                    )
                ]
            resources.append(Resource.model_validate(payload))
        manifest.ingestion_model.resources = resources
        manifest.ingestion_model = IngestionModel.model_validate(
            manifest.ingestion_model.to_dict(skip_defaults=False)
        )


def apply_remove_edge_ids(
    manifest: GraphManifest, removed_edge_ids: set[EdgeId]
) -> None:
    """Remove edges by logical triple and prune related references."""
    if not removed_edge_ids:
        return
    schema = manifest.graph_schema
    if schema is None:
        raise ValueError("remove_edge_ids requires graph_schema")

    apply_edge_id_removal_to_db_profile(schema.db_profile, removed_edge_ids)
    schema.db_profile = _revalidate_db_profile(schema.db_profile)
    schema.core_schema = CoreSchema(
        vertex_config=schema.core_schema.vertex_config,
        edge_config=EdgeConfig(
            edges=[
                edge
                for edge in schema.core_schema.edge_config.edges
                if edge.edge_id not in removed_edge_ids
            ]
        ),
    )
    schema.finish_init()

    if manifest.ingestion_model is None:
        return

    from graflo.architecture.contract.ingestion.resource import Resource

    resources: list[Resource] = []
    for resource in manifest.ingestion_model.resources:
        payload = resource.to_dict(skip_defaults=False)
        pipeline = payload.get("pipeline")
        if isinstance(pipeline, list):
            payload["pipeline"] = rewrite_remove_edge_ids_in_pipeline(
                pipeline, removed_edge_ids
            )
        for key in ("infer_edge_only", "infer_edge_except"):
            specs = payload.get(key)
            if isinstance(specs, list):
                payload[key] = [
                    spec
                    for spec in specs
                    if _edge_id_from_resource_spec(spec) not in removed_edge_ids
                ]
        extra_weights = payload.get("extra_weights")
        if isinstance(extra_weights, list):
            payload["extra_weights"] = [
                entry
                for entry in extra_weights
                if _edge_id_from_resource_spec(entry) not in removed_edge_ids
            ]
        resources.append(Resource.model_validate(payload))
    manifest.ingestion_model.resources = resources
    manifest.ingestion_model = IngestionModel.model_validate(
        manifest.ingestion_model.to_dict(skip_defaults=False)
    )


def apply_project_manifest(manifest: GraphManifest, op: ProjectManifestOp) -> None:
    """Project manifest to surviving vertices/edges with consistent cascade."""
    plan = compute_projection(manifest, op)
    if plan.removed_edge_ids:
        apply_remove_edge_ids(manifest, plan.removed_edge_ids)
    if plan.removed_vertices:
        apply_remove_vertices(
            manifest,
            RemoveVerticesOp(names=sorted(plan.removed_vertices)),
        )
    if op.keep_resources is not None:
        _apply_keep_resources(manifest, set(op.keep_resources))


def apply_merge_edges(manifest: GraphManifest, op: MergeEdgesOp) -> None:
    """Merge edge relation names into one canonical relation."""
    if op.into in set(op.sources):
        raise ValueError("merge_edges: `sources` must not include `into`")
    relation_map = {source: op.into for source in op.sources}
    apply_rename_relations(manifest, RenameRelationsOp(relations=relation_map))
    schema = manifest.graph_schema
    if schema is None:
        return
    merged_edges = remap_relation_and_merge_edges(
        schema.core_schema.edge_config.edges, relation_map
    )
    schema.core_schema = CoreSchema(
        vertex_config=schema.core_schema.vertex_config,
        edge_config=edge_config_from_edges(merged_edges),
    )
    schema.finish_init()


def apply_rename_edge_properties(
    manifest: GraphManifest, op: RenameEdgePropertiesOp
) -> None:
    """Rename edge properties by relation and propagate references."""
    schema = manifest.graph_schema
    if schema is None:
        raise ValueError("rename_edge_properties requires graph_schema")
    for edge in schema.core_schema.edge_config.edges:
        per_relation = (
            op.renames.get(edge.relation, {}) if edge.relation is not None else {}
        )
        if not per_relation:
            continue
        seen: set[str] = set()
        new_properties: list[Field] = []
        for field in edge.properties:
            new_name = per_relation.get(field.name, field.name)
            if new_name in seen:
                continue
            seen.add(new_name)
            new_properties.append(field.model_copy(update={"name": new_name}))
        edge.properties = new_properties
        edge.identities = [
            [
                per_relation.get(token, token)
                if token not in {"source", "target", "relation"}
                else token
                for token in identity
            ]
            for identity in edge.identities
        ]
    apply_edge_property_rename_to_db_profile(schema.db_profile, op.renames)
    schema.db_profile = _revalidate_db_profile(schema.db_profile)
    schema.finish_init()

    _rebuild_ingestion_with_pipeline_rewrite(
        manifest,
        lambda pipeline: rewrite_edge_properties_in_pipeline(
            pipeline, renames_by_relation=op.renames
        ),
    )


def apply_remove_edge_properties(
    manifest: GraphManifest, op: RemoveEdgePropertiesOp
) -> None:
    """Remove edge properties by relation and clean references."""
    schema = manifest.graph_schema
    if schema is None:
        raise ValueError("remove_edge_properties requires graph_schema")
    removals = {
        relation: {field for field in fields if isinstance(field, str)}
        for relation, fields in op.removals.items()
    }
    for edge in schema.core_schema.edge_config.edges:
        remove_fields = (
            removals.get(edge.relation, set()) if edge.relation is not None else set()
        )
        if not remove_fields:
            continue
        blocked_tokens = set().union(
            *[
                set(identity) - {"source", "target", "relation"}
                for identity in edge.identities
            ]
        )
        overlap = sorted(blocked_tokens & remove_fields)
        if overlap:
            raise ValueError(
                "remove_edge_properties cannot remove identity fields "
                f"for relation {edge.relation}: {overlap}"
            )
        edge.properties = [
            field for field in edge.properties if field.name not in remove_fields
        ]
    apply_edge_property_removal_to_db_profile(schema.db_profile, removals)
    schema.db_profile = _revalidate_db_profile(schema.db_profile)
    schema.finish_init()
    _rebuild_ingestion_with_pipeline_rewrite(
        manifest,
        lambda pipeline: rewrite_edge_properties_in_pipeline(
            pipeline, removals_by_relation=removals
        ),
    )


def apply_add_vertex_properties(
    manifest: GraphManifest, op: AddVertexPropertiesOp
) -> None:
    """Append new vertex properties to existing vertices."""
    schema = manifest.graph_schema
    if schema is None:
        raise ValueError("add_vertex_properties requires graph_schema")
    unknown = set(op.additions) - schema.core_schema.vertex_config.vertex_set
    if unknown:
        raise ValueError(f"add_vertex_properties: unknown vertices: {sorted(unknown)}")
    for vertex in schema.core_schema.vertex_config.vertices:
        additions = op.additions.get(vertex.name, [])
        if not additions:
            continue
        existing = {field.name for field in vertex.properties}
        for name in additions:
            if name in existing:
                continue
            vertex.properties.append(Field(name=name, type=None))
            existing.add(name)
    schema.finish_init()


def apply_add_edge_properties(manifest: GraphManifest, op: AddEdgePropertiesOp) -> None:
    """Append new edge properties to existing relations."""
    schema = manifest.graph_schema
    if schema is None:
        raise ValueError("add_edge_properties requires graph_schema")
    for edge in schema.core_schema.edge_config.edges:
        additions = (
            op.additions.get(edge.relation, []) if edge.relation is not None else []
        )
        if not additions:
            continue
        existing = {field.name for field in edge.properties}
        for name in additions:
            if name in existing:
                continue
            edge.properties.append(Field(name=name, type=None))
            existing.add(name)
    schema.finish_init()


def apply_add_inverse_edges(manifest: GraphManifest, op: AddInverseEdgesOp) -> None:
    """Add inverse edges for mapped relations across schema and ingestion resources."""
    relation_map = {
        source: target
        for source, target in op.relations.items()
        if isinstance(source, str) and isinstance(target, str)
    }
    if not relation_map:
        return

    schema = manifest.graph_schema
    if schema is None:
        raise ValueError("add_inverse_edges requires graph_schema")

    new_edges = _schema_edges_with_inverses(
        list(schema.core_schema.edge_config.edges),
        relation_map,
        schema.db_profile,
    )
    schema.core_schema = CoreSchema(
        vertex_config=schema.core_schema.vertex_config,
        edge_config=edge_config_from_edges(new_edges),
    )
    apply_inverse_edges_to_db_profile(schema.db_profile, relation_map, new_edges)
    schema.db_profile = _revalidate_db_profile(schema.db_profile)
    schema.finish_init()

    if manifest.ingestion_model is None:
        return

    from graflo.architecture.contract.ingestion.resource import Resource

    resources: list[Resource] = []
    for resource in manifest.ingestion_model.resources:
        payload = resource.to_dict(skip_defaults=False)

        pipeline_steps = _as_dict_list(payload.get("pipeline"))
        if pipeline_steps:
            payload["pipeline"] = append_inverses_to_pipeline(
                pipeline_steps,
                relation_map,
                new_edges,
            )

        for spec_key in ("infer_edge_only", "infer_edge_except"):
            spec_dicts = _as_dict_list(payload.get(spec_key))
            if spec_dicts:
                payload[spec_key] = _append_inverse_flat_specs(spec_dicts, relation_map)

        extra_entries = _as_dict_list(payload.get("extra_weights"))
        if extra_entries:
            payload["extra_weights"] = _append_inverses_for_nested_edges(
                extra_entries,
                relation_map,
                edge_key="edge",
                schema_edges=new_edges,
            )

        resources.append(Resource.model_validate(payload))

    manifest.ingestion_model.resources = resources
    manifest.ingestion_model = IngestionModel.model_validate(
        manifest.ingestion_model.to_dict(skip_defaults=False)
    )


def apply_sanitize(manifest: GraphManifest, op: SanitizeOp) -> None:
    """Apply DB-flavor-specific sanitization to *manifest* in place.

    Composes:

    1. Storage-name sanitization on :class:`DatabaseProfile`.
    2. Reserved-word vertex field renames (via ``apply_rename_vertex_properties``).
    3. TigerGraph identity normalization (cross-relation), propagated to
       ingestion via the same field-rename code path.
    """
    from graflo.db.util import load_reserved_words
    from graflo.onto import DBType

    if manifest.graph_schema is None:
        return

    schema = manifest.graph_schema
    if op.reserved_words is not None:
        reserved_words = {word.upper() for word in op.reserved_words}
    else:
        reserved_words = load_reserved_words(op.db_flavor)

    run_name_sanitization = bool(reserved_words) or op.db_flavor == DBType.TIGERGRAPH
    if run_name_sanitization:
        apply_storage_name_sanitization_to_db_profile(
            schema.db_profile,
            schema,
            reserved_words,
            db_flavor=op.db_flavor,
        )
        schema.db_profile = _revalidate_db_profile(schema.db_profile)

        field_renames = compute_vertex_field_renames(
            schema, reserved_words, db_flavor=op.db_flavor
        )
        if field_renames:
            apply_rename_vertex_properties(
                manifest,
                RenameVertexPropertiesOp(renames=field_renames),
            )

    identity_renames = normalize_relation_identity(schema, op.db_flavor)
    if identity_renames:
        apply_field_rename_to_db_profile(schema.db_profile, identity_renames)
        schema.db_profile = _revalidate_db_profile(schema.db_profile)
        schema.finish_init()
        _rebuild_ingestion_with_pipeline_rewrite(
            manifest,
            lambda pipeline: rewrite_vertex_field_names_in_pipeline(
                pipeline, identity_renames
            ),
            vertex_field_renames=identity_renames,
        )


def _dispatch_op(manifest: GraphManifest, op: Any) -> None:
    """Dispatch a single evolution op to its in-place apply function."""
    if isinstance(op, RemoveVerticesOp):
        apply_remove_vertices(manifest, op)
    elif isinstance(op, MergeVerticesOp):
        apply_merge_vertices(manifest, op)
    elif isinstance(op, RenameVertexPropertiesOp):
        apply_rename_vertex_properties(manifest, op)
    elif isinstance(op, RemoveVertexPropertiesOp):
        apply_remove_vertex_properties(manifest, op)
    elif isinstance(op, AddVertexPropertiesOp):
        apply_add_vertex_properties(manifest, op)
    elif isinstance(op, RenameVerticesOp):
        apply_rename_vertices(manifest, op)
    elif isinstance(op, RenameRelationsOp):
        apply_rename_relations(manifest, op)
    elif isinstance(op, RenameResourcesOp):
        apply_rename_resources(manifest, op)
    elif isinstance(op, RemoveEdgesOp):
        apply_remove_edges(manifest, op)
    elif isinstance(op, MergeEdgesOp):
        apply_merge_edges(manifest, op)
    elif isinstance(op, RenameEdgePropertiesOp):
        apply_rename_edge_properties(manifest, op)
    elif isinstance(op, RemoveEdgePropertiesOp):
        apply_remove_edge_properties(manifest, op)
    elif isinstance(op, AddEdgePropertiesOp):
        apply_add_edge_properties(manifest, op)
    elif isinstance(op, AddInverseEdgesOp):
        apply_add_inverse_edges(manifest, op)
    elif isinstance(op, ProjectManifestOp):
        apply_project_manifest(manifest, op)
    elif isinstance(op, SanitizeOp):
        apply_sanitize(manifest, op)
    else:
        raise TypeError(f"Unsupported evolution op: {type(op)!r}")


def apply_manifest_ops_inplace(
    manifest: GraphManifest,
    ops: Sequence[
        RemoveVerticesOp
        | MergeVerticesOp
        | RenameVertexPropertiesOp
        | RemoveVertexPropertiesOp
        | AddVertexPropertiesOp
        | RenameVerticesOp
        | RenameRelationsOp
        | RenameResourcesOp
        | RemoveEdgesOp
        | MergeEdgesOp
        | RenameEdgePropertiesOp
        | RemoveEdgePropertiesOp
        | AddEdgePropertiesOp
        | AddInverseEdgesOp
        | ProjectManifestOp
        | SanitizeOp
    ],
) -> None:
    """Apply each evolution op to *manifest* in place.

    Does not copy the manifest, bump schema version, or call :meth:`GraphManifest.finish_init`.
    Callers that need re-validation after mutation should invoke ``finish_init`` themselves.
    """
    for op in ops:
        _dispatch_op(manifest, op)


def apply_evolution(
    manifest: GraphManifest,
    ops: Sequence[
        RemoveVerticesOp
        | MergeVerticesOp
        | RenameVertexPropertiesOp
        | RemoveVertexPropertiesOp
        | AddVertexPropertiesOp
        | RenameVerticesOp
        | RenameRelationsOp
        | RenameResourcesOp
        | RemoveEdgesOp
        | MergeEdgesOp
        | RenameEdgePropertiesOp
        | RemoveEdgePropertiesOp
        | AddEdgePropertiesOp
        | AddInverseEdgesOp
        | ProjectManifestOp
        | SanitizeOp
    ],
    *,
    bump_version: bool | Literal["minor"] = "minor",
    finish_init: bool = True,
    strict_references: bool = False,
    dynamic_edge_feedback: bool = False,
) -> GraphManifest:
    """Return a deep copy of *manifest* with *ops* applied and optionally re-initialized.

    Compare before/after contract identity with :func:`graflo.migrate.io.manifest_hash`
    (stable hash over schema, ingestion_model, and bindings blocks).
    """
    out = manifest.model_copy(deep=True)

    for op in ops:
        _dispatch_op(out, op)

    _bump_schema_version(out, bump_version)

    if finish_init:
        out.finish_init(
            strict_references=strict_references,
            dynamic_edge_feedback=dynamic_edge_feedback,
        )
    return out
