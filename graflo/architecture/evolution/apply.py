"""Apply manifest evolution operations to a copy of a :class:`~graflo.architecture.contract.manifest.GraphManifest`."""

from __future__ import annotations

from typing import Any, Literal, Sequence

from graflo.architecture.contract.declarations.ingestion_model import IngestionModel
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.database_features import DatabaseProfile
from graflo.architecture.schema.core import CoreSchema
from graflo.architecture.schema.edge import EdgeConfig
from graflo.architecture.schema.vertex import Vertex, VertexConfig

from .db_profile import (
    apply_vertex_merge_to_db_profile,
    apply_vertex_removal_to_db_profile,
)
from .merge_core import (
    edge_config_from_edges,
    merge_vertex_models,
    redirect_and_merge_edges,
)
from .ops import MergeVerticesOp, RemoveVerticesOp
from .rewrite import (
    pipeline_mentions_any_vertex,
    rewrite_vertex_names_in_pipeline,
    rewrite_vertex_names_in_value,
)
from .version import bump_semver_minor


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
        root = resource.root
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

    new_blank = [b for b in vc.blank_vertices if b not in sset]
    was_blank = any(b in sset for b in vc.blank_vertices) or (
        into_exists and into in vc.blank_vertices
    )
    if was_blank and into not in new_blank:
        new_blank.append(into)

    new_force = {k: v for k, v in vc.force_types.items() if k not in sset and k != into}
    if deduped_ft:
        new_force[into] = deduped_ft

    return VertexConfig(
        vertices=new_vertices,
        blank_vertices=new_blank,
        force_types=new_force,
    )


def _rewrite_ingestion_for_merge(im: IngestionModel, mapping: dict[str, str]) -> None:
    from graflo.architecture.contract.declarations.resource import Resource

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


def apply_evolution(
    manifest: GraphManifest,
    ops: Sequence[RemoveVerticesOp | MergeVerticesOp],
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
        if isinstance(op, RemoveVerticesOp):
            apply_remove_vertices(out, op)
        elif isinstance(op, MergeVerticesOp):
            apply_merge_vertices(out, op)
        else:
            raise TypeError(f"Unsupported evolution op: {type(op)!r}")

    _bump_schema_version(out, bump_version)

    if finish_init:
        out.finish_init(
            strict_references=strict_references,
            dynamic_edge_feedback=dynamic_edge_feedback,
        )
    return out
