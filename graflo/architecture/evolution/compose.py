"""Binary compose of two :class:`~graflo.architecture.contract.manifest.GraphManifest`s."""

from __future__ import annotations

from typing import Any, Literal

from graflo.architecture.contract.bindings import Bindings
from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.database_features import DatabaseProfile
from graflo.architecture.graph_types import EdgeId
from graflo.architecture.schema.core import CoreSchema
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.vertex import Vertex, VertexConfig

from .apply import (
    _bump_schema_version,
    _revalidate_db_profile,
    apply_rename_relations,
    apply_rename_resources,
    apply_rename_vertex_properties,
    apply_rename_vertices,
)
from .merge_core import (
    edge_config_from_edges,
    merge_edge_pair,
    merge_vertex_models,
)
from .ops import (
    ComposeManifestsOp,
    RenameRelationsOp,
    RenameResourcesOp,
    RenameVertexPropertiesOp,
    RenameVerticesOp,
    VertexEquivalence,
)

_RIGHT_PREFIX = "r_"


def _prefixed(name: str) -> str:
    if name.startswith(_RIGHT_PREFIX):
        return name
    return f"{_RIGHT_PREFIX}{name}"


def _property_rename_maps(
    equivalences: list[VertexEquivalence],
) -> tuple[dict[str, dict[str, str]], dict[str, dict[str, str]]]:
    """Build per-side ``{vertex: {old: into}}`` maps from property equivalences."""
    left_renames: dict[str, dict[str, str]] = {}
    right_renames: dict[str, dict[str, str]] = {}
    for veq in equivalences:
        left_map: dict[str, str] = {}
        right_map: dict[str, str] = {}
        for pe in veq.properties:
            if pe.left is not None:
                left_map[pe.left] = pe.into
            if pe.right is not None:
                right_map[pe.right] = pe.into
        if left_map:
            left_renames[veq.left] = left_map
        if right_map:
            right_renames[veq.right] = right_map
    return left_renames, right_renames


def _vertex_rename_map(
    equivalences: list[VertexEquivalence], *, side: Literal["left", "right"]
) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for veq in equivalences:
        source = veq.left if side == "left" else veq.right
        if source != veq.into:
            mapping[source] = veq.into
    return mapping


def _relation_rename_map(
    equivalences: list, *, side: Literal["left", "right"]
) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for req in equivalences:
        source = req.left if side == "left" else req.right
        if source != req.into:
            mapping[source] = req.into
    return mapping


def _resolve_name_collisions(
    occupied: set[str],
    candidates: list[str],
    *,
    name_conflict: Literal["error", "prefix_right"],
    kind: str,
) -> dict[str, str]:
    """Return rename map for *candidates* that collide with *occupied*."""
    renames: dict[str, str] = {}
    taken = set(occupied)
    for name in candidates:
        if name not in taken:
            taken.add(name)
            continue
        if name_conflict == "error":
            raise ValueError(
                f"compose_manifests: {kind} name collision on {name!r}; "
                "provide an equivalence / resource_renames, or set "
                "name_conflict='prefix_right'"
            )
        new_name = _prefixed(name)
        while new_name in taken:
            new_name = _prefixed(new_name)
        renames[name] = new_name
        taken.add(new_name)
    return renames


def _require_schema(manifest: GraphManifest, side: str) -> Schema:
    if manifest.graph_schema is None:
        raise ValueError(
            f"compose_manifests requires graph_schema on the {side} manifest"
        )
    return manifest.graph_schema


def _collapse_duplicate_vertices(manifest: GraphManifest) -> None:
    """Merge same-named vertex definitions (e.g. after multi-source rename to one ``into``)."""
    schema = manifest.graph_schema
    if schema is None:
        return
    by_name: dict[str, list[Vertex]] = {}
    for vertex in schema.core_schema.vertex_config.vertices:
        by_name.setdefault(vertex.name, []).append(vertex)
    if all(len(group) == 1 for group in by_name.values()):
        return

    merged_vertices = [
        group[0] if len(group) == 1 else merge_vertex_models(group, name)
        for name, group in by_name.items()
    ]
    mapping = {name: name for name, group in by_name.items() if len(group) > 1}
    edges = list(schema.core_schema.edge_config.edges)
    if mapping:
        from .merge_core import redirect_and_merge_edges

        edges = redirect_and_merge_edges(edges, mapping)

    schema.core_schema = CoreSchema(
        vertex_config=VertexConfig(
            vertices=merged_vertices,
            force_types=dict(schema.core_schema.vertex_config.force_types or {}),
        ),
        edge_config=edge_config_from_edges(edges),
    )
    if mapping:
        from .db_profile import apply_vertex_merge_to_db_profile

        # No-op-ish when keys already share the name; still revalidate.
        for name in mapping:
            apply_vertex_merge_to_db_profile(schema.db_profile, {name}, name)
        schema.db_profile = _revalidate_db_profile(schema.db_profile)


def _align_side(
    manifest: GraphManifest,
    *,
    side: Literal["left", "right"],
    op: ComposeManifestsOp,
) -> None:
    """Property-align then rename boundary vertices/relations on one side in place."""
    left_props, right_props = _property_rename_maps(op.vertices)
    prop_map = left_props if side == "left" else right_props
    if prop_map:
        apply_rename_vertex_properties(
            manifest, RenameVertexPropertiesOp(renames=prop_map)
        )

    vmap = _vertex_rename_map(op.vertices, side=side)
    if vmap:
        apply_rename_vertices(manifest, RenameVerticesOp(vertices=vmap))
        _collapse_duplicate_vertices(manifest)

    rmap = _relation_rename_map(op.relations, side=side)
    if rmap:
        apply_rename_relations(manifest, RenameRelationsOp(relations=rmap))


def _apply_right_resource_policy(
    right: GraphManifest,
    op: ComposeManifestsOp,
    left_resource_names: set[str],
) -> None:
    if op.resource_renames:
        apply_rename_resources(
            right, RenameResourcesOp(resources=dict(op.resource_renames))
        )

    if right.ingestion_model is None:
        return

    right_names = [r.name for r in right.ingestion_model.resources]
    collisions = _resolve_name_collisions(
        left_resource_names,
        right_names,
        name_conflict=op.name_conflict,
        kind="resource",
    )
    if collisions:
        apply_rename_resources(right, RenameResourcesOp(resources=collisions))


def _apply_right_schema_collision_policy(
    left: GraphManifest,
    right: GraphManifest,
    op: ComposeManifestsOp,
) -> None:
    """Prefix or error on non-equivalent right vertex/relation names that collide."""
    left_schema = _require_schema(left, "left")
    right_schema = _require_schema(right, "right")

    boundary_intos = {veq.into for veq in op.vertices}
    left_vertices = left_schema.core_schema.vertex_config.vertex_set
    right_vertex_names = sorted(right_schema.core_schema.vertex_config.vertex_set)
    candidates_v = [
        name
        for name in right_vertex_names
        if name in left_vertices and name not in boundary_intos
    ]
    if candidates_v and op.name_conflict == "error":
        raise ValueError(
            f"compose_manifests: vertex name collision on "
            f"{sorted(candidates_v)!r}; provide a VertexEquivalence or set "
            "name_conflict='prefix_right'"
        )
    if candidates_v and op.name_conflict == "prefix_right":
        v_renames: dict[str, str] = {}
        taken = set(left_vertices) | set(right_vertex_names)
        for name in candidates_v:
            new_name = _prefixed(name)
            while new_name in taken:
                new_name = _prefixed(new_name)
            v_renames[name] = new_name
            taken.add(new_name)
        apply_rename_vertices(right, RenameVerticesOp(vertices=v_renames))

    left_relations = {
        e.relation
        for e in left_schema.core_schema.edge_config.edges
        if e.relation is not None
    }
    boundary_rel_intos = {req.into for req in op.relations}
    right_relations = sorted(
        {
            e.relation
            for e in right_schema.core_schema.edge_config.edges
            if e.relation is not None
        }
    )
    candidates_r = [
        name
        for name in right_relations
        if name in left_relations and name not in boundary_rel_intos
    ]
    if candidates_r and op.name_conflict == "error":
        raise ValueError(
            f"compose_manifests: relation name collision on "
            f"{sorted(candidates_r)!r}; provide a RelationEquivalence or set "
            "name_conflict='prefix_right'"
        )
    if candidates_r and op.name_conflict == "prefix_right":
        r_renames: dict[str, str] = {}
        taken_r = set(left_relations) | set(right_relations)
        for name in candidates_r:
            new_name = _prefixed(name)
            while new_name in taken_r:
                new_name = _prefixed(new_name)
            r_renames[name] = new_name
            taken_r.add(new_name)
        apply_rename_relations(right, RenameRelationsOp(relations=r_renames))


def _composed_identity(veq: VertexEquivalence, merged: Vertex) -> list[str]:
    if veq.identity is not None:
        return list(veq.identity)
    identity_out = list(merged.identity)
    seen = set(identity_out)
    for pe in veq.properties:
        if pe.identity and pe.into not in seen:
            identity_out.append(pe.into)
            seen.add(pe.into)
    return identity_out


def _merge_db_profiles(
    left: DatabaseProfile, right: DatabaseProfile
) -> DatabaseProfile:
    data = left.to_dict(skip_defaults=False)
    right_data = right.to_dict(skip_defaults=False)

    vs = dict(data.get("vertex_storage_names") or {})
    for k, v in (right_data.get("vertex_storage_names") or {}).items():
        if k in vs and vs[k] != v:
            raise ValueError(
                f"compose_manifests: conflicting vertex_storage_names for {k!r}: "
                f"{vs[k]!r} vs {v!r}"
            )
        vs[k] = v
    data["vertex_storage_names"] = vs

    vi = {k: list(v) for k, v in (data.get("vertex_indexes") or {}).items()}
    for k, vlist in (right_data.get("vertex_indexes") or {}).items():
        vi.setdefault(k, []).extend(list(vlist))
    data["vertex_indexes"] = vi

    edge_specs = list(data.get("edge_specs") or [])
    edge_specs.extend(list(right_data.get("edge_specs") or []))
    data["edge_specs"] = edge_specs

    return _revalidate_db_profile(DatabaseProfile.model_validate(data))


def _union_schema(left: Schema, right: Schema, op: ComposeManifestsOp) -> Schema:
    left_vc = left.core_schema.vertex_config
    right_vc = right.core_schema.vertex_config
    left_by_name = {v.name: v for v in left_vc.vertices}
    right_by_name = {v.name: v for v in right_vc.vertices}

    into_by_name = {veq.into: veq for veq in op.vertices}

    out_vertices: list[Vertex] = []
    seen: set[str] = set()

    for name, veq in into_by_name.items():
        if name not in left_by_name or name not in right_by_name:
            missing_side = "left" if name not in left_by_name else "right"
            raise ValueError(
                f"compose_manifests: boundary vertex {name!r} missing on {missing_side} "
                f"after alignment (left={veq.left!r}, right={veq.right!r})"
            )
        merged = merge_vertex_models([left_by_name[name], right_by_name[name]], name)
        identity = _composed_identity(veq, merged)
        out_vertices.append(merged.model_copy(update={"identity": identity}))
        seen.add(name)

    for v in left_vc.vertices:
        if v.name in seen:
            continue
        out_vertices.append(v)
        seen.add(v.name)

    for v in right_vc.vertices:
        if v.name in seen:
            continue
        out_vertices.append(v)
        seen.add(v.name)

    force_types: dict[str, list] = dict(left_vc.force_types or {})
    for k, v in (right_vc.force_types or {}).items():
        if k in force_types and force_types[k] != v:
            raise ValueError(
                f"compose_manifests: conflicting force_types for vertex {k!r}"
            )
        force_types[k] = v

    by_id: dict[EdgeId, Edge] = {}
    for edge in list(left.core_schema.edge_config.edges) + list(
        right.core_schema.edge_config.edges
    ):
        eid = edge.edge_id
        if eid in by_id:
            by_id[eid] = merge_edge_pair(by_id[eid], edge)
        else:
            by_id[eid] = edge

    meta = left.metadata.model_copy(deep=True)
    if right.metadata.name and meta.name and right.metadata.name != meta.name:
        meta.name = f"{meta.name}+{right.metadata.name}"
    elif right.metadata.name and not meta.name:
        meta.name = right.metadata.name

    db_profile = _merge_db_profiles(left.db_profile, right.db_profile)

    return Schema(
        metadata=meta,
        core_schema=CoreSchema(
            vertex_config=VertexConfig(vertices=out_vertices, force_types=force_types),
            edge_config=edge_config_from_edges(list(by_id.values())),
        ),
        db_profile=db_profile,
    )


def _union_transforms(
    left: IngestionModel | None, right: IngestionModel | None
) -> list:
    left_t = list(left.transforms) if left is not None else []
    right_t = list(right.transforms) if right is not None else []
    by_name: dict[str, Any] = {}
    out: list = []
    for t in left_t + right_t:
        name = t.name
        if name is None:
            out.append(t)
            continue
        if name in by_name:
            existing = by_name[name]
            if existing.to_dict(skip_defaults=False) != t.to_dict(skip_defaults=False):
                raise ValueError(
                    f"compose_manifests: incompatible transform definitions for {name!r}"
                )
            continue
        by_name[name] = t
        out.append(t)
    return out


def _union_ingestion(
    left: IngestionModel | None, right: IngestionModel | None
) -> IngestionModel | None:
    if left is None and right is None:
        return None
    if left is None:
        return right.model_copy(deep=True) if right is not None else None
    if right is None:
        return left.model_copy(deep=True)

    resources = list(left.resources) + list(right.resources)
    transforms = _union_transforms(left, right)
    # Model-level write policies follow the left manifest, as composition treats
    # it as the base being extended.
    edges_on_duplicate = left.edges_on_duplicate
    endpoints_on_ambiguous = left.endpoints_on_ambiguous
    return IngestionModel.model_validate(
        {
            "edges_on_duplicate": edges_on_duplicate,
            "endpoints_on_ambiguous": endpoints_on_ambiguous,
            "resources": [r.to_dict(skip_defaults=False) for r in resources],
            "transforms": [t.to_dict(skip_defaults=False) for t in transforms],
        }
    )


def _connector_name(connector: Any) -> str | None:
    if isinstance(connector, dict):
        name = connector.get("name")
        return name if isinstance(name, str) else None
    name = getattr(connector, "name", None)
    return name if isinstance(name, str) else None


def _union_bindings(
    left: Bindings | None,
    right: Bindings | None,
    *,
    name_conflict: Literal["error", "prefix_right"],
) -> Bindings | None:
    if left is None and right is None:
        return None
    if left is None:
        return right.model_copy(deep=True) if right is not None else None
    if right is None:
        return left.model_copy(deep=True)

    left_data = left.to_dict(skip_defaults=False)
    right_data = right.to_dict(skip_defaults=False)

    left_connectors = list(left_data.get("connectors") or [])
    right_connectors = list(right_data.get("connectors") or [])
    left_names = {n for c in left_connectors if (n := _connector_name(c)) is not None}
    rename_connectors: dict[str, str] = {}
    for connector in right_connectors:
        name = _connector_name(connector)
        if name is None or name not in left_names:
            if name is not None:
                left_names.add(name)
            continue
        if name_conflict == "error":
            raise ValueError(
                f"compose_manifests: connector name collision on {name!r}; "
                "rename before compose or set name_conflict='prefix_right'"
            )
        new_name = _prefixed(name)
        while new_name in left_names:
            new_name = _prefixed(new_name)
        rename_connectors[name] = new_name
        left_names.add(new_name)
        if isinstance(connector, dict):
            connector["name"] = new_name
        else:
            # Re-serialize path: mutate via dict rebuild below
            pass

    if rename_connectors:
        # Rebuild right connectors/bindings with renamed connector names.
        rebuilt_right: list[Any] = []
        for connector in right_connectors:
            d = (
                dict(connector)
                if isinstance(connector, dict)
                else connector.to_dict(skip_defaults=False)
            )
            cname = d.get("name")
            if isinstance(cname, str) and cname in rename_connectors:
                d["name"] = rename_connectors[cname]
            rebuilt_right.append(d)
        right_connectors = rebuilt_right

        def _remap_connector_ref(entries: list[Any], key: str) -> list[Any]:
            out: list[Any] = []
            for entry in entries:
                d = (
                    dict(entry)
                    if isinstance(entry, dict)
                    else (
                        entry.to_dict(skip_defaults=False)
                        if hasattr(entry, "to_dict")
                        else dict(entry)
                    )
                )
                ref = d.get(key)
                if isinstance(ref, str) and ref in rename_connectors:
                    d[key] = rename_connectors[ref]
                out.append(d)
            return out

        right_data["resource_connector"] = _remap_connector_ref(
            list(right_data.get("resource_connector") or []), "connector"
        )
        right_data["connector_connection"] = _remap_connector_ref(
            list(right_data.get("connector_connection") or []), "connector"
        )

    merged = {
        "connector_templates": list(left_data.get("connector_templates") or [])
        + list(right_data.get("connector_templates") or []),
        "conn_proxy": left_data.get("conn_proxy") or right_data.get("conn_proxy"),
        "connectors": left_connectors + right_connectors,
        "resource_connector": list(left_data.get("resource_connector") or [])
        + list(right_data.get("resource_connector") or []),
        "connector_connection": list(left_data.get("connector_connection") or [])
        + list(right_data.get("connector_connection") or []),
        "staging_proxy": list(left_data.get("staging_proxy") or [])
        + list(right_data.get("staging_proxy") or []),
    }
    return Bindings.model_validate(merged)


def compose_manifests(
    left: GraphManifest,
    right: GraphManifest,
    op: ComposeManifestsOp,
    *,
    bump_version: bool | Literal["minor"] = "minor",
    finish_init: bool = True,
    strict_references: bool = False,
    dynamic_edge_feedback: bool = False,
) -> GraphManifest:
    """Return a new manifest that is the deterministic compose of *left* and *right*.

    Applies explicit equivalences in *op* (property alignment → boundary rename →
    union of schema / resources / bindings → merge equivalent types). Does not
    invent semantic matches.
    """
    if not isinstance(op, ComposeManifestsOp):
        raise TypeError(
            f"compose_manifests expects ComposeManifestsOp, got {type(op)!r}"
        )

    out_left = left.model_copy(deep=True)
    out_right = right.model_copy(deep=True)

    left_schema = _require_schema(out_left, "left")
    right_schema = _require_schema(out_right, "right")

    left_vertices = left_schema.core_schema.vertex_config.vertex_set
    right_vertices = right_schema.core_schema.vertex_config.vertex_set
    for veq in op.vertices:
        if veq.left not in left_vertices:
            raise ValueError(
                f"compose_manifests: left vertex {veq.left!r} not in left manifest"
            )
        if veq.right not in right_vertices:
            raise ValueError(
                f"compose_manifests: right vertex {veq.right!r} not in right manifest"
            )

    _align_side(out_left, side="left", op=op)
    _align_side(out_right, side="right", op=op)

    left_resource_names: set[str] = set()
    if out_left.ingestion_model is not None:
        left_resource_names = {r.name for r in out_left.ingestion_model.resources}
    _apply_right_resource_policy(out_right, op, left_resource_names)
    _apply_right_schema_collision_policy(out_left, out_right, op)

    composed_schema = _union_schema(
        _require_schema(out_left, "left"),
        _require_schema(out_right, "right"),
        op,
    )
    composed_ingestion = _union_ingestion(
        out_left.ingestion_model, out_right.ingestion_model
    )
    composed_bindings = _union_bindings(
        out_left.bindings, out_right.bindings, name_conflict=op.name_conflict
    )

    result = GraphManifest(
        graph_schema=composed_schema,
        ingestion_model=composed_ingestion,
        bindings=composed_bindings,
    )
    _bump_schema_version(result, bump_version)

    if finish_init:
        result.finish_init(
            strict_references=strict_references,
            dynamic_edge_feedback=dynamic_edge_feedback,
        )
    return result
