"""Binary compose of two :class:`~graflo.architecture.contract.manifest.GraphManifest`s."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Literal

from graflo.architecture.contract.bindings import Bindings
from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.graph_types import EdgeId
from graflo.architecture.schema.core import CoreSchema
from graflo.architecture.schema.database_features import DatabaseProfile
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.naming import canonical_slug
from graflo.architecture.schema.vertex import Vertex, VertexConfig

from .apply import (
    _bump_schema_version,
    _rename_vertices_inplace,
    _revalidate_db_profile,
    apply_merge_vertices,
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
    MergeVerticesOp,
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
    name_conflict: Literal["error", "prefix_right", "fuse_right"],
    kind: str,
) -> dict[str, str]:
    """Return rename map for *candidates* that collide with *occupied*.

    Exact matching only, and deliberately so: resources and connectors are
    *addresses*, not concepts. Two whose names key alike cause no split -- each
    keeps its own name, each is looked up by that name, each targets its own
    vertex -- so canonical matching here would be pure false positive.
    ``fuse_right`` is meaningless for the same reason and behaves as ``error``.
    """
    renames: dict[str, str] = {}
    taken = set(occupied)
    for name in candidates:
        if name not in taken:
            taken.add(name)
            continue
        if name_conflict in ("error", "fuse_right"):
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


def _apply_vertex_alignment(manifest: GraphManifest, vmap: dict[str, str]) -> None:
    """Apply a boundary vertex map, merging where several sources share one target.

    Two equivalences on the same side may legitimately name the same ``into``. That
    is a merge, not a rename, so it goes through the merge machinery — which unions
    properties and identity and redirects edges — rather than leaving the schema
    transiently holding two definitions under one name.
    """
    groups: dict[str, list[str]] = {}
    for source, target in vmap.items():
        groups.setdefault(target, []).append(source)

    injective: dict[str, str] = {}
    for target, sources in sorted(groups.items()):
        if len(sources) == 1:
            injective[sources[0]] = target
            continue
        merge_sources = sorted(name for name in sources if name != target)
        # Composition is where merging two types onto one boundary name is the whole
        # point, and the caller stated the equivalence explicitly, so the merge
        # guards do not apply here.
        apply_merge_vertices(
            manifest,
            MergeVerticesOp(
                sources=merge_sources,
                into=target,
                allow_self_relations=True,
                allow_row_fusion=True,
            ),
        )
    if injective:
        _rename_vertices_inplace(manifest, injective)


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
        _apply_vertex_alignment(manifest, vmap)
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


class ComposeNameConflictError(ValueError):
    """Two names denote one concept under different naming conventions.

    Distinct from ``ComposeCanonicalConflictError`` in ``canonical.py``, which
    reports a *declared* CanonicalMap contradicting the op. This one fires on
    the residue neither side declared -- the undeclared path, where compose
    would otherwise produce two unrelated types with the data split between
    them and nothing raising.
    """


def _canonical_near_collisions(
    left_names: Iterable[str],
    right_names: Iterable[str],
    *,
    exempt: frozenset[str],
) -> list[tuple[str, str]]:
    """``(left, right)`` pairs that key alike but are spelled differently.

    Exact matches are excluded: those are the pre-existing collision path, so
    the two checks can never report the same pair twice.
    """
    by_key: dict[str, list[str]] = {}
    for name in left_names:
        by_key.setdefault(canonical_slug(name), []).append(name)

    pairs: list[tuple[str, str]] = []
    for right_name in right_names:
        if right_name in exempt:
            continue
        for left_name in by_key.get(canonical_slug(right_name), []):
            if left_name != right_name:
                pairs.append((left_name, right_name))
    return sorted(set(pairs))


def _resolve_schema_collisions(
    *,
    left_names: set[str],
    right_names: list[str],
    exempt: frozenset[str],
    name_conflict: str,
    kind: str,
    equivalence_hint: str,
) -> dict[str, str]:
    """The rename map to apply to the right side, or raise under ``error``.

    Two kinds of collision are handled together because they are the same
    question asked with different confidence. An **exact** collision
    (``Customer`` on both sides) has always raised by default. A **canonical**
    one (``Customer`` / ``customer``, or ``OrderLine`` / ``order_line``) is the
    weaker signal, so it cannot trigger a *more* aggressive action than the
    stronger one -- if exact equality refuses to fuse silently, a naming-
    convention guess certainly must not.

    Left the right names alone and they compose into two unrelated types with
    the source data split between them and nothing raising, which is the whole
    of ``CORE-MERGE-001``.
    """
    exact = [name for name in right_names if name in left_names and name not in exempt]
    near = _canonical_near_collisions(left_names, right_names, exempt=exempt)

    if name_conflict == "error":
        if exact:
            raise ValueError(
                f"compose_manifests: {kind} name collision on "
                f"{sorted(exact)!r}; provide a {equivalence_hint} or set "
                "name_conflict='prefix_right'"
            )
        if near:
            pairs = ", ".join(f"{left!r} / {right!r}" for left, right in near)
            raise ComposeNameConflictError(
                f"compose_manifests: {pairs} denote the same concept under "
                f"different naming conventions, so they would compose into two "
                f"unrelated {kind} types with the source data split between "
                f"them. Declare a {equivalence_hint} (or a CanonicalMap) "
                "to combine them, set name_conflict='fuse_right' to adopt the "
                "left spelling, or name_conflict='prefix_right' to keep them "
                "apart."
            )
        return {}

    if name_conflict == "fuse_right":
        if exact:
            raise ValueError(
                f"compose_manifests: {kind} name collision on "
                f"{sorted(exact)!r}; provide a {equivalence_hint} or set "
                "name_conflict='prefix_right'"
            )
        # Adopt the left spelling; the ordinary same-name union then merges
        # them. The left side keeps its name because it is the side the
        # canonical vocabulary is expected to live on.
        return {right: left for left, right in near}

    # prefix_right: keep both, explicitly, under distinguishable names.
    renames: dict[str, str] = {}
    taken = set(left_names) | set(right_names)
    for name in [*exact, *(right for _left, right in near)]:
        if name in renames:
            continue
        new_name = _prefixed(name)
        while new_name in taken:
            new_name = _prefixed(new_name)
        renames[name] = new_name
        taken.add(new_name)
    return renames


def _did_you_mean(name: str, candidates: Iterable[str]) -> str:
    """A suffix naming a candidate that denotes the same concept, if any.

    Authoring an equivalence in the wrong convention is the likeliest mistake
    at this boundary, and "not in left manifest" alone is a dead end when the
    vertex is right there under another spelling.
    """
    key = canonical_slug(name)
    near = sorted(c for c in candidates if c != name and canonical_slug(c) == key)
    if not near:
        return ""
    return (
        f"; it has {near[0]!r}, which denotes the same concept — author the "
        "equivalence in the manifest's own spelling"
    )


def _assert_no_canonical_split(schema: Schema) -> None:
    """No two composed vertex types or relations may denote one concept.

    The invariant ``CORE-MERGE-001`` is actually about, asserted on the result
    rather than only at the sites that could violate it -- so a future path
    into the union is covered without anyone remembering to add a check.
    """
    for kind, names in (
        ("vertex", [v.name for v in schema.core_schema.vertex_config.vertices]),
        (
            "relation",
            [
                e.relation
                for e in schema.core_schema.edge_config.edges
                if e.relation is not None
            ],
        ),
    ):
        by_key: dict[str, set[str]] = {}
        for name in names:
            by_key.setdefault(canonical_slug(name), set()).add(name)
        split = {key: sorted(group) for key, group in by_key.items() if len(group) > 1}
        if split:
            raise ComposeNameConflictError(
                f"compose_manifests produced {kind} types that denote the same "
                f"concept under different spellings: {split}. This is an "
                "unhandled compose path, not an authoring error -- the result "
                "would split data between them silently."
            )


def _apply_right_schema_collision_policy(
    left: GraphManifest,
    right: GraphManifest,
    op: ComposeManifestsOp,
) -> None:
    """Prefix, fuse or error on non-equivalent right vertex/relation names.

    Names are compared both exactly and by :func:`canonical_slug`, so two
    spellings of one concept are a collision rather than two types.

    Deliberately **not** applied to properties, resources, connectors or
    transforms. A property name binds to a key in the source document, so
    fusing ``customer_email`` with ``customerEmail`` would fuse two columns fed
    by different keys; the rest are addresses looked up by exact name, where a
    near-collision splits nothing and the check would be pure false positive.
    """
    left_schema = _require_schema(left, "left")
    right_schema = _require_schema(right, "right")

    v_renames = _resolve_schema_collisions(
        left_names=set(left_schema.core_schema.vertex_config.vertex_set),
        right_names=sorted(right_schema.core_schema.vertex_config.vertex_set),
        exempt=frozenset({veq.into for veq in op.vertices}),
        name_conflict=op.name_conflict,
        kind="vertex",
        equivalence_hint="VertexEquivalence",
    )
    if v_renames:
        apply_rename_vertices(right, RenameVerticesOp(vertices=v_renames))

    r_renames = _resolve_schema_collisions(
        left_names={
            e.relation
            for e in left_schema.core_schema.edge_config.edges
            if e.relation is not None
        },
        right_names=sorted(
            {
                e.relation
                for e in right_schema.core_schema.edge_config.edges
                if e.relation is not None
            }
        ),
        exempt=frozenset({req.into for req in op.relations}),
        name_conflict=op.name_conflict,
        kind="relation",
        equivalence_hint="RelationEquivalence",
    )
    if r_renames:
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
    name_conflict: Literal["error", "prefix_right", "fuse_right"],
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
                f"compose_manifests: left vertex {veq.left!r} not in left "
                f"manifest{_did_you_mean(veq.left, left_vertices)}"
            )
        if veq.right not in right_vertices:
            raise ValueError(
                f"compose_manifests: right vertex {veq.right!r} not in right "
                f"manifest{_did_you_mean(veq.right, right_vertices)}"
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
    _assert_no_canonical_split(composed_schema)
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
