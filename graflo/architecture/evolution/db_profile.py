"""Update :class:`~graflo.architecture.database_features.DatabaseProfile` after vertex changes."""

from __future__ import annotations

import logging
from typing import Any

from graflo.architecture.database_features import (
    DatabaseProfile,
    EdgePropertyDefaults,
    EdgeVariantSpec,
)
from graflo.architecture.graph_types import EdgeId, Index
from graflo.architecture.schema import Schema

logger = logging.getLogger(__name__)

VERTEX_SUFFIX = "vertex"
RELATION_SUFFIX = "relation"


def _merge_vertex_default_maps(
    a: dict[str, Any], b: dict[str, Any], *, label: str
) -> dict[str, Any]:
    out = dict(a)
    for pk, pv in b.items():
        if pk in out and out[pk] != pv:
            raise ValueError(
                f"Conflicting {label} for property {pk!r}: {out[pk]!r} vs {pv!r}"
            )
        out[pk] = pv
    return out


def apply_vertex_removal_to_db_profile(
    profile: DatabaseProfile, removed: set[str]
) -> None:
    """Drop logical vertex keys and edge entries that reference removed vertices."""
    if not removed:
        return
    for name in removed:
        profile.vertex_storage_names.pop(name, None)
        profile.vertex_indexes.pop(name, None)

    dpv = profile.default_property_values
    if dpv is not None:
        for name in removed:
            dpv.vertices.pop(name, None)
        dpv.edges = [
            e for e in dpv.edges if e.source not in removed and e.target not in removed
        ]

    profile.edge_specs = [
        s
        for s in profile.edge_specs
        if s.source not in removed and s.target not in removed
    ]


def apply_vertex_merge_to_db_profile(
    profile: DatabaseProfile,
    from_vertices: set[str],
    into: str,
) -> None:
    """Remap logical vertex keys in *profile* when merging *from_vertices* into *into*."""
    if not from_vertices:
        return
    m = {v: into for v in from_vertices if v != into}
    if not m:
        return

    new_vs: dict[str, str] = {}
    for k, v in profile.vertex_storage_names.items():
        nk = m.get(k, k)
        if nk in new_vs and new_vs[nk] != v:
            raise ValueError(
                f"Conflicting vertex_storage_names for logical {nk!r}: "
                f"{new_vs[nk]!r} vs {v!r}"
            )
        new_vs[nk] = v
    profile.vertex_storage_names = new_vs

    new_vi: dict[str, list[Any]] = {}
    for k, vlist in profile.vertex_indexes.items():
        nk = m.get(k, k)
        new_vi.setdefault(nk, []).extend(list(vlist))
    profile.vertex_indexes = new_vi

    new_specs: list[EdgeVariantSpec] = []
    for s in profile.edge_specs:
        src = m.get(s.source, s.source)
        tgt = m.get(s.target, s.target)
        new_specs.append(
            EdgeVariantSpec(
                source=src,
                target=tgt,
                relation=s.relation,
                purpose=s.purpose,
                relation_name=s.relation_name,
                indexes=list(s.indexes),
                indexes_mode=s.indexes_mode,
            )
        )
    profile.edge_specs = new_specs

    dpv = profile.default_property_values
    if dpv is None:
        return

    merged_vert: dict[str, dict[str, Any]] = {}
    for k, props in dpv.vertices.items():
        nk = m.get(k, k)
        if nk not in merged_vert:
            merged_vert[nk] = dict(props)
        else:
            merged_vert[nk] = _merge_vertex_default_maps(
                merged_vert[nk],
                dict(props),
                label="default_property_values.vertices",
            )
    object.__setattr__(dpv, "vertices", merged_vert)

    new_edges: list[EdgePropertyDefaults] = []
    for e in dpv.edges:
        src = m.get(e.source, e.source)
        tgt = m.get(e.target, e.target)
        new_edges.append(
            EdgePropertyDefaults(
                source=src,
                target=tgt,
                relation=e.relation,
                values=dict(e.values),
            )
        )
    object.__setattr__(dpv, "edges", new_edges)


def apply_storage_name_sanitization_to_db_profile(
    profile: DatabaseProfile,
    schema: Schema,
    reserved_words: set[str],
) -> None:
    """Sanitize physical storage/relation names against a flavor's reserved words.

    Walks ``schema.core_schema.vertex_config.vertices`` and rewrites
    ``profile.vertex_storage_names[vertex.name]`` when the current effective
    storage name collides with a reserved word.

    Walks ``schema.core_schema.edge_config.edges`` and rewrites the variant
    spec's ``relation_name`` (via :meth:`DatabaseProfile.set_edge_name_spec`)
    when the current effective relation name collides with a reserved word or
    an existing vertex storage name.

    Mutates ``profile`` in place.
    """
    from graflo.db.util import sanitize_attribute_name

    if not reserved_words:
        return

    for vertex in schema.core_schema.vertex_config.vertices:
        dbname = profile.vertex_storage_name(vertex.name)
        sanitized = sanitize_attribute_name(
            dbname, reserved_words, suffix=f"_{VERTEX_SUFFIX}"
        )
        if sanitized != dbname:
            logger.debug("Sanitizing vertex name '%s' -> '%s'", dbname, sanitized)
            profile.vertex_storage_names[vertex.name] = sanitized

    vertex_storage_names = {
        profile.vertex_storage_name(vertex.name)
        for vertex in schema.core_schema.vertex_config.vertices
    }

    for edge in schema.core_schema.edge_config.edges:
        if not edge.relation:
            continue
        original = profile.edge_relation_name(
            edge.edge_id,
            default_relation=edge.relation,
        )
        if original is None:
            continue
        sanitized = sanitize_attribute_name(
            original,
            reserved_words,
            suffix=f"_{RELATION_SUFFIX}",
        )
        if sanitized in vertex_storage_names:
            base = f"{sanitized}_{RELATION_SUFFIX}"
            candidate = base
            counter = 1
            while candidate in vertex_storage_names:
                candidate = f"{base}_{counter}"
                counter += 1
            sanitized = candidate

        if sanitized != original:
            profile.set_edge_name_spec(
                edge.edge_id,
                relation_name=sanitized,
            )


def _rewrite_index_fields(indexes: list[Index], renames: dict[str, str]) -> list[Index]:
    if not renames:
        return indexes
    new_indexes: list[Index] = []
    for idx in indexes:
        new_fields = [renames.get(f, f) for f in idx.fields]
        if new_fields == list(idx.fields):
            new_indexes.append(idx)
            continue
        new_indexes.append(idx.model_copy(update={"fields": new_fields}, deep=True))
    return new_indexes


def apply_field_rename_to_db_profile(
    profile: DatabaseProfile,
    renames: dict[str, dict[str, str]],
    *,
    edge_vertex_lookup: dict[EdgeId, tuple[str, str]] | None = None,
) -> None:
    """Rewrite field names referenced by a :class:`DatabaseProfile`.

    ``renames`` maps each vertex name to a per-vertex ``{old_field: new_field}``
    map. Updates:

    - ``profile.vertex_indexes[vertex_name]`` field tuples.
    - ``profile.edge_specs[*].indexes`` field tuples (using both source and
      target vertex renames; an explicit ``edge_vertex_lookup`` may be passed
      to map ``EdgeId -> (source, target)`` when source/target names changed).
    - ``profile.default_property_values.vertices[vertex_name]`` keys.
    """
    if not renames:
        return

    new_vertex_indexes: dict[str, list[Index]] = {}
    for vertex_name, indexes in profile.vertex_indexes.items():
        per_vertex = renames.get(vertex_name) or {}
        new_vertex_indexes[vertex_name] = _rewrite_index_fields(
            list(indexes), per_vertex
        )
    profile.vertex_indexes = new_vertex_indexes

    new_specs: list[EdgeVariantSpec] = []
    for spec in profile.edge_specs:
        if edge_vertex_lookup is not None:
            source_name, target_name = edge_vertex_lookup.get(
                spec.edge_id, (spec.source, spec.target)
            )
        else:
            source_name, target_name = spec.source, spec.target
        merged: dict[str, str] = {}
        merged.update(renames.get(source_name) or {})
        merged.update(renames.get(target_name) or {})
        new_specs.append(
            EdgeVariantSpec(
                source=spec.source,
                target=spec.target,
                relation=spec.relation,
                purpose=spec.purpose,
                relation_name=spec.relation_name,
                indexes=_rewrite_index_fields(list(spec.indexes), merged),
                indexes_mode=spec.indexes_mode,
            )
        )
    profile.edge_specs = new_specs

    dpv = profile.default_property_values
    if dpv is None:
        return

    new_dpv_vertices: dict[str, dict[str, Any]] = {}
    for vertex_name, props in dpv.vertices.items():
        per_vertex = renames.get(vertex_name) or {}
        new_props: dict[str, Any] = {}
        for prop_name, value in props.items():
            new_props[per_vertex.get(prop_name, prop_name)] = value
        new_dpv_vertices[vertex_name] = new_props
    object.__setattr__(dpv, "vertices", new_dpv_vertices)
