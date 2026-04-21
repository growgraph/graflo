"""Update :class:`~graflo.architecture.database_features.DatabaseProfile` after vertex changes."""

from __future__ import annotations

from typing import Any

from graflo.architecture.database_features import (
    DatabaseProfile,
    EdgePropertyDefaults,
    EdgeVariantSpec,
)


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
