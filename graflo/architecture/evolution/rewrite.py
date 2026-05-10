"""Structured rewrite of vertex names in pipeline dicts and related resource fields."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from graflo.architecture.pipeline.runtime.actor.config.normalize import (
    normalize_actor_step,
)


def rewrite_vertex_weights_vertex_field_names(
    weights: list[Any],
    renames_by_vertex: dict[str, dict[str, str]],
) -> list[dict[str, Any]]:
    """Rewrite :class:`~graflo.architecture.graph_types.Weight` field/map/filter keys.

    Each weight's ``name`` selects the logical vertex whose ``renames_by_vertex[name]``
    map applies (old field name -> new field name).
    """
    from graflo.architecture.graph_types import Weight

    if not weights:
        return []
    out: list[dict[str, Any]] = []
    for raw in weights:
        w = Weight.model_validate(raw)
        per = {}
        vn = w.name
        if isinstance(vn, str) and vn in renames_by_vertex:
            per = renames_by_vertex[vn]
        if per:
            new_fields = [
                per.get(fname, fname) if isinstance(fname, str) else fname
                for fname in w.fields
            ]

            def _remap_obs_key(obs_key: Any) -> Any:
                if isinstance(obs_key, str):
                    return per.get(obs_key, obs_key)
                return obs_key

            new_map = {_remap_obs_key(k): v for k, v in dict(w.map).items()}
            new_filter = {_remap_obs_key(k): v for k, v in dict(w.filter).items()}
            w = w.model_copy(
                update={"fields": new_fields, "map": new_map, "filter": new_filter}
            )
        out.append(w.to_dict(skip_defaults=False))
    return out


def rewrite_extra_weights_vertex_field_names(
    entries: list[Any],
    renames_by_vertex: dict[str, dict[str, str]],
) -> list[dict[str, Any]]:
    """Rewrite ``extra_weights[*].vertex_weights`` for vertex field renames."""
    if not entries:
        return []
    result: list[dict[str, Any]] = []
    for entry in entries:
        d = (
            dict(entry)
            if isinstance(entry, dict)
            else entry.to_dict(skip_defaults=False)
        )
        vw = d.get("vertex_weights")
        if isinstance(vw, list) and renames_by_vertex:
            d["vertex_weights"] = rewrite_vertex_weights_vertex_field_names(
                vw, renames_by_vertex
            )
        result.append(d)
    return result


def _map_name(name: str | None, mapping: dict[str, str]) -> str | None:
    if name is None:
        return None
    return mapping.get(name, name)


def rewrite_vertex_names_in_step(
    step: dict[str, Any], mapping: dict[str, str]
) -> dict[str, Any]:
    """Return a deep-copied step with vertex names rewritten per *mapping*."""
    if not mapping:
        return deepcopy(step)
    s = normalize_actor_step(dict(step))
    out = deepcopy(s)
    t = out.get("type")

    if t == "vertex":
        v = out.get("vertex")
        if isinstance(v, str) and v in mapping:
            out["vertex"] = mapping[v]

    elif t == "vertex_router":
        tm = out.get("type_map")
        if isinstance(tm, dict):
            out["type_map"] = {
                k: _map_name(str(v), mapping) or v for k, v in tm.items()
            }
        vfm = out.get("vertex_from_map")
        if isinstance(vfm, dict):
            new_vfm: dict[str, Any] = {}
            for k, v in vfm.items():
                nk = mapping.get(k, k)
                new_vfm[nk] = v
            out["vertex_from_map"] = new_vfm

    elif t == "edge":
        for key in ("source", "from"):
            if key in out:
                val = out[key]
                if isinstance(val, str) and val in mapping:
                    out[key] = mapping[val]
        for key in ("target", "to"):
            if key in out:
                val = out[key]
                if isinstance(val, str) and val in mapping:
                    out[key] = mapping[val]

    elif t == "descend":
        pl = out.get("pipeline")
        if isinstance(pl, list):
            out["pipeline"] = [
                rewrite_vertex_names_in_step(cast_step(x), mapping)
                for x in pl
                if isinstance(x, dict)
            ]

    return out


def cast_step(x: Any) -> dict[str, Any]:
    if not isinstance(x, dict):
        raise TypeError(f"expected dict step, got {type(x)}")
    return x


def rewrite_vertex_names_in_pipeline(
    pipeline: list[dict[str, Any]], mapping: dict[str, str]
) -> list[dict[str, Any]]:
    """Rewrite all steps in a resource pipeline."""
    if not mapping:
        return deepcopy(pipeline)
    return [rewrite_vertex_names_in_step(s, mapping) for s in pipeline]


def rewrite_vertex_names_in_value(obj: Any, mapping: dict[str, str]) -> Any:
    """Deep-rewrite *obj* (pipelines, infer specs, extra_weights, nested dicts)."""
    if not mapping:
        return deepcopy(obj) if isinstance(obj, (dict, list)) else obj
    if isinstance(obj, list):
        return [rewrite_vertex_names_in_value(x, mapping) for x in obj]
    if isinstance(obj, dict):
        if "edge" in obj and isinstance(obj["edge"], dict):
            inner = dict(obj)
            inner["edge"] = rewrite_vertex_names_in_value(obj["edge"], mapping)
            return inner
        t = obj.get("type")
        if t in ("vertex", "edge", "descend", "vertex_router"):
            return rewrite_vertex_names_in_step(obj, mapping)
        if t == "transform":
            return deepcopy(obj)
        if all(k in obj for k in ("source", "target")):
            out = deepcopy(obj)
            src = out.get("source")
            tgt = out.get("target")
            if isinstance(src, str) and src in mapping:
                out["source"] = mapping[src]
            if isinstance(tgt, str) and tgt in mapping:
                out["target"] = mapping[tgt]
            return out
        if "vertex" in obj and isinstance(obj["vertex"], str) and t is None:
            out = deepcopy(obj)
            if out["vertex"] in mapping:
                out["vertex"] = mapping[out["vertex"]]
            return out
        return {k: rewrite_vertex_names_in_value(v, mapping) for k, v in obj.items()}
    return obj


def _apply_vertex_field_rename_to_from_doc(
    from_doc: dict[str, Any] | None, renames: dict[str, str]
) -> dict[str, str]:
    """Update a VertexActor ``from`` map for a per-vertex field rename.

    ``from_doc`` is ``{vertex_field: doc_field}`` (alias ``from``).

    Strategy:

    - Existing ``{old_field: doc_col}`` becomes ``{new_field: doc_col}`` (rename key).
    - For renames whose ``new_field`` is not yet present in the resulting map,
      inject ``{new_field: old_field}`` so the doc continues to address the
      attribute via its original name.
    """
    out: dict[str, str] = {}
    if isinstance(from_doc, dict):
        for v_f, d_f in from_doc.items():
            if not isinstance(v_f, str):
                continue
            mapped_v = renames.get(v_f, v_f)
            out[mapped_v] = d_f if isinstance(d_f, str) else v_f
    for old_field, new_field in renames.items():
        if new_field in out:
            continue
        out[new_field] = old_field
    return out


def _apply_vertex_field_rename_to_transform_rename(
    rename_map: dict[str, Any] | None,
    in_scope_renames: dict[str, str],
) -> dict[str, str]:
    """Update a TransformActor ``rename`` map for in-scope vertex field renames.

    ``rename`` is ``{input_key: output_key}`` (doc-side -> vertex-side).

    Rewrites existing entry *values* that match old vertex field names in scope.
    Injection of entirely new mappings is delegated to ``vertex`` ``from:`` /
    `_apply_vertex_field_rename_to_from_doc` so pipelines without a rename step
    still map renamed fields from raw doc columns correctly.
    """
    out: dict[str, str] = {}
    if isinstance(rename_map, dict):
        for k, v in rename_map.items():
            if not isinstance(k, str):
                continue
            mapped_v = in_scope_renames.get(v, v) if isinstance(v, str) else v
            out[k] = mapped_v if isinstance(mapped_v, str) else str(mapped_v)
    return out


def _step_vertices(step: dict[str, Any]) -> set[str]:
    """Return vertex names introduced by a single (non-recursive) actor step."""
    s = normalize_actor_step(dict(step))
    t = s.get("type")
    if t == "vertex" and isinstance(s.get("vertex"), str):
        return {s["vertex"]}
    if t == "vertex_router":
        names: set[str] = set()
        type_map = s.get("type_map")
        if isinstance(type_map, dict):
            for v in type_map.values():
                if isinstance(v, str):
                    names.add(v)
        vfm = s.get("vertex_from_map")
        if isinstance(vfm, dict):
            for k in vfm:
                if isinstance(k, str):
                    names.add(k)
        return names
    return set()


def _collect_level_vertices(steps: list[Any]) -> set[str]:
    """Collect vertex names introduced at the immediate (non-recursive) level."""
    out: set[str] = set()
    for step in steps:
        if isinstance(step, dict):
            out |= _step_vertices(step)
    return out


def _rewrite_vertex_field_step(
    step: dict[str, Any],
    renames: dict[str, dict[str, str]],
    available_vertices: set[str],
) -> dict[str, Any]:
    """Rewrite a single normalized step for vertex field renames.

    ``available_vertices`` is the set of vertex names in scope at the call site
    (vertices created at this level or by ancestors). It bounds which renames
    apply to ``transform`` rename maps.
    """
    s = normalize_actor_step(dict(step))
    out = deepcopy(s)
    t = out.get("type")

    if t == "vertex":
        v_name = out.get("vertex")
        if isinstance(v_name, str) and v_name in renames and renames[v_name]:
            per_vertex = renames[v_name]
            new_from = _apply_vertex_field_rename_to_from_doc(
                out.get("from") if isinstance(out.get("from"), dict) else None,
                per_vertex,
            )
            if new_from:
                out["from"] = new_from
            keep_fields = out.get("keep_fields")
            if isinstance(keep_fields, list):
                out["keep_fields"] = [
                    per_vertex.get(name, name) if isinstance(name, str) else name
                    for name in keep_fields
                ]

    elif t == "transform":
        in_scope_renames: dict[str, str] = {}
        for v_name in available_vertices:
            in_scope_renames.update(renames.get(v_name, {}))
        if in_scope_renames:
            current = out.get("rename")
            # Call-mode transforms omit ``rename``. Never synthesize rename.
            if isinstance(current, dict):
                new_rename = _apply_vertex_field_rename_to_transform_rename(
                    current, in_scope_renames
                )
                if new_rename:
                    out["rename"] = new_rename

    elif t == "edge":
        vw = out.get("vertex_weights")
        if isinstance(vw, list):
            out["vertex_weights"] = rewrite_vertex_weights_vertex_field_names(
                vw, renames
            )

    elif t == "descend":
        pl = out.get("pipeline")
        if isinstance(pl, list):
            descend_level_vertices = _collect_level_vertices(pl)
            nested_available = available_vertices | descend_level_vertices
            out["pipeline"] = [
                _rewrite_vertex_field_step(x, renames, nested_available)
                for x in pl
                if isinstance(x, dict)
            ]

    return out


def rewrite_vertex_field_names_in_pipeline(
    pipeline: list[dict[str, Any]],
    renames: dict[str, dict[str, str]],
    *,
    available_vertices: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Rewrite vertex field names across a resource pipeline.

    Walks the dict pipeline (no runtime tree mutation):

    - ``vertex`` steps: ensure ``from:`` covers the rename. Existing
      ``{old_field: doc_col}`` becomes ``{new_field: doc_col}``; missing entries
      are injected as ``{new_field: old_field}`` so the doc can still address
      the property by its original name.
    - ``transform`` steps with ``rename``: rewrite values that pointed at renamed
      vertex fields. ``call`` steps are unchanged.
    - ``edge`` steps: rewrite ``vertex_weights`` field/map/filter keys per
      ``Weight.name``.
    - ``descend`` steps: recurse with an extended ``available_vertices`` set.

    ``available_vertices`` is the set of vertex names visible from the parent
    scope. Vertex names introduced at the current level are added automatically.
    """
    if not renames:
        return deepcopy(pipeline)
    parent_scope = set(available_vertices) if available_vertices else set()
    level_vertices = _collect_level_vertices(pipeline)
    scope = parent_scope | level_vertices
    return [
        _rewrite_vertex_field_step(step, renames, scope)
        for step in pipeline
        if isinstance(step, dict)
    ]


def pipeline_mentions_any_vertex(steps: list[dict[str, Any]], names: set[str]) -> bool:
    """Return True if any pipeline step references a vertex name in *names*."""
    if not names:
        return False
    for step in steps:
        if not isinstance(step, dict):
            continue
        s = normalize_actor_step(dict(step))
        t = s.get("type")
        if t == "vertex":
            if s.get("vertex") in names:
                return True
        elif t == "vertex_router":
            tm = s.get("type_map") or {}
            if any(v in names for v in tm.values() if isinstance(v, str)):
                return True
            vfm = s.get("vertex_from_map") or {}
            if any(k in names for k in vfm if isinstance(k, str)):
                return True
        elif t == "edge":
            for key in ("source", "from", "target", "to"):
                val = s.get(key)
                if isinstance(val, str) and val in names:
                    return True
        elif t == "descend":
            pl = s.get("pipeline") or []
            if isinstance(pl, list) and pipeline_mentions_any_vertex(
                [cast_step(x) for x in pl if isinstance(x, dict)], names
            ):
                return True
    return False
