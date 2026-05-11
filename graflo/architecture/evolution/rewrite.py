"""Structured rewrite of vertex names in pipeline dicts and related resource fields."""

from __future__ import annotations

from collections.abc import Callable
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


def _build_name_transformer(
    transform: dict[str, str] | None,
) -> Callable[[str], str]:
    if transform is None:
        return lambda value: value
    return lambda value: transform.get(value, value)


def _rewrite_entity_names_in_edge_step(
    payload: dict[str, Any],
    *,
    vertex_name: Callable[[str], str],
    edge_name: Callable[[str], str],
) -> None:
    for key in ("from", "to", "source", "target"):
        value = payload.get(key)
        if isinstance(value, str):
            payload[key] = vertex_name(value)

    relation = payload.get("relation")
    if isinstance(relation, str):
        payload["relation"] = edge_name(relation)

    relation_map = payload.get("relation_map")
    if isinstance(relation_map, dict):
        payload["relation_map"] = {
            raw: edge_name(mapped) if isinstance(mapped, str) else mapped
            for raw, mapped in relation_map.items()
        }

    links = payload.get("links")
    if isinstance(links, list):
        for link in links:
            if isinstance(link, dict):
                _rewrite_entity_names_in_edge_step(
                    link,
                    vertex_name=vertex_name,
                    edge_name=edge_name,
                )


def rewrite_entity_names_in_pipeline(
    step: Any,
    *,
    vertices: dict[str, str] | None = None,
    edges: dict[str, str] | None = None,
) -> None:
    """Mutate a pipeline payload in place to rename vertices/relations."""
    vertex_name = _build_name_transformer(vertices)
    edge_name = _build_name_transformer(edges)

    if isinstance(step, list):
        for item in step:
            rewrite_entity_names_in_pipeline(
                item,
                vertices=vertices,
                edges=edges,
            )
        return
    if not isinstance(step, dict):
        return

    if isinstance(step.get("vertex"), str):
        step["vertex"] = vertex_name(step["vertex"])

    if isinstance(step.get("type_map"), dict):
        step["type_map"] = {
            raw: vertex_name(mapped) if isinstance(mapped, str) else mapped
            for raw, mapped in step["type_map"].items()
        }

    if isinstance(step.get("vertex_from_map"), dict):
        step["vertex_from_map"] = {
            vertex_name(k): v for k, v in step["vertex_from_map"].items()
        }

    edge_payload = step.get("edge")
    if isinstance(edge_payload, dict):
        _rewrite_entity_names_in_edge_step(
            edge_payload,
            vertex_name=vertex_name,
            edge_name=edge_name,
        )

    create_edge_payload = step.get("create_edge")
    if isinstance(create_edge_payload, dict):
        _rewrite_entity_names_in_edge_step(
            create_edge_payload,
            vertex_name=vertex_name,
            edge_name=edge_name,
        )

    descend_payload = step.get("descend")
    if isinstance(descend_payload, dict):
        apply_payload = descend_payload.get("apply")
        if apply_payload is not None:
            rewrite_entity_names_in_pipeline(
                apply_payload,
                vertices=vertices,
                edges=edges,
            )
        pipeline_payload = descend_payload.get("pipeline")
        if pipeline_payload is not None:
            rewrite_entity_names_in_pipeline(
                pipeline_payload,
                vertices=vertices,
                edges=edges,
            )

    if isinstance(step.get("apply"), list):
        rewrite_entity_names_in_pipeline(
            step["apply"],
            vertices=vertices,
            edges=edges,
        )
    if isinstance(step.get("pipeline"), list):
        rewrite_entity_names_in_pipeline(
            step["pipeline"],
            vertices=vertices,
            edges=edges,
        )


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


def rewrite_remove_vertex_properties_in_pipeline(
    pipeline: list[dict[str, Any]],
    removals: dict[str, set[str]],
) -> list[dict[str, Any]]:
    """Remove references to dropped vertex fields from pipeline steps."""
    if not removals:
        return deepcopy(pipeline)

    def _rewrite_step(step: dict[str, Any]) -> dict[str, Any]:
        out = deepcopy(normalize_actor_step(dict(step)))
        step_type = out.get("type")

        if step_type == "vertex":
            vertex_name = out.get("vertex")
            if isinstance(vertex_name, str):
                removed = removals.get(vertex_name, set())
                if removed:
                    from_map = out.get("from")
                    if isinstance(from_map, dict):
                        out["from"] = {
                            key: value
                            for key, value in from_map.items()
                            if isinstance(key, str) and key not in removed
                        }
                    keep_fields = out.get("keep_fields")
                    if isinstance(keep_fields, list):
                        out["keep_fields"] = [
                            key
                            for key in keep_fields
                            if not (isinstance(key, str) and key in removed)
                        ]

        elif step_type == "transform":
            rename_map = out.get("rename")
            if isinstance(rename_map, dict):
                blocked_fields = set().union(*removals.values())
                out["rename"] = {
                    key: value
                    for key, value in rename_map.items()
                    if not (isinstance(value, str) and value in blocked_fields)
                }

        elif step_type == "edge":
            weights = out.get("vertex_weights")
            if isinstance(weights, list):
                filtered_weights: list[dict[str, Any]] = []
                for entry in weights:
                    if not isinstance(entry, dict):
                        continue
                    name = entry.get("name")
                    if not isinstance(name, str):
                        filtered_weights.append(dict(entry))
                        continue
                    removed = removals.get(name, set())
                    if not removed:
                        filtered_weights.append(dict(entry))
                        continue
                    rewritten = dict(entry)
                    fields = rewritten.get("fields")
                    if isinstance(fields, list):
                        rewritten["fields"] = [
                            f
                            for f in fields
                            if not (isinstance(f, str) and f in removed)
                        ]
                    map_payload = rewritten.get("map")
                    if isinstance(map_payload, dict):
                        rewritten["map"] = {
                            k: v
                            for k, v in map_payload.items()
                            if not (isinstance(k, str) and k in removed)
                        }
                    filter_payload = rewritten.get("filter")
                    if isinstance(filter_payload, dict):
                        rewritten["filter"] = {
                            k: v
                            for k, v in filter_payload.items()
                            if not (isinstance(k, str) and k in removed)
                        }
                    filtered_weights.append(rewritten)
                out["vertex_weights"] = filtered_weights

        elif step_type == "descend":
            nested = out.get("pipeline")
            if isinstance(nested, list):
                out["pipeline"] = [
                    _rewrite_step(item) for item in nested if isinstance(item, dict)
                ]

        return out

    return [_rewrite_step(step) for step in pipeline if isinstance(step, dict)]


def rewrite_remove_relations_in_pipeline(
    pipeline: list[dict[str, Any]], removed_relations: set[str]
) -> list[dict[str, Any]]:
    """Drop edge/create_edge steps (and links) targeting removed relations."""
    if not removed_relations:
        return deepcopy(pipeline)

    def _rewrite_step(step: dict[str, Any]) -> dict[str, Any] | None:
        out = deepcopy(step)
        edge_payload = out.get("edge")
        if isinstance(edge_payload, dict):
            relation = edge_payload.get("relation")
            if relation in removed_relations:
                out.pop("edge", None)
            elif isinstance(edge_payload.get("relation_map"), dict):
                edge_payload["relation_map"] = {
                    k: v
                    for k, v in edge_payload["relation_map"].items()
                    if not (isinstance(v, str) and v in removed_relations)
                }
            links = edge_payload.get("links")
            if isinstance(links, list):
                edge_payload["links"] = [
                    link
                    for link in links
                    if not (
                        isinstance(link, dict)
                        and link.get("relation") in removed_relations
                    )
                ]
        create_edge_payload = out.get("create_edge")
        if isinstance(create_edge_payload, dict):
            relation = create_edge_payload.get("relation")
            if relation in removed_relations:
                out.pop("create_edge", None)
            elif isinstance(create_edge_payload.get("relation_map"), dict):
                create_edge_payload["relation_map"] = {
                    k: v
                    for k, v in create_edge_payload["relation_map"].items()
                    if not (isinstance(v, str) and v in removed_relations)
                }
        descend_payload = out.get("descend")
        if isinstance(descend_payload, dict) and isinstance(
            descend_payload.get("pipeline"), list
        ):
            descend_payload["pipeline"] = [
                nested
                for nested in (
                    _rewrite_step(item)
                    for item in descend_payload["pipeline"]
                    if isinstance(item, dict)
                )
                if nested is not None
            ]
        if "edge" not in out and "create_edge" not in out and out.get("type") == "edge":
            return None
        return out

    return [
        rewritten
        for rewritten in (
            _rewrite_step(step) for step in pipeline if isinstance(step, dict)
        )
        if rewritten is not None
    ]


def _rewrite_edge_properties_payload(
    payload: dict[str, Any],
    *,
    renames: dict[str, str] | None = None,
    removals: set[str] | None = None,
) -> None:
    properties = payload.get("properties")
    if not isinstance(properties, list):
        return
    rename_map = renames or {}
    remove_set = removals or set()
    rewritten: list[Any] = []
    seen: set[str] = set()
    for prop in properties:
        if isinstance(prop, str):
            new_name = rename_map.get(prop, prop)
            if new_name in remove_set or new_name in seen:
                continue
            seen.add(new_name)
            rewritten.append(new_name)
            continue
        if isinstance(prop, dict) and isinstance(prop.get("name"), str):
            new_name = rename_map.get(prop["name"], prop["name"])
            if new_name in remove_set or new_name in seen:
                continue
            item = dict(prop)
            item["name"] = new_name
            seen.add(new_name)
            rewritten.append(item)
            continue
        rewritten.append(prop)
    payload["properties"] = rewritten


def rewrite_edge_properties_in_pipeline(
    pipeline: list[dict[str, Any]],
    *,
    renames_by_relation: dict[str, dict[str, str]] | None = None,
    removals_by_relation: dict[str, set[str]] | None = None,
) -> list[dict[str, Any]]:
    """Rewrite edge actor `properties` declarations by relation."""
    renames_ctx = renames_by_relation or {}
    removals_ctx = removals_by_relation or {}
    if not renames_ctx and not removals_ctx:
        return deepcopy(pipeline)

    def _rewrite_edge_payload(payload: dict[str, Any]) -> None:
        relation = payload.get("relation")
        if isinstance(relation, str):
            renames = renames_ctx.get(relation, {})
            removals = removals_ctx.get(relation, set())
        else:
            renames = {}
            removals = set()
        _rewrite_edge_properties_payload(payload, renames=renames, removals=removals)
        links = payload.get("links")
        if isinstance(links, list):
            for link in links:
                if not isinstance(link, dict):
                    continue
                link_relation = link.get("relation")
                _rewrite_edge_properties_payload(
                    link,
                    renames=renames_ctx.get(link_relation, {})
                    if isinstance(link_relation, str)
                    else {},
                    removals=removals_ctx.get(link_relation, set())
                    if isinstance(link_relation, str)
                    else set(),
                )

    def _rewrite_step(step: dict[str, Any]) -> dict[str, Any]:
        out = deepcopy(step)
        for key in ("edge", "create_edge"):
            payload = out.get(key)
            if isinstance(payload, dict):
                _rewrite_edge_payload(payload)
        descend_payload = out.get("descend")
        if isinstance(descend_payload, dict):
            nested_pipeline = descend_payload.get("pipeline")
            if isinstance(nested_pipeline, list):
                descend_payload["pipeline"] = [
                    _rewrite_step(item)
                    for item in nested_pipeline
                    if isinstance(item, dict)
                ]
        return out

    return [_rewrite_step(step) for step in pipeline if isinstance(step, dict)]


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
