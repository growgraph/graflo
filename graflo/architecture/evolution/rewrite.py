"""Structured rewrite of vertex names in pipeline dicts and related resource fields."""

from __future__ import annotations

import logging
from collections.abc import Callable, Collection
from copy import deepcopy
from typing import Any

from graflo.architecture.contract.ingestion.steps.normalize import (
    normalize_actor_step,
)

logger = logging.getLogger(__name__)


def rewrite_vertex_weights_vertex_field_names(
    weights: list[Any],
    renames_by_vertex: dict[str, dict[str, str]],
) -> list[dict[str, Any]]:
    """Rewrite :class:`~graflo.architecture.graph_types.Weight` references to vertex fields.

    Each weight's ``name`` selects the logical vertex whose ``renames_by_vertex[name]``
    map applies (old field name -> new field name). ``filter`` and ``map`` keys
    name vertex fields and follow the rename.

    The edge attribute a weight writes does **not** follow it: that name belongs
    to the edge, and renaming it is ``rename_edge_properties``' job. A renamed
    entry of ``fields`` -- whose attribute name *is* the vertex field's name -- is
    therefore moved to ``map`` as ``{new_field: old_attribute}`` -- unless ``map``
    already uses that key, in which case it cannot name a second attribute and the
    ``fields`` entry is renamed instead (with a warning).
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

            def _remap_obs_key(obs_key: Any, _per: dict = per) -> Any:
                if isinstance(obs_key, str):
                    return _per.get(obs_key, obs_key)
                return obs_key

            new_map = {_remap_obs_key(k): v for k, v in dict(w.map).items()}
            new_fields: list[Any] = []
            for fname in w.fields:
                if not isinstance(fname, str) or fname not in per:
                    new_fields.append(fname)
                elif per[fname] not in new_map:
                    new_map[per[fname]] = w.cfield(fname)
                else:
                    # The field also feeds a `map` entry, and one map key cannot
                    # name two attributes: the fields-derived one follows the rename.
                    logger.warning(
                        "vertex_weights on %r: %r is both in `fields` and a `map` "
                        "key, so its `fields` attribute is renamed to %r",
                        vn,
                        fname,
                        w.cfield(per[fname]),
                    )
                    new_fields.append(per[fname])
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


def rewrite_vertex_weight_names(
    payload: dict[str, Any], vertex_name: Callable[[str], str]
) -> None:
    """Rewrite ``vertex_weights[].name`` in place.

    ``Weight.name`` on an edge step is a *vertex* name — it selects which endpoint's
    observation columns the weight reads — and ``collect_vertex_names_from_pipeline``
    counts it as a vertex reference. Missing it leaves a pipeline pointing at a type
    the schema no longer has.
    """
    weights = payload.get("vertex_weights")
    if not isinstance(weights, list):
        return
    for weight in weights:
        if isinstance(weight, dict) and isinstance(weight.get("name"), str):
            weight["name"] = vertex_name(weight["name"])


_MATCH_KEYS = ("source_match", "target_match")


def _renamed_selector(selector: Any, vertex_name: Callable[[str], str]) -> Any:
    """A per-class endpoint selector with its classes renamed.

    Classes merged into one keep a single entry when they selected the same
    identity. Selecting different ones leaves the merged class with no single
    way to be matched, which is refused rather than settled by dict order.
    """
    if not isinstance(selector, dict):
        return selector
    out: dict[str, Any] = {}
    for vertex, plain in selector.items():
        new = vertex_name(vertex) if isinstance(vertex, str) else vertex
        if new in out and out[new] != plain:
            raise ValueError(
                f"merging {vertex!r} into {new!r} leaves an edge endpoint with two "
                f"selectors for it ({out[new]!r} and {plain!r}); give the step one "
                "selector for the merged class first"
            )
        out[new] = plain
    return out


def rename_endpoint_selector_classes(
    payload: dict[str, Any], vertex_name: Callable[[str], str]
) -> None:
    """Rename the classes of *payload*'s per-class ``source_match`` / ``target_match``."""
    for key in _MATCH_KEYS:
        if isinstance(payload.get(key), dict):
            payload[key] = _renamed_selector(payload[key], vertex_name)


def renamed_lookup_classes(value: Any, vertex_name: Callable[[str], str]) -> Any:
    """A router's ``lookup_only`` list with its classes renamed (merges deduplicated)."""
    if not isinstance(value, list):
        return value
    out: list[Any] = []
    for name in value:
        new = vertex_name(name) if isinstance(name, str) else name
        if new not in out:
            out.append(new)
    return out


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

    rewrite_vertex_weight_names(payload, vertex_name)
    rename_endpoint_selector_classes(payload, vertex_name)

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


def _is_untyped_flat_edge(step: dict[str, Any]) -> bool:
    """An edge step written flat and without ``type``, as the normalizer reads one."""
    return (
        "type" not in step
        and "vertex" not in step
        and ("source" in step or "from" in step)
        and ("target" in step or "to" in step)
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

    # A router's table lives at the top level in the flat spelling and under
    # ``vertex_router`` in the shorthand; the authored payload is what is here.
    router_payload = step.get("vertex_router")
    if not isinstance(router_payload, dict):
        router_payload = (
            step
            if step.get("type") == "vertex_router" or "type_field" in step
            else None
        )
    if router_payload is not None:
        if isinstance(router_payload.get("type_map"), dict):
            router_payload["type_map"] = {
                raw: vertex_name(mapped) if isinstance(mapped, str) else mapped
                for raw, mapped in router_payload["type_map"].items()
            }
        # A closed router's raw values are fixed by its table.
        if vertices and not router_payload.get("type_map_only"):
            materialized = materialize_router_renames(
                router_payload.get("type_map"), vertices
            )
            if materialized is not None:
                router_payload["type_map"] = materialized
        if isinstance(router_payload.get("lookup_only"), list):
            router_payload["lookup_only"] = renamed_lookup_classes(
                router_payload["lookup_only"], vertex_name
            )
    elif isinstance(step.get("type_map"), dict):
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
    elif step.get("type") == "edge" or _is_untyped_flat_edge(step):
        # Flat form: the edge payload *is* the step. Only string-valued endpoint keys
        # are touched, so a vertex step's dict-valued ``from`` column map is unaffected.
        # The untyped spelling (``{from, to, relation}``) is an edge too: it is how
        # ``normalize_actor_step`` reads it, so a rename that skipped it would leave
        # the ingested edge pointing at a vertex that no longer exists.
        _rewrite_entity_names_in_edge_step(
            step,
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


_PRIMARY_SELECTORS = (None, "identity")


def edge_payloads(step: dict[str, Any]) -> list[dict[str, Any]]:
    """The edge payloads *step* carries, as mutable references into it.

    An edge step is spelled three ways: nested under ``edge`` or
    ``create_edge``, or flat, with the endpoints on the step itself
    (``{source, target}`` / ``{from, to}``, or ``type: edge``). A walker that
    only looks under the two keys silently skips every flat step -- the form
    most hand-written pipelines use.
    """
    payloads = [
        payload
        for key in ("edge", "create_edge")
        if isinstance(payload := step.get(key), dict)
    ]
    if payloads or "vertex" in step:
        return payloads
    if step.get("type") == "edge" or (
        ("source" in step or "from" in step) and ("target" in step or "to" in step)
    ):
        return [step]
    return []


def _endpoint_vertex(payload: dict[str, Any], *keys: str) -> str | None:
    """First string endpoint name among *keys* (``source``/``from``, ``target``/``to``)."""
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str):
            return value
    return None


#: A role a ``vertex_router`` fills: it can hold a row of any class.
_ANY_CLASS: frozenset[str] = frozenset({"*"})


def _role_classes(
    pipeline: Any, out: dict[str, frozenset[str]] | None = None
) -> dict[str, frozenset[str]]:
    """What each accumulator role in *pipeline* can hold, at any level.

    A ``vertex`` step with a ``role`` holds its class; an open router's role
    (its ``role``, else its ``type_field``) holds any class, :data:`_ANY_CLASS`,
    and a closed one's (``type_map_only``) the classes its table names.
    """
    roles: dict[str, frozenset[str]] = {} if out is None else out
    if isinstance(pipeline, list):
        for item in pipeline:
            _role_classes(item, roles)
        return roles
    if not isinstance(pipeline, dict):
        return roles
    step = normalize_actor_step(dict(pipeline))
    step_type = step.get("type")
    if step_type == "vertex" and isinstance(step.get("role"), str):
        role = step["role"]
        if roles.get(role) != _ANY_CLASS:
            roles[role] = roles.get(role, frozenset()) | {step.get("vertex")}
    elif step_type == "vertex_router":
        role = step.get("role") or step.get("type_field")
        if isinstance(role, str):
            if not step.get("type_map_only"):
                roles[role] = _ANY_CLASS
            elif roles.get(role) != _ANY_CLASS:
                roles[role] = roles.get(role, frozenset()) | _table_classes(step)
    elif step_type == "descend":
        _role_classes(step.get("pipeline"), roles)
    return roles


def _table_classes(router: dict[str, Any]) -> frozenset[str]:
    """The classes a router's ``type_map`` routes to."""
    type_map = router.get("type_map")
    if not isinstance(type_map, dict):
        return frozenset()
    return frozenset(v for v in type_map.values() if isinstance(v, str))


def _pin_endpoint_selectors_in_edge_payload(
    payload: dict[str, Any],
    selectors: dict[str, str],
    roles: dict[str, frozenset[str]],
) -> None:
    """Point primary-identity endpoints at a named secondary identity, in place.

    Only endpoints currently resolving via the primary identity are touched: a step
    that already names a secondary identity is expressing an explicit intent that an
    identity replacement must not override. An endpoint a role fills gets a
    per-class entry for each pinned class the role can hold, so the other classes
    routed there keep matching on their own primary identity.
    """
    for endpoint_keys, role_keys, match_key in (
        (("source", "from"), ("source_role", "source_type_field"), "source_match"),
        (("target", "to"), ("target_role", "target_type_field"), "target_match"),
    ):
        current = payload.get(match_key)
        vertex_name = _endpoint_vertex(payload, *endpoint_keys)
        if vertex_name is not None:
            pinned = (
                {vertex_name: selectors[vertex_name]}
                if vertex_name in selectors
                else {}
            )
            if pinned and current in _PRIMARY_SELECTORS:
                payload[match_key] = pinned[vertex_name]
                continue
        else:
            role = next(
                (payload[k] for k in role_keys if isinstance(payload.get(k), str)),
                None,
            )
            held = roles.get(role) if role is not None else None
            if held is None:
                continue
            pinned = {
                vertex: selector
                for vertex, selector in selectors.items()
                if held == _ANY_CLASS or vertex in held
            }
            if pinned and current in _PRIMARY_SELECTORS:
                payload[match_key] = pinned
                continue
        if pinned and isinstance(current, dict):
            payload[match_key] = {
                **current,
                **{
                    vertex: selector
                    for vertex, selector in pinned.items()
                    if current.get(vertex) in _PRIMARY_SELECTORS
                },
            }

    links = payload.get("links")
    if isinstance(links, list):
        for link in links:
            if isinstance(link, dict):
                _pin_endpoint_selectors_in_edge_payload(link, selectors, roles)


def _pin_endpoint_selectors_in_step(
    step: Any, selectors: dict[str, str], roles: dict[str, frozenset[str]]
) -> None:
    if isinstance(step, list):
        for item in step:
            _pin_endpoint_selectors_in_step(item, selectors, roles)
        return
    if not isinstance(step, dict):
        return

    for payload in edge_payloads(step):
        _pin_endpoint_selectors_in_edge_payload(payload, selectors, roles)

    descend_payload = step.get("descend")
    if isinstance(descend_payload, dict):
        for key in ("apply", "pipeline"):
            nested = descend_payload.get(key)
            if nested is not None:
                _pin_endpoint_selectors_in_step(nested, selectors, roles)

    for key in ("apply", "pipeline"):
        nested = step.get(key)
        if isinstance(nested, list):
            _pin_endpoint_selectors_in_step(nested, selectors, roles)


def rewrite_endpoint_selectors_in_pipeline(
    pipeline: list[dict[str, Any]], selectors: dict[str, str]
) -> list[dict[str, Any]]:
    """Pin primary-identity edge endpoints of *selectors* keys to a secondary identity.

    ``selectors`` maps a vertex name to the secondary identity name its endpoints
    should select. Used by ``ReplaceIdentityOp`` with ``endpoints: pin_to_retired`` so
    edge steps keep matching on the identity that was just retired, and by merge
    for a resource that references a re-keyed member. An endpoint a router role
    fills is pinned per class (``{Class: selector}``).
    """
    out = deepcopy(pipeline)
    if not selectors:
        return out
    _pin_endpoint_selectors_in_step(out, selectors, _role_classes(out))
    return out


def close_routers_in_pipeline(
    pipeline: list[dict[str, Any]], vocabulary: Collection[str]
) -> list[dict[str, Any]]:
    """*pipeline* with every open router closed over *vocabulary*.

    An open router passes a value missing from its ``type_map`` through as the
    class name. Closing it lists each class of *vocabulary* the table does not
    already key -- as itself -- and sets ``type_map_only``, so it routes
    exactly what it routed when *vocabulary* was the whole schema. Entries
    already there, authored or written by a rename, are kept. Steps keep their
    authored spelling, at every level.
    """
    out = deepcopy(pipeline)
    _close_routers(out, sorted(vocabulary))
    return out


def _close_routers(step: Any, vocabulary: list[str]) -> None:
    if isinstance(step, list):
        for item in step:
            _close_routers(item, vocabulary)
        return
    if not isinstance(step, dict):
        return
    normalized = normalize_actor_step(dict(step))
    if normalized.get("type") == "vertex_router" and not normalized.get(
        "type_map_only"
    ):
        # The table lives under ``vertex_router`` in the shorthand and at the
        # top level in the flat spelling.
        nested = step.get("vertex_router")
        payload = nested if isinstance(nested, dict) else step
        table = dict(payload.get("type_map") or {})
        for name in vocabulary:
            table.setdefault(name, name)
        payload["type_map"] = table
        payload["type_map_only"] = True

    descend_payload = step.get("descend")
    if isinstance(descend_payload, dict):
        for key in ("apply", "pipeline"):
            _close_routers(descend_payload.get(key), vocabulary)
    for key in ("apply", "pipeline"):
        if isinstance(step.get(key), list):
            _close_routers(step[key], vocabulary)


def mark_lookup_only_in_pipeline(
    pipeline: list[dict[str, Any]], vertex: str
) -> list[dict[str, Any]]:
    """*pipeline* with every production of *vertex* turned into a lookup.

    A ``vertex`` step for it gains ``lookup_only: true``. An open
    ``vertex_router`` routes an unmapped value as the class name, so it can
    produce *vertex*, and a closed one (``type_map_only``) can when its table
    routes to it: such a router gains *vertex* in its ``lookup_only`` list,
    and the other classes it routes to are still written. Steps keep their
    authored spelling, at every level.
    """
    out = deepcopy(pipeline)
    _mark_lookup_only(out, vertex)
    return out


def _mark_lookup_only(step: Any, vertex: str) -> None:
    if isinstance(step, list):
        for item in step:
            _mark_lookup_only(item, vertex)
        return
    if not isinstance(step, dict):
        return
    normalized = normalize_actor_step(dict(step))
    step_type = normalized.get("type")
    if step_type == "vertex" and normalized.get("vertex") == vertex:
        step["lookup_only"] = True
    elif step_type == "vertex_router":
        # The table lives under ``vertex_router`` in the shorthand and at the
        # top level in the flat spelling.
        nested = step.get("vertex_router")
        payload = nested if isinstance(nested, dict) else step
        current = payload.get("lookup_only")
        routes_to_vertex = not normalized.get(
            "type_map_only"
        ) or vertex in _table_classes(normalized)
        if routes_to_vertex and isinstance(current, list):
            if vertex not in current:
                payload["lookup_only"] = [*current, vertex]
        elif routes_to_vertex and current is not True:
            payload["lookup_only"] = [vertex]

    descend_payload = step.get("descend")
    if isinstance(descend_payload, dict):
        for key in ("apply", "pipeline"):
            _mark_lookup_only(descend_payload.get(key), vertex)
    for key in ("apply", "pipeline"):
        if isinstance(step.get(key), list):
            _mark_lookup_only(step[key], vertex)


def _collect_endpoint_selectors_in_edge_payload(
    payload: dict[str, Any], out: list[tuple[str, str | list[str]]]
) -> None:
    for endpoint_keys, match_key in (
        (("source", "from"), "source_match"),
        (("target", "to"), "target_match"),
    ):
        per_class = payload.get(match_key)
        if isinstance(per_class, dict):
            out.extend(
                (vertex, plain)
                for vertex, plain in per_class.items()
                if plain not in _PRIMARY_SELECTORS and isinstance(plain, (str, list))
            )
            continue
        vertex_name = _endpoint_vertex(payload, *endpoint_keys)
        if vertex_name is None:
            continue
        selector = payload.get(match_key)
        if selector in _PRIMARY_SELECTORS:
            continue
        if isinstance(selector, (str, list)):
            out.append((vertex_name, selector))

    links = payload.get("links")
    if isinstance(links, list):
        for link in links:
            if isinstance(link, dict):
                _collect_endpoint_selectors_in_edge_payload(link, out)


def _collect_endpoint_selectors_in_step(
    step: Any, out: list[tuple[str, str | list[str]]]
) -> None:
    if isinstance(step, list):
        for item in step:
            _collect_endpoint_selectors_in_step(item, out)
        return
    if not isinstance(step, dict):
        return

    for payload in edge_payloads(step):
        _collect_endpoint_selectors_in_edge_payload(payload, out)

    descend_payload = step.get("descend")
    if isinstance(descend_payload, dict):
        for key in ("apply", "pipeline"):
            nested = descend_payload.get(key)
            if nested is not None:
                _collect_endpoint_selectors_in_step(nested, out)

    for key in ("apply", "pipeline"):
        nested = step.get(key)
        if isinstance(nested, list):
            _collect_endpoint_selectors_in_step(nested, out)


def collect_endpoint_selectors(
    pipeline: list[dict[str, Any]],
) -> list[tuple[str, str | list[str]]]:
    """``(vertex_name, selector)`` for every endpoint matched on a secondary identity.

    Endpoints resolving through the primary identity are omitted — they carry no
    dependency on a named secondary identity.
    """
    out: list[tuple[str, str | list[str]]] = []
    _collect_endpoint_selectors_in_step(pipeline, out)
    return out


def _retarget_edge_payload(
    payload: dict[str, Any],
    mapping: dict[tuple[str, str, str | None], tuple[str, str]],
) -> None:
    source = _endpoint_vertex(payload, "source", "from")
    target = _endpoint_vertex(payload, "target", "to")
    if source is None or target is None:
        return
    relation = payload.get("relation")
    new_endpoints = mapping.get(
        (source, target, relation if isinstance(relation, str) else None)
    )
    if new_endpoints is not None:
        new_source, new_target = new_endpoints
        payload["source" if "source" in payload else "from"] = new_source
        payload["target" if "target" in payload else "to"] = new_target
        for key, old, new in (
            ("source_match", source, new_source),
            ("target_match", target, new_target),
        ):
            payload_selector = payload.get(key)
            if isinstance(payload_selector, dict) and old != new:
                payload[key] = _renamed_selector(
                    payload_selector,
                    lambda name, o=old, n=new: n if name == o else name,
                )

    links = payload.get("links")
    if isinstance(links, list):
        for link in links:
            if isinstance(link, dict):
                _retarget_edge_payload(link, mapping)


def _retarget_edges_in_step(
    step: Any, mapping: dict[tuple[str, str, str | None], tuple[str, str]]
) -> None:
    if isinstance(step, list):
        for item in step:
            _retarget_edges_in_step(item, mapping)
        return
    if not isinstance(step, dict):
        return

    for payload in edge_payloads(step):
        _retarget_edge_payload(payload, mapping)

    descend_payload = step.get("descend")
    if isinstance(descend_payload, dict):
        for key in ("apply", "pipeline"):
            nested = descend_payload.get(key)
            if nested is not None:
                _retarget_edges_in_step(nested, mapping)

    for key in ("apply", "pipeline"):
        nested = step.get(key)
        if isinstance(nested, list):
            _retarget_edges_in_step(nested, mapping)


def rewrite_edge_endpoints_in_pipeline(
    pipeline: list[dict[str, Any]],
    mapping: dict[tuple[str, str, str | None], tuple[str, str]],
) -> list[dict[str, Any]]:
    """Repoint edge steps whose ``EdgeId`` appears in *mapping* at new endpoints.

    Keyed on the full ``(source, target, relation)`` triple rather than on vertex
    names, so an edge step between the same pair of types under a different relation
    is left alone.
    """
    out = deepcopy(pipeline)
    if not mapping:
        return out
    _retarget_edges_in_step(out, mapping)
    return out


def _merge_vertex_from_map(
    vfm: dict[str, Any], mapping: dict[str, str]
) -> dict[str, Any]:
    """Remap ``vertex_from_map`` keys, unioning the column maps that collide.

    A merge points several vertex names at one, so their per-vertex ``from`` maps
    collide. Keeping the last one silently drops the other sources' column mappings,
    which is exactly the routed data going missing. Fields present in both must agree.
    """
    out: dict[str, Any] = {}
    origin: dict[str, dict[str, str]] = {}
    for name, columns in vfm.items():
        new_name = mapping.get(name, name)
        if new_name not in out:
            out[new_name] = columns
            origin[new_name] = {field: name for field in columns or {}}
            continue
        existing = out[new_name]
        if not isinstance(existing, dict) or not isinstance(columns, dict):
            raise ValueError(
                f"cannot merge vertex_from_map entries for {new_name!r}: "
                "expected per-vertex field maps"
            )
        merged = dict(existing)
        for field, column in columns.items():
            if field in merged and merged[field] != column:
                raise ValueError(
                    f"cannot merge vertex_from_map for {new_name!r}: field {field!r} "
                    f"reads {merged[field]!r} for {origin[new_name][field]!r} but "
                    f"{column!r} for {name!r}"
                )
            merged[field] = column
            origin[new_name][field] = name
        out[new_name] = merged
    return out


def _map_name(name: str | None, mapping: dict[str, str]) -> str | None:
    if name is None:
        return None
    return mapping.get(name, name)


def materialize_router_renames(
    type_map: dict[str, Any] | None, mapping: dict[str, str]
) -> dict[str, Any] | None:
    """*type_map* with an entry for every class *mapping* renames away.

    An open router routes an unmapped discriminator value as the class name,
    so before the rename a raw ``old`` reached ``old`` with no table entry;
    after it only an entry can send it to ``new``, or the value names a class
    that no longer exists and the record is skipped at runtime without a word.
    Keys the table already has were rewritten in place. ``None`` when there is
    no table and nothing to add. A closed router (``type_map_only``) takes no
    entries: only its table's values ever routed.
    """
    present = type_map or {}
    added = {
        old: new for old, new in mapping.items() if old != new and old not in present
    }
    if not added:
        return type_map
    return {**present, **added}


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
        if not out.get("type_map_only"):
            # A closed router's raw values are fixed by its table.
            materialized = materialize_router_renames(out.get("type_map"), mapping)
            if materialized is not None:
                out["type_map"] = materialized
        vfm = out.get("vertex_from_map")
        if isinstance(vfm, dict):
            out["vertex_from_map"] = _merge_vertex_from_map(vfm, mapping)
        if isinstance(out.get("lookup_only"), list):
            out["lookup_only"] = renamed_lookup_classes(
                out["lookup_only"], lambda name: mapping.get(name, name)
            )

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
        rewrite_vertex_weight_names(out, lambda name: mapping.get(name, name))
        rename_endpoint_selector_classes(out, lambda name: mapping.get(name, name))

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
            inner = deepcopy(obj)
            inner["edge"] = rewrite_vertex_names_in_value(obj["edge"], mapping)
            # An extra_weights entry carries vertex_weights alongside its edge.
            rewrite_vertex_weight_names(inner, lambda name: mapping.get(name, name))
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
            rewrite_vertex_weight_names(out, lambda name: mapping.get(name, name))
            rename_endpoint_selector_classes(out, lambda name: mapping.get(name, name))
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


def _drop_edge_payloads(
    pipeline: list[dict[str, Any]],
    removed: Callable[[dict[str, Any]], bool],
    prune: Callable[[dict[str, Any]], None],
) -> list[dict[str, Any]]:
    """Drop the edge payloads *removed* selects, in every spelling, and *prune* the rest.

    A step goes when every edge payload it carried is removed. Nested
    ``descend`` pipelines are walked; a step that is not an edge is kept as is.
    """

    def _rewrite_step(step: dict[str, Any]) -> dict[str, Any] | None:
        out = deepcopy(step)
        payloads = edge_payloads(out)
        if payloads and payloads[0] is out:
            # The flat spelling: the step is its own payload.
            if removed(out):
                return None
            prune(out)
        elif payloads:
            for key in ("edge", "create_edge"):
                payload = out.get(key)
                if not isinstance(payload, dict):
                    continue
                if removed(payload):
                    out.pop(key)
                else:
                    prune(payload)
            if "edge" not in out and "create_edge" not in out:
                return None
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
        return out or None

    return [
        rewritten
        for rewritten in (
            _rewrite_step(step) for step in pipeline if isinstance(step, dict)
        )
        if rewritten is not None
    ]


def rewrite_remove_relations_in_pipeline(
    pipeline: list[dict[str, Any]], removed_relations: set[str]
) -> list[dict[str, Any]]:
    """Drop edge/create_edge steps (and links) targeting removed relations."""
    if not removed_relations:
        return deepcopy(pipeline)

    def _removed(payload: dict[str, Any]) -> bool:
        return payload.get("relation") in removed_relations

    def _prune(payload: dict[str, Any]) -> None:
        relation_map = payload.get("relation_map")
        if isinstance(relation_map, dict):
            payload["relation_map"] = {
                k: v
                for k, v in relation_map.items()
                if not (isinstance(v, str) and v in removed_relations)
            }
        links = payload.get("links")
        if isinstance(links, list):
            payload["links"] = [
                link
                for link in links
                if not (isinstance(link, dict) and _removed(link))
            ]

    return _drop_edge_payloads(pipeline, _removed, _prune)


def _payload_edge_id(payload: dict[str, Any]) -> tuple[str, str, str | None] | None:
    """Return logical edge id from static ``from``/``to`` (or ``source``/``target``) fields."""
    source = payload.get("from")
    if not isinstance(source, str):
        source = payload.get("source")
    target = payload.get("to")
    if not isinstance(target, str):
        target = payload.get("target")
    if not isinstance(source, str) or not isinstance(target, str):
        return None
    relation = payload.get("relation")
    rel = relation if isinstance(relation, str) else None
    return source, target, rel


def _payload_targets_removed_edge(
    payload: dict[str, Any], removed_edge_ids: set[tuple[str, str, str | None]]
) -> bool:
    edge_id = _payload_edge_id(payload)
    return edge_id is not None and edge_id in removed_edge_ids


def _prune_relation_map_for_removed_edge_ids(
    payload: dict[str, Any],
    removed_edge_ids: set[tuple[str, str, str | None]],
) -> None:
    relation_map = payload.get("relation_map")
    edge_id = _payload_edge_id(payload)
    if not isinstance(relation_map, dict) or edge_id is None:
        return
    source, target, _relation = edge_id
    payload["relation_map"] = {
        raw_key: mapped
        for raw_key, mapped in relation_map.items()
        if not (
            isinstance(mapped, str) and (source, target, mapped) in removed_edge_ids
        )
    }


def rewrite_remove_edge_ids_in_pipeline(
    pipeline: list[dict[str, Any]],
    removed_edge_ids: set[tuple[str, str, str | None]],
) -> list[dict[str, Any]]:
    """Drop edge/create_edge steps (and links) targeting removed edge triples."""
    if not removed_edge_ids:
        return deepcopy(pipeline)

    def _removed(payload: dict[str, Any]) -> bool:
        return _payload_targets_removed_edge(payload, removed_edge_ids)

    def _prune(payload: dict[str, Any]) -> None:
        _prune_relation_map_for_removed_edge_ids(payload, removed_edge_ids)
        links = payload.get("links")
        if isinstance(links, list):
            payload["links"] = [
                link
                for link in links
                if not (isinstance(link, dict) and _removed(link))
            ]

    return _drop_edge_payloads(pipeline, _removed, _prune)


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
        for payload in edge_payloads(out):
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


def rewrite_remove_vertices_in_pipeline(
    pipeline: list[dict[str, Any]], removed: set[str]
) -> list[dict[str, Any]]:
    """Trim *pipeline* to what survives removing the classes in *removed*.

    Step-wise, never resource-wise. A ``vertex`` or ``edge`` step naming a
    removed class goes. A ``vertex_router`` loses only the ``type_map`` and
    ``vertex_from_map`` entries for it and keeps routing the rest: a raw
    discriminator value naming a class the schema no longer declares is
    skipped at ingestion, which *is* the removal, so a pass-through router
    needs no edit at all; a closed router (``type_map_only``) stops routing
    the values that led to it. The class also leaves a router's ``lookup_only``
    list and the per-class selectors of the edge steps that survive. A
    ``descend`` stays while anything survives under it; transforms are left
    as authored. Untouched steps keep their authored
    spelling; a trimmed router is edited in place, and a ``descend`` that lost
    a step comes back normalized.
    """
    if not removed:
        return deepcopy(pipeline)

    def _router_payload(step: dict[str, Any]) -> dict[str, Any]:
        # The table lives under ``vertex_router`` in the shorthand and at the
        # top level in the flat spelling.
        payload = step.get("vertex_router")
        return payload if isinstance(payload, dict) else step

    def _rewrite_step(step: dict[str, Any]) -> dict[str, Any] | None:
        normalized = normalize_actor_step(dict(step))
        step_type = normalized.get("type")
        if step_type == "vertex":
            return None if normalized.get("vertex") in removed else deepcopy(step)
        if step_type == "edge":
            endpoints = (normalized.get(k) for k in ("source", "from", "target", "to"))
            if any(e in removed for e in endpoints):
                return None
            out = deepcopy(step)
            for edge_payload in edge_payloads(out):
                _drop_selector_classes(edge_payload, removed)
            return out
        if step_type == "vertex_router":
            out = deepcopy(step)
            payload = _router_payload(out)
            lookup = payload.get("lookup_only")
            if isinstance(lookup, list):
                kept_lookup = [name for name in lookup if name not in removed]
                if kept_lookup != lookup:
                    if kept_lookup:
                        payload["lookup_only"] = kept_lookup
                    else:
                        payload.pop("lookup_only")
            type_map = payload.get("type_map")
            if isinstance(type_map, dict):
                kept = {k: v for k, v in type_map.items() if v not in removed}
                if kept != type_map:
                    payload["type_map"] = kept or None
            vertex_from_map = payload.get("vertex_from_map")
            if isinstance(vertex_from_map, dict):
                kept_from = {
                    k: v for k, v in vertex_from_map.items() if k not in removed
                }
                if kept_from != vertex_from_map:
                    payload["vertex_from_map"] = kept_from or None
            return out
        if step_type == "descend":
            nested = normalized.get("pipeline")
            if not isinstance(nested, list):
                return deepcopy(step)
            items = [item for item in nested if isinstance(item, dict)]
            survivors = [
                rewritten
                for rewritten in (_rewrite_step(item) for item in items)
                if rewritten is not None
            ]
            if not survivors:
                return None
            if survivors == items:
                return deepcopy(step)
            out = deepcopy(normalized)
            out["pipeline"] = survivors
            return out
        return deepcopy(step)

    return [
        rewritten
        for rewritten in (
            _rewrite_step(step) for step in pipeline if isinstance(step, dict)
        )
        if rewritten is not None
    ]


def _drop_selector_classes(payload: dict[str, Any], removed: set[str]) -> None:
    """Remove *removed* classes from *payload*'s per-class selectors, in place."""
    for key in _MATCH_KEYS:
        selector = payload.get(key)
        if isinstance(selector, dict):
            kept = {k: v for k, v in selector.items() if k not in removed}
            if kept != selector:
                if kept:
                    payload[key] = kept
                else:
                    payload.pop(key)
    links = payload.get("links")
    if isinstance(links, list):
        for link in links:
            if isinstance(link, dict):
                _drop_selector_classes(link, removed)


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
