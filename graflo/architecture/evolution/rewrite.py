"""Structured rewrite of vertex names in pipeline dicts and related resource fields."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Collection, Mapping
from copy import deepcopy
from functools import partial
from typing import Any

from graflo.architecture.contract.ingestion.resource import (
    role_reach,
    route_discriminator,
    router_reach,
    step_produces_vertices,
)
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


def router_payload(step: dict[str, Any]) -> dict[str, Any] | None:
    """The dict holding *step*'s router fields, or ``None`` for another step.

    Nested under ``vertex_router`` in the shorthand spelling, the step itself
    in the flat one; editing it keeps the authored spelling.
    """
    nested = step.get("vertex_router")
    if isinstance(nested, dict):
        return nested
    if step.get("type") == "vertex_router" or (
        "type_field" in step and "type" not in step
    ):
        return step
    return None


def evolve_router(
    payload: dict[str, Any],
    classes: Mapping[str, str | None],
    *,
    declared: Collection[str] | None = None,
) -> list[dict[str, Any]]:
    """The routers that replace a router once an op maps its classes by *classes*.

    *classes* maps an old class to its new name, or to ``None`` when the op
    removes it; a class it does not name is unchanged. *declared* is the
    schema's classes before the op. Every discriminator value must route as
    before, to its class renamed (or nowhere, if it was removed): the table's
    values and ``vertex_types`` are renamed, and a pass-through router gains
    ``{old: new}`` for each renamed class it reaches. A value that named no
    class may come to route to a new name -- as an open router always routes
    a declared name -- but only when the router reached every class renamed
    onto it.

    Where those fields cannot say that -- a ``vertex_types`` router reaching
    one class of a merge and not another, or a removed table entry whose key
    names a class -- the router is closed instead: ``type_map_only`` over
    exactly the values it accepted, then renamed.

    A merge of classes the router projects differently -- each its own
    ``vertex_from_map`` entry, or the shared ``from`` -- cannot keep one
    projection per class: the router is split into closed routers on the same
    ``type_field`` and ``role``, one per projection, each keyed by the values
    that reached its classes. The values stay disjoint, so no row is routed
    twice. Without *declared* the router's reach is unknown; it is neither
    closed nor split, and colliding projections are unioned.

    Without *declared*, a new name is taken to be new to the schema and every
    other name to be declared.

    Returns the replacing payloads: one as a rule, none when the router routes
    nothing any more, several when it was split.

    Raises:
        ValueError: If a merge joins a class the router only looks up with one
            it writes; one class cannot be both.
    """
    mapping = {old: new for old, new in classes.items() if old != new}
    before = _router_view(payload)
    reach = router_reach(before, known_vertices=declared)
    known_before = (
        set(declared) if declared is not None else _assumed_declared(before, mapping)
    )
    _refuse_mixed_lookup(before, mapping, reach, known_before)
    if reach is not None:
        pieces = _projection_pieces(payload, mapping, reach, known_before)
        if len(pieces) > 1:
            closed = _closed_router(payload, reach)
            return [
                renamed
                for piece in pieces
                if not _routes_nothing(
                    renamed := _renamed_router(
                        _router_piece(closed, piece), mapping, reach
                    )
                )
            ]

    known_after = (known_before - set(mapping)) | {
        new for new in mapping.values() if new is not None
    }
    candidate = _renamed_router(payload, mapping, reach)
    if not _routes_as_before(
        before, candidate, mapping, reach, known_before, known_after
    ):
        if reach is None:
            logger.warning(
                "vertex_router on %r: renaming %s changes what it routes, and "
                "without the schema's classes it cannot be closed",
                before.get("type_field"),
                sorted(mapping),
            )
        else:
            candidate = _renamed_router(_closed_router(payload, reach), mapping, reach)
    return [] if _routes_nothing(candidate) else [candidate]


def _projection_pieces(
    payload: dict[str, Any],
    mapping: Mapping[str, str | None],
    reach: frozenset[str],
    known_before: set[str],
) -> list[frozenset[str]]:
    """The classes of *reach* grouped into routers that each keep one projection.

    A class merged with others the router projects differently goes to the
    piece of its projection; every other class stays in the first piece. One
    piece when no merge mixes projections.
    """
    vertex_from_map = payload.get("vertex_from_map") or {}
    shared = _router_shared_from(payload)
    groups: dict[str, set[str]] = {}
    for old, new in mapping.items():
        if new is not None:
            groups.setdefault(new, {new} & known_before).add(old)
    by_projection: dict[str, set[str]] = {}
    first: set[str] = set(reach)
    for _target, members in sorted(groups.items()):
        projections: dict[str, set[str]] = {}
        for member in sorted(members & reach):
            projection = vertex_from_map.get(member, shared)
            key = json.dumps(projection, sort_keys=True, default=str)
            projections.setdefault(key, set()).add(member)
        if len(projections) < 2:
            continue
        # The first projection stays; the others move to their own pieces.
        for key, classes in list(projections.items())[1:]:
            by_projection.setdefault(key, set()).update(classes)
            first -= classes
    return [frozenset(first)] + [frozenset(c) for c in by_projection.values()]


def _router_piece(closed: dict[str, Any], classes: frozenset[str]) -> dict[str, Any]:
    """The part of a *closed* router whose values route to one of *classes*."""
    out = deepcopy(closed)
    table = closed.get("type_map") or {}
    out["type_map"] = {raw: c for raw, c in table.items() if c in classes} or None
    vertex_from_map = closed.get("vertex_from_map")
    if isinstance(vertex_from_map, dict):
        kept = {c: m for c, m in vertex_from_map.items() if c in classes}
        if kept:
            out["vertex_from_map"] = kept
        else:
            out.pop("vertex_from_map")
    lookup = closed.get("lookup_only")
    if isinstance(lookup, list):
        kept_lookup = [c for c in lookup if c in classes]
        if kept_lookup:
            out["lookup_only"] = kept_lookup
        else:
            out.pop("lookup_only")
    return out


def _router_view(payload: dict[str, Any]) -> dict[str, Any]:
    return normalize_actor_step({"type": "vertex_router", **payload})


def _assumed_declared(
    router: dict[str, Any], mapping: Mapping[str, str | None]
) -> set[str]:
    """Every name the router or the op mentions, but the op's new names."""
    names = set(mapping)
    for value in (router.get("type_map") or {}).values():
        if isinstance(value, str):
            names.add(value)
    names |= set(router.get("vertex_types") or [])
    names |= {key for key in router.get("type_map") or {} if isinstance(key, str)}
    fresh = {new for new in mapping.values() if new is not None} - set(mapping)
    return names - fresh


def _refuse_mixed_lookup(
    router: dict[str, Any],
    mapping: Mapping[str, str | None],
    reach: frozenset[str] | None,
    known_before: set[str],
) -> None:
    lookup = router.get("lookup_only")
    if not isinstance(lookup, list):
        return
    groups: dict[str, set[str]] = {}
    for old, new in mapping.items():
        if new is not None:
            # A target the schema already declared is merged into, not named.
            groups.setdefault(new, {new} & known_before).add(old)
    for target, members in sorted(groups.items()):
        routed = {m for m in members if reach is None or m in reach}
        looked_up = routed & set(lookup)
        if looked_up and routed - looked_up:
            raise ValueError(
                f"vertex_router on {router.get('type_field')!r}: merging "
                f"{sorted(routed)} into {target!r} joins classes it only looks up "
                f"({sorted(looked_up)}) with classes it writes "
                f"({sorted(routed - looked_up)}); split the router first"
            )


def _renamed_router(
    payload: dict[str, Any],
    mapping: Mapping[str, str | None],
    reach: frozenset[str] | None,
) -> dict[str, Any]:
    """*payload* with its classes mapped; pass-through renames materialized."""
    out = deepcopy(payload)

    def new_name(name: str) -> str | None:
        return mapping.get(name, name)

    table = out.get("type_map")
    if isinstance(table, dict):
        renamed: dict[Any, Any] = {}
        for raw, target in table.items():
            if not isinstance(target, str):
                renamed[raw] = target
            elif (mapped := new_name(target)) is not None:
                renamed[raw] = mapped
        table = renamed
    if not out.get("type_map_only"):
        # Before the op a raw `old` reached `old` with no entry; after it only
        # an entry can send it on to `new`.
        original = payload.get("type_map") or {}
        added = {
            old: new
            for old, new in mapping.items()
            if new is not None
            and old not in original
            and (reach is None or old in reach)
        }
        if added:
            table = {**(table or {}), **added}
    if table is not None:
        out["type_map"] = table or None

    bound = out.get("vertex_types")
    if isinstance(bound, list):
        out["vertex_types"] = sorted(
            {mapped for name in bound if (mapped := new_name(name)) is not None}
        )

    lookup = out.get("lookup_only")
    if isinstance(lookup, list):
        kept = [
            mapped
            for mapped in dict.fromkeys(new_name(name) for name in lookup)
            if mapped is not None
        ]
        if kept:
            out["lookup_only"] = kept
        else:
            out.pop("lookup_only")

    vertex_from_map = out.get("vertex_from_map")
    if isinstance(vertex_from_map, dict):
        # A projection for a class the router never routes is dead, and must
        # not merge into the projection of one it does.
        kept_from = _merge_vertex_from_map(
            {
                name: columns
                for name, columns in vertex_from_map.items()
                if new_name(name) is not None and (reach is None or name in reach)
            },
            {old: new for old, new in mapping.items() if new is not None},
        )
        out["vertex_from_map"] = kept_from or None
    return out


def _closed_router(payload: dict[str, Any], reach: frozenset[str]) -> dict[str, Any]:
    """*payload* closed over the raw values that reached a class of *reach*."""
    out = deepcopy(payload)
    table = payload.get("type_map") or {}
    closed = {raw: target for raw, target in table.items() if target in reach}
    if not payload.get("type_map_only"):
        # A class name passed through as itself -- unless the table keys it.
        closed |= {name: name for name in sorted(reach) if name not in table}
    out["type_map"] = closed or None
    out["type_map_only"] = True
    return out


def _routes_as_before(
    before: dict[str, Any],
    after_payload: dict[str, Any],
    mapping: Mapping[str, str | None],
    reach: frozenset[str] | None,
    known_before: set[str],
    known_after: set[str],
) -> bool:
    """Whether *after_payload* routes each value as *before* did, renamed.

    Checked on every value whose route the op can change: the table keys,
    the classes it lists or maps, and the op's names. Any other value names
    the same class (or none) on both sides of the op.
    """
    after = _router_view(after_payload)
    witnesses: set[Any] = set(mapping) | {n for n in mapping.values() if n}
    for router in (before, after):
        witnesses |= set(router.get("type_map") or {})
        witnesses |= set(router.get("vertex_types") or [])
    preimages: dict[str, set[str]] = {}
    for old, new in mapping.items():
        if new is not None:
            preimages.setdefault(new, set()).add(old)
    for value in witnesses:
        was = route_discriminator(before, value, known_before)
        now = route_discriminator(after, value, known_after)
        expected = None if was is None else mapping.get(was, was)
        if now == expected:
            continue
        fresh = (
            was is None
            and now == value
            and value not in known_before
            and all(reach is None or old in reach for old in preimages.get(value, ()))
        )
        if not fresh:
            return False
    return True


def _routes_nothing(payload: dict[str, Any]) -> bool:
    if payload.get("vertex_types") == []:
        return True
    return bool(payload.get("type_map_only")) and not payload.get("type_map")


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
    declared: Collection[str] | None = None,
) -> None:
    """Mutate a pipeline payload in place to rename vertices/relations.

    *declared* is the schema's classes before the rename; a router is evolved
    by :func:`evolve_router` against it.
    """
    vertex_name = _build_name_transformer(vertices)
    edge_name = _build_name_transformer(edges)

    if isinstance(step, list):
        for item in step:
            rewrite_entity_names_in_pipeline(
                item,
                vertices=vertices,
                edges=edges,
                declared=declared,
            )
        return
    if not isinstance(step, dict):
        return

    if isinstance(step.get("vertex"), str):
        step["vertex"] = vertex_name(step["vertex"])

    payload = router_payload(step)
    if payload is not None and vertices:
        evolved = evolve_router(payload, vertices, declared=declared)
        if len(evolved) > 1:
            raise ValueError(
                f"vertex_router on {payload.get('type_field')!r}: renaming "
                f"{sorted(vertices)} folds classes it projects differently; "
                "merge them with merge_vertices or canonicalize instead"
            )
        if evolved:
            payload.clear()
            payload.update(evolved[0])

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
                declared=declared,
            )
        pipeline_payload = descend_payload.get("pipeline")
        if pipeline_payload is not None:
            rewrite_entity_names_in_pipeline(
                pipeline_payload,
                vertices=vertices,
                edges=edges,
                declared=declared,
            )

    if isinstance(step.get("apply"), list):
        rewrite_entity_names_in_pipeline(
            step["apply"],
            vertices=vertices,
            edges=edges,
            declared=declared,
        )
    if isinstance(step.get("pipeline"), list):
        rewrite_entity_names_in_pipeline(
            step["pipeline"],
            vertices=vertices,
            edges=edges,
            declared=declared,
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


def _pin_endpoint_selectors_in_edge_payload(
    payload: dict[str, Any],
    selectors: dict[str, str],
    roles: dict[str, frozenset[str] | None],
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
            if role is None or role not in roles:
                continue
            held = roles[role]
            pinned = {
                vertex: selector
                for vertex, selector in selectors.items()
                if held is None or vertex in held
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
    step: Any, selectors: dict[str, str], roles: dict[str, frozenset[str] | None]
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
    _pin_endpoint_selectors_in_step(out, selectors, role_reach(out))
    return out


def close_routers_in_pipeline(
    pipeline: list[dict[str, Any]],
    vocabulary: Collection[str],
    *,
    declared_properties: Mapping[str, Collection[str]] | None = None,
    pass_through: Collection[str] | None = None,
) -> list[dict[str, Any]]:
    """*pipeline* with every open router closed over *vocabulary*.

    An open router passes a value missing from its ``type_map`` through as the
    class name. Closing it lists each class of *vocabulary* the table does not
    already key -- as itself -- and sets ``type_map_only``, so it routes
    exactly what it routed when *vocabulary* was the whole schema. A router
    with ``vertex_types`` gets entries only for the classes it lists. Entries
    already there, authored or written by a rename, are kept. Steps keep their
    authored spelling, at every level.

    A class the router reached only by pass-through took the part of the
    router-level ``from`` it declares; a listed class must declare all of it.
    Given *declared_properties* (property names per class), every class such
    a value routes to after closing that lacks a target gets that declared
    part as its ``vertex_from_map`` entry, so closing never refuses a
    projection the open router accepted. *pass_through* names those values
    when the table was rewritten since (see :func:`pass_through_classes`);
    by default they are the values closing itself lists.
    """
    out = deepcopy(pipeline)
    _close_routers(out, sorted(vocabulary), declared_properties, pass_through)
    return out


def pass_through_classes(
    pipeline: list[dict[str, Any]], vocabulary: Collection[str]
) -> set[str]:
    """Classes of *vocabulary* the routers of *pipeline* reach only by pass-through.

    Empty unless some router is open and unbounded. A class any router names
    -- a table target, or a ``vertex_types`` entry -- is excluded: the router
    already checked its ``from`` against that class at load.
    """
    named: set[str] = set()
    unbounded = False

    def walk(step: Any) -> None:
        nonlocal unbounded
        if isinstance(step, list):
            for item in step:
                walk(item)
            return
        if not isinstance(step, dict):
            return
        normalized = normalize_actor_step(dict(step))
        if normalized.get("type") == "vertex_router":
            named.update((normalized.get("type_map") or {}).values())
            named.update(normalized.get("vertex_types") or ())
            if not normalized.get("type_map_only") and not normalized.get(
                "vertex_types"
            ):
                unbounded = True
        for container in (step.get("descend"), step):
            if isinstance(container, dict):
                for key in ("apply", "pipeline"):
                    walk(container.get(key))

    walk(pipeline)
    return set(vocabulary) - named if unbounded else set()


def _keep_pass_through_projection(
    payload: dict[str, Any],
    classes: Collection[str],
    declared_properties: Mapping[str, Collection[str]],
) -> None:
    """Pin each of *classes* to the part of the router's ``from`` it declares."""
    from_doc = payload.get("from", payload.get("from_doc"))
    if not isinstance(from_doc, dict) or not from_doc:
        return
    per_class = dict(payload.get("vertex_from_map") or {})
    for name in sorted(classes):
        declared = declared_properties.get(name)
        if declared is None or name in per_class:
            continue
        part = {
            target: column for target, column in from_doc.items() if target in declared
        }
        if len(part) < len(from_doc):
            per_class[name] = part
    if per_class:
        payload["vertex_from_map"] = per_class


def _close_routers(
    step: Any,
    vocabulary: list[str],
    declared_properties: Mapping[str, Collection[str]] | None = None,
    pass_through: Collection[str] | None = None,
) -> None:
    if isinstance(step, list):
        for item in step:
            _close_routers(item, vocabulary, declared_properties, pass_through)
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
        bound = normalized.get("vertex_types")
        table = dict(payload.get("type_map") or {})
        # What the open router checked its `from` against at load: the
        # classes its table sends to, or every class it is bounded to.
        named = set(table.values()) if bound is None else set(bound)
        listed: list[str] = []
        for name in vocabulary:
            if bound is None or name in bound:
                if name not in table and name not in named:
                    listed.append(name)
                table.setdefault(name, name)
        payload["type_map"] = table
        payload["type_map_only"] = True
        if declared_properties is not None and bound is None:
            values = listed if pass_through is None else pass_through
            targets = {table[value] for value in values if value in table}
            _keep_pass_through_projection(payload, targets, declared_properties)

    descend_payload = step.get("descend")
    if isinstance(descend_payload, dict):
        for key in ("apply", "pipeline"):
            _close_routers(
                descend_payload.get(key), vocabulary, declared_properties, pass_through
            )
    for key in ("apply", "pipeline"):
        if isinstance(step.get(key), list):
            _close_routers(step[key], vocabulary, declared_properties, pass_through)


def mark_lookup_only_in_pipeline(
    pipeline: list[dict[str, Any]], vertex: str
) -> list[dict[str, Any]]:
    """*pipeline* with every production of *vertex* turned into a lookup.

    A ``vertex`` step for it gains ``lookup_only: true``. An open
    ``vertex_router`` routes an unmapped value as the class name, so it can
    produce *vertex* unless its ``vertex_types`` leaves it out, and a closed
    one (``type_map_only``) can when its table routes to it -- its
    :func:`router_reach`: such a router gains *vertex* in its ``lookup_only`` list,
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
        reach = router_reach(normalized)
        routes_to_vertex = reach is None or vertex in reach
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


def rewrite_vertex_names_in_step(
    step: dict[str, Any],
    mapping: dict[str, str],
    *,
    declared: Collection[str] | None = None,
) -> dict[str, Any]:
    """Return a deep-copied step with vertex names rewritten per *mapping*.

    *declared* is the schema's classes before the rewrite; a router is evolved
    by :func:`evolve_router` against it.

    Raises:
        ValueError: If the rewrite splits a router into several steps; use
            :func:`rewrite_vertex_names_in_pipeline`, which can hold them.
    """
    steps = _rewrite_vertex_names(step, mapping, declared=declared)
    if len(steps) != 1:
        raise ValueError(
            f"rewriting {sorted(mapping)} turns one step into {len(steps)}"
        )
    return steps[0]


def _rewrite_vertex_names(
    step: dict[str, Any],
    mapping: dict[str, str],
    *,
    declared: Collection[str] | None,
) -> list[dict[str, Any]]:
    """The steps *step* becomes once its vertex names follow *mapping*.

    One as a rule; none when a router routes nothing any more; several when a
    router is split (see :func:`evolve_router`).
    """
    if not mapping:
        return [deepcopy(step)]
    s = normalize_actor_step(dict(step))
    out = deepcopy(s)
    t = out.get("type")

    if t == "vertex":
        v = out.get("vertex")
        if isinstance(v, str) and v in mapping:
            out["vertex"] = mapping[v]

    elif t == "vertex_router":
        return evolve_router(out, mapping, declared=declared)

    elif t == "edge":
        for payload in _edge_payload_and_links(out):
            for key in ("source", "from", "target", "to"):
                value = payload.get(key)
                if isinstance(value, str) and value in mapping:
                    payload[key] = mapping[value]
            rewrite_vertex_weight_names(payload, lambda name: mapping.get(name, name))
            rename_endpoint_selector_classes(
                payload, lambda name: mapping.get(name, name)
            )

    elif t == "descend":
        pl = out.get("pipeline")
        if isinstance(pl, list):
            out["pipeline"] = [
                rewritten
                for x in pl
                if isinstance(x, dict)
                for rewritten in _rewrite_vertex_names(
                    cast_step(x), mapping, declared=declared
                )
            ]

    return [out]


def _edge_payload_and_links(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """*payload* and each link of its ``links`` list, as mutable references."""
    links = payload.get("links")
    return [
        payload,
        *(link for link in links or [] if isinstance(link, dict)),
    ]


def cast_step(x: Any) -> dict[str, Any]:
    if not isinstance(x, dict):
        raise TypeError(f"expected dict step, got {type(x)}")
    return x


def rewrite_vertex_names_in_pipeline(
    pipeline: list[dict[str, Any]],
    mapping: dict[str, str],
    *,
    declared: Collection[str] | None = None,
) -> list[dict[str, Any]]:
    """Rewrite all steps in a resource pipeline (see :func:`rewrite_vertex_names_in_step`)."""
    if not mapping:
        return deepcopy(pipeline)
    return [
        rewritten
        for s in pipeline
        for rewritten in _rewrite_vertex_names(s, mapping, declared=declared)
    ]


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


def _collect_level_vertices(
    steps: list[Any], *, declared: Collection[str] | None = None
) -> set[str]:
    """Vertex names the immediate (non-recursive) level can produce.

    With *declared*, an unbounded router contributes every declared class.
    """
    out: set[str] = set()
    for step in steps:
        if isinstance(step, dict):
            out |= step_produces_vertices(step, known_vertices=declared)
    return out


def _router_shared_from(payload: dict[str, Any]) -> dict[str, Any] | None:
    shared = payload.get("from", payload.get("from_doc"))
    return shared if isinstance(shared, dict) else None


def _rewrite_router_projections(
    step: dict[str, Any],
    per_class: Mapping[str, Callable[[dict[str, Any] | None], dict[str, Any] | None]],
    declared: Collection[str] | None,
) -> dict[str, Any]:
    """*step* with ``vertex_from_map[V]`` rewritten for each class in *per_class*.

    A router's ``from`` is shared by every class it routes, so a change for
    one class goes into that class's own ``vertex_from_map`` entry, seeded
    from the shared map. Only classes the router reaches are touched. Each
    callable takes the class's current map (``None`` when it has none) and
    returns the new one, or ``None`` to leave the class as it is. Keeps the
    authored spelling.
    """
    out = deepcopy(step)
    payload = router_payload(out)
    if payload is None:
        return out
    reach = router_reach(normalize_actor_step(dict(step)), known_vertices=declared)
    vertex_from_map = dict(payload.get("vertex_from_map") or {})
    shared = _router_shared_from(payload)
    changed = False
    for vertex, rewrite in per_class.items():
        if reach is not None and vertex not in reach:
            continue
        current = vertex_from_map.get(vertex) if vertex in vertex_from_map else shared
        rewritten = rewrite(dict(current) if isinstance(current, dict) else None)
        if rewritten is not None:
            vertex_from_map[vertex] = rewritten
            changed = True
    if changed:
        payload["vertex_from_map"] = vertex_from_map
    return out


def _rewrite_vertex_field_step(
    step: dict[str, Any],
    renames: dict[str, dict[str, str]],
    available_vertices: set[str],
    declared: Collection[str] | None = None,
) -> dict[str, Any]:
    """Rewrite a single normalized step for vertex field renames.

    ``available_vertices`` is the set of vertex names in scope at the call site
    (vertices created at this level or by ancestors). It bounds which renames
    apply to ``transform`` rename maps. *declared* is the schema's classes, the
    ones an unbounded router reaches.
    """
    s = normalize_actor_step(dict(step))
    out = deepcopy(s)
    t = out.get("type")

    if t == "vertex_router":
        return _rewrite_router_projections(
            step,
            {
                vertex: partial(_apply_vertex_field_rename_to_from_doc, renames=per)
                for vertex, per in renames.items()
                if per
            },
            declared,
        )

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
            descend_level_vertices = _collect_level_vertices(pl, declared=declared)
            nested_available = available_vertices | descend_level_vertices
            out["pipeline"] = [
                _rewrite_vertex_field_step(x, renames, nested_available, declared)
                for x in pl
                if isinstance(x, dict)
            ]

    return out


def rewrite_vertex_field_names_in_pipeline(
    pipeline: list[dict[str, Any]],
    renames: dict[str, dict[str, str]],
    *,
    available_vertices: set[str] | None = None,
    declared: Collection[str] | None = None,
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
    - ``vertex_router`` steps: the same ``from`` rewrite, per class the router
      reaches, in that class's ``vertex_from_map`` entry.
    - ``descend`` steps: recurse with an extended ``available_vertices`` set.

    ``available_vertices`` is the set of vertex names visible from the parent
    scope. Vertex names introduced at the current level are added automatically.
    *declared* is the schema's classes: the ones an unbounded router reaches.
    """
    if not renames:
        return deepcopy(pipeline)
    parent_scope = set(available_vertices) if available_vertices else set()
    level_vertices = _collect_level_vertices(pipeline, declared=declared)
    scope = parent_scope | level_vertices
    return [
        _rewrite_vertex_field_step(step, renames, scope, declared)
        for step in pipeline
        if isinstance(step, dict)
    ]


def rewrite_remove_vertex_properties_in_pipeline(
    pipeline: list[dict[str, Any]],
    removals: dict[str, set[str]],
    *,
    declared: Collection[str] | None = None,
) -> list[dict[str, Any]]:
    """Remove references to dropped vertex fields from pipeline steps.

    A router stops mapping them for each class it reaches -- *declared* is the
    schema's classes, the ones an unbounded router reaches -- in that class's
    own ``vertex_from_map`` entry, so the classes sharing its ``from`` keep it.
    """
    if not removals:
        return deepcopy(pipeline)

    def _without(
        removed: set[str],
    ) -> Callable[[dict[str, Any] | None], dict[str, Any] | None]:
        def drop(current: dict[str, Any] | None) -> dict[str, Any] | None:
            if current is None or not set(current) & removed:
                return None
            return {k: v for k, v in current.items() if k not in removed}

        return drop

    def _rewrite_step(step: dict[str, Any]) -> dict[str, Any]:
        out = deepcopy(normalize_actor_step(dict(step)))
        step_type = out.get("type")

        if step_type == "vertex_router":
            return _rewrite_router_projections(
                step,
                {v: _without(fields) for v, fields in removals.items() if fields},
                declared,
            )

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

    A payload *prune* leaves with an empty ``links`` list goes too: it writes
    nothing. A step goes when every edge payload it carried is removed. Nested
    ``descend`` pipelines are walked; a step that is not an edge is kept as is.
    """

    def _gone(payload: dict[str, Any]) -> bool:
        """Removed, or pruned down to an empty ``links`` list."""
        if removed(payload):
            return True
        had_links = bool(payload.get("links"))
        prune(payload)
        return had_links and not payload.get("links")

    def _rewrite_step(step: dict[str, Any]) -> dict[str, Any] | None:
        out = deepcopy(step)
        payloads = edge_payloads(out)
        if payloads and payloads[0] is out:
            # The flat spelling: the step is its own payload.
            if _gone(out):
                return None
        elif payloads:
            for key in ("edge", "create_edge"):
                payload = out.get(key)
                if isinstance(payload, dict) and _gone(payload):
                    out.pop(key)
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
    pipeline: list[dict[str, Any]],
    removed: set[str],
    *,
    declared: Collection[str] | None = None,
) -> list[dict[str, Any]]:
    """Trim *pipeline* to what survives removing the classes in *removed*.

    Step-wise, never resource-wise. A ``vertex`` or ``edge`` step naming a
    removed class goes. A ``vertex_router`` is evolved by
    :func:`evolve_router` against *declared* (the schema's classes before the
    removal): it loses its entries for the class and keeps routing the rest,
    and a router left routing nothing goes. The class also leaves the
    per-class selectors of the edge steps that survive. An edge step
    addressing a role that no step fills any more goes too. A ``descend``
    stays while anything survives under it; transforms are left as authored.
    Untouched steps keep their authored spelling; a trimmed router is edited
    in place, and a ``descend`` that lost a step comes back normalized.
    """
    if not removed:
        return deepcopy(pipeline)
    removal: dict[str, str | None] = dict.fromkeys(removed)

    def _rewrite_step(step: dict[str, Any]) -> dict[str, Any] | None:
        normalized = normalize_actor_step(dict(step))
        step_type = normalized.get("type")
        if step_type == "vertex":
            return None if normalized.get("vertex") in removed else deepcopy(step)
        if step_type == "edge":
            if _names_any(normalized, removed):
                return None
            out = deepcopy(step)
            for edge_payload in edge_payloads(out):
                if not _keep_links(
                    edge_payload, lambda link: _names_any(link, removed)
                ):
                    return None
                _drop_selector_classes(edge_payload, removed)
            return out
        if step_type == "vertex_router":
            out = deepcopy(step)
            payload = router_payload(out)
            if payload is None:
                return out
            evolved = evolve_router(payload, removal, declared=declared)
            if not evolved:
                return None
            # A removal folds no classes, so it never splits a router.
            payload.clear()
            payload.update(evolved[0])
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

    out = [
        rewritten
        for rewritten in (
            _rewrite_step(step) for step in pipeline if isinstance(step, dict)
        )
        if rewritten is not None
    ]
    unfilled = set(role_reach(pipeline)) - {
        role for role, held in role_reach(out).items() if held != frozenset()
    }
    return _drop_edges_on_roles(out, unfilled) if unfilled else out


_ROLE_KEYS = ("source_role", "target_role", "source_type_field", "target_type_field")
_ENDPOINT_KEYS = ("source", "from", "target", "to")


def _names_any(payload: dict[str, Any], classes: Collection[str]) -> bool:
    """Whether an edge payload or link names one of *classes* as a static endpoint."""
    return any(payload.get(key) in classes for key in _ENDPOINT_KEYS)


def _keep_links(
    payload: dict[str, Any], drop: Callable[[dict[str, Any]], bool]
) -> bool:
    """Drop the links of *payload* that *drop* selects, in place.

    ``False`` when it had links and none is left: the payload writes nothing.
    """
    links = payload.get("links")
    if not isinstance(links, list) or not links:
        return True
    kept = [link for link in links if not (isinstance(link, dict) and drop(link))]
    payload["links"] = kept
    return bool(kept)


def _drop_edges_on_roles(
    pipeline: list[dict[str, Any]], roles: set[str]
) -> list[dict[str, Any]]:
    """*pipeline* without the edge steps, and links, that address one of *roles*."""

    def addresses(payload: dict[str, Any]) -> bool:
        return any(payload.get(key) in roles for key in _ROLE_KEYS)

    out: list[dict[str, Any]] = []
    for step in pipeline:
        step = deepcopy(step)
        if any(
            addresses(payload) or not _keep_links(payload, addresses)
            for payload in edge_payloads(step)
        ):
            continue
        normalized = normalize_actor_step(dict(step))
        nested = normalized.get("pipeline")
        if normalized.get("type") == "descend" and isinstance(nested, list):
            kept = _drop_edges_on_roles(nested, roles)
            if len(kept) != len(nested):
                if not kept:
                    continue
                step = {**normalized, "pipeline": kept}
        out.append(step)
    return out


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
            if any(v in names for v in s.get("vertex_types") or []):
                return True
        elif t == "edge":
            if any(_names_any(p, names) for p in _edge_payload_and_links(s)):
                return True
        elif t == "descend":
            pl = s.get("pipeline") or []
            if isinstance(pl, list) and pipeline_mentions_any_vertex(
                [cast_step(x) for x in pl if isinstance(x, dict)], names
            ):
                return True
    return False
