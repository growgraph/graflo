"""EdgeActor-aware inverse edge helpers for manifest evolution."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, cast

from graflo.architecture.database_features import DatabaseProfile
from graflo.architecture.pipeline.runtime.actor.config.models import (
    EdgeActorConfig,
    EdgeLinkConfig,
)
from graflo.architecture.pipeline.runtime.actor.config.normalize import (
    normalize_actor_step,
)
from graflo.architecture.schema.edge import Edge

EdgeTriple = tuple[str, str, str]


def _as_dict_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [cast(dict[str, Any], item) for item in value if isinstance(item, dict)]


def _edge_triple_from_payload(payload: dict[str, Any]) -> EdgeTriple | None:
    """Return (source, target, relation) from a pipeline or schema edge dict."""
    source = payload.get("from")
    target = payload.get("to")
    if not isinstance(source, str) or not isinstance(target, str):
        source = payload.get("source")
        target = payload.get("target")
    relation = payload.get("relation")
    if (
        isinstance(source, str)
        and isinstance(target, str)
        and isinstance(relation, str)
    ):
        return source, target, relation
    return None


def _collect_edge_triples(payloads: Iterable[dict[str, Any]]) -> set[EdgeTriple]:
    triples: set[EdgeTriple] = set()
    for payload in payloads:
        triple = _edge_triple_from_payload(payload)
        if triple is not None:
            triples.add(triple)
    return triples


def _edge_has_reverse_on_profile(
    edge: Edge, db_profile: DatabaseProfile | None
) -> bool:
    if db_profile is None:
        return False
    return db_profile.edge_reverse_edge_name(edge.edge_id) is not None


def _schema_endpoint_pairs(edges: list[Edge]) -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    for edge in edges:
        if edge.directed:
            pairs.add((edge.source, edge.target))
    return pairs


def _schema_has_template(edges: list[Edge], source: str, target: str) -> bool:
    return any(
        e.source == source and e.target == target and e.relation is None and e.directed
        for e in edges
    )


def _schema_has_forward_relation(
    edges: list[Edge],
    source: str,
    target: str,
    relation_map: dict[str, str],
) -> bool:
    forward_relations = set(relation_map)
    return any(
        e.source == source
        and e.target == target
        and e.relation in forward_relations
        and e.directed
        for e in edges
    )


def _schema_has_directed_forward_relations(
    edges: list[Edge], relations: set[str]
) -> bool:
    return any(e.relation in relations and e.directed for e in edges)


def _invert_relation_map(
    forward_map: dict[str, str], op_relations: dict[str, str]
) -> dict[str, str]:
    return {
        raw: op_relations[canonical]
        for raw, canonical in forward_map.items()
        if canonical in op_relations
    }


def _swap_match_fields(payload: dict[str, Any]) -> None:
    match_source = payload.get("match_source")
    match_target = payload.get("match_target")
    if match_source is not None or match_target is not None:
        payload["match_source"] = match_target
        payload["match_target"] = match_source


def _swap_edge_endpoints(payload: dict[str, Any]) -> dict[str, Any]:
    """Swap endpoint resolution fields on a flat edge-actor step dict."""
    out = dict(payload)
    source = out.get("from") or out.get("source")
    target = out.get("to") or out.get("target")
    source_role = out.get("source_role") or out.get("source_type_field")
    target_role = out.get("target_role") or out.get("target_type_field")

    for key in (
        "from",
        "to",
        "source",
        "target",
        "source_role",
        "target_role",
        "source_type_field",
        "target_type_field",
    ):
        out.pop(key, None)

    if isinstance(source, str) and isinstance(target, str):
        out["from"] = target
        out["to"] = source
    elif isinstance(source_role, str) and isinstance(target_role, str):
        out["source_role"] = target_role
        out["target_role"] = source_role
    elif isinstance(source, str) and isinstance(target_role, str):
        out["to"] = source
        out["source_role"] = target_role
    elif isinstance(source_role, str) and isinstance(target, str):
        out["from"] = target
        out["target_role"] = source_role
    _swap_match_fields(out)
    return out


def _invert_link_payload(
    link: dict[str, Any], op_relations: dict[str, str]
) -> dict[str, Any] | None:
    swapped = _swap_edge_endpoints(link)
    relation = swapped.get("relation")
    if isinstance(relation, str) and relation in op_relations:
        swapped["relation"] = op_relations[relation]
        return swapped
    relation_field = swapped.get("relation_field")
    if isinstance(relation_field, str):
        return swapped
    return None


def _has_swappable_endpoints(payload: dict[str, Any]) -> bool:
    source = payload.get("from") or payload.get("source")
    target = payload.get("to") or payload.get("target")
    source_role = payload.get("source_role") or payload.get("source_type_field")
    target_role = payload.get("target_role") or payload.get("target_type_field")
    if isinstance(source, str) and isinstance(target, str):
        return True
    if isinstance(source_role, str) and isinstance(target_role, str):
        return True
    if isinstance(source, str) and isinstance(target_role, str):
        return True
    return isinstance(source_role, str) and isinstance(target, str)


def _is_invertible_link(
    link: EdgeLinkConfig,
    op_relations: dict[str, str],
    schema_edges: list[Edge],
) -> bool:
    data = link.model_dump(by_alias=True, exclude_none=True)
    if not _has_swappable_endpoints(data):
        return False
    if link.relation is not None and link.relation in op_relations:
        return True
    if link.relation_field is not None:
        source = link.source
        target = link.target
        if source is not None and target is not None:
            if _schema_has_template(schema_edges, source, target):
                return True
            if _schema_has_forward_relation(schema_edges, source, target, op_relations):
                return True
        return True
    return False


def _is_invertible_edge_config(
    config: EdgeActorConfig,
    op_relations: dict[str, str],
    schema_edges: list[Edge],
) -> bool:
    if config.links:
        return any(
            _is_invertible_link(link, op_relations, schema_edges)
            for link in config.links
        )

    data = config.model_dump(by_alias=True, exclude_none=True)
    if not _has_swappable_endpoints(data):
        return False

    if config.relation is not None and config.relation in op_relations:
        return True

    if config.relation_field is not None or config.relation_from_key:
        source = config.source
        target = config.target
        if source is not None and target is not None:
            if _schema_has_template(schema_edges, source, target):
                return True
            if _schema_has_forward_relation(schema_edges, source, target, op_relations):
                return True
        if config.relation_map:
            forward_relations = {
                canonical
                for canonical in config.relation_map.values()
                if canonical in op_relations
            }
            return _schema_has_directed_forward_relations(
                schema_edges, forward_relations
            )
        if config.relation_field is not None or config.relation_from_key:
            return _schema_has_directed_forward_relations(
                schema_edges, set(op_relations)
            )

    return False


def _ensure_edge_step_dict(payload: dict[str, Any]) -> dict[str, Any]:
    data = dict(payload)
    if data.get("type") == "edge":
        return data
    if data.get("links") or data.get("relation_field") or data.get("relation_from_key"):
        data["type"] = "edge"
        return data
    if (
        data.get("source_role")
        or data.get("target_role")
        or data.get("source_type_field")
        or data.get("target_type_field")
    ):
        data["type"] = "edge"
        return data
    if ("from" in data or "source" in data) and ("to" in data or "target" in data):
        data["type"] = "edge"
    return data


def _invert_edge_actor_payload(
    payload: dict[str, Any],
    op_relations: dict[str, str],
    schema_edges: list[Edge],
) -> dict[str, Any] | None:
    normalized = normalize_actor_step(_ensure_edge_step_dict(payload))
    if normalized.get("type") != "edge":
        return None

    try:
        config = EdgeActorConfig.model_validate(normalized)
    except Exception:
        return None

    if not _is_invertible_edge_config(config, op_relations, schema_edges):
        return None

    if config.links:
        inverted_links: list[dict[str, Any]] = []
        for link in config.links:
            link_data = link.model_dump(by_alias=True, exclude_none=True)
            inverted = _invert_link_payload(link_data, op_relations)
            if inverted is not None:
                inverted_links.append(inverted)
        if not inverted_links:
            return None
        out: dict[str, Any] = {"type": "edge", "links": inverted_links}
        return out

    out = _swap_edge_endpoints(normalized)
    if config.relation is not None and config.relation in op_relations:
        out["relation"] = op_relations[config.relation]
    if config.relation_map:
        inverted_map = _invert_relation_map(config.relation_map, op_relations)
        if inverted_map:
            out["relation_map"] = inverted_map
        elif config.relation is None:
            out.pop("relation_map", None)
    return out


def _edge_actor_fingerprint(payload: dict[str, Any]) -> tuple[Any, ...]:
    normalized = normalize_actor_step(_ensure_edge_step_dict(payload))
    if normalized.get("type") != "edge":
        return ("not_edge",)
    if normalized.get("links"):
        links = normalized.get("links")
        if isinstance(links, list):
            return ("links", tuple(sorted(str(link) for link in links)))
    keys = (
        normalized.get("from"),
        normalized.get("to"),
        normalized.get("source"),
        normalized.get("target"),
        normalized.get("source_role"),
        normalized.get("target_role"),
        normalized.get("relation"),
        normalized.get("relation_field"),
        normalized.get("relation_from_key"),
        tuple(sorted((normalized.get("relation_map") or {}).items())),
    )
    return ("edge",) + keys


def _unwrap_edge_step(step: dict[str, Any]) -> dict[str, Any] | None:
    if isinstance(step.get("edge"), dict):
        return cast(dict[str, Any], step["edge"])
    normalized = normalize_actor_step(dict(step))
    if normalized.get("type") == "edge":
        return normalized
    return None


def _collect_pipeline_fingerprints(steps: list[dict[str, Any]]) -> set[tuple[Any, ...]]:
    fps: set[tuple[Any, ...]] = set()
    for step in steps:
        if normalize_actor_step(dict(step)).get("type") == "descend":
            pipeline = step.get("pipeline") or step.get("apply")
            if isinstance(pipeline, list):
                fps |= _collect_pipeline_fingerprints(_as_dict_list(pipeline))
            continue
        edge_payload = _unwrap_edge_step(step)
        if edge_payload is not None:
            wrapped = (
                {"edge": edge_payload}
                if isinstance(step.get("edge"), dict)
                else edge_payload
            )
            fps.add(_edge_actor_fingerprint(wrapped))
    return fps


def _append_inverse_edge_steps(
    steps: list[dict[str, Any]],
    op_relations: dict[str, str],
    schema_edges: list[Edge],
    existing: set[tuple[Any, ...]],
) -> list[dict[str, Any]]:
    out = list(steps)
    for idx, step in enumerate(steps):
        normalized = normalize_actor_step(dict(step))
        if normalized.get("type") == "descend":
            pipeline = step.get("pipeline") or step.get("apply")
            if isinstance(pipeline, list):
                nested = _as_dict_list(pipeline)
                updated_nested = _append_inverse_edge_steps(
                    nested, op_relations, schema_edges, existing
                )
                if updated_nested != nested:
                    new_step = dict(step)
                    new_step["pipeline"] = updated_nested
                    out[idx] = new_step
            continue

        edge_payload = _unwrap_edge_step(step)
        if edge_payload is None:
            continue

        inverted_inner = _invert_edge_actor_payload(
            edge_payload, op_relations, schema_edges
        )
        if inverted_inner is None:
            continue

        if isinstance(step.get("edge"), dict):
            inverse_step: dict[str, Any] = {"edge": inverted_inner}
        else:
            inverse_step = inverted_inner

        fp = _edge_actor_fingerprint(inverse_step)
        if fp in existing:
            continue
        out.append(inverse_step)
        existing.add(fp)
    return out


def _swapped_edge_payload(
    edge_payload: dict[str, Any], inverse_relation: str
) -> dict[str, Any]:
    triple = _edge_triple_from_payload(edge_payload)
    if triple is None:
        raise ValueError("edge payload must define source, target, and relation")
    source, target, _ = triple
    inverse_edge = dict(edge_payload)
    if isinstance(edge_payload.get("from"), str):
        inverse_edge["from"] = target
    if isinstance(edge_payload.get("to"), str):
        inverse_edge["to"] = source
    if isinstance(edge_payload.get("source"), str):
        inverse_edge["source"] = target
    if isinstance(edge_payload.get("target"), str):
        inverse_edge["target"] = source
    inverse_edge["relation"] = inverse_relation
    return inverse_edge


def _append_inverse_flat_specs(
    specs: list[dict[str, Any]], relation_map: dict[str, str]
) -> list[dict[str, Any]]:
    existing = _collect_edge_triples(specs)
    out = list(specs)
    for spec in specs:
        triple = _edge_triple_from_payload(spec)
        if triple is None:
            continue
        source, target, relation = triple
        inverse_relation = relation_map.get(relation)
        if inverse_relation is None:
            continue
        inverse_triple = (target, source, inverse_relation)
        if inverse_triple in existing:
            continue
        inverse_spec = dict(spec)
        inverse_spec["source"] = target
        inverse_spec["target"] = source
        inverse_spec["relation"] = inverse_relation
        out.append(inverse_spec)
        existing.add(inverse_triple)
    return out


def _append_inverses_for_nested_edges(
    entries: list[dict[str, Any]],
    relation_map: dict[str, str],
    *,
    edge_key: str,
    schema_edges: list[Edge],
) -> list[dict[str, Any]]:
    edge_payloads = [
        cast(dict[str, Any], entry[edge_key])
        for entry in entries
        if isinstance(entry.get(edge_key), dict)
    ]
    existing_triples = _collect_edge_triples(edge_payloads)
    existing_fps = {_edge_actor_fingerprint({edge_key: p}) for p in edge_payloads}
    out = list(entries)

    for entry in entries:
        edge_raw = entry.get(edge_key)
        if not isinstance(edge_raw, dict):
            continue
        edge_payload = cast(dict[str, Any], edge_raw)

        triple = _edge_triple_from_payload(edge_payload)
        if triple is not None:
            source, target, relation = triple
            inverse_relation = relation_map.get(relation)
            if inverse_relation is not None:
                inverse_triple = (target, source, inverse_relation)
                if inverse_triple not in existing_triples:
                    inverse_entry = dict(entry)
                    inverse_entry[edge_key] = _swapped_edge_payload(
                        edge_payload, inverse_relation
                    )
                    out.append(inverse_entry)
                    existing_triples.add(inverse_triple)
            continue

        inverted_inner = _invert_edge_actor_payload(
            edge_payload, relation_map, schema_edges
        )
        if inverted_inner is None:
            continue
        inverse_entry = dict(entry)
        inverse_entry[edge_key] = inverted_inner
        fp = _edge_actor_fingerprint({edge_key: inverted_inner})
        if fp in existing_fps:
            continue
        out.append(inverse_entry)
        existing_fps.add(fp)

    return out


def append_inverses_to_pipeline(
    pipeline: list[dict[str, Any]],
    op_relations: dict[str, str],
    schema_edges: list[Edge],
) -> list[dict[str, Any]]:
    existing = _collect_pipeline_fingerprints(pipeline)
    return _append_inverse_edge_steps(pipeline, op_relations, schema_edges, existing)


def _schema_edges_with_inverses(
    edges: list[Edge],
    relation_map: dict[str, str],
    db_profile: DatabaseProfile | None = None,
) -> list[Edge]:
    existing_edge_ids = {edge.edge_id for edge in edges}
    out = list(edges)

    for edge in edges:
        if not edge.directed:
            continue
        if edge.relation is None and edge.directed:
            inverse_template_id = (edge.target, edge.source, None)
            if inverse_template_id not in existing_edge_ids:
                out.append(
                    edge.model_copy(
                        deep=True,
                        update={"source": edge.target, "target": edge.source},
                    )
                )
                existing_edge_ids.add(inverse_template_id)

    for edge in list(out):
        if not edge.directed:
            continue
        if _edge_has_reverse_on_profile(edge, db_profile):
            continue
        if edge.relation is None:
            continue
        inverse_relation = relation_map.get(edge.relation)
        if inverse_relation is None:
            continue
        inverse_edge_id = (edge.target, edge.source, inverse_relation)
        if inverse_edge_id in existing_edge_ids:
            continue
        out.append(
            edge.model_copy(
                deep=True,
                update={
                    "source": edge.target,
                    "target": edge.source,
                    "relation": inverse_relation,
                    "directed": True,
                },
            )
        )
        existing_edge_ids.add(inverse_edge_id)

    return out


def materialize_inverse_infer_specs(
    edges: list[Edge],
    op_relations: dict[str, str],
) -> list[dict[str, str]]:
    specs: list[dict[str, str]] = []
    seen: set[EdgeTriple] = set()
    for edge in edges:
        if not edge.directed or edge.relation is None:
            continue
        inverse_relation = op_relations.get(edge.relation)
        if inverse_relation is None:
            continue
        triple = (edge.target, edge.source, inverse_relation)
        if triple in seen:
            continue
        specs.append(
            {
                "source": edge.target,
                "target": edge.source,
                "relation": inverse_relation,
            }
        )
        seen.add(triple)
    return specs
