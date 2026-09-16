"""Realize declared inverses as explicit logical edges (``add_inverse_edges``).

Three layers are touched, and each follows from the edges the op *creates*:

- schema: for every directed edge ``(S, T, r)`` with ``r`` mapped to ``inv``, the
  edge ``(T, S, inv)`` is added unless it already exists. An inverse edge that
  the user already declared explicitly is left alone, including its ingestion.
- profile: the new edge gets a copy of the forward edge's physical spec, minus
  its physical name (two logical types must not share one storage type).
- ingestion: a step writes the inverse only for created edges, and only in a
  form that cannot write anything else. A step that cannot be inverted that
  precisely is skipped with a warning rather than guessed at.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any, cast

from graflo.architecture.contract.ingestion.steps.models import (
    EdgeActorConfig,
    EdgeLinkConfig,
)
from graflo.architecture.contract.ingestion.steps.normalize import (
    normalize_actor_step,
)
from graflo.architecture.graph_types import EdgeId
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import Edge, EdgeConfig, inverse_map

logger = logging.getLogger(__name__)

EdgeTriple = tuple[str, str, str]

_ENDPOINT_KEYS = (
    "from",
    "to",
    "source",
    "target",
    "source_role",
    "target_role",
    "source_type_field",
    "target_type_field",
)


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


# -- schema -------------------------------------------------------------


def declared_relation_map(
    edge_config: EdgeConfig, relations: list[str] | None
) -> dict[str, str]:
    """``{relation: declared inverse}`` for the relations an op realizes.

    ``None`` selects every paired relation, both sides of each pair. Symmetric
    relations are never selected: their realization is ``directed: false``.

    Raises:
        ValueError: when no pair is declared, or a named relation has no pair.
    """
    declared = inverse_map(edge_config.inverses)
    if relations is None:
        if not declared:
            raise ValueError(
                "add_inverse_edges: no inverse pairs declared in "
                "edge_config.inverses; declare pairs first (declare_edge_inverses)"
            )
        return declared
    symmetric = sorted(r for r in relations if edge_config.is_symmetric(r))
    if symmetric:
        raise ValueError(
            f"add_inverse_edges: {symmetric} are symmetric; a symmetric relation "
            "has no inverse edge, its edges are undirected"
        )
    undeclared = sorted(set(relations) - set(declared))
    if undeclared:
        raise ValueError(
            f"add_inverse_edges: no declared inverse for relations {undeclared}; "
            "declare the pairs first (declare_edge_inverses)"
        )
    return {relation: declared[relation] for relation in relations}


def schema_edges_with_inverses(
    edges: list[Edge], relation_map: dict[str, str]
) -> list[Edge]:
    """``edges`` plus ``(T, S, inv)`` for each directed ``(S, T, r)`` with ``r -> inv``.

    The inverse keeps the forward edge's payload (properties, identities, type)
    but not its ``semantics`` or ``description``: those describe the forward
    reading, and grounding ``employs`` to the IRI of ``employed_by`` is wrong.
    Undirected edges are not inverted; callers refuse them before getting here.
    """
    existing = {edge.edge_id for edge in edges}
    out = list(edges)
    for edge in edges:
        if not edge.directed or edge.relation is None:
            continue
        inverse_relation = relation_map.get(edge.relation)
        if inverse_relation is None:
            continue
        inverse_id = (edge.target, edge.source, inverse_relation)
        if inverse_id in existing:
            continue
        out.append(
            edge.model_copy(
                deep=True,
                update={
                    "source": edge.target,
                    "target": edge.source,
                    "relation": inverse_relation,
                    "directed": True,
                    "semantics": None,
                    "description": None,
                },
            )
        )
        existing.add(inverse_id)
    return out


def plan_inverse_edges(
    schema: Schema, relations: list[str] | None
) -> tuple[dict[str, str], list[Edge]]:
    """The relation map and the inverse edges ``add_inverse_edges`` would create.

    Shared by the op and its inverse, so undoing removes exactly what applying
    added. Relations whose inverse is native are refused when named and skipped
    when the whole table is realized: they already have their inverse.

    Raises:
        ValueError: for an undeclared relation, or a named natively realized one.
    """
    edge_config = schema.core_schema.edge_config
    relation_map = declared_relation_map(edge_config, relations)
    profile = schema.db_profile
    # Either side of a native pair: the database already maintains both names.
    natively_paired = {
        name
        for relation in profile.native_inverses
        for name in (relation, edge_config.inverse_of(relation))
        if name is not None
    }
    native = sorted(set(relation_map) & natively_paired)
    if native and relations is not None:
        raise ValueError(
            "add_inverse_edges: the inverse of these relations is already "
            f"maintained natively by the database: {native}. An explicit inverse "
            "edge would store the same fact twice; withdraw it with "
            "set_native_inverses (enabled=false) first, or keep the native inverse"
        )
    existing = {edge.edge_id for edge in edge_config.edges}
    candidates = [
        edge for edge in edge_config.edges if edge.relation not in natively_paired
    ]
    created = [
        edge
        for edge in schema_edges_with_inverses(candidates, relation_map)
        if edge.edge_id not in existing
    ]
    return relation_map, created


# -- ingestion ----------------------------------------------------------


def _swap_edge_endpoints(payload: dict[str, Any]) -> dict[str, Any]:
    """Swap endpoint resolution fields on a flat edge-actor step dict."""
    out = dict(payload)
    source = out.get("from") or out.get("source")
    target = out.get("to") or out.get("target")
    source_role = out.get("source_role") or out.get("source_type_field")
    target_role = out.get("target_role") or out.get("target_type_field")
    for key in _ENDPOINT_KEYS:
        out.pop(key, None)

    if isinstance(target, str):
        out["from"] = target
    elif isinstance(target_role, str):
        out["source_role"] = target_role
    if isinstance(source, str):
        out["to"] = source
    elif isinstance(source_role, str):
        out["target_role"] = source_role

    match_source, match_target = out.get("match_source"), out.get("match_target")
    if match_source is not None or match_target is not None:
        out["match_source"] = match_target
        out["match_target"] = match_source
    exclude_source, exclude_target = (
        out.get("exclude_source"),
        out.get("exclude_target"),
    )
    if exclude_source is not None or exclude_target is not None:
        out["exclude_source"] = exclude_target
        out["exclude_target"] = exclude_source
    source_match, target_match = out.get("source_match"), out.get("target_match")
    if source_match is not None or target_match is not None:
        out["source_match"] = target_match
        out["target_match"] = source_match
    return {k: v for k, v in out.items() if v is not None}


def _ensure_edge_step_dict(payload: dict[str, Any]) -> dict[str, Any]:
    data = dict(payload)
    if data.get("type") == "edge":
        return data
    if (
        data.get("links")
        or data.get("relation_field")
        or data.get("relation_from_key")
        or any(data.get(key) for key in _ENDPOINT_KEYS[4:])
        or (("from" in data or "source" in data) and ("to" in data or "target" in data))
    ):
        data["type"] = "edge"
    return data


class _Inverter:
    """Invert edge steps for one op application, against the edges it created."""

    def __init__(self, relation_map: dict[str, str], created: set[EdgeId]) -> None:
        self.created = created
        created_relations = {relation for _, _, relation in created}
        # Only forward relations whose inverse actually got created drive
        # ingestion; an inverse that already existed is the user's to feed.
        self.active = {
            relation: inverse
            for relation, inverse in relation_map.items()
            if inverse in created_relations
        }

    def invert_static(self, triple: EdgeTriple) -> EdgeTriple | None:
        source, target, relation = triple
        inverse = self.active.get(relation)
        if inverse is None or (target, source, inverse) not in self.created:
            return None
        return target, source, inverse

    def invert_step(
        self, payload: dict[str, Any], *, where: str
    ) -> dict[str, Any] | None:
        """Inverse of one edge step payload, or ``None`` when it writes no created edge."""
        normalized = normalize_actor_step(_ensure_edge_step_dict(payload))
        if normalized.get("type") != "edge":
            return None
        try:
            config = EdgeActorConfig.model_validate(normalized)
        except Exception:
            return None

        if config.links:
            links = [
                inverted
                for link in config.links
                if (inverted := self._invert_link(link, where=where)) is not None
            ]
            return {"type": "edge", "links": links} if links else None

        if config.relation_from_key:
            if self.active:
                logger.warning(
                    "add_inverse_edges: %s derives its relation from document keys; "
                    "no inverse step is generated for it (add one explicitly)",
                    where,
                )
            return None

        dynamic_endpoints = (
            config.source_role is not None or config.target_role is not None
        )
        if config.relation_field is not None:
            if not dynamic_endpoints:
                # Static endpoints read the relation at assembly time, where no
                # relation_map applies, so the step cannot be restricted to the
                # mapped relations.
                if self.active:
                    logger.warning(
                        "add_inverse_edges: %s reads relation_field with static "
                        "endpoints; no inverse step is generated for it",
                        where,
                    )
                return None
            return self._invert_dynamic_relation(config, normalized)

        if config.relation is None or config.relation not in self.active:
            return None
        if not dynamic_endpoints:
            assert config.source is not None and config.target is not None
            if (
                self.invert_static((config.source, config.target, config.relation))
                is None
            ):
                return None
        out = _swap_edge_endpoints(normalized)
        out["relation"] = self.active[config.relation]
        return out

    def _invert_dynamic_relation(
        self, config: EdgeActorConfig, normalized: dict[str, Any]
    ) -> dict[str, Any] | None:
        if config.relation_map:
            inverse_map = {
                raw: self.active[canonical]
                for raw, canonical in config.relation_map.items()
                if canonical in self.active
            }
        else:
            # Raw values are relation names themselves.
            inverse_map = dict(self.active)
        if not inverse_map:
            return None
        out = _swap_edge_endpoints(normalized)
        out["relation_map"] = inverse_map
        out["relation_map_only"] = True
        # The static relation is the fallback when the field is empty; a
        # forward name there would be written reversed.
        fallback = config.relation
        if fallback is not None and fallback in self.active:
            out["relation"] = self.active[fallback]
        else:
            out.pop("relation", None)
        return out

    def _invert_link(
        self, link: EdgeLinkConfig, *, where: str
    ) -> dict[str, Any] | None:
        data = link.model_dump(by_alias=True, exclude_none=True)
        if link.relation_field is not None:
            # Links carry no relation_map, so they cannot be restricted.
            if self.active:
                logger.warning(
                    "add_inverse_edges: a link in %s reads relation_field; no "
                    "inverse link is generated for it",
                    where,
                )
            return None
        if link.relation is None or link.relation not in self.active:
            return None
        if (
            link.source is not None
            and link.target is not None
            and self.invert_static((link.source, link.target, link.relation)) is None
        ):
            return None
        out = _swap_edge_endpoints(data)
        out["relation"] = self.active[link.relation]
        return out


def _step_fingerprint(payload: dict[str, Any]) -> tuple[Any, ...]:
    normalized = normalize_actor_step(_ensure_edge_step_dict(payload))
    triple = _edge_triple_from_payload(normalized)
    if triple is not None and not normalized.get("relation_field"):
        return ("triple", *triple)
    links = normalized.get("links")
    if isinstance(links, list):
        return ("links", tuple(sorted(repr(sorted(link.items())) for link in links)))
    return (
        "edge",
        normalized.get("source_role"),
        normalized.get("target_role"),
        normalized.get("from"),
        normalized.get("to"),
        normalized.get("relation"),
        normalized.get("relation_field"),
        tuple(sorted((normalized.get("relation_map") or {}).items())),
    )


def _unwrap_edge_step(step: dict[str, Any]) -> dict[str, Any] | None:
    if isinstance(step.get("edge"), dict):
        return cast(dict[str, Any], step["edge"])
    normalized = normalize_actor_step(dict(step))
    if normalized.get("type") == "edge":
        return normalized
    return None


def _nested_pipeline(step: dict[str, Any]) -> list[dict[str, Any]] | None:
    if normalize_actor_step(dict(step)).get("type") != "descend":
        return None
    pipeline = step.get("pipeline") or step.get("apply")
    return _as_dict_list(pipeline) if isinstance(pipeline, list) else None


def _collect_fingerprints(steps: Iterable[dict[str, Any]]) -> set[tuple[Any, ...]]:
    fingerprints: set[tuple[Any, ...]] = set()
    for step in steps:
        nested = _nested_pipeline(step)
        if nested is not None:
            fingerprints |= _collect_fingerprints(nested)
            continue
        payload = _unwrap_edge_step(step)
        if payload is not None:
            fingerprints.add(_step_fingerprint(payload))
    return fingerprints


def _append_inverse_steps(
    steps: list[dict[str, Any]],
    inverter: _Inverter,
    existing: set[tuple[Any, ...]],
    *,
    where: str,
) -> list[dict[str, Any]]:
    out = list(steps)
    for idx, step in enumerate(steps):
        nested = _nested_pipeline(step)
        if nested is not None:
            updated = _append_inverse_steps(nested, inverter, existing, where=where)
            if updated != nested:
                new_step = dict(step)
                new_step.pop("apply", None)
                new_step["pipeline"] = updated
                out[idx] = new_step
            continue
        payload = _unwrap_edge_step(step)
        if payload is None:
            continue
        inverted = inverter.invert_step(payload, where=where)
        if inverted is None:
            continue
        # Deduplicated by what the step writes, not by how it is spelled: an
        # explicit `{source, target}` step and a generated `{from, to}` one for
        # the same triple would otherwise both run.
        fingerprint = _step_fingerprint(inverted)
        if fingerprint in existing:
            continue
        out.append(
            {"edge": inverted} if isinstance(step.get("edge"), dict) else inverted
        )
        existing.add(fingerprint)
    return out


def append_inverses_to_resource(
    payload: dict[str, Any],
    relation_map: dict[str, str],
    created: set[EdgeId],
) -> dict[str, Any]:
    """Add inverse ingestion for ``created`` edges to one serialized resource."""
    inverter = _Inverter(relation_map, created)
    if not inverter.active:
        return payload
    out = dict(payload)
    where = f"resource {payload.get('name')!r}"

    pipeline = _as_dict_list(out.get("pipeline"))
    if pipeline:
        out["pipeline"] = _append_inverse_steps(
            pipeline, inverter, _collect_fingerprints(pipeline), where=where
        )

    for spec_key in ("infer_edge_only", "infer_edge_except"):
        specs = _as_dict_list(out.get(spec_key))
        if not specs:
            continue
        present = {
            triple for spec in specs if (triple := _edge_triple_from_payload(spec))
        }
        appended = list(specs)
        for spec in specs:
            triple = _edge_triple_from_payload(spec)
            inverse = inverter.invert_static(triple) if triple else None
            if inverse is None or inverse in present:
                continue
            appended.append(
                dict(spec, source=inverse[0], target=inverse[1], relation=inverse[2])
            )
            present.add(inverse)
        out[spec_key] = appended

    extra = _as_dict_list(out.get("extra_weights"))
    if extra:
        present_fps = {
            _step_fingerprint(cast(dict[str, Any], entry["edge"]))
            for entry in extra
            if isinstance(entry.get("edge"), dict)
        }
        appended = list(extra)
        for entry in extra:
            edge_payload = entry.get("edge")
            if not isinstance(edge_payload, dict):
                continue
            edge_payload = cast(dict[str, Any], edge_payload)
            triple = _edge_triple_from_payload(edge_payload)
            if triple is not None:
                inverse = inverter.invert_static(triple)
                if inverse is None:
                    continue
                keys = (
                    ("from", "to") if "from" in edge_payload else ("source", "target")
                )
                inverted: dict[str, Any] | None = {
                    **edge_payload,
                    keys[0]: inverse[0],
                    keys[1]: inverse[1],
                    "relation": inverse[2],
                }
            else:
                inverted = inverter.invert_step(edge_payload, where=where)
            if inverted is None:
                continue
            fingerprint = _step_fingerprint(inverted)
            if fingerprint in present_fps:
                continue
            appended.append(dict(entry, edge=inverted))
            present_fps.add(fingerprint)
        out["extra_weights"] = appended

    return out
