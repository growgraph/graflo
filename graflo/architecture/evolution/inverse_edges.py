"""Realize declared inverses as explicit logical edges (``add_inverse_edges``).

Three layers are touched, and each follows from the edges the op *creates*:

- schema: for every directed edge ``(S, T, r)`` with ``r`` mapped to ``inv``, the
  edge ``(T, S, inv)`` is added unless it already exists. An inverse edge that
  the user already declared explicitly is left alone, including its ingestion.
- profile: the new edge gets a copy of the forward edge's physical spec, minus
  its physical name (two logical types must not share one storage type).
- ingestion: the edge steps that write a forward relation whose inverse edge
  was created get ``emit_inverse``, so the same rows write both. Nothing is
  generated: the mirror is taken at assembly, after the relation is resolved,
  so it does not matter how a step names its relation. A resource that already
  writes the inverse with a step of its own is left alone, and the resource's
  edge selectors (``infer_edge_only`` / ``infer_edge_except`` /
  ``extra_weights``) gain the mirrored triple.
"""

from __future__ import annotations

from typing import Any, cast

from graflo.architecture.contract.ingestion.steps.ref import (
    EdgeStepRef,
    EdgeStepView,
    iter_edge_steps,
)
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.graph_types import EdgeId
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import Edge, EdgeConfig, inverse_map

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


# -- ingestion ----------------------------------------------------------


def _active_relations(
    relation_map: dict[str, str], created: set[EdgeId]
) -> dict[str, str]:
    """Forward relations whose inverse edge this application actually created.

    An inverse that already existed is the author's to feed: only what the op
    adds to the schema drives what it changes in the pipelines.
    """
    created_relations = {relation for _source, _target, relation in created}
    return {
        relation: inverse
        for relation, inverse in relation_map.items()
        if inverse in created_relations
    }


def _feeds_inverse_itself(
    views: list[EdgeStepView], active: dict[str, str], created: set[EdgeId]
) -> bool:
    """Whether the resource has a step of its own that writes a created inverse.

    A source that reports both readings of a fact maps both; mirroring on top of
    that would write the inverse twice.
    """
    inverse_names = set(active.values())
    for view in views:
        if view.static_edge_id in created:
            return True
        written = view.relations_written()
        if view.static_edge_id is None and written and written <= inverse_names:
            return True
    return False


def _mirrors_created_edge(
    view: EdgeStepView, active: dict[str, str], created: set[EdgeId]
) -> bool:
    """Whether setting ``emit_inverse`` on *view* would feed a created inverse edge."""
    static = view.static_edge_id
    if static is not None:
        source, target, relation = static
        inverse = active.get(relation)
        return inverse is not None and (target, source, inverse) in created
    # The writer learns which identity an endpoint is matched on per edge id, at
    # load time; a mirror known only per document has nowhere to register that.
    if view.payload.get("source_match") or view.payload.get("target_match"):
        return False
    written = view.relations_written()
    forward_of = {inverse: relation for relation, inverse in active.items()}
    for mirror_source, mirror_target, inverse in created:
        relation = forward_of.get(inverse)
        if relation is None or (written is not None and relation not in written):
            continue
        # The mirror (T, S, inv) is fed by a step that can write (S, T, relation).
        if view.source in (None, mirror_target) and view.target in (
            None,
            mirror_source,
        ):
            return True
    return False


def plan_inverse_emission(
    manifest: GraphManifest, relation_map: dict[str, str], created: set[EdgeId]
) -> dict[str, list[EdgeStepRef]]:
    """The edge steps that should mirror into the inverse edges in ``created``.

    Pure: reads the manifest, returns ``{resource: [step ref, ...]}``. A step is
    selected when what it writes has its inverse edge among ``created`` -- one
    edge for a step with fixed endpoints and relation, any of them for a step
    whose relation or endpoints come from the data. Steps already flagged, and
    resources that write the inverse with a step of their own, are left out.
    """
    ingestion = manifest.ingestion_model
    active = _active_relations(relation_map, created)
    if ingestion is None or not active:
        return {}
    plan: dict[str, list[EdgeStepRef]] = {}
    for resource in ingestion.resources:
        views = list(iter_edge_steps(resource.pipeline))
        if _feeds_inverse_itself(views, active, created):
            continue
        refs = [
            view.ref
            for view in views
            if not view.emit_inverse and _mirrors_created_edge(view, active, created)
        ]
        if refs:
            plan[resource.name] = refs
    return plan


def _mirrored_triple(
    triple: EdgeTriple | None, active: dict[str, str], created: set[EdgeId]
) -> EdgeTriple | None:
    if triple is None:
        return None
    source, target, relation = triple
    inverse = active.get(relation)
    if inverse is None or (target, source, inverse) not in created:
        return None
    return target, source, inverse


def mirror_resource_selectors(
    payload: dict[str, Any],
    relation_map: dict[str, str],
    created: set[EdgeId],
) -> dict[str, Any]:
    """*payload* with its edge selectors extended to the created inverse edges.

    ``infer_edge_only`` / ``infer_edge_except`` and ``extra_weights`` name edges
    by triple, outside the pipeline. A selector that covers a forward edge is
    given a twin covering its mirror, so the inverse is included in, excluded
    from, or weighted by whatever the forward edge is.
    """
    active = _active_relations(relation_map, created)
    if not active:
        return payload
    out = dict(payload)

    for spec_key in ("infer_edge_only", "infer_edge_except"):
        specs = _as_dict_list(out.get(spec_key))
        if not specs:
            continue
        present = {
            triple for spec in specs if (triple := _edge_triple_from_payload(spec))
        }
        appended = list(specs)
        for spec in specs:
            inverse = _mirrored_triple(_edge_triple_from_payload(spec), active, created)
            if inverse is None or inverse in present:
                continue
            appended.append(
                dict(spec, source=inverse[0], target=inverse[1], relation=inverse[2])
            )
            present.add(inverse)
        out[spec_key] = appended

    extra = _as_dict_list(out.get("extra_weights"))
    if extra:
        present = {
            triple
            for entry in extra
            if isinstance(entry.get("edge"), dict)
            and (triple := _edge_triple_from_payload(entry["edge"]))
        }
        appended = list(extra)
        for entry in extra:
            edge_payload = entry.get("edge")
            if not isinstance(edge_payload, dict):
                continue
            edge_payload = cast(dict[str, Any], edge_payload)
            inverse = _mirrored_triple(
                _edge_triple_from_payload(edge_payload), active, created
            )
            if inverse is None or inverse in present:
                continue
            keys = ("from", "to") if "from" in edge_payload else ("source", "target")
            appended.append(
                dict(
                    entry,
                    edge={
                        **edge_payload,
                        keys[0]: inverse[0],
                        keys[1]: inverse[1],
                        "relation": inverse[2],
                    },
                )
            )
            present.add(inverse)
        out["extra_weights"] = appended

    return out
