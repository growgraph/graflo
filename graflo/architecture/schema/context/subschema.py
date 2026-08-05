"""Bounded, seeded schema slicing.

The deliverable of schema context is *not* a getter: it is a valid standalone
:class:`~graflo.architecture.schema.document.Schema` small enough to hand an
agent, paired with an explicit account of what was left out and how to get it.
"""

from __future__ import annotations

from collections.abc import Sequence

from graflo.architecture.graph_types import EdgeDirection, EdgeId
from graflo.architecture.schema.context.budget import (
    Budget,
    BudgetAccounting,
    estimate_tokens,
    serialize_compact,
)
from graflo.architecture.schema.context.elision import (
    ElidedEdge,
    ElidedVertex,
    ElisionReport,
)
from graflo.architecture.schema.context.graph import SchemaGraph, edge_sort_key
from graflo.architecture.schema.context.rank import (
    RankingWeights,
    VertexSignals,
    score_vertices,
)
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.projection import build_subschema, select_induced
from graflo.architecture.schema.vertex import Vertex


def protected_property_names(vertex: Vertex) -> set[str]:
    """Property names that must never be elided from *vertex*.

    Identity-bearing fields: the primary identity, every secondary identity, and
    whatever feeds a hash or funnel digest. Dropping one of these yields a schema
    that still validates and is semantically a lie — the agent would believe a
    type is addressable by fields it cannot actually be addressed by.
    """
    protected: set[str] = set(vertex.identity)
    for entry in vertex.secondary_identities:
        protected.update(entry.fields)
    protected.update(vertex.digest_source_fields)
    return protected


def _drop_properties_for(vertex: Vertex, max_properties: int | None) -> list[str]:
    """Property names to omit from *vertex* under a per-vertex cap."""
    if max_properties is None:
        return []
    protected = protected_property_names(vertex)
    kept = [name for name in vertex.property_names if name in protected]
    dropped: list[str] = []
    for name in vertex.property_names:
        if name in protected:
            continue
        if len(kept) < max_properties:
            kept.append(name)
        else:
            dropped.append(name)
    return dropped


def subschema(
    schema: Schema,
    seeds: Sequence[str],
    *,
    budget: Budget | None = None,
    max_hops: int = 3,
    weights: RankingWeights | None = None,
    direction: EdgeDirection = EdgeDirection.ANY,
    graph: SchemaGraph | None = None,
) -> tuple[Schema, ElisionReport]:
    """Slice *schema* down to a budgeted neighbourhood around *seeds*.

    Seeds are always admitted, even when the budget cannot afford them: a slice
    that omits what the caller explicitly asked about answers a different
    question than the one posed.

    Args:
        schema: Source schema. Never mutated.
        seeds: Vertex types to centre the slice on. Must all be declared.
        budget: Element and token ceilings. Defaults to :class:`Budget`.
        max_hops: How far from a seed a type may be and still be a candidate.
        weights: Ranking weights for admission order.
        direction: Orientation followed when expanding from seeds.
        graph: Prebuilt index, if the caller already has one.

    Returns:
        tuple: the slice, and an :class:`ElisionReport` describing what it omits.

    Raises:
        ValueError: if *seeds* is empty.
        KeyError: if a seed is not declared in *schema*.
    """
    if not seeds:
        raise ValueError("subschema requires at least one seed vertex type")

    graph = graph or SchemaGraph.from_schema(schema)
    budget = budget or Budget()
    seed_list = list(dict.fromkeys(seeds))
    for seed in seed_list:
        if seed not in graph.vertex_types:
            raise KeyError(
                f"Unknown seed vertex type {seed!r}; declared: {sorted(graph.vertex_types)}"
            )
    seed_set = set(seed_list)

    ranked = score_vertices(
        graph,
        seed_list,
        weights=weights,
        max_hops=max_hops,
        direction=direction,
    )
    signals_by_name = {signal.name: signal for signal in ranked}

    vertex_config = schema.core_schema.vertex_config
    vertex_cost = {
        name: estimate_tokens(vertex_config[name].to_minimal_canonical_dict())
        for name in graph.vertex_types
    }
    edge_cost = {
        edge_id: estimate_tokens(graph.edge(edge_id).to_minimal_canonical_dict())
        for edge_id in graph.edge_ids
    }

    # Fixed cost of the envelope (metadata + db profile). Counting only element
    # costs would let an assembled slice sail past ``max_tokens`` — the caller
    # would be handed a payload larger than the ceiling they set. The source
    # profile is an upper bound on the projected one, so this errs conservative.
    envelope_cost = estimate_tokens(
        {
            "metadata": schema.metadata.to_minimal_canonical_dict(),
            "db_profile": schema.db_profile.to_minimal_canonical_dict(),
        }
    )

    admitted: list[str] = list(seed_list)
    elements_used = len(admitted)
    tokens_used = envelope_cost + sum(vertex_cost[name] for name in admitted)
    exhausted: str = "none"

    for signal in ranked:
        if signal.name in seed_set:
            continue
        # Unreachable within max_hops: not a budget decision, so it never sets
        # `exhausted` and never stops admission of reachable candidates.
        if signal.hop_distance is None:
            continue
        if budget.max_elements is not None and elements_used + 1 > budget.max_elements:
            exhausted = "elements"
            break
        cost = vertex_cost[signal.name]
        if budget.max_tokens is not None and tokens_used + cost > budget.max_tokens:
            exhausted = "tokens"
            break
        admitted.append(signal.name)
        elements_used += 1
        tokens_used += cost

    admitted_set = set(admitted)
    rank_position = {signal.name: index for index, signal in enumerate(ranked)}
    candidate_edges = sorted(
        (
            edge_id
            for edge_id in graph.edge_ids
            if edge_id[0] in admitted_set and edge_id[1] in admitted_set
        ),
        key=lambda edge_id: (
            min(rank_position[edge_id[0]], rank_position[edge_id[1]]),
            edge_sort_key(edge_id),
        ),
    )

    admitted_edges: list[EdgeId] = []
    for edge_id in candidate_edges:
        if budget.max_elements is not None and elements_used + 1 > budget.max_elements:
            exhausted = "elements"
            break
        cost = edge_cost[edge_id]
        if budget.max_tokens is not None and tokens_used + cost > budget.max_tokens:
            exhausted = "tokens"
            break
        admitted_edges.append(edge_id)
        elements_used += 1
        tokens_used += cost

    drop_properties = {}
    for name in admitted:
        dropped = _drop_properties_for(
            vertex_config[name], budget.max_properties_per_vertex
        )
        if dropped:
            drop_properties[name] = set(dropped)

    # Per-element costs omit the structural overhead of nesting them (keys,
    # separators, list wrappers), so the greedy pass can overshoot by a few
    # percent. Measure the assembled slice and trim until the ceiling actually
    # holds — a budget that is exceeded by the payload it produced is not a
    # budget. Least valuable first: edges, then lowest-ranked non-seed vertices.
    trimmable_edges = list(admitted_edges)
    trimmable_vertices = [name for name in admitted if name not in seed_set]
    while True:
        selection = select_induced(
            schema.core_schema,
            keep_vertices=set(seed_list) | set(trimmable_vertices),
            keep_edge_ids=set(trimmable_edges),
            connectivity="induced",
        )
        sliced = build_subschema(schema, selection, drop_properties=drop_properties)
        payload = sliced.to_minimal_canonical_dict()
        estimated = estimate_tokens(payload)
        if budget.max_tokens is None or estimated <= budget.max_tokens:
            break
        # Seeds are never trimmed: a slice that drops what the caller asked about
        # answers a different question. If the seeds alone blow the budget, the
        # overrun is reported rather than hidden.
        if trimmable_edges:
            trimmable_edges.pop()
        elif trimmable_vertices:
            trimmable_vertices.pop()
        else:
            break
        exhausted = "tokens"

    accounting = BudgetAccounting(
        requested=budget,
        elements_used=len(selection.surviving_vertices)
        + len(selection.surviving_edge_ids),
        estimated_tokens=estimate_tokens(payload),
        serialized_chars=len(serialize_compact(payload)),
        exhausted_by=exhausted,
    )

    return sliced, _build_report(
        graph=graph,
        selection_vertices=selection.surviving_vertices,
        selection_edges=selection.surviving_edge_ids,
        signals_by_name=signals_by_name,
        drop_properties=drop_properties,
        accounting=accounting,
    )


def _build_report(
    *,
    graph: SchemaGraph,
    selection_vertices: set[str],
    selection_edges: set[EdgeId],
    signals_by_name: dict[str, VertexSignals],
    drop_properties: dict[str, set[str]],
    accounting: BudgetAccounting,
) -> ElisionReport:
    """Describe everything the slice omits."""
    vertex_config = graph.schema.core_schema.vertex_config

    elided_vertices: list[ElidedVertex] = []
    for name in sorted(graph.vertex_types - selection_vertices):
        signal = signals_by_name.get(name)
        hop_distance = signal.hop_distance if signal else None
        elided_vertices.append(
            ElidedVertex(
                name=name,
                reason="unreachable" if hop_distance is None else "budget",
                degree=graph.degree(name),
                hop_distance=hop_distance,
                description=vertex_config[name].description,
                drill_in=f"subschema(seeds=[{name!r}])",
            )
        )

    elided_edges: list[ElidedEdge] = []
    for edge_id in sorted(set(graph.edge_ids) - selection_edges, key=edge_sort_key):
        endpoints_present = (
            edge_id[0] in selection_vertices and edge_id[1] in selection_vertices
        )
        elided_edges.append(
            ElidedEdge(
                edge_id=edge_id,
                reason="budget" if endpoints_present else "endpoint_elided",
                description=graph.edge(edge_id).description,
            )
        )

    return ElisionReport(
        elided_vertices=elided_vertices,
        elided_edges=elided_edges,
        elided_properties={
            name: sorted(names)
            for name, names in sorted(drop_properties.items())
            # A trimmed vertex is reported as an elided *vertex*; listing its
            # properties too would double-count it.
            if name in selection_vertices
        },
        budget=accounting,
    )
