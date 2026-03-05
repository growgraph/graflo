"""Edge creation and weight management for graph assembly."""

from __future__ import annotations

import logging
from collections import defaultdict
from functools import partial
from itertools import combinations, product, zip_longest
from typing import Any, Callable, Iterable, Iterator

from graflo.architecture.edge import Edge
from graflo.architecture.onto import (
    ActionContext,
    AssemblyContext,
    EdgeCastingType,
    LocationIndex,
    TransformPayload,
    VertexRep,
)
from graflo.architecture.util import project_dict
from graflo.architecture.vertex import VertexConfig

logger = logging.getLogger(__name__)


def add_blank_collections(
    ctx: AssemblyContext | ActionContext, vertex_conf: VertexConfig
) -> AssemblyContext | ActionContext:
    """Add blank collections for vertices that require them."""
    buffer_transforms = [
        item for sublist in ctx.buffer_transforms.values() for item in sublist
    ]
    for vname in vertex_conf.blank_vertices:
        v = vertex_conf[vname]
        for item in buffer_transforms:
            prep_doc = {f: item[f] for f in v.field_names if f in item}
            if vname not in ctx.acc_global:
                ctx.acc_global[vname] = [prep_doc]
    return ctx


def _transform_context_doc(item: Any) -> dict[str, Any]:
    if isinstance(item, TransformPayload):
        return item.context_doc()
    if isinstance(item, dict):
        return dict(item)
    return {}


def dress_vertices(
    items_dd: defaultdict[LocationIndex, list[VertexRep]],
    buffer_transforms: defaultdict[LocationIndex, list[Any]],
) -> defaultdict[LocationIndex, list[tuple[VertexRep, dict]]]:
    new_items_dd: defaultdict[LocationIndex, list[tuple[VertexRep, dict]]] = (
        defaultdict(list)
    )
    for va, vlist in items_dd.items():
        if va in buffer_transforms and len(buffer_transforms[va]) == len(vlist):
            transformed_docs = [
                _transform_context_doc(x) for x in buffer_transforms[va]
            ]
            new_items_dd[va] = list(zip(vlist, transformed_docs))
        else:
            new_items_dd[va] = list(zip(vlist, [{}] * len(vlist)))
    return new_items_dd


def select_iterator(casting_type: EdgeCastingType):
    if casting_type == EdgeCastingType.PAIR:
        iterator: Callable[..., Iterable[Any]] = zip
    elif casting_type == EdgeCastingType.PRODUCT:
        iterator = product
    elif casting_type == EdgeCastingType.COMBINATIONS:

        def iterator(*x):
            return partial(combinations, r=2)(x[0])

    return iterator


def filter_nonindexed(
    items_tdressed: defaultdict[LocationIndex, list[tuple[VertexRep, dict]]],
    index: Any,
) -> defaultdict[LocationIndex, list[tuple[VertexRep, dict]]]:
    """Filter items to only include those with indexed fields."""
    for va, vlist in items_tdressed.items():
        items_tdressed[va] = [
            item for item in vlist if any(k in item[0].vertex for k in index)
        ]
    return items_tdressed


def count_unique_by_position_variable(tuples_list: list, fillvalue: Any = None) -> list:
    """For each position in the tuples, returns the number of different elements."""
    if not tuples_list:
        return []
    transposed = zip_longest(*tuples_list, fillvalue=fillvalue)
    return [len(set(position)) for position in transposed]


def _filter_source_target_lindexes(
    edge: Edge,
    source_locs: list[LocationIndex],
    target_locs: list[LocationIndex],
) -> tuple[list[LocationIndex], list[LocationIndex]]:
    """Apply match/exclude filters from edge config to source and target locations."""
    if edge.match_source is not None:
        source_locs = [loc for loc in source_locs if edge.match_source in loc]
    if edge.exclude_source is not None:
        source_locs = [loc for loc in source_locs if edge.exclude_source not in loc]
    if edge.match_target is not None:
        target_locs = [loc for loc in target_locs if edge.match_target in loc]
    if edge.exclude_target is not None:
        target_locs = [loc for loc in target_locs if edge.exclude_target not in loc]
    if edge.match is not None:
        source_locs = [loc for loc in source_locs if edge.match in loc]
        target_locs = [loc for loc in target_locs if edge.match in loc]
    return source_locs, target_locs


def _compute_location_groups(
    source_locs: list[LocationIndex],
    target_locs: list[LocationIndex],
    source_path_spec: list[int],
    target_path_spec: list[int],
) -> tuple[list[list[LocationIndex]], list[list[LocationIndex]]]:
    """Group source and target locations for edge iteration."""
    source_branch_idx = next(
        (i for i, n in enumerate(source_path_spec) if n != 1), len(source_path_spec)
    )
    target_branch_idx = next(
        (i for i, n in enumerate(target_path_spec) if n != 1), len(target_path_spec)
    )

    if set(source_locs) == set(target_locs):
        return [source_locs], [target_locs]

    if (
        source_branch_idx < len(source_path_spec) - 1
        and target_branch_idx < len(target_path_spec) - 1
        and source_path_spec[source_branch_idx] == target_path_spec[target_branch_idx]
    ):
        branch_size = source_path_spec[source_branch_idx]
        source_by_branch: dict[int, list[LocationIndex]] = {
            i: [] for i in range(branch_size)
        }
        target_by_branch: dict[int, list[LocationIndex]] = {
            i: [] for i in range(branch_size)
        }
        for loc in source_locs:
            source_by_branch[loc[source_branch_idx]].append(loc)
        for loc in target_locs:
            target_by_branch[loc[target_branch_idx]].append(loc)
        return (
            [source_by_branch[i] for i in range(branch_size)],
            [target_by_branch[i] for i in range(branch_size)],
        )

    return [source_locs], [target_locs]


def _iter_emitter_receiver_group_pairs(
    source_groups: list[list[LocationIndex]],
    target_groups: list[list[LocationIndex]],
    edge: Edge,
    source_name: str,
    target_name: str,
) -> Iterator[tuple[list[LocationIndex], list[LocationIndex]]]:
    """Yield (emitter_group, receiver_group) pairs for edge iteration."""
    if source_name != target_name:
        yield from zip(source_groups, target_groups)
        return

    if edge.match_source is not None and edge.match_target is not None:
        yield from zip(source_groups, target_groups)
        return

    for source_group, target_group in zip(source_groups, target_groups):
        same_group = set(source_group) == set(target_group)
        if same_group:
            if len(source_group) <= 1:
                if source_group:
                    yield (source_group, target_group)
            yield (source_group[:1], source_group[1:])
        else:
            if not source_group:
                continue
            yield (source_group[:1], target_group)


def _choose_casting(
    source_loc: LocationIndex,
    target_loc: LocationIndex,
    source_name: str,
    target_name: str,
) -> EdgeCastingType:
    """Choose casting strategy per (source_loc, target_loc) pair."""
    if source_loc == target_loc:
        if source_name == target_name:
            return EdgeCastingType.COMBINATIONS
        if source_name != target_name:
            return EdgeCastingType.PRODUCT
        return EdgeCastingType.PAIR
    return EdgeCastingType.PRODUCT


def _extract_relation_from_key(
    source_loc: LocationIndex,
    target_loc: LocationIndex,
    source_min_depth: int,
    target_min_depth: int,
) -> str | None:
    """Extract relation name from location path."""
    if source_min_depth <= target_min_depth and len(target_loc) > 1:
        rel = target_loc[-2]
    elif len(source_loc) > 1:
        rel = source_loc[-2]
    else:
        return None
    return str(rel).replace("-", "_") if rel is not None else None


def render_edge(
    edge: Edge,
    vertex_config: VertexConfig,
    ctx: AssemblyContext | ActionContext,
    lindex: LocationIndex | None = None,
) -> defaultdict[str | None, list]:
    """Create edges between source and target vertices."""
    acc_vertex = ctx.acc_vertex
    buffer_transforms = ctx.buffer_transforms
    source = edge.source
    target = edge.target

    source_identity = vertex_config.identity_fields(source)
    target_identity = vertex_config.identity_fields(target)

    source_by_loc = acc_vertex[source]
    target_by_loc = acc_vertex[target]
    if not source_by_loc or not target_by_loc:
        return defaultdict(list)

    source_locs = list(source_by_loc)
    target_locs = list(target_by_loc)

    if lindex is not None:
        source_locs = sorted(lindex.filter(source_locs))
        target_locs = sorted(lindex.filter(target_locs))

    source_locs, target_locs = _filter_source_target_lindexes(
        edge, source_locs, target_locs
    )

    if not (source_locs and target_locs):
        return defaultdict(list)

    source_by_loc = defaultdict(list, {loc: source_by_loc[loc] for loc in source_locs})
    target_by_loc = defaultdict(list, {loc: target_by_loc[loc] for loc in target_locs})

    source_min_depth = min(loc.depth() for loc in source_by_loc)
    target_min_depth = min(loc.depth() for loc in target_by_loc)

    source_dressed = dress_vertices(source_by_loc, buffer_transforms)
    target_dressed = dress_vertices(target_by_loc, buffer_transforms)
    source_dressed = filter_nonindexed(source_dressed, source_identity)
    target_dressed = filter_nonindexed(target_dressed, target_identity)

    edges: defaultdict[str | None, list] = defaultdict(list)

    if source == target and source_locs is target_locs:
        path_spec = count_unique_by_position_variable([loc.path for loc in source_locs])
        source_path_spec = target_path_spec = path_spec
    else:
        source_path_spec = count_unique_by_position_variable(
            [loc.path for loc in source_locs]
        )
        target_path_spec = count_unique_by_position_variable(
            [loc.path for loc in target_locs]
        )

    source_groups, target_groups = _compute_location_groups(
        source_locs, target_locs, source_path_spec, target_path_spec
    )

    for source_group, target_group in _iter_emitter_receiver_group_pairs(
        source_groups, target_groups, edge, source, target
    ):
        for source_loc in source_group:
            source_items = source_dressed[source_loc]
            for target_loc in target_group:
                target_items = target_dressed[target_loc]

                casting = _choose_casting(
                    source_loc,
                    target_loc,
                    source,
                    target,
                )
                iterator = select_iterator(casting)

                for (u_rep, u_tr), (v_rep, v_tr) in iterator(
                    source_items, target_items
                ):
                    u_doc = u_rep.vertex
                    v_doc = v_rep.vertex

                    weight: dict[str, Any] = {}
                    if edge.weights is not None:
                        for field in edge.weights.direct:
                            field_name = field.name
                            if field in u_rep.ctx:
                                weight[field_name] = u_rep.ctx[field]
                            if field in v_rep.ctx:
                                weight[field_name] = v_rep.ctx[field]
                            if field in u_tr:
                                weight[field_name] = u_tr[field]
                            if field in v_tr:
                                weight[field_name] = v_tr[field]

                    source_proj = project_dict(u_doc, source_identity)
                    target_proj = project_dict(v_doc, target_identity)

                    extracted_relation = None

                    if edge.relation_field is not None:
                        u_relation = u_rep.ctx.pop(edge.relation_field, None)
                        if u_relation is None:
                            v_relation = v_rep.ctx.pop(edge.relation_field, None)
                            if v_relation is not None:
                                source_proj, target_proj = target_proj, source_proj
                                extracted_relation = v_relation
                        else:
                            extracted_relation = u_relation

                    if (
                        extracted_relation is None
                        and edge.relation_from_key
                        and len(target_loc) > 1
                    ):
                        extracted_relation = _extract_relation_from_key(
                            source_loc, target_loc, source_min_depth, target_min_depth
                        )

                    if edge.relation_from_key and extracted_relation is None:
                        continue

                    relation = (
                        extracted_relation
                        if extracted_relation is not None
                        else edge.relation
                    )
                    edges[relation].append((source_proj, target_proj, weight))
    return edges


def render_weights(
    edge: Edge,
    vertex_config: VertexConfig,
    acc_vertex: defaultdict[str, defaultdict[LocationIndex, list]],
    edges: defaultdict[str | None, list],
) -> defaultdict[str | None, list]:
    """Process and apply weights to edge documents."""
    vertex_weights = [] if edge.weights is None else edge.weights.vertices
    weights: list = []

    for w in vertex_weights:
        vertex = w.name
        if vertex is None or vertex not in vertex_config.vertex_set:
            continue
        vertex_lists = acc_vertex[vertex]

        keys = sorted(vertex_lists)
        if not keys:
            continue
        vertex_sample = [item.vertex for item in vertex_lists[keys[0]]]

        if w.filter:
            vertex_sample = [
                doc
                for doc in vertex_sample
                if all([doc[q] == v in doc for q, v in w.filter.items()])
            ]
        if vertex_sample:
            for doc in vertex_sample:
                weight = {}
                if w.fields:
                    weight = {
                        **{
                            w.cfield(field): doc[field]
                            for field in w.fields
                            if field in doc
                        },
                    }
                if w.map:
                    weight = {
                        **weight,
                        **{q: doc[k] for k, q in w.map.items()},
                    }
                if not w.fields and not w.map:
                    try:
                        weight = {
                            f"{vertex}.{k}": doc[k]
                            for k in vertex_config.identity_fields(vertex)
                            if k in doc
                        }
                    except ValueError:
                        weight = {}
                        logger.error(
                            " weights mapper error : weight definition on"
                            f" {edge.source} {edge.target} refers to"
                            f" a non existent vcollection {vertex}"
                        )
                weights += [weight]
    if weights:
        for r, edocs in edges.items():
            edges[r] = [
                (u, v, {**w, **weight}) for (u, v, w), weight in zip(edocs, weights)
            ]
    return edges
