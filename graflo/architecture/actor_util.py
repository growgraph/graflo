"""Edge creation and weight management utilities for graph actors.

This module provides core functionality for creating and managing edges in the graph
database system. It handles edge rendering, weight management, and blank collection
creation. The module is central to the graph construction process, implementing the
logic for connecting vertices and managing their relationships.

Key Components:
    - add_blank_collections: Creates blank collections for vertices
    - render_edge: Core edge creation logic, handling different edge types and weights
    - render_weights: Manages edge weights and their relationships

Edge Creation Process:
    1. Edge rendering (render_edge):
       - Handles both PAIR_LIKE and PRODUCT_LIKE edge types
       - Manages source and target vertex relationships
       - Processes edge weights and relation fields
       - Creates edge documents with proper source/target mappings

    2. Weight management (render_weights):
       - Processes vertex-based weights
       - Handles direct field mappings
       - Manages weight filtering and transformation
       - Applies weights to edge documents

Example:
    >>> edge = Edge(source="user", target="post")
    >>> edges = render_edge(edge, vertex_config, acc_vertex)
    >>> edges = render_weights(edge, vertex_config, acc_vertex, cdoc, edges)
"""

from __future__ import annotations

import logging
from collections import defaultdict
from functools import partial
from itertools import combinations, product, zip_longest
from typing import Any, Callable, Iterable

from graflo.architecture.edge import Edge
from graflo.architecture.onto import (
    ActionContext,
    EdgeCastingType,
    LocationIndex,
    VertexRep,
)
from graflo.architecture.util import project_dict
from graflo.architecture.vertex import VertexConfig

logger = logging.getLogger(__name__)


def add_blank_collections(
    ctx: ActionContext, vertex_conf: VertexConfig
) -> ActionContext:
    """Add blank collections for vertices that require them.

    This function creates blank collections for vertices marked as blank in the
    vertex configuration. It copies relevant fields from the current document
    to create the blank vertex documents.

    Args:
            ctx: Current action context containing document and accumulator
            vertex_conf: Vertex configuration containing blank vertex definitions

        Returns:
            ActionContext: Updated context with new blank collections

        Example:
            >>> ctx = add_blank_collections(ctx, vertex_config)
            >>> print(ctx.acc_global['blank_vertex'])
            [{'field1': 'value1', 'field2': 'value2'}]
    """

    # add blank collections
    buffer_transforms = [
        item for sublist in ctx.buffer_transforms.values() for item in sublist
    ]

    for vname in vertex_conf.blank_vertices:
        v = vertex_conf[vname]
        for item in buffer_transforms:
            # Use field_names property for cleaner dict comprehension
            prep_doc = {f: item[f] for f in v.field_names if f in item}
            if vname not in ctx.acc_global:
                ctx.acc_global[vname] = [prep_doc]
    return ctx


def dress_vertices(
    items_dd: defaultdict[LocationIndex, list[VertexRep]],
    buffer_transforms: defaultdict[LocationIndex, list[dict]],
) -> defaultdict[LocationIndex, list[tuple[VertexRep, dict]]]:
    new_items_dd: defaultdict[LocationIndex, list[tuple[VertexRep, dict]]] = (
        defaultdict(list)
    )
    for va, vlist in items_dd.items():
        if va in buffer_transforms and len(buffer_transforms[va]) == len(vlist):
            new_items_dd[va] = list(zip(vlist, buffer_transforms[va]))
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
    index,
) -> defaultdict[LocationIndex, list[tuple[VertexRep, dict]]]:
    """Filter items to only include those with indexed fields.

    Args:
        items_tdressed: Dictionary of dressed vertex items
        index: Index fields to check

    Returns:
        Filtered dictionary of dressed vertex items
    """
    for va, vlist in items_tdressed.items():
        items_tdressed[va] = [
            item for item in vlist if any(k in item[0].vertex for k in index)
        ]
    return items_tdressed


def count_unique_by_position_variable(tuples_list, fillvalue=None):
    """
    For each position in the tuples, returns the number of different elements.
    Handles tuples of different lengths using a fill value.

    Args:
        tuples_list: List of tuples (they can have different lengths)
        fillvalue: Value to use for missing positions (default: None)

    Returns:
        List with counts of unique elements for each position
    """
    if not tuples_list:
        return []

    # Transpose the list of tuples, filling missing positions
    transposed = zip_longest(*tuples_list, fillvalue=fillvalue)

    # Count unique elements for each position
    result = [len(set(position)) for position in transposed]

    return result


def render_edge(
    edge: Edge,
    vertex_config: VertexConfig,
    ctx: ActionContext,
    lindex: LocationIndex | None = None,
) -> defaultdict[str | None, list]:
    """Create edges between source and target vertices.

    This is the core edge creation function that handles different edge types
    (PAIR_LIKE and PRODUCT_LIKE) and manages edge weights. It processes source
    and target vertices, and creates appropriate edge
    documents with proper source/target mappings.

    Args:
        edge: Edge configuration defining the relationship
        vertex_config: Vertex configuration for source and target
        ctx:
        lindex: Location index of the source vertex

    Returns:
        defaultdict[str | None, list]: Created edges organized by relation type

    Note:
        - PAIR_LIKE edges create one-to-one relationships
        - PRODUCT_LIKE edges create cartesian product relationships
        - Edge weights are extracted from source and target vertices
        - Relation fields can be specified in either source or target
    """

    acc_vertex = ctx.acc_vertex
    buffer_transforms = ctx.buffer_transforms

    source, target = edge.source, edge.target
    relation = edge.relation

    # get source and target edge fields
    source_index, target_index = (
        vertex_config.index(source),
        vertex_config.index(target),
    )

    # get source and target items
    source_items_, target_items_ = (acc_vertex[source], acc_vertex[target])
    if not source_items_ or not target_items_:
        return defaultdict(None, [])

    source_lindexes = list(source_items_)
    target_lindexes = list(target_items_)

    if lindex is not None:
        source_lindexes = sorted(lindex.filter(source_lindexes))
        target_lindexes = sorted(lindex.filter(target_lindexes))

        if source == target and len(source_lindexes) > 1:
            source_lindexes = source_lindexes[:1]
            target_lindexes = target_lindexes[1:]

    if edge.match_source is not None:
        source_lindexes = [li for li in source_lindexes if edge.match_source in li]

    if edge.exclude_source is not None:
        source_lindexes = [
            li for li in source_lindexes if edge.exclude_source not in li
        ]

    if edge.match_target is not None:
        target_lindexes = [li for li in target_lindexes if edge.match_target in li]

    if edge.exclude_target is not None:
        target_lindexes = [
            li for li in target_lindexes if edge.exclude_target not in li
        ]

    if edge.match is not None:
        source_lindexes = [li for li in source_lindexes if edge.match in li]
        target_lindexes = [li for li in target_lindexes if edge.match in li]

    if not (source_lindexes and target_lindexes):
        return defaultdict(list)

    source_items_ = defaultdict(list, {k: source_items_[k] for k in source_lindexes})

    target_items_ = defaultdict(list, {k: target_items_[k] for k in target_lindexes})

    source_min_level = min([k.depth() for k in source_items_.keys()])

    target_min_level = min([k.depth() for k in target_items_.keys()])

    # source/target items from many levels

    source_items_tdressed = dress_vertices(source_items_, buffer_transforms)
    target_items_tdressed = dress_vertices(target_items_, buffer_transforms)

    source_items_tdressed = filter_nonindexed(source_items_tdressed, source_index)
    target_items_tdressed = filter_nonindexed(target_items_tdressed, target_index)

    edges: defaultdict[str | None, list] = defaultdict(list)

    source_spec = count_unique_by_position_variable([x.path for x in source_lindexes])
    target_spec = count_unique_by_position_variable([x.path for x in target_lindexes])

    source_uni = next(
        (i for i, x in enumerate(source_spec) if x != 1), len(source_spec)
    )
    target_uni = next(
        (i for i, x in enumerate(target_spec) if x != 1), len(target_spec)
    )

    flag_same_vertex_same_leaf = False

    if source == target and set(source_lindexes) == set(target_lindexes):
        # prepare combinations: we confirmed the set

        combos = list(combinations(source_lindexes, 2))
        source_groups, target_groups = zip(*combos) if combos else ([], [])

        # and edge case when samples of the same vertex are encoded in the same leaf (like a table row)
        # see example/3-ingest-csv-edge-weights

        if not combos and len(source_items_tdressed[source_lindexes[0]]) > 1:
            source_groups, target_groups = [source_lindexes], [target_lindexes]
            flag_same_vertex_same_leaf = True
    elif (
        source_uni < len(source_spec) - 1
        and target_uni < len(target_spec) - 1
        and source_spec[source_uni] == target_spec[target_uni]
    ):
        # zip sources and targets in case there is a non-trivial brunching at a non-ultimate level
        common_branching = source_uni
        items_size = source_spec[source_uni]

        source_groups_map: dict[int, list] = {ix: [] for ix in range(items_size)}
        target_groups_map: dict[int, list] = {ix: [] for ix in range(items_size)}
        for li in source_lindexes:
            source_groups_map[li[common_branching]] += [li]
        for li in target_lindexes:
            target_groups_map[li[common_branching]] += [li]
        source_groups = [source_groups_map[ix] for ix in range(items_size)]
        target_groups = [target_groups_map[ix] for ix in range(items_size)]
    else:
        source_groups = [source_lindexes]
        target_groups = [target_lindexes]

    for source_lis, target_lis in zip(source_groups, target_groups):
        for source_lindex in source_lis:
            source_items = source_items_tdressed[source_lindex]
            for target_lindex in target_lis:
                target_items = target_items_tdressed[target_lindex]

                if flag_same_vertex_same_leaf:
                    # edge case when samples of the same vertex are encoded in the same leaf
                    iterator = select_iterator(EdgeCastingType.COMBINATIONS)
                else:
                    # in this case by construction source_items and target_items have only one element

                    iterator = select_iterator(EdgeCastingType.PAIR)

                for (u_, u_tr), (v_, v_tr) in iterator(source_items, target_items):
                    u = u_.vertex
                    v = v_.vertex
                    # adding weight from source or target
                    weight = dict()
                    if edge.weights is not None:
                        for field in edge.weights.direct:
                            if field in u_.ctx:
                                weight[field] = u_.ctx[field]

                            if field in v_.ctx:
                                weight[field] = v_.ctx[field]

                            if field in u_tr:
                                weight[field] = u_tr[field]
                            if field in v_tr:
                                weight[field] = v_tr[field]

                    a = project_dict(u, source_index)
                    b = project_dict(v, target_index)

                    if edge.relation_field is not None:
                        u_relation = u_.ctx.pop(edge.relation_field, None)

                        if u_relation is None:
                            v_relation = v_.ctx.pop(edge.relation_field, None)
                            if v_relation is not None:
                                a, b = b, a
                                relation = v_relation
                        else:
                            relation = u_relation
                    elif edge.relation_from_key and len(target_lindex) > 1:
                        if source_min_level <= target_min_level:
                            if len(target_lindex) > 1:
                                relation = target_lindex[-2]
                        elif len(source_lindex) > 1:
                            relation = source_lindex[-2]
                        if relation is not None:
                            relation = relation.replace("-", "_")
                    edges[relation] += [(a, b, weight)]
    return edges


def render_weights(
    edge: Edge,
    vertex_config: VertexConfig,
    acc_vertex: defaultdict[str, defaultdict[LocationIndex, list]],
    edges: defaultdict[str | None, list],
) -> defaultdict[str | None, list]:
    """Process and apply weights to edge documents.

    This function handles the complex weight management system, including:
    - Vertex-based weights from related vertices
    - Direct field mappings from the current document
    - Weight filtering and transformation
    - Application of weights to edge documents

    Args:
        edge: Edge configuration containing weight definitions
        vertex_config: Vertex configuration for weight processing
        acc_vertex: Accumulated vertex documents
        edges: Edge documents to apply weights to

    Returns:
        defaultdict[str | None, list]: Updated edge documents with applied weights

    Note:
        Weights can come from:
        1. Related vertices (vertex_classes)
        2. Direct field mappings (direct)
        3. Field transformations (map)
        4. Default index fields
    """
    vertex_weights = [] if edge.weights is None else edge.weights.vertices
    weight: dict = {}

    for w in vertex_weights:
        vertex = w.name
        if vertex is None or vertex not in vertex_config.vertex_set:
            continue
        vertex_lists = acc_vertex[vertex]

        # TODO logic here may be potentially improved
        keys = sorted(vertex_lists)
        if not keys:
            continue
        vertex_sample = [item.vertex for item in vertex_lists[keys[0]]]

        # find all vertices satisfying condition
        if w.filter:
            vertex_sample = [
                doc
                for doc in vertex_sample
                if all([doc[q] == v in doc for q, v in w.filter.items()])
            ]
        if vertex_sample:
            for doc in vertex_sample:
                if w.fields:
                    weight = {
                        **weight,
                        **{
                            w.cfield(field): doc[field]
                            for field in w.fields
                            if field
                            in doc  # w.fields are strings from Weight, so this is fine
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
                            for k in vertex_config.index(vertex)
                            if k in doc
                        }
                    except ValueError:
                        weight = {}
                        logger.error(
                            " weights mapper error : weight definition on"
                            f" {edge.source} {edge.target} refers to"
                            f" a non existent vcollection {vertex}"
                        )

    if weight:
        for r, edocs in edges.items():
            edges[r] = [(u, v, {**w, **weight}) for u, v, w in edocs]
    return edges
