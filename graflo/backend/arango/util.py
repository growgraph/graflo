"""ArangoDB utility functions for graph operations.

This module provides utility functions for working with ArangoDB graphs and
queries. It includes functions for edge definition, filter rendering, and
query generation.

Key Functions:
    - define_extra_edges: Generate queries for creating derived edges
    - render_filters: Convert filter expressions to AQL filter clauses

Example:
    >>> query = define_extra_edges(edge_config)
    >>> filter_clause = render_filters({"field": "value"}, doc_name="d")
"""

import logging

from graflo.architecture.edge import Edge
from graflo.filter.onto import Clause, Expression
from graflo.onto import ExpressionFlavor

logger = logging.getLogger(__name__)


def define_extra_edges(g: Edge):
    """Generate AQL query for creating derived edges.

    This function creates a query to generate edges from source to target
    vertices through an intermediate vertex, copying properties from the
    intermediate vertex to the new edge.

    Args:
        g: Edge configuration containing source, target, and intermediate
            vertex information

    Returns:
        str: AQL query string for creating the derived edges

    Example:
        >>> edge = Edge(source="user", target="post", by="comment")
        >>> query = define_extra_edges(edge)
        >>> # Generates query to create user->post edges through comments
    """
    ucol, vcol, wcol = g.source, g.target, g.by
    weight = g.weight_dict
    s = f"""FOR w IN {wcol}
        LET uset = (FOR u IN 1..1 INBOUND w {ucol}_{wcol}_edges RETURN u)
        LET vset = (FOR v IN 1..1 INBOUND w {vcol}_{wcol}_edges RETURN v)
        FOR u in uset
        FOR v in vset
    """
    s_ins_ = ", ".join([f"{v}: w.{k}" for k, v in weight.items()])
    s_ins_ = f"_from: u._id, _to: v._id, {s_ins_}"
    s_ins = f"          INSERT {{{s_ins_}}} "
    s_last = f"IN {ucol}_{vcol}_edges"
    query0 = s + s_ins + s_last
    return query0


def render_filters(filters: None | list | dict | Clause = None, doc_name="d") -> str:
    """Convert filter expressions to AQL filter clauses.

    This function converts filter expressions into AQL filter clauses that
    can be used in queries. It supports various filter types and formats.

    Args:
        filters: Filter expression to convert
        doc_name: Name of the document variable in the query

    Returns:
        str: AQL filter clause string

    Example:
        >>> filters = {"field": "value", "age": {"$gt": 18}}
        >>> clause = render_filters(filters, doc_name="user")
        >>> # Returns: "FILTER user.field == 'value' && user.age > 18"
    """
    if filters is not None:
        if not isinstance(filters, Clause):
            ff = Expression.from_dict(filters)
        else:
            ff = filters
        literal_condition = ff(doc_name=doc_name, kind=ExpressionFlavor.ARANGO)
        filter_clause = f"FILTER {literal_condition}"
    else:
        filter_clause = ""

    return filter_clause
