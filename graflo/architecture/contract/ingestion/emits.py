"""Which edges a resource can write, read from its authored steps.

The answer is an over-approximation: a step whose endpoints or relation are
taken from the data may write any edge they could name.
"""

from __future__ import annotations

from collections.abc import Collection

from graflo.architecture.contract.ingestion.resource import ResourceConfig
from graflo.architecture.contract.ingestion.steps.ref import (
    EdgeStepView,
    iter_edge_steps,
)
from graflo.architecture.graph_types import EdgeId


def inference_feeds(resource: ResourceConfig, edge_id: EdgeId) -> bool:
    """Whether edge inference in *resource* would write ``edge_id``.

    Mirrors the runtime's rule: inference is on, the edge passes the resource's
    ``infer_edge_only`` / ``infer_edge_except`` selectors, and -- unless an
    allow-list is given -- no fixed-endpoint step already owns the pair. Whether
    the resource produces both endpoints is the caller's question.
    """
    if not resource.infer_edges:
        return False
    if resource.infer_edge_only:
        return any(spec.matches(edge_id) for spec in resource.infer_edge_only)
    if any(spec.matches(edge_id) for spec in resource.infer_edge_except):
        return False
    source, target, _relation = edge_id
    for view in iter_edge_steps(resource.pipeline):
        if view.source is None or view.target is None:
            continue
        owned = {(view.source, view.target)}
        if view.emit_inverse:
            owned.add((view.target, view.source))
        if (source, target) in owned:
            return False
    return True


def step_may_write(view: EdgeStepView, edge_id: EdgeId) -> bool:
    """Whether the edge step *view* can write ``edge_id``.

    A step with ``emit_inverse`` may also write the reversed pair, under the
    inverse's relation.
    """
    source, target, relation = edge_id
    if view.source in (None, source) and view.target in (None, target):
        written = view.relations_written()
        if written is None or (relation in written if relation else not written):
            return True
    return (
        view.emit_inverse
        and view.source in (None, target)
        and (view.target in (None, source))
    )


def may_emit(
    resource: ResourceConfig, edge_id: EdgeId, produced: Collection[str]
) -> bool:
    """Whether *resource*, producing the vertex types *produced*, can write ``edge_id``."""
    if any(
        step_may_write(view, edge_id) for view in iter_edge_steps(resource.pipeline)
    ):
        return True
    source, target, _relation = edge_id
    return (
        source in produced and target in produced and inference_feeds(resource, edge_id)
    )
