"""What a sync run cannot do: write an element no ``sync`` block owns.

A sync run replaces what owner records wrote, so everything it writes must be
replaceable: every edge is owned by a resource in the run, and every owned
type carries a hash field (the ``sync`` aspect). Checked before anything is
staged. A vertex type the run does not own must be written key-only; that is
checked on the folded rows (:func:`graflo.hq.sync.run.check_key_only`).
"""

from __future__ import annotations

from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.contract.ingestion.emits import may_emit
from graflo.architecture.contract.ingestion.sync import owned_edges
from graflo.architecture.schema import Schema


def sync_refusals(
    schema: Schema, ingestion_model: IngestionModel, resources: list[str] | None = None
) -> list[str]:
    """Why a sync run of *resources* (all when ``None``) cannot start; empty when it can."""
    in_scope = [
        r for r in ingestion_model.resources if resources is None or r.name in resources
    ]
    owners = [r for r in in_scope if r.sync is not None]
    if not owners:
        return ["no resource in the run declares a sync block"]
    reasons: list[str] = []
    owned = owned_edges(in_scope)
    core = schema.core_schema
    vc = core.vertex_config
    for resource in in_scope:
        produced = ingestion_model.fetch_resource(resource.name).collect_vertex_names()
        for edge_id in dict(core.edge_config.items()):
            if edge_id not in owned and may_emit(resource, edge_id, produced):
                reasons.append(
                    f"{resource.name}: writes edge {edge_id}, which no sync block owns"
                )
    for resource in owners:
        assert resource.sync is not None
        owns = resource.sync.owns
        if not [f for f in vc.properties(owns) if f.set_by == "sync"]:
            reasons.append(
                f"{resource.name}: {owns!r} has no field set by sync; lift the "
                "sync aspect (graflo lift --aspect sync)"
            )
    return reasons


def hash_fields(schema: Schema, owned: set[str]) -> dict[str, str]:
    """Each type of *owned* that has one, mapped to its ``set_by: sync`` field."""
    vc = schema.core_schema.vertex_config
    out: dict[str, str] = {}
    for vertex in sorted(owned):
        fields = [f.name for f in vc.properties(vertex) if f.set_by == "sync"]
        if len(fields) > 1:
            raise ValueError(
                f"vertex {vertex!r} has several fields set by sync: {fields}"
            )
        if fields:
            out[vertex] = fields[0]
    return out
