"""What a bulk run cannot load: steps that read the target while ingesting.

The target of a bulk run is empty, so a read during the run finds nothing; such
steps are refused before anything is staged, by resource and step.
"""

from __future__ import annotations

from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.pipeline.runtime.actor.vertex import VertexActor
from graflo.architecture.pipeline.runtime.actor.vertex_router import VertexRouterActor
from graflo.architecture.schema import Schema


def bulk_refusals(
    schema: Schema, ingestion_model: IngestionModel, resources: list[str] | None = None
) -> list[str]:
    """Why each of *resources* (all when ``None``) cannot be loaded in bulk.

    An empty list means every one can.
    """
    blank = set(schema.core_schema.vertex_config.blank_vertices)
    reasons: list[str] = []
    for config in ingestion_model.resources:
        name = config.name
        if resources is not None and name not in resources:
            continue
        runtime = ingestion_model.fetch_resource(name)
        for actor in runtime.root.collect_actors():
            if isinstance(actor, VertexActor):
                if actor.lookup_only:
                    reasons.append(f"{name}: vertex step {actor.name!r} is lookup_only")
                if actor.find is not None:
                    reasons.append(
                        f"{name}: vertex step {actor.name!r} finds its vertex by "
                        f"{actor.find!r}"
                    )
            elif isinstance(actor, VertexRouterActor):
                if actor.config.lookup_only:
                    reasons.append(f"{name}: a vertex_router step is lookup_only")
                if actor.config.find:
                    reasons.append(
                        f"{name}: a vertex_router step finds vertices by "
                        f"{sorted(actor.config.find)}"
                    )
        if runtime.config.extra_weights:
            reasons.append(f"{name}: extra_weights read stored vertices")
        if runtime.edge_derivation.has_endpoint_matches():
            reasons.append(
                f"{name}: an edge matches its endpoints by a secondary identity"
            )
        produced = runtime.collect_vertex_names() & blank
        if produced:
            reasons.append(
                f"{name}: blank vertices {sorted(produced)} are keyed by the target"
            )
    return reasons


def union_fields(schema: Schema) -> list[str]:
    """Every ``merge: union`` field of the schema, as ``type.field``.

    A bulk run refuses them where the target's upsert does, so a later upsert
    can always extend a bulk-loaded graph.
    """
    core = schema.core_schema
    vc = core.vertex_config
    found = [
        f"{vertex}.{f.name}"
        for vertex in vc.vertex_set
        for f in vc.properties(vertex)
        if f.merge == "union"
    ]
    found += [
        f"{edge.edge_id}.{f.name}"
        for edge in core.edge_config.values()
        for f in edge.properties
        if f.merge == "union"
    ]
    return sorted(found)
