"""Which vertex types a manifest's connectors own.

A type that a bound resource ingests has an authoritative source: the
connector. A producer that only reads about entities -- an extractor reading
text -- should link to such an entity, never create it or write its
properties, so a misread key cannot become a vertex the source never had.
"""

from __future__ import annotations

from typing import Any

from graflo.architecture.contract.ingestion.resource import (
    step_finds,
    step_looks_up,
    step_produces_vertices,
)
from graflo.architecture.contract.ingestion.steps.normalize import (
    normalize_actor_step,
)
from graflo.architecture.contract.manifest import GraphManifest


def connector_owned_types(manifest: GraphManifest) -> frozenset[str]:
    """Vertex types written by a resource that some connector feeds.

    A step that only looks a type up (``lookup_only``) or finds it by a
    secondary identity (``find``) references it and does not own it. A
    manifest without bindings or an ingestion model owns nothing.
    """
    if manifest.bindings is None or manifest.ingestion_model is None:
        return frozenset()
    known = (
        {v.name for v in manifest.graph_schema.core_schema.vertex_config.vertices}
        if manifest.graph_schema is not None
        else None
    )
    owned: set[str] = set()

    def walk(level: Any) -> None:
        if not isinstance(level, list):
            return
        for item in level:
            if not isinstance(item, dict):
                continue
            step = normalize_actor_step(dict(item))
            for vertex in step_produces_vertices(step, known_vertices=known):
                if step_looks_up(step, vertex):
                    continue
                if step_finds(step, vertex, known_vertices=known) is not None:
                    continue
                owned.add(vertex)
            if step.get("type") == "descend":
                walk(step.get("pipeline"))

    for resource in manifest.ingestion_model.resources:
        if manifest.bindings.get_connectors_for_resource(resource.name):
            walk(resource.pipeline)
    return frozenset(owned)
