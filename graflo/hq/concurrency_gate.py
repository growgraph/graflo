"""Decide how many batches of one data source may be in flight concurrently.

Casting is per-document pure and every real backend upserts idempotently, so
overlapping cast/write of sibling batches is safe by default. A handful of
configurations are genuinely order- or state-dependent; this module is the
single place that names them and forces those sources back to strictly serial
batch processing.
"""

from __future__ import annotations

import logging
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from graflo.architecture.pipeline.runtime.resource import ResourceRuntime
    from graflo.hq.ingestion_parameters import IngestionParams

logger = logging.getLogger(__name__)


class SerialReason(StrEnum):
    """Why a data source must process its batches strictly one at a time."""

    USER_OVERRIDE = "user_override"
    DYNAMIC_EDGES = "dynamic_edges"
    EXTRA_WEIGHTS = "extra_weights"
    BLANK_VERTICES = "blank_vertices"
    SECONDARY_IDENTITY = "secondary_identity_endpoints"


def effective_in_flight(
    runtime: ResourceRuntime | None,
    params: IngestionParams,
) -> tuple[int, SerialReason | None]:
    """Return ``(max_in_flight, reason)`` for one data source.

    ``max_in_flight`` is ``params.max_in_flight_batches`` unless a serial
    condition holds, in which case it is 1 and ``reason`` says why:

    - ``USER_OVERRIDE`` — ``max_in_flight_batches=1`` requested explicitly;
    - ``DYNAMIC_EDGES`` — dynamic edge feedback mutates the shared edge config
      *during* casting, so document order across batches is semantic;
    - ``EXTRA_WEIGHTS`` — weight enrichment reads the DB between the vertex and
      edge pushes (read-modify-write, racy across concurrent writers);
    - ``BLANK_VERTICES`` — blank-edge resolution pairs source/target docs
      positionally within a batch, so batch composition and order matter;
    - ``SECONDARY_IDENTITY`` — edges located by a secondary identity resolve
      their endpoints against database state, so a later batch's edges must not
      race an earlier batch's vertex writes.
    """
    if params.max_in_flight_batches == 1:
        return 1, SerialReason.USER_OVERRIDE
    if params.dynamic_edges:
        return 1, SerialReason.DYNAMIC_EDGES
    if runtime is not None:
        # getattr, not attribute access: duck-typed runtimes (test doubles,
        # minimal stubs) may not carry the full ResourceRuntime surface.
        config = getattr(runtime, "config", None)
        if config is not None and getattr(config, "extra_weights", None):
            return 1, SerialReason.EXTRA_WEIGHTS
        derivation = getattr(runtime, "edge_derivation", None)
        if derivation is not None and derivation.has_endpoint_matches():
            return 1, SerialReason.SECONDARY_IDENTITY
        vertex_config = getattr(runtime, "vertex_config", None)
        blank = (
            set(vertex_config.blank_vertices) if vertex_config is not None else set()
        )
        if blank:
            names = runtime.collect_vertex_names()
            # An empty name set means the pipeline's vertices could not be
            # enumerated; stay conservative when the schema has blank vertices.
            if not names or names & blank:
                return 1, SerialReason.BLANK_VERTICES
    return params.max_in_flight_batches, None
