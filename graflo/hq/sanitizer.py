"""Sanitization entry point for schema and ingestion contracts.

:class:`Sanitizer` is the public, stand-alone, DB-flavor-aware orchestrator
that encodes the policy "sanitize this manifest for a given target DB flavor".

It owns no mutation logic of its own. Instead, it builds a list of
:mod:`graflo.architecture.evolution` ops and applies them to the manifest in
place, preserving the long-standing
:meth:`Sanitizer.sanitize_manifest` API for callers that prefer a one-liner
over assembling ops by hand.

Examples
--------
Sanitize an inferred manifest a posteriori for TigerGraph::

    from graflo.hq.sanitizer import Sanitizer
    from graflo.onto import DBType

    Sanitizer(DBType.TIGERGRAPH).sanitize_manifest(manifest)

Equivalent low-level call (skip the policy layer)::

    from graflo.architecture.evolution import SanitizeOp, apply_sanitize

    apply_sanitize(manifest, SanitizeOp(db_flavor=DBType.TIGERGRAPH))
"""

from __future__ import annotations

import logging
from collections.abc import Iterable

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    ManifestOp,
    SanitizeOp,
)
from graflo.architecture.evolution.apply import apply_manifest_ops_inplace
from graflo.onto import DBType

logger = logging.getLogger(__name__)


class Sanitizer:
    """DB-flavor-aware orchestrator for manifest sanitization.

    The class encodes the per-flavor policy ("which evolution ops sanitize a
    manifest for *db_flavor*") and applies them in place. Callers that want a
    different sanitization recipe can either subclass and override
    :meth:`build_ops` or build ops directly via
    :mod:`graflo.architecture.evolution`.
    """

    def __init__(self, db_flavor: DBType):
        """Initialize the sanitizer for a given target DB flavor."""
        self.db_flavor = db_flavor

    def build_ops(
        self,
        manifest: GraphManifest,
        *,
        reserved_words: Iterable[str] | None = None,
    ) -> list[ManifestOp]:
        """Return the ordered list of evolution ops that sanitize *manifest*.

        Today the list collapses to ``[SanitizeOp(db_flavor=...)]``; exposing
        it as a list keeps the door open for future per-flavor composition
        (e.g. flavor-specific identity-normalization variants, future
        rename-relation ops).
        """
        del manifest  # currently policy is purely a function of db_flavor
        rw = list(reserved_words) if reserved_words is not None else None
        return [SanitizeOp(db_flavor=self.db_flavor, reserved_words=rw)]

    def sanitize_manifest(self, manifest: GraphManifest) -> GraphManifest:
        """Mutate *manifest* in place per :meth:`build_ops` and return it.

        Returns the same manifest object so callers can chain or simply assert
        that the in-place result is the original input.
        """
        if manifest.graph_schema is None:
            return manifest

        apply_manifest_ops_inplace(manifest, self.build_ops(manifest))

        manifest.finish_init()
        return manifest
