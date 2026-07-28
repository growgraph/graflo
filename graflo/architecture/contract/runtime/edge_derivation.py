"""Runtime registry for ingestion-only edge derivation (per resource)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from graflo.architecture.schema.vertex import PRIMARY_IDENTITY_SELECTOR

from graflo.architecture.graph_types import EdgeId, Weight
from graflo.onto import EndpointAmbiguityPolicy


@dataclass(frozen=True)
class EndpointMatch:
    """Which identity each endpoint of an edge is matched on at write time.

    ``None`` selectors mean the vertex's primary identity, which is the default
    and leaves the write path exactly as it has always been.
    """

    source: str | list[str] | None = None
    target: str | list[str] | None = None
    on_ambiguous: EndpointAmbiguityPolicy | None = None

    def is_default(self) -> bool:
        return (
            self.source in (None, PRIMARY_IDENTITY_SELECTOR)
            and self.target in (None, PRIMARY_IDENTITY_SELECTOR)
            and self.on_ambiguous is None
        )


class EdgeDerivationRegistry:
    """Mutable store for ingestion-time edge behavior keyed by :class:`EdgeId`.

    Lives under the ingestion layer (typically one instance per :class:`ResourceRuntime`),
    not on :class:`~graflo.architecture.schema.core.CoreSchema`.
    """

    def __init__(self) -> None:
        self._relation_from_key: dict[EdgeId, bool] = {}
        self._vertex_weights: dict[EdgeId, list[Weight]] = {}
        self._endpoint_match: dict[EdgeId, EndpointMatch] = {}

    def mark_relation_from_key(self, edge_id: EdgeId) -> None:
        self._relation_from_key[edge_id] = True

    def uses_relation_from_key(self, edge_id: EdgeId) -> bool:
        return self._relation_from_key.get(edge_id, False)

    def set_endpoint_match(self, edge_id: EdgeId, match: EndpointMatch) -> None:
        """Record how *edge_id* locates its endpoints, for the write stage.

        Only non-default selections are stored, so an edge matching on primary
        identity — the overwhelming majority — leaves no entry and costs nothing.
        """
        if match.is_default():
            return
        self._endpoint_match[edge_id] = match

    def endpoint_match_for(self, edge_id: EdgeId) -> EndpointMatch | None:
        return self._endpoint_match.get(edge_id)

    def merge_vertex_weights(self, edge_id: EdgeId, rules: list[Weight]) -> None:
        """Append vertex weight rules for *edge_id*, deduplicating by stable fingerprint."""
        if not rules:
            return
        bucket = self._vertex_weights.setdefault(edge_id, [])
        seen = {_weight_fingerprint(w) for w in bucket}
        for w in rules:
            fp = _weight_fingerprint(w)
            if fp in seen:
                continue
            seen.add(fp)
            bucket.append(w)

    def vertex_weights_for(self, edge_id: EdgeId) -> list[Weight]:
        return list(self._vertex_weights.get(edge_id, ()))

    def copy(self) -> EdgeDerivationRegistry:
        out = EdgeDerivationRegistry()
        out._relation_from_key = dict(self._relation_from_key)
        out._vertex_weights = {
            k: [w.model_copy(deep=True) for w in v]
            for k, v in self._vertex_weights.items()
        }
        out._endpoint_match = dict(self._endpoint_match)
        return out

    def merge_from(self, other: EdgeDerivationRegistry) -> None:
        for eid, flag in other._relation_from_key.items():
            if flag:
                self.mark_relation_from_key(eid)
        for eid, weights in other._vertex_weights.items():
            self.merge_vertex_weights(eid, weights)
        for eid, match in other._endpoint_match.items():
            self.set_endpoint_match(eid, match)


def _weight_fingerprint(w: Weight) -> str:
    """JSON-stable fingerprint for deduplication."""
    payload: dict[str, Any] = w.model_dump(mode="json")
    return json.dumps(payload, sort_keys=True, default=str)
