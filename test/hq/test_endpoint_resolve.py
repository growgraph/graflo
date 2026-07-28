"""Ambiguity policy applied when a secondary identity matches several vertices."""

from __future__ import annotations

from typing import Any

import pytest

from graflo.hq.endpoint_resolve import (
    AmbiguousEndpointError,
    resolve_edge_endpoints,
)


class FakeConnection:
    """Returns canned matches and records the lookups it was asked to make."""

    def __init__(self, matches: dict[str, dict[int, list[dict[str, Any]]]]):
        self._matches = matches
        self.calls: list[tuple[str, tuple[str, ...]]] = []

    def resolve_vertices(
        self, class_name, key_docs, match_keys, return_keys, **kwargs
    ) -> dict[int, list[dict[str, Any]]]:
        self.calls.append((class_name, tuple(match_keys)))
        return self._matches.get(class_name, {})


def _resolve(db: FakeConnection, docs: list, policy: str, **overrides):
    kwargs: dict[str, Any] = dict(
        source_class="instrument",
        target_class="issuer",
        source_match_fields=["isin"],
        target_match_fields=["iid"],
        source_identity_fields=["sid"],
        target_identity_fields=["iid"],
        resolve_source=True,
        resolve_target=False,
        policy=policy,
    )
    kwargs.update(overrides)
    return resolve_edge_endpoints(db, docs, **kwargs)


ONE_MATCH = {"instrument": {0: [{"sid": "S1"}]}}
TWO_MATCHES = {"instrument": {0: [{"sid": "S2"}, {"sid": "S1"}]}}


class TestResolution:
    def test_single_match_rewrites_to_primary_identity(self) -> None:
        db = FakeConnection(ONE_MATCH)
        docs = [({"isin": "US001"}, {"iid": "I1"}, {"share": "1"})]
        resolved, stats = _resolve(db, docs, "all")
        assert resolved == [({"sid": "S1"}, {"iid": "I1"}, {"share": "1"})]
        assert stats.written == 1 and stats.dropped == 0

    def test_unresolved_endpoint_is_only_looked_up_when_selected(self) -> None:
        """The endpoint on its primary identity is passed through untouched."""
        db = FakeConnection(ONE_MATCH)
        docs = [({"isin": "US001"}, {"iid": "I1"}, {})]
        _resolve(db, docs, "all")
        assert db.calls == [("instrument", ("isin",))]

    def test_unmatched_key_drops_the_row(self) -> None:
        db = FakeConnection({})
        docs = [({"isin": "NOPE"}, {"iid": "I1"}, {})]
        resolved, stats = _resolve(db, docs, "all")
        assert resolved == []
        assert stats.unmatched == 1 and stats.dropped == 1

    def test_partial_key_is_never_partially_matched(self) -> None:
        db = FakeConnection(ONE_MATCH)
        docs = [({"org": "acme"}, {"iid": "I1"}, {})]
        resolved, stats = _resolve(
            db, docs, "all", source_match_fields=["org", "code"]
        )
        assert resolved == []
        assert stats.unresolvable == 1 and stats.dropped == 1


class TestAmbiguityPolicy:
    def test_all_attaches_to_every_match(self) -> None:
        db = FakeConnection(TWO_MATCHES)
        docs = [({"isin": "US002"}, {"iid": "I1"}, {})]
        resolved, stats = _resolve(db, docs, "all")
        assert sorted(doc[0]["sid"] for doc in resolved) == ["S1", "S2"]
        assert stats.ambiguous == 1 and stats.written == 2

    def test_first_is_deterministic_by_primary_identity(self) -> None:
        """Backends return matches in arbitrary order, so 'first' must sort."""
        db = FakeConnection(TWO_MATCHES)
        docs = [({"isin": "US002"}, {"iid": "I1"}, {})]
        resolved, stats = _resolve(db, docs, "first")
        assert [doc[0]["sid"] for doc in resolved] == ["S1"]
        assert stats.ambiguous == 1 and stats.written == 1

    def test_skip_drops_the_ambiguous_row(self) -> None:
        db = FakeConnection(TWO_MATCHES)
        docs = [({"isin": "US002"}, {"iid": "I1"}, {})]
        resolved, stats = _resolve(db, docs, "skip")
        assert resolved == []
        assert stats.ambiguous == 1 and stats.dropped == 1

    def test_error_raises_naming_the_key(self) -> None:
        db = FakeConnection(TWO_MATCHES)
        docs = [({"isin": "US002"}, {"iid": "I1"}, {})]
        with pytest.raises(AmbiguousEndpointError, match="US002"):
            _resolve(db, docs, "error")

    def test_unambiguous_rows_are_unaffected_by_policy(self) -> None:
        db = FakeConnection(ONE_MATCH)
        docs = [({"isin": "US001"}, {"iid": "I1"}, {})]
        for policy in ("all", "first", "skip", "error"):
            resolved, stats = _resolve(db, docs, policy)
            assert [doc[0]["sid"] for doc in resolved] == ["S1"]
            assert stats.ambiguous == 0


class TestBothEndpointsResolved:
    def test_fan_out_is_the_cartesian_product(self) -> None:
        db = FakeConnection(
            {
                "instrument": {0: [{"sid": "S1"}, {"sid": "S2"}]},
                "issuer": {0: [{"iid": "I1"}, {"iid": "I2"}]},
            }
        )
        docs = [({"isin": "US001"}, {"lei": "LEI-A"}, {})]
        resolved, stats = _resolve(
            db,
            docs,
            "all",
            target_match_fields=["lei"],
            resolve_target=True,
        )
        pairs = sorted((doc[0]["sid"], doc[1]["iid"]) for doc in resolved)
        assert pairs == [("S1", "I1"), ("S1", "I2"), ("S2", "I1"), ("S2", "I2")]
        assert stats.written == 4

    def test_one_unmatched_endpoint_drops_the_row(self) -> None:
        db = FakeConnection({"instrument": {0: [{"sid": "S1"}]}})
        docs = [({"isin": "US001"}, {"lei": "NOPE"}, {})]
        resolved, stats = _resolve(
            db, docs, "all", target_match_fields=["lei"], resolve_target=True
        )
        assert resolved == []
        assert stats.dropped == 1


class TestStats:
    def test_summary_reports_findings(self) -> None:
        db = FakeConnection({})
        docs = [({"isin": "NOPE"}, {"iid": "I1"}, {})]
        _, stats = _resolve(db, docs, "all")
        assert stats.has_findings()
        assert "unmatched=1" in stats.summary()

    def test_clean_run_has_no_findings(self) -> None:
        db = FakeConnection(ONE_MATCH)
        docs = [({"isin": "US001"}, {"iid": "I1"}, {})]
        _, stats = _resolve(db, docs, "all")
        assert not stats.has_findings()
