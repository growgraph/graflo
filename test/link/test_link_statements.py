"""Resolving statements into the facts that hold, by time, denial, modality and basis."""

from __future__ import annotations

import itertools
from datetime import UTC, datetime

from graflo.architecture.contract.manifest import GraphManifest
from graflo.link.statements import (
    Basis,
    SlotRule,
    Statement,
    resolve,
    slot_rules,
)

RUNS = ("Unit", "runs")
SINGLE = {RUNS: SlotRule(single=True, stateful=True)}


def _t(day: int, hour: int = 12) -> datetime:
    return datetime(2026, 10, day, hour, tzinfo=UTC)


def _runs(sid: str, release: str, **kw) -> Statement:
    return Statement(
        id=sid,
        vertex_type="Unit",
        key="U-1",
        slot="runs",
        kind="relation",
        value=release,
        target_type="Release",
        **kw,
    )


# A charger said to run 3.1, corrected to 3.0 the next day, with 3.2 planned
# for a later evening.
CHARGER = [
    _runs("a", "R-3.1", said_at=_t(13, 11), record="m1", thread="t1"),
    _runs("b", "R-3.1", polarity="denied", said_at=_t(14), record="m2", thread="t1"),
    _runs("c", "R-3.0", said_at=_t(14), record="m2", thread="t1"),
    _runs(
        "d",
        "R-3.2",
        modality="planned",
        valid_from=_t(16, 20),
        said_at=_t(16, 9),
        record="m3",
    ),
]


def _values(facts) -> list:
    return sorted(f.value for f in facts)


def test_a_correction_withdraws_what_it_denies_and_the_rest_holds() -> None:
    result = resolve(CHARGER, SINGLE, as_of=_t(16, 11))
    assert _values(result.current) == ["R-3.0"]
    assert [(w.statement, w.by) for w in result.withdrawn] == [("a", "b")]
    assert not result.conflicts


def test_a_planned_value_is_pending_until_its_time() -> None:
    before = resolve(CHARGER, SINGLE, as_of=_t(16, 11))
    assert [(f.value, f.modality) for f in before.pending] == [("R-3.2", "planned")]
    after = resolve(CHARGER, SINGLE, as_of=_t(17))
    assert _values(after.current) == ["R-3.2"]
    assert not after.pending
    # The value it replaced is history, closed when the new one took over.
    (past,) = after.history
    assert (past.value, past.valid_to) == ("R-3.0", _t(16, 20))


def test_without_a_time_to_resolve_at_every_plan_waits() -> None:
    result = resolve(CHARGER, SINGLE)
    assert _values(result.current) == ["R-3.0"]
    assert _values(result.pending) == ["R-3.2"]


def test_the_result_does_not_depend_on_the_order_of_statements() -> None:
    expected = resolve(CHARGER, SINGLE, as_of=_t(17))
    for order in itertools.permutations(CHARGER):
        assert resolve(order, SINGLE, as_of=_t(17)) == expected


def test_a_missing_time_is_unknown_never_the_oldest() -> None:
    statements = [
        _runs("a", "R-3.0", said_at=_t(14)),
        _runs("b", "R-3.1"),  # undated, same basis
    ]
    result = resolve(statements, SINGLE)
    assert not result.current
    (conflict,) = result.conflicts
    assert conflict.reason == "no time or basis orders the values"


def test_basis_decides_only_when_time_cannot_and_says_so() -> None:
    statements = [
        _runs("row", "R-3.0", basis=Basis.RECORDED),
        _runs("cast", "R-3.1", said_at=_t(14)),
    ]
    result = resolve(statements, SINGLE)
    assert _values(result.current) == ["R-3.0"]
    (conflict,) = result.conflicts
    assert conflict.reason == "ordered by basis, not by time"


def test_a_stated_time_outranks_a_said_time() -> None:
    statements = [
        _runs("old", "R-3.0", valid_from=_t(15), said_at=_t(16)),
        _runs("new", "R-3.1", valid_from=_t(16), said_at=_t(14)),
    ]
    assert _values(resolve(statements, SINGLE).current) == ["R-3.1"]


def test_a_hedge_never_decides() -> None:
    statements = [
        _runs("sure", "R-3.0", said_at=_t(13)),
        _runs("maybe", "R-3.1", modality="hedged", said_at=_t(15)),
    ]
    result = resolve(statements, SINGLE)
    assert _values(result.current) == ["R-3.0"]
    assert [(f.value, f.modality) for f in result.pending] == [("R-3.1", "hedged")]


def test_a_denial_said_before_the_statement_leaves_it_standing() -> None:
    statements = [
        _runs("no", "R-3.1", polarity="denied", said_at=_t(13)),
        _runs("yes", "R-3.1", said_at=_t(15)),
    ]
    result = resolve(statements, SINGLE)
    assert _values(result.current) == ["R-3.1"]
    assert not result.withdrawn


def test_an_unordered_denial_is_a_conflict() -> None:
    statements = [
        _runs("yes", "R-3.1", record="m1"),
        _runs("no", "R-3.1", polarity="denied", record="m2"),
    ]
    result = resolve(statements, SINGLE)
    assert [c.reason for c in result.conflicts] == [
        "a denial that cannot be ordered against the statement"
    ]


def test_an_exclusive_slot_reports_a_second_value_and_keeps_the_strongest() -> None:
    rules = {("Unit", "model"): SlotRule(single=True, exclusive=True)}
    statements = [
        Statement(
            id="row",
            vertex_type="Unit",
            key="U-1",
            slot="model",
            value="SD-150",
            basis=Basis.RECORDED,
        ),
        Statement(
            id="text",
            vertex_type="Unit",
            key="U-1",
            slot="model",
            value="SD-60",
            said_at=_t(16),
        ),
    ]
    result = resolve(statements, rules)
    assert _values(result.current) == ["SD-150"]
    assert [c.reason for c in result.conflicts] == ["an exclusive slot holds one value"]


def test_a_multi_valued_slot_keeps_every_value() -> None:
    statements = [_runs("a", "R-3.0"), _runs("b", "R-3.1")]
    assert _values(resolve(statements, {}).current) == ["R-3.0", "R-3.1"]


def test_supports_name_every_statement_behind_a_value() -> None:
    statements = [
        _runs("a", "R-3.0", said_at=_t(14)),
        _runs("b", "R-3.0", said_at=_t(16)),
    ]
    (fact,) = resolve(statements, SINGLE).current
    assert fact.supports == ["a", "b"]


def test_rules_come_from_the_schema() -> None:
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "fleet", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "Unit",
                                "properties": [
                                    {"name": "serial", "type": "STRING"},
                                    {
                                        "name": "tags",
                                        "type": "LIST",
                                        "item_type": "STRING",
                                        "merge": "union",
                                    },
                                ],
                                "identity": ["serial"],
                            },
                            {
                                "name": "UnitState",
                                "properties": [
                                    {"name": "serial", "type": "STRING"},
                                    {"name": "runs", "type": "STRING"},
                                    {"name": "valid_from", "type": "DATETIME"},
                                ],
                                "hash_identity_properties": ["serial", "valid_from"],
                            },
                            {
                                "name": "Release",
                                "properties": [{"name": "rid", "type": "STRING"}],
                                "identity": ["rid"],
                            },
                        ]
                    },
                    "edge_config": {
                        "edges": [
                            {
                                "source": "Unit",
                                "target": "Release",
                                "relation": "runs",
                                "merge": "replace",
                            },
                            {
                                "source": "Unit",
                                "target": "Release",
                                "relation": "supports",
                            },
                        ]
                    },
                },
            }
        }
    )
    manifest.finish_init()
    rules = slot_rules(manifest.require_schema())
    assert rules[RUNS] == SlotRule(single=True, stateful=True)
    assert ("Unit", "supports") not in rules
    assert ("Unit", "tags") not in rules
    assert ("Unit", "serial") not in rules
