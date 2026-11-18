"""``fallback: own_key``: records no declared branch keys stay keyed on their own key.

Each member's own key is kept on the merged type under its key space
(``<space>__<field>``). With the fallback, those kept keys are also the last
branches of the merged funnel, one per member, so nothing is named by the
mechanism itself and a merged type merges again with a third manifest.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    MergeIdentityError,
    MergeManifestsOp,
    merge_manifests,
)
from graflo.architecture.evolution.naming_graph import (
    MergeNamingError,
    build_naming,
    suggest_merge_op,
)
from graflo.architecture.schema.identity_digest import compute_vertex_identity
from graflo.architecture.schema.vertex import Vertex
from graflo.hq.document_caster import DocumentCaster
from graflo.hq.ingestion_parameters import IngestionParams


def _source(name: str, vertex: str, key: str, label: str, code: str) -> GraphManifest:
    """One class keyed on *key*, a resource writing it, and one finding it by key."""
    note = f"Note{name.upper()}"
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": name, "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": vertex,
                                "properties": [key, label, code],
                                "identity": [key],
                            },
                            {"name": note, "properties": ["nid"], "identity": ["nid"]},
                        ]
                    },
                    "edge_config": {
                        "edges": [
                            {"source": note, "target": vertex, "relation": f"on_{name}"}
                        ]
                    },
                },
            },
            "ingestion_model": {
                "resources": [
                    {"name": f"{name}_items", "pipeline": [{"vertex": vertex}]},
                    {
                        "name": f"{name}_notes",
                        "pipeline": [
                            {"vertex": note},
                            {"vertex": vertex, "lookup_only": True},
                            {"from": note, "to": vertex, "relation": f"on_{name}"},
                        ],
                    },
                ]
            },
        }
    )
    manifest.finish_init()
    return manifest


def _a() -> GraphManifest:
    return _source("a", "X", "i_a", "name", "sku")


def _b() -> GraphManifest:
    return _source("b", "Y", "i_b", "pname", "unit")


def _c() -> GraphManifest:
    return _source("c", "Z", "i_c", "zname", "zsku")


def _op(
    left: str, right: str, renames: dict[str, str], **extra: Any
) -> MergeManifestsOp:
    return MergeManifestsOp.model_validate(
        {
            "canonical_maps": {"right": {"properties": {right: renames}}},
            "vertex_equivalences": [
                {
                    "left": left,
                    "right": right,
                    "into": "M",
                    "identity": [["name", "sku"]],
                    "fallback": "own_key",
                    **extra,
                }
            ],
        }
    )


B_NAMES = {"pname": "name", "unit": "sku"}
C_NAMES = {"zname": "name", "zsku": "sku"}


def _merged(manifest: GraphManifest, name: str = "M") -> Vertex:
    return manifest.require_schema().core_schema.vertex_config[name]


def _branches(vertex: Vertex) -> list[list[str]]:
    assert vertex.identity_funnel is not None
    return [list(b.fields) for b in vertex.identity_funnel.branches]


def _ab() -> GraphManifest:
    return merge_manifests(_a(), _b(), _op("X", "Y", B_NAMES))


class TestOneBranchPerSource:
    def test_each_member_falls_back_on_its_kept_key(self) -> None:
        m = _merged(_ab())

        assert m.identity == ["id"]
        assert _branches(m) == [["name", "sku"], ["a__i_a"], ["b__i_b"]]
        assert [(s.name, s.fields) for s in m.secondary_identities] == [
            ("a", ["a__i_a"]),
            ("b", ["b__i_b"]),
        ]

    def test_nothing_is_added_for_it(self) -> None:
        out = _ab()

        assert set(_merged(out).property_names) == {
            "a__i_a",
            "name",
            "sku",
            "b__i_b",
            "id",
        }
        steps = [
            step
            for resource in out.require_ingestion_model().resources
            for step in resource.pipeline
        ]
        assert not any(type(step).__name__ == "TransformActorConfig" for step in steps)

    def test_without_it_the_key_is_the_declared_one(self) -> None:
        op = _op("X", "Y", B_NAMES)
        op.vertex_equivalences[0].fallback = None

        m = _merged(merge_manifests(_a(), _b(), op))

        assert m.identity == ["name", "sku"]
        assert m.identity_funnel is None

    def test_a_key_space_names_the_branch(self) -> None:
        op = _op("X", "Y", B_NAMES, key_spaces={"left": {"X": "plant"}})

        m = _merged(merge_manifests(_a(), _b(), op))

        assert _branches(m)[1:] == [["plant__i_a"], ["b__i_b"]]
        assert [s.name for s in m.secondary_identities] == ["plant", "b"]

    def test_a_key_the_declared_identity_lists_stays_a_lookup(self) -> None:
        """One declared branch over a member's key is a funnel under the fallback."""
        op = MergeManifestsOp.model_validate(
            {
                "canonical_maps": {"right": {"properties": {"Y": B_NAMES}}},
                "vertex_equivalences": [
                    {
                        "left": "X",
                        "right": "Y",
                        "into": "M",
                        "identity": ["i_a"],
                        "fallback": "own_key",
                    }
                ],
            }
        )

        m = _merged(merge_manifests(_a(), _b(), op))

        assert _branches(m) == [["i_a"], ["b__i_b"]]
        assert [(s.name, s.fields) for s in m.secondary_identities] == [
            ("a", ["i_a"]),
            ("b", ["b__i_b"]),
        ]


class TestChaining:
    def test_a_third_manifest_adds_its_own_branch(self) -> None:
        abc = merge_manifests(_ab(), _c(), _op("M", "Z", C_NAMES))

        assert _branches(_merged(abc)) == [
            ["name", "sku"],
            ["a__i_a"],
            ["b__i_b"],
            ["c__i_c"],
        ]
        assert [s.name for s in _merged(abc).secondary_identities] == ["a", "b", "c"]

    def test_the_order_of_merges_does_not_change_the_key(self) -> None:
        abc = merge_manifests(_ab(), _c(), _op("M", "Z", C_NAMES))
        bc = merge_manifests(_b(), _c(), _op("Y", "Z", C_NAMES))
        a_bc = merge_manifests(
            _a(),
            bc,
            _op("X", "M", {}),
        )

        assert _branches(_merged(a_bc)) == _branches(_merged(abc))
        for record in (
            {"name": "n", "sku": "s"},
            {"a__i_a": "7"},
            {"b__i_b": "7"},
            {"c__i_c": "7"},
        ):
            assert compute_vertex_identity(
                record, _merged(a_bc)
            ) == compute_vertex_identity(record, _merged(abc))

    def test_a_key_space_already_on_the_type_is_refused(self) -> None:
        op = _op("M", "Z", C_NAMES).model_copy(
            update={"origins": {"left": "ab", "right": "a"}}
        )

        with pytest.raises(MergeIdentityError, match="secondary identity named 'a'"):
            merge_manifests(_ab(), _c(), op)

    def test_a_type_keyed_by_an_earlier_fallback_property_keeps_it(self) -> None:
        """A merged type written before ``fallback`` existed is an ordinary funnel."""
        earlier = GraphManifest.from_config(
            {
                "schema": {
                    "metadata": {"name": "ab", "version": "1.0.0"},
                    "graph": {
                        "vertex_config": {
                            "vertices": [
                                {
                                    "name": "M",
                                    "properties": ["name", "sku", "local_key", "id"],
                                    "identity": ["id"],
                                    "identity_funnel": {
                                        "branches": [
                                            {
                                                "id": "name_sku",
                                                "fields": ["name", "sku"],
                                            },
                                            {
                                                "id": "local_key",
                                                "fields": ["local_key"],
                                            },
                                        ]
                                    },
                                }
                            ]
                        },
                        "edge_config": {"edges": []},
                    },
                },
                "ingestion_model": {
                    "resources": [
                        {
                            "name": "ab_items",
                            "pipeline": [
                                {"vertex": "M"},
                                {
                                    "transform": {
                                        "call": {
                                            "module": "graflo.util.transform",
                                            "foo": "tagged_key",
                                            "params": {"tag": "a", "sep": ":"},
                                            "input": ["ref"],
                                            "output": ["local_key"],
                                        }
                                    }
                                },
                            ],
                        }
                    ]
                },
            }
        )
        earlier.finish_init()

        out = merge_manifests(earlier, _c(), _op("M", "Z", C_NAMES))

        assert _branches(_merged(out)) == [["name", "sku"], ["local_key"], ["c__i_c"]]


class TestSameSideMembers:
    """Two classes of one manifest keyed on the same field, mapped onto one."""

    @staticmethod
    def _sides() -> tuple[GraphManifest, GraphManifest]:
        left = GraphManifest.from_config(
            {
                "schema": {
                    "metadata": {"name": "plant", "version": "1.0.0"},
                    "graph": {
                        "vertex_config": {
                            "vertices": [
                                {
                                    "name": "Machine",
                                    "properties": ["asset_id", "serial"],
                                    "identity": ["asset_id"],
                                },
                                {
                                    "name": "Bench",
                                    "properties": ["asset_id", "serial"],
                                    "identity": ["asset_id"],
                                },
                            ]
                        },
                        "edge_config": {"edges": []},
                    },
                },
                "ingestion_model": {
                    "resources": [
                        {"name": "machines", "pipeline": [{"vertex": "Machine"}]},
                        {"name": "benches", "pipeline": [{"vertex": "Bench"}]},
                    ]
                },
            }
        )
        right = _source("feed", "Device", "device_id", "serial", "model")
        left.finish_init()
        return left, right

    @staticmethod
    def _op(**extra: Any) -> MergeManifestsOp:
        return MergeManifestsOp.model_validate(
            {
                "vertex_equivalences": [
                    {
                        "left": ["Machine", "Bench"],
                        "right": "Device",
                        "into": "Equipment",
                        "identity": ["serial"],
                        "fallback": "own_key",
                        "allow": ["observation_fusion"],
                        **extra,
                    }
                ]
            }
        )

    def test_sharing_one_fallback_key_is_refused(self) -> None:
        left, right = self._sides()

        with pytest.raises(MergeNamingError, match="would fall back on one key") as e:
            merge_manifests(left, right, self._op())

        assert any(f.check == "key-space" for f in e.value.findings)

    def test_one_registry_shares_one_branch(self) -> None:
        left, right = self._sides()
        spaces = {"left": {"Machine": "plant", "Bench": "plant"}}

        out = merge_manifests(left, right, self._op(key_spaces=spaces))

        assert _branches(_merged(out, "Equipment")) == [
            ["serial"],
            ["plant__asset_id"],
            ["feed__device_id"],
        ]

    def test_a_name_that_cannot_be_a_key_space_is_refused_as_such(self) -> None:
        left, right = self._sides()
        spaces = {"left": {"Machine": "p:1"}}

        with pytest.raises(MergeIdentityError, match="is put in 'p:1'"):
            merge_manifests(left, right, self._op(key_spaces=spaces))

    def test_two_registries_get_two_branches(self) -> None:
        left, right = self._sides()
        spaces = {"left": {"Machine": "plant", "Bench": "bench"}}

        out = merge_manifests(left, right, self._op(key_spaces=spaces))

        assert _branches(_merged(out, "Equipment")) == [
            ["serial"],
            ["plant__asset_id"],
            ["bench__asset_id"],
            ["feed__device_id"],
        ]


class TestMemberKinds:
    def _right(self, vertex: dict[str, Any]) -> GraphManifest:
        manifest = GraphManifest.from_config(
            {
                "schema": {
                    "metadata": {"name": "b", "version": "1.0.0"},
                    "graph": {
                        "vertex_config": {"vertices": [vertex]},
                        "edge_config": {"edges": []},
                    },
                },
                "ingestion_model": {
                    "resources": [{"name": "b_items", "pipeline": [{"vertex": "Y"}]}]
                },
            }
        )
        manifest.finish_init()
        return manifest

    def _merged_with(self, vertex: dict[str, Any]) -> Vertex:
        return _merged(merge_manifests(_a(), self._right(vertex), _op("X", "Y", {})))

    def test_a_composite_key_is_one_branch(self) -> None:
        m = self._merged_with(
            {
                "name": "Y",
                "properties": ["plant", "tag", "name", "sku"],
                "identity": ["plant", "tag"],
            }
        )

        assert _branches(m)[-1] == ["b__plant", "b__tag"]

    def test_a_hash_key_falls_back_on_its_fields(self) -> None:
        m = self._merged_with(
            {
                "name": "Y",
                "properties": ["name", "sku", "batch", "lot"],
                "hash_identity_properties": ["batch", "lot"],
            }
        )

        assert _branches(m)[-1] == ["b__batch", "b__lot"]

    def test_a_funnel_member_keeps_its_branches(self) -> None:
        m = self._merged_with(
            {
                "name": "Y",
                "properties": ["name", "sku", "ean", "upc", "id"],
                "identity": ["id"],
                "identity_funnel": {
                    "branches": [
                        {"id": "ean", "fields": ["ean"]},
                        {"id": "upc", "fields": ["upc"]},
                    ]
                },
            }
        )

        assert _branches(m) == [["name", "sku"], ["a__i_a"], ["ean"], ["upc"]]


class TestNothingIsKeyedSilently:
    """A member some resource leaves without a key is refused; the repair sets the fallback."""

    @staticmethod
    def _sides() -> tuple[GraphManifest, GraphManifest]:
        left, right = TestSameSideMembers._sides()
        return left, right

    @staticmethod
    def _op() -> MergeManifestsOp:
        """A vocabulary joins benches to machines; only machines derive the key."""
        return MergeManifestsOp.model_validate(
            {
                "canonical_maps": {
                    "left": {
                        "vertices": {"Machine": "Equipment", "Bench": "Equipment"},
                        "allow_merges": True,
                    }
                },
                "vertex_equivalences": [
                    {
                        "left": "Machine",
                        "right": "Device",
                        "identity": [
                            {
                                "name": "match_key",
                                "sources": {
                                    "machines": {"input": ["serial"]},
                                    "feed_items": {"input": ["serial"]},
                                },
                            }
                        ],
                    }
                ],
            }
        )

    def test_an_uncovered_member_is_refused_with_a_repair(self) -> None:
        left, right = self._sides()

        result = build_naming(self._op(), left=left, right=right)

        (finding,) = [f for f in result.blocking if f.check == "identity-coverage"]
        assert "left:Bench" in finding.message
        assert [r.kind for r in finding.repairs] == ["set_fallback"]

    def test_the_suggested_op_sets_the_fallback(self) -> None:
        left, right = self._sides()

        suggested = suggest_merge_op(left, right, self._op())

        assert suggested.vertex_equivalences[0].fallback == "own_key"


class TestRecords:
    """Records through the merged manifest: one vertex per entity, sources kept apart."""

    def _ids(self, out: GraphManifest, resource: str, rows: list[dict]) -> list[str]:
        caster = DocumentCaster(out.require_ingestion_model())
        result = asyncio.run(
            caster.cast_batch(rows, resource, params=IngestionParams())
        )
        return [doc["id"] for doc in result.graph.vertices["M"]]

    def test_a_record_without_the_merged_key_keys_on_its_own(self) -> None:
        out = _ab()
        out.finish_init()

        (matched_a,) = self._ids(
            out, "a_items", [{"i_a": "1", "name": "n", "sku": "s"}]
        )
        (matched_b,) = self._ids(
            out, "b_items", [{"i_b": "9", "pname": "n", "unit": "s"}]
        )
        (own_a,) = self._ids(out, "a_items", [{"i_a": "1"}])
        (own_b,) = self._ids(out, "b_items", [{"i_b": "1"}])

        assert matched_a == matched_b
        assert len({matched_a, own_a, own_b}) == 3
