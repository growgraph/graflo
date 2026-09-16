"""Declared inverses, and their two mutually exclusive realizations.

``edge_config.inverses`` is the logical declaration. It is realized either as an
explicit inverse edge (portable) or as a native inverse on the db_profile
(TigerGraph maintains the pair) -- never both for one edge.
"""

from __future__ import annotations

import copy
from typing import Any

import pytest

from graflo.architecture.contract import GraphManifest
from graflo.architecture.evolution import apply_evolution
from graflo.architecture.evolution.autogenerate import diff_manifests_verified
from graflo.architecture.evolution.codec import op_from_dict
from graflo.architecture.evolution.hashing import manifest_hash
from graflo.architecture.evolution.merge3 import op_slots
from graflo.architecture.schema import Schema

FORWARD = {"source": "person", "target": "institution", "relation": "employed_by"}
INVERSE = {"source": "institution", "target": "person", "relation": "employs"}
PAIR = {"relation": "employed_by", "inverse": "employs"}


def _schema_payload(
    *,
    edges: list[dict[str, Any]] | None = None,
    inverses: list[dict[str, str]] | None = None,
    db_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "metadata": {"name": "inverses", "version": "1.0.0"},
        "graph": {
            "vertex_config": {
                "vertices": [
                    {"name": "person", "properties": ["id"], "identity": ["id"]},
                    {"name": "institution", "properties": ["id"], "identity": ["id"]},
                ]
            },
            "edge_config": {
                "edges": copy.deepcopy(edges if edges is not None else [FORWARD]),
                "inverses": copy.deepcopy(inverses if inverses is not None else [PAIR]),
            },
        },
    }
    if db_profile is not None:
        payload["db_profile"] = db_profile
    return payload


def _schema(**kwargs: Any) -> Schema:
    return Schema.model_validate(_schema_payload(**kwargs))


def _manifest(**kwargs: Any) -> GraphManifest:
    return GraphManifest.model_validate({"schema": _schema_payload(**kwargs)})


def _tigergraph(*, native: bool = True) -> dict[str, Any]:
    return {
        "db_flavor": "tigergraph",
        "edge_specs": [{**FORWARD, "native_inverse": native}],
    }


def _apply(manifest: GraphManifest, *ops: dict[str, Any]) -> GraphManifest:
    return apply_evolution(
        manifest, [op_from_dict(op) for op in ops], bump_version=False
    )


def _pairs(manifest: GraphManifest) -> list[tuple[str, str]]:
    assert manifest.graph_schema is not None
    return [
        (p.relation, p.inverse)
        for p in manifest.graph_schema.core_schema.edge_config.inverses
    ]


class TestDeclaration:
    def test_a_declared_pair_answers_both_ways(self) -> None:
        edge_config = _schema().core_schema.edge_config
        assert edge_config.inverse_of("employed_by") == "employs"
        assert edge_config.inverse_of("employs") == "employed_by"
        assert edge_config.inverse_of("unrelated") is None

    def test_a_relation_cannot_be_its_own_inverse(self) -> None:
        with pytest.raises(ValueError, match="directed: false"):
            _schema(inverses=[{"relation": "knows", "inverse": "knows"}])

    @pytest.mark.parametrize(
        "inverses",
        [
            [PAIR, {"relation": "employs", "inverse": "employed_by"}],
            [PAIR, {"relation": "employs", "inverse": "pays"}],
        ],
        ids=["restated-reversed", "chain"],
    )
    def test_a_relation_belongs_to_at_most_one_pair(
        self, inverses: list[dict[str, str]]
    ) -> None:
        with pytest.raises(ValueError, match="at most one inverse pair"):
            _schema(inverses=inverses)

    def test_a_pair_naming_no_edge_is_dangling(self) -> None:
        with pytest.raises(ValueError, match="name no declared edge"):
            _schema(inverses=[{"relation": "a", "inverse": "b"}])

    def test_a_template_edge_admits_relations_named_at_ingest_time(self) -> None:
        _schema(
            edges=[{"source": "person", "target": "institution"}],
            inverses=[{"relation": "a", "inverse": "b"}],
        )

    def test_an_undirected_relation_has_no_inverse(self) -> None:
        with pytest.raises(ValueError, match="undirected edge has no inverse"):
            _schema(edges=[{**FORWARD, "directed": False}])

    def test_explicit_inverse_edges_realize_a_declaration(self) -> None:
        _schema(edges=[FORWARD, INVERSE])


class TestNativeInverse:
    def test_a_native_inverse_realizes_a_declared_pair(self) -> None:
        schema = _schema(db_profile=_tigergraph())
        name = schema.db_profile.native_inverse_name(
            ("person", "institution", "employed_by"), schema.core_schema.edge_config
        )
        assert name == "employs"

    def test_it_needs_a_declared_inverse(self) -> None:
        with pytest.raises(ValueError, match="needs a declared inverse"):
            _schema(inverses=[], db_profile=_tigergraph())

    def test_it_must_not_duplicate_an_explicit_inverse_edge(self) -> None:
        with pytest.raises(ValueError, match="duplicates the explicit inverse edge"):
            _schema(edges=[FORWARD, INVERSE], db_profile=_tigergraph())

    def test_it_is_tigergraph_only(self) -> None:
        profile = {
            "db_flavor": "neo4j",
            "edge_specs": [{**FORWARD, "native_inverse": True}],
        }
        with pytest.raises(ValueError, match="TigerGraph-only"):
            _schema(db_profile=profile)

    def test_its_name_must_not_shadow_another_type(self) -> None:
        other = {"source": "person", "target": "person", "relation": "employs"}
        with pytest.raises(ValueError, match="collides with a declared edge type"):
            _schema(edges=[FORWARD, other], db_profile=_tigergraph())
        with pytest.raises(ValueError, match="collides with a vertex type"):
            _schema(
                inverses=[{"relation": "employed_by", "inverse": "person"}],
                db_profile=_tigergraph(),
            )


class TestOps:
    def test_declare_then_retract(self) -> None:
        manifest = _manifest(inverses=[])
        declared = _apply(
            manifest,
            {"op": "declare_edge_inverses", "inverses": {"employed_by": "employs"}},
        )
        assert _pairs(declared) == [("employed_by", "employs")]
        retracted = _apply(
            declared, {"op": "retract_edge_inverses", "relations": ["employs"]}
        )
        assert _pairs(retracted) == []

    def test_declaring_a_reversed_restatement_is_a_no_op(self) -> None:
        manifest = _manifest()
        out = _apply(
            manifest,
            {"op": "declare_edge_inverses", "inverses": {"employs": "employed_by"}},
        )
        assert manifest_hash(out) == manifest_hash(manifest)

    def test_declaring_a_contradicting_pair_is_refused(self) -> None:
        with pytest.raises(ValueError, match="declared inverse of 'employs'"):
            _apply(
                _manifest(),
                {"op": "declare_edge_inverses", "inverses": {"employed_by": "hires"}},
            )

    def test_retracting_an_undeclared_relation_is_refused(self) -> None:
        with pytest.raises(ValueError, match="no declared inverse"):
            _apply(_manifest(), {"op": "retract_edge_inverses", "relations": ["x"]})

    def test_retracting_a_natively_realized_pair_is_refused(self) -> None:
        manifest = _manifest(db_profile=_tigergraph())
        with pytest.raises(ValueError, match="still realized by native inverses"):
            _apply(
                manifest, {"op": "retract_edge_inverses", "relations": ["employed_by"]}
            )

    def test_set_native_inverses_enables_and_withdraws(self) -> None:
        manifest = _manifest(db_profile={"db_flavor": "tigergraph"})
        enabled = _apply(manifest, {"op": "set_native_inverses", "edges": [FORWARD]})
        assert enabled.graph_schema is not None
        assert enabled.graph_schema.db_profile.edge_has_native_inverse(
            ("person", "institution", "employed_by")
        )
        withdrawn = _apply(
            enabled, {"op": "set_native_inverses", "edges": [FORWARD], "enabled": False}
        )
        assert manifest_hash(withdrawn) == manifest_hash(manifest)

    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            ({"inverses": []}, "needs a declared inverse"),
            ({"edges": [FORWARD, INVERSE]}, "duplicates the explicit inverse edge"),
            ({"db_profile": {"db_flavor": "arango"}}, "TigerGraph-only"),
        ],
        ids=["undeclared", "explicit-inverse", "not-tigergraph"],
    )
    def test_set_native_inverses_refusals_name_the_op(
        self, kwargs: dict[str, Any], match: str
    ) -> None:
        kwargs.setdefault("db_profile", {"db_flavor": "tigergraph"})
        with pytest.raises(ValueError, match=f"set_native_inverses: .*{match}"):
            _apply(
                _manifest(**kwargs), {"op": "set_native_inverses", "edges": [FORWARD]}
            )

    def test_set_native_inverses_refuses_an_unknown_edge(self) -> None:
        with pytest.raises(ValueError, match="unknown edges"):
            _apply(
                _manifest(db_profile={"db_flavor": "tigergraph"}),
                {"op": "set_native_inverses", "edges": [INVERSE]},
            )

    def test_slots_separate_declaration_from_realization(self) -> None:
        declare = op_slots(
            op_from_dict(
                {"op": "declare_edge_inverses", "inverses": {"employed_by": "employs"}}
            )
        )
        native = op_slots(
            op_from_dict({"op": "set_native_inverses", "edges": [FORWARD]})
        )
        assert all(slot[-1] == "inverse" for slot in declare)
        assert all(slot[-1] == "native_inverse" for slot in native)


class TestTheTableSurvivesOtherOps:
    def test_rename_relations_renames_both_columns(self) -> None:
        out = _apply(
            _manifest(),
            {"op": "rename_relations", "renames": {"employed_by": "works_at"}},
        )
        assert _pairs(out) == [("works_at", "employs")]

    def test_merge_edges_refuses_collapsing_a_pair_onto_itself(self) -> None:
        manifest = _manifest(edges=[FORWARD, {**FORWARD, "relation": "employs"}])
        with pytest.raises(ValueError, match="would both become"):
            _apply(
                manifest,
                {"op": "merge_edges", "sources": ["employs"], "into": "employed_by"},
            )

    def test_removing_every_edge_of_a_pair_prunes_it(self) -> None:
        manifest = _manifest(
            edges=[
                FORWARD,
                {"source": "person", "target": "person", "relation": "knows"},
            ]
        )
        out = _apply(manifest, {"op": "remove_edges", "relations": ["employed_by"]})
        assert _pairs(out) == []

    def test_unrelated_structural_ops_keep_the_table(self) -> None:
        manifest = _manifest()
        out = _apply(
            manifest,
            {
                "op": "add_vertices",
                "vertices": [
                    {"name": "audit", "properties": ["id"], "identity": ["id"]}
                ],
            },
            {
                "op": "add_edges",
                "edges": [{"source": "audit", "target": "person", "relation": "logs"}],
            },
            {"op": "rename_vertices", "renames": {"institution": "org"}},
            {"op": "remove_vertices", "names": ["audit"]},
        )
        assert _pairs(out) == [("employed_by", "employs")]


class TestDiff:
    @pytest.mark.parametrize(
        ("base", "target"),
        [
            ({"inverses": []}, {}),
            ({}, {"inverses": []}),
            (
                {"db_profile": {"db_flavor": "tigergraph"}},
                {"db_profile": _tigergraph()},
            ),
            (
                {"inverses": [], "db_profile": {"db_flavor": "tigergraph"}},
                {"db_profile": _tigergraph()},
            ),
            (
                {"db_profile": _tigergraph()},
                {"inverses": [], "db_profile": {"db_flavor": "tigergraph"}},
            ),
        ],
        ids=["declare", "retract", "enable-native", "declare-and-native", "undo-both"],
    )
    def test_the_differ_replays_declarations_and_native_inverses(
        self, base: dict[str, Any], target: dict[str, Any]
    ) -> None:
        ops, warnings = diff_manifests_verified(_manifest(**base), _manifest(**target))
        assert ops
        assert warnings == []
        assert "set_db_profile" not in {op.op for op in ops}
