"""Declared inverses, and how each kind is realized.

``edge_config.inverses`` (pairs of distinct relations) and
``edge_config.symmetric`` (relations that are their own inverse) together define
``inv`` over relation names. A pair is realized either by explicit inverse edges
(portable) or by a native inverse on the db_profile (TigerGraph maintains the
pair) -- never both for one relation. A symmetric relation is realized by its
edges being undirected.
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
KNOWS = {"source": "person", "target": "person", "relation": "knows", "directed": False}


def _schema_payload(
    *,
    edges: list[dict[str, Any]] | None = None,
    inverses: list[dict[str, str]] | None = None,
    symmetric: list[str] | None = None,
    db_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    edge_config: dict[str, Any] = {
        "edges": copy.deepcopy(edges if edges is not None else [FORWARD]),
        "inverses": copy.deepcopy(inverses if inverses is not None else [PAIR]),
    }
    if symmetric is not None:
        edge_config["symmetric"] = list(symmetric)
    payload: dict[str, Any] = {
        "metadata": {"name": "inverses", "version": "1.0.0"},
        "graph": {
            "vertex_config": {
                "vertices": [
                    {"name": "person", "properties": ["id"], "identity": ["id"]},
                    {"name": "institution", "properties": ["id"], "identity": ["id"]},
                    {"name": "company", "properties": ["id"], "identity": ["id"]},
                ]
            },
            "edge_config": edge_config,
        },
    }
    if db_profile is not None:
        payload["db_profile"] = db_profile
    return payload


def _schema(**kwargs: Any) -> Schema:
    return Schema.model_validate(_schema_payload(**kwargs))


def _manifest(**kwargs: Any) -> GraphManifest:
    return GraphManifest.model_validate({"schema": _schema_payload(**kwargs)})


def _tigergraph(*native: str, **extra: Any) -> dict[str, Any]:
    return {"db_flavor": "tigergraph", "native_inverses": list(native), **extra}


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


def _symmetric(manifest: GraphManifest) -> list[str]:
    assert manifest.graph_schema is not None
    return list(manifest.graph_schema.core_schema.edge_config.symmetric)


def _native(manifest: GraphManifest) -> list[str]:
    assert manifest.graph_schema is not None
    return list(manifest.graph_schema.db_profile.native_inverses)


class TestDeclaration:
    def test_a_declared_pair_answers_both_ways(self) -> None:
        edge_config = _schema().core_schema.edge_config
        assert edge_config.inverse_of("employed_by") == "employs"
        assert edge_config.inverse_of("employs") == "employed_by"
        assert edge_config.inverse_of("unrelated") is None

    def test_a_symmetric_relation_is_its_own_inverse(self) -> None:
        edge_config = _schema(
            edges=[FORWARD, KNOWS], symmetric=["knows"]
        ).core_schema.edge_config
        assert edge_config.inverse_of("knows") == "knows"
        assert edge_config.is_symmetric("knows")
        assert not edge_config.is_symmetric("employed_by")

    def test_a_pair_of_one_name_points_to_symmetric(self) -> None:
        with pytest.raises(ValueError, match="symmetric"):
            _schema(inverses=[{"relation": "knows", "inverse": "knows"}])

    def test_a_pair_restated_in_either_order_is_kept_once(self) -> None:
        reversed_pair = {"relation": "employs", "inverse": "employed_by"}
        edge_config = _schema(inverses=[reversed_pair, PAIR]).core_schema.edge_config
        assert [(p.relation, p.inverse) for p in edge_config.inverses] == [
            ("employed_by", "employs")
        ]

    def test_the_orientation_a_pair_is_written_in_does_not_change_the_hash(
        self,
    ) -> None:
        forward = _manifest(inverses=[PAIR])
        backward = _manifest(
            inverses=[{"relation": "employs", "inverse": "employed_by"}]
        )
        assert manifest_hash(forward) == manifest_hash(backward)

    @pytest.mark.parametrize(
        ("inverses", "symmetric"),
        [
            ([PAIR, {"relation": "employs", "inverse": "pays"}], []),
            ([PAIR], ["employs"]),
        ],
        ids=["chain", "paired-and-symmetric"],
    )
    def test_a_relation_has_at_most_one_inverse(
        self, inverses: list[dict[str, str]], symmetric: list[str]
    ) -> None:
        with pytest.raises(ValueError, match="at most one inverse.*'employs'"):
            _schema(inverses=inverses, symmetric=symmetric)

    def test_a_declaration_naming_no_edge_is_dangling(self) -> None:
        with pytest.raises(ValueError, match="name no declared edge"):
            _schema(inverses=[{"relation": "a", "inverse": "b"}])
        with pytest.raises(ValueError, match="name no declared edge"):
            _schema(inverses=[], symmetric=["knows"])

    def test_a_template_edge_admits_relations_named_at_ingest_time(self) -> None:
        _schema(
            edges=[{"source": "person", "target": "institution"}],
            inverses=[{"relation": "a", "inverse": "b"}],
            symmetric=["c"],
        )

    def test_an_undirected_edge_has_no_inverse_pair(self) -> None:
        with pytest.raises(ValueError, match="undirected edge has no inverse pair"):
            _schema(edges=[{**FORWARD, "directed": False}])

    def test_a_symmetric_relation_requires_undirected_edges(self) -> None:
        with pytest.raises(ValueError, match="must be undirected"):
            _schema(edges=[FORWARD, {**KNOWS, "directed": True}], symmetric=["knows"])

    def test_an_undirected_edge_need_not_be_declared_symmetric(self) -> None:
        _schema(edges=[FORWARD, KNOWS])

    def test_explicit_inverse_edges_realize_a_declaration(self) -> None:
        _schema(edges=[FORWARD, INVERSE])


class TestNativeInverse:
    def test_a_native_inverse_realizes_a_declared_pair(self) -> None:
        schema = _schema(db_profile=_tigergraph("employed_by"))
        edge_config = schema.core_schema.edge_config
        assert schema.db_profile.native_inverse_of("employed_by", edge_config) == (
            "employs"
        )
        assert schema.db_profile.native_inverse_of("employs", edge_config) is None

    def test_it_covers_every_edge_of_the_relation(self) -> None:
        other = {**FORWARD, "target": "company"}
        schema = _schema(edges=[FORWARD, other], db_profile=_tigergraph("employed_by"))
        assert schema.db_profile.has_native_inverse("employed_by")

    def test_it_needs_a_declared_pair(self) -> None:
        with pytest.raises(ValueError, match="needs a declared pair"):
            _schema(inverses=[], db_profile=_tigergraph("employed_by"))

    def test_it_refuses_a_symmetric_relation(self) -> None:
        with pytest.raises(ValueError, match="symmetric relation has no reverse type"):
            _schema(
                edges=[FORWARD, KNOWS],
                symmetric=["knows"],
                db_profile=_tigergraph("knows"),
            )

    def test_it_must_not_duplicate_explicit_inverse_edges(self) -> None:
        with pytest.raises(ValueError, match="keep one realization"):
            _schema(edges=[FORWARD, INVERSE], db_profile=_tigergraph("employed_by"))

    def test_only_one_side_of_a_pair_can_be_native(self) -> None:
        with pytest.raises(ValueError, match="only one side of a pair"):
            _schema(
                edges=[FORWARD, INVERSE],
                db_profile=_tigergraph("employed_by", "employs"),
            )

    def test_it_is_tigergraph_only(self) -> None:
        with pytest.raises(ValueError, match="TigerGraph-only"):
            _schema(
                db_profile={"db_flavor": "neo4j", "native_inverses": ["employed_by"]}
            )

    def test_it_names_a_declared_edge(self) -> None:
        with pytest.raises(ValueError, match="names no declared edge"):
            _schema(db_profile=_tigergraph("employs"))

    def test_its_name_must_not_shadow_a_vertex_type(self) -> None:
        with pytest.raises(ValueError, match="collides with a vertex type"):
            _schema(
                inverses=[{"relation": "employed_by", "inverse": "person"}],
                db_profile=_tigergraph("employed_by"),
            )

    def test_it_needs_the_relation_stored_as_one_edge_type(self) -> None:
        other = {**FORWARD, "target": "company"}
        profile = _tigergraph(
            "employed_by",
            edge_specs=[
                {**FORWARD, "relation_name": "employed_by_institution"},
                {**other, "relation_name": "employed_by_company"},
            ],
        )
        with pytest.raises(ValueError, match="one edge type"):
            _schema(edges=[FORWARD, other], db_profile=profile)

    def test_the_list_is_a_set(self) -> None:
        schema = _schema(db_profile=_tigergraph("employed_by", "employed_by"))
        assert schema.db_profile.native_inverses == ["employed_by"]


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

    def test_a_mapping_may_state_both_orders(self) -> None:
        out = _apply(
            _manifest(inverses=[]),
            {
                "op": "declare_edge_inverses",
                "inverses": {"employed_by": "employs", "employs": "employed_by"},
            },
        )
        assert _pairs(out) == [("employed_by", "employs")]

    def test_declaring_a_reversed_restatement_is_a_no_op(self) -> None:
        manifest = _manifest()
        out = _apply(
            manifest,
            {"op": "declare_edge_inverses", "inverses": {"employs": "employed_by"}},
        )
        assert manifest_hash(out) == manifest_hash(manifest)

    def test_declaring_a_contradicting_pair_is_refused(self) -> None:
        with pytest.raises(ValueError, match="'employed_by': 'employs' vs 'hires'"):
            _apply(
                _manifest(),
                {"op": "declare_edge_inverses", "inverses": {"employed_by": "hires"}},
            )

    def test_a_self_contradicting_mapping_is_refused_at_parse_time(self) -> None:
        with pytest.raises(ValueError, match="at most one inverse"):
            op_from_dict(
                {
                    "op": "declare_edge_inverses",
                    "inverses": {"a": "b", "b": "c"},
                }
            )

    def test_declare_then_retract_a_symmetric_relation(self) -> None:
        manifest = _manifest(edges=[FORWARD, KNOWS])
        declared = _apply(
            manifest, {"op": "declare_edge_inverses", "symmetric": ["knows"]}
        )
        assert _symmetric(declared) == ["knows"]
        retracted = _apply(
            declared, {"op": "retract_edge_inverses", "relations": ["knows"]}
        )
        assert manifest_hash(retracted) == manifest_hash(manifest)

    def test_declaring_symmetric_on_a_directed_edge_is_refused(self) -> None:
        manifest = _manifest(edges=[FORWARD, {**KNOWS, "directed": True}])
        with pytest.raises(ValueError, match="must be undirected"):
            _apply(manifest, {"op": "declare_edge_inverses", "symmetric": ["knows"]})

    def test_symmetric_after_set_edge_directed(self) -> None:
        selector = {k: v for k, v in KNOWS.items() if k != "directed"}
        out = _apply(
            _manifest(edges=[FORWARD, {**KNOWS, "directed": True}]),
            {"op": "set_edge_directed", "edges": [selector], "directed": False},
            {"op": "declare_edge_inverses", "symmetric": ["knows"]},
        )
        assert _symmetric(out) == ["knows"]
        with pytest.raises(ValueError, match="must be undirected"):
            _apply(
                out,
                {"op": "set_edge_directed", "edges": [selector], "directed": True},
            )

    def test_an_empty_declaration_is_refused(self) -> None:
        with pytest.raises(ValueError, match="nothing to declare"):
            op_from_dict({"op": "declare_edge_inverses"})

    def test_retracting_an_undeclared_relation_is_refused(self) -> None:
        with pytest.raises(ValueError, match="no declared inverse"):
            _apply(_manifest(), {"op": "retract_edge_inverses", "relations": ["x"]})

    def test_retracting_a_natively_realized_pair_is_refused(self) -> None:
        manifest = _manifest(db_profile=_tigergraph("employed_by"))
        with pytest.raises(ValueError, match="still realized by native inverses"):
            _apply(manifest, {"op": "retract_edge_inverses", "relations": ["employs"]})

    def test_set_native_inverses_enables_and_withdraws(self) -> None:
        manifest = _manifest(db_profile=_tigergraph())
        enabled = _apply(
            manifest, {"op": "set_native_inverses", "relations": ["employed_by"]}
        )
        assert _native(enabled) == ["employed_by"]
        withdrawn = _apply(
            enabled,
            {
                "op": "set_native_inverses",
                "relations": ["employed_by"],
                "enabled": False,
            },
        )
        assert manifest_hash(withdrawn) == manifest_hash(manifest)

    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            ({"inverses": []}, "needs a declared pair"),
            ({"edges": [FORWARD, INVERSE]}, "keep one realization"),
            ({"db_profile": {"db_flavor": "arango"}}, "TigerGraph-only"),
        ],
        ids=["undeclared", "explicit-inverse", "not-tigergraph"],
    )
    def test_set_native_inverses_refusals_name_the_op(
        self, kwargs: dict[str, Any], match: str
    ) -> None:
        kwargs.setdefault("db_profile", _tigergraph())
        with pytest.raises(ValueError, match=f"set_native_inverses: .*{match}"):
            _apply(
                _manifest(**kwargs),
                {"op": "set_native_inverses", "relations": ["employed_by"]},
            )

    def test_set_native_inverses_refuses_an_unknown_relation(self) -> None:
        with pytest.raises(ValueError, match="unknown relations"):
            _apply(
                _manifest(db_profile=_tigergraph()),
                {"op": "set_native_inverses", "relations": ["employs"]},
            )

    def test_add_inverse_edges_refuses_a_symmetric_relation(self) -> None:
        with pytest.raises(ValueError, match="symmetric"):
            _apply(
                _manifest(edges=[FORWARD, KNOWS], symmetric=["knows"]),
                {"op": "add_inverse_edges", "relations": ["knows"]},
            )

    def test_add_inverse_edges_leaves_symmetric_relations_alone(self) -> None:
        out = _apply(
            _manifest(edges=[FORWARD, KNOWS], symmetric=["knows"]),
            {"op": "add_inverse_edges"},
        )
        assert out.graph_schema is not None
        relations = sorted(
            str(e.relation) for e in out.graph_schema.core_schema.edge_config.edges
        )
        assert relations == ["employed_by", "employs", "knows"]

    def test_add_inverse_edges_refuses_a_natively_realized_relation(self) -> None:
        with pytest.raises(ValueError, match="maintained natively"):
            _apply(
                _manifest(db_profile=_tigergraph("employed_by")),
                {"op": "add_inverse_edges", "relations": ["employed_by"]},
            )

    def test_slots_separate_declaration_from_realization(self) -> None:
        declare = op_slots(
            op_from_dict(
                {
                    "op": "declare_edge_inverses",
                    "inverses": {"employed_by": "employs"},
                    "symmetric": ["knows"],
                }
            )
        )
        native = op_slots(
            op_from_dict({"op": "set_native_inverses", "relations": ["employed_by"]})
        )
        assert len(declare) == 3
        assert all(slot[-1] == "inverse" for slot in declare)
        assert all(slot[-1] == "native_inverse" for slot in native)


class TestTheTableSurvivesOtherOps:
    def test_rename_relations_renames_pairs_symmetric_and_native(self) -> None:
        manifest = _manifest(
            edges=[FORWARD, KNOWS],
            symmetric=["knows"],
            db_profile=_tigergraph("employed_by"),
        )
        out = _apply(
            manifest,
            {
                "op": "rename_relations",
                "renames": {"employed_by": "works_at", "knows": "acquainted"},
            },
        )
        assert _pairs(out) == [("employs", "works_at")]
        assert _symmetric(out) == ["acquainted"]
        assert _native(out) == ["works_at"]

    def test_merge_edges_refuses_collapsing_a_pair_onto_itself(self) -> None:
        manifest = _manifest(edges=[FORWARD, {**FORWARD, "relation": "employs"}])
        with pytest.raises(ValueError, match="collapse onto one relation"):
            _apply(
                manifest,
                {"op": "merge_edges", "sources": ["employs"], "into": "employed_by"},
            )

    def test_merge_edges_refuses_giving_a_relation_two_inverses(self) -> None:
        manifest = _manifest(
            edges=[FORWARD, {**FORWARD, "relation": "works_for"}],
            inverses=[PAIR, {"relation": "works_for", "inverse": "hires"}],
        )
        with pytest.raises(ValueError, match="at most one inverse"):
            _apply(
                manifest,
                {"op": "merge_edges", "sources": ["works_for"], "into": "employed_by"},
            )

    def test_removing_every_edge_of_a_relation_prunes_its_declarations(self) -> None:
        manifest = _manifest(
            edges=[FORWARD, KNOWS],
            symmetric=["knows"],
            db_profile=_tigergraph("employed_by"),
        )
        out = _apply(
            manifest, {"op": "remove_edges", "relations": ["employed_by", "knows"]}
        )
        assert _pairs(out) == []
        assert _symmetric(out) == []
        assert _native(out) == []

    def test_a_native_inverse_survives_while_its_relation_has_an_edge(self) -> None:
        other = {**FORWARD, "target": "company"}
        manifest = _manifest(
            edges=[FORWARD, other], db_profile=_tigergraph("employed_by")
        )
        kept = _apply(manifest, {"op": "remove_edges", "edges": [FORWARD]})
        assert _native(kept) == ["employed_by"]
        gone = _apply(kept, {"op": "remove_vertices", "names": ["company"]})
        assert _native(gone) == []
        assert _pairs(gone) == []

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
            ({"db_profile": _tigergraph()}, {"db_profile": _tigergraph("employed_by")}),
            (
                {"inverses": [], "db_profile": _tigergraph()},
                {"db_profile": _tigergraph("employed_by")},
            ),
            (
                {"db_profile": _tigergraph("employed_by")},
                {"inverses": [], "db_profile": _tigergraph()},
            ),
            (
                {"edges": [FORWARD, {**KNOWS, "directed": True}]},
                {"edges": [FORWARD, KNOWS], "symmetric": ["knows"]},
            ),
            (
                {"edges": [FORWARD, KNOWS], "symmetric": ["knows"]},
                {"edges": [FORWARD, {**KNOWS, "directed": True}]},
            ),
            ({}, {"edges": [{**FORWARD, "directed": False}], "inverses": []}),
            (
                {"edges": [{**FORWARD, "directed": False}], "inverses": []},
                {},
            ),
        ],
        ids=[
            "declare",
            "retract",
            "enable-native",
            "declare-and-native",
            "undo-both",
            "undirect-then-declare-symmetric",
            "retract-symmetric-then-direct",
            "retract-pair-then-undirect",
            "direct-then-declare-pair",
        ],
    )
    def test_the_differ_replays_declarations_and_native_inverses(
        self, base: dict[str, Any], target: dict[str, Any]
    ) -> None:
        ops, warnings = diff_manifests_verified(_manifest(**base), _manifest(**target))
        assert ops
        assert warnings == []
        assert "set_db_profile" not in {op.op for op in ops}


class TestAdvisories:
    def test_a_consistent_realization_has_none(self) -> None:
        assert (
            _schema(
                edges=[FORWARD, INVERSE]
            ).core_schema.edge_config.inverse_advisories()
            == []
        )

    def test_an_unrealized_pair_has_none(self) -> None:
        assert _schema().core_schema.edge_config.inverse_advisories() == []

    def test_drifted_properties_and_identities_are_reported(self) -> None:
        drifted = {**INVERSE, "properties": ["since"], "identities": [["since"]]}
        findings = _schema(
            edges=[FORWARD, drifted]
        ).core_schema.edge_config.inverse_advisories()
        assert len(findings) == 2
        assert "different properties" in findings[0]
        assert "different identity keys" in findings[1]

    def test_a_partial_realization_is_reported(self) -> None:
        other = {**FORWARD, "target": "company"}
        findings = _schema(
            edges=[FORWARD, INVERSE, other]
        ).core_schema.edge_config.inverse_advisories()
        assert len(findings) == 1
        assert "'employed_by' has explicit inverse edges for some" in findings[0]

    def test_a_pair_read_from_the_same_side_is_reported(self) -> None:
        same_side = {**FORWARD, "relation": "employs"}
        findings = _schema(
            edges=[FORWARD, same_side]
        ).core_schema.edge_config.inverse_advisories()
        assert len(findings) == 1
        assert "same side" in findings[0]

    def test_ops_that_introduce_drift_log_it(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        manifest = _manifest(edges=[FORWARD, INVERSE])
        with caplog.at_level("WARNING"):
            _apply(
                manifest,
                {"op": "add_edge_properties", "additions": {"employs": ["since"]}},
            )
        assert "different properties" in caplog.text
