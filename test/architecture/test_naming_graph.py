"""One naming graph over the original class names: groups, merged names, findings.

A vocabulary (``canonical_maps``) and the merge op are resolved together, over
the classes each manifest actually declares. A group is everything linked by a
merge: a vocabulary sending several classes to one name, an equivalence, or a
name union. Per-member keys are keyed by the members' own names, so they
survive the relabel.
"""

from __future__ import annotations

import asyncio
from typing import Literal

import pytest

from graflo.architecture.contract.ingestion.steps.normalize import (
    normalize_actor_step,
)
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import (
    CanonicalMap,
    DerivationSpec,
    DerivedBranch,
    IdentityBranchDecl,
    LocalKeyBranch,
    LocalKeySource,
    MergeManifestsOp,
    PropertyEquivalence,
    VertexEquivalence,
    merge_manifests,
    resolve_clusters,
)
from graflo.architecture.evolution.alignment import AlignmentConflictError
from graflo.architecture.evolution.naming_graph import (
    MergeNamingError,
    NamingFinding,
    build_naming,
    naming_table,
    suggest_merge_op,
)
from graflo.architecture.evolution.preview import preview_merge
from graflo.hq.document_caster import DocumentCaster
from graflo.hq.ingestion_parameters import IngestionParams

# --------------------------------------------------------------------------- #
# A vocabulary merging several classes, and an equivalence naming only some.
# --------------------------------------------------------------------------- #


def _register_manifest(*, bench_identity: list[str] | None = None) -> GraphManifest:
    """One register routing rows to three classes by their ``kind`` column."""
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "maintenance", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "Press",
                                "properties": ["asset_id", "press_serial", "name"],
                                "identity": ["asset_id"],
                            },
                            {
                                "name": "Lathe",
                                "properties": ["asset_id", "lathe_serial", "name"],
                                "identity": ["asset_id"],
                            },
                            {
                                "name": "Bench",
                                "properties": ["asset_id", "name"],
                                "identity": bench_identity or ["asset_id"],
                            },
                        ]
                    },
                    "edge_config": {"edges": []},
                },
            },
            "ingestion_model": {
                "resources": [
                    {
                        "name": "register",
                        "pipeline": [
                            {
                                "vertex_router": {
                                    "type_field": "kind",
                                    "type_map": {
                                        "press": "Press",
                                        "lathe": "Lathe",
                                        "bench": "Bench",
                                    },
                                }
                            }
                        ],
                    }
                ],
                "transforms": [],
            },
        }
    )
    manifest.finish_init()
    return manifest


def _devices_manifest(*, gauges: bool = False) -> GraphManifest:
    vertices = [
        {
            "name": "Device",
            "properties": ["device_id", "serial"],
            "identity": ["device_id"],
        }
    ]
    resources = [{"name": "devices", "pipeline": [{"vertex": "Device"}]}]
    if gauges:
        vertices.append(
            {
                "name": "Gauge",
                "properties": ["gauge_id", "serial"],
                "identity": ["gauge_id"],
            }
        )
        resources.append({"name": "gauges", "pipeline": [{"vertex": "Gauge"}]})
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "sensors", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {"vertices": vertices},
                    "edge_config": {"edges": []},
                },
            },
            "ingestion_model": {"resources": resources, "transforms": []},
        }
    )
    manifest.finish_init()
    return manifest


_VOCABULARY = CanonicalMap(
    vertices={"Press": "Machine", "Lathe": "Machine", "Bench": "Machine"},
    allow_merges=True,
)

#: Class-specific triage: each member derives its match key from its own
#: column and keeps its own key under its own tag. Bench is named nowhere.
_TRIAGE: list[IdentityBranchDecl] = [
    DerivedBranch(
        name="match_key",
        sources={
            "register": {
                "Press": DerivationSpec(input=["press_serial"]),
                "Lathe": DerivationSpec(input=["lathe_serial"]),
            },
            "devices": DerivationSpec(input=["serial"]),
        },
    ),
    LocalKeyBranch(
        local_key={
            "register": {
                "Press": LocalKeySource(field="asset_id", tag="press"),
                "Lathe": LocalKeySource(field="asset_id", tag="lathe"),
            },
            "devices": LocalKeySource(field="device_id", tag="device"),
        }
    ),
]

_SERIALS = [
    PropertyEquivalence(
        left={"Press": "press_serial", "Lathe": "lathe_serial"},
        right="serial",
        into="serial",
    )
]


def _engineer_op(**equivalence: object) -> MergeManifestsOp:
    fields: dict[str, object] = {
        "left": ["Press", "Lathe"],
        "right": "Device",
        "identity": _TRIAGE,
        "properties": _SERIALS,
    }
    fields.update(equivalence)
    return MergeManifestsOp(
        vertex_equivalences=[VertexEquivalence.model_validate(fields)],
        canonical_maps={"left": _VOCABULARY},
    )


_REGISTER_ROWS = [
    {"kind": "press", "asset_id": "P1", "press_serial": "HP-1", "name": "Press"},
    {"kind": "lathe", "asset_id": "L1", "lathe_serial": "LT-7", "name": "Lathe"},
    # A bench row carrying a value a press serial could match. Bench is not in
    # the equivalence, so it is never matched against a device.
    {"kind": "bench", "asset_id": "B1", "press_serial": "HP-1", "name": "Bench"},
]
_DEVICE_ROWS = [
    {"device_id": "D1", "serial": "hp-1"},
    {"device_id": "D2", "serial": "lt-7"},
]


def _cast(
    manifest: GraphManifest, resource: str, rows: list[dict], vertex: str
) -> list[dict]:
    caster = DocumentCaster(manifest.require_ingestion_model())
    result = asyncio.run(caster.cast_batch(rows, resource, params=IngestionParams()))
    return list(result.graph.vertices.get(vertex, []))


def _vertex_set(manifest: GraphManifest) -> set[str]:
    schema = manifest.graph_schema
    assert schema is not None
    return set(schema.core_schema.vertex_config.vertex_set)


def _notes(findings: tuple[NamingFinding, ...], kind: str) -> list[NamingFinding]:
    return [f for f in findings if f.kind == kind and f.severity == "note"]


class TestVocabularyGroupWithPartialEquivalence:
    """Vocabulary ``Press, Lathe, Bench -> Machine``; equivalence ``[Press, Lathe] ~ Device``."""

    def test_the_group_closes_over_the_vocabulary(self) -> None:
        resolution = resolve_clusters(
            _engineer_op(), left=_register_manifest(), right=_devices_manifest()
        )

        (cluster,) = resolution.index.vertices
        assert cluster.into == "Machine"
        assert set(cluster.left) == {"Press", "Lathe", "Bench"}
        assert cluster.right == ("Device",)
        assert set(cluster.declared_left) == {"Press", "Lathe"}

    def test_the_union_has_one_class(self) -> None:
        union = merge_manifests(
            _register_manifest(), _devices_manifest(), _engineer_op()
        )

        assert _vertex_set(union) == {"Machine"}

    def test_each_member_derives_from_its_own_column(self) -> None:
        union = merge_manifests(
            _register_manifest(), _devices_manifest(), _engineer_op()
        )

        register = _cast(union, "register", _REGISTER_ROWS, "Machine")
        devices = _cast(union, "devices", _DEVICE_ROWS, "Machine")

        by_local = {doc["local_key"]: doc for doc in register}
        device_ids = {doc["device_id"]: doc["id"] for doc in devices}
        assert by_local["press:P1"]["match_key"] == "hp-1"
        assert by_local["press:P1"]["id"] == device_ids["D1"]
        assert by_local["lathe:L1"]["match_key"] == "lt-7"
        assert by_local["lathe:L1"]["id"] == device_ids["D2"]

    def test_a_vocabulary_only_member_keeps_its_own_key_and_never_matches(
        self,
    ) -> None:
        union = merge_manifests(
            _register_manifest(), _devices_manifest(), _engineer_op()
        )

        register = _cast(union, "register", _REGISTER_ROWS, "Machine")
        devices = _cast(union, "devices", _DEVICE_ROWS, "Machine")

        bench = next(doc for doc in register if doc["asset_id"] == "B1")
        assert bench["local_key"] == "left:Bench:B1"
        assert bench.get("match_key") is None
        assert bench["id"] not in {doc["id"] for doc in devices}

    def test_the_automatic_key_is_reported(self) -> None:
        resolution = resolve_clusters(
            _engineer_op(), left=_register_manifest(), right=_devices_manifest()
        )

        (note,) = _notes(resolution.findings, "auto_local_key")
        assert "left:Bench" in note.subjects

    @pytest.mark.parametrize(
        "equivalence",
        [
            pytest.param({}, id="into-omitted"),
            pytest.param({"into": "Machine"}, id="into-agrees"),
            pytest.param({"left": "Machine"}, id="canonical-spelling"),
            pytest.param({"left": "Press"}, id="one-member-named"),
        ],
    )
    def test_every_spelling_yields_the_same_records(
        self, equivalence: dict[str, object]
    ) -> None:
        reference = merge_manifests(
            _register_manifest(), _devices_manifest(), _engineer_op()
        )
        union = merge_manifests(
            _register_manifest(), _devices_manifest(), _engineer_op(**equivalence)
        )

        assert _vertex_set(union) == {"Machine"}
        assert _cast(union, "register", _REGISTER_ROWS, "Machine") == _cast(
            reference, "register", _REGISTER_ROWS, "Machine"
        )

    @pytest.mark.parametrize("into", ["Equipment", "Device"])
    def test_into_names_the_whole_group_and_says_it_overrides(self, into: str) -> None:
        op = _engineer_op(into=into)
        resolution = resolve_clusters(
            op, left=_register_manifest(), right=_devices_manifest()
        )
        union = merge_manifests(_register_manifest(), _devices_manifest(), op)

        assert _vertex_set(union) == {into}
        (note,) = _notes(resolution.findings, "vocabulary_override")
        assert "Machine" in note.message

    def test_triage_may_key_a_member_the_equivalence_does_not_list(self) -> None:
        triage = [
            DerivedBranch(
                name="match_key",
                sources={
                    "register": {
                        "Press": DerivationSpec(input=["press_serial"]),
                        "Lathe": DerivationSpec(input=["lathe_serial"]),
                        "Bench": DerivationSpec(input=["press_serial"]),
                    },
                    "devices": DerivationSpec(input=["serial"]),
                },
            ),
            _TRIAGE[1],
        ]
        union = merge_manifests(
            _register_manifest(), _devices_manifest(), _engineer_op(identity=triage)
        )

        register = _cast(union, "register", _REGISTER_ROWS, "Machine")
        bench = next(doc for doc in register if doc["asset_id"] == "B1")
        assert bench["match_key"] == "hp-1"

    def test_a_composite_key_cannot_be_keyed_automatically(self) -> None:
        with pytest.raises(MergeNamingError) as excinfo:
            merge_manifests(
                _register_manifest(bench_identity=["asset_id", "name"]),
                _devices_manifest(),
                _engineer_op(),
            )

        (finding,) = excinfo.value.findings
        assert finding.kind == "identity_coverage"
        assert "left:Bench" in finding.subjects
        assert finding.repairs and finding.repairs[0].kind == "add_key_source"

    def test_two_vocabulary_names_need_into(self) -> None:
        op = _engineer_op()
        op = op.model_copy(
            update={
                "canonical_maps": {
                    "left": _VOCABULARY,
                    "right": CanonicalMap(vertices={"Device": "Sensor"}),
                }
            }
        )
        with pytest.raises(MergeNamingError) as excinfo:
            resolve_clusters(op, left=_register_manifest(), right=_devices_manifest())

        (finding,) = excinfo.value.findings
        assert finding.kind == "unnamed_cluster"
        assert {r.kind for r in finding.repairs} == {"set_into"}
        assert {r.vertex_equivalences[0]["into"] for r in finding.repairs} == {
            "Machine",
            "Sensor",
        }

    def test_two_equivalences_in_one_group_share_one_identity(self) -> None:
        op = MergeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(
                    left="Press", right="Device", identity=_TRIAGE, properties=_SERIALS
                ),
                VertexEquivalence(left="Lathe", right="Gauge"),
            ],
            canonical_maps={"left": _VOCABULARY},
        )

        resolution = resolve_clusters(
            op, left=_register_manifest(), right=_devices_manifest(gauges=True)
        )

        (cluster,) = resolution.index.vertices
        assert set(cluster.right) == {"Device", "Gauge"}

    def test_two_identities_in_one_group_are_refused(self) -> None:
        op = MergeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(left="Press", right="Device", identity=_TRIAGE),
                VertexEquivalence(left="Lathe", right="Gauge", identity=["serial"]),
            ],
            canonical_maps={"left": _VOCABULARY},
        )

        with pytest.raises(MergeNamingError) as excinfo:
            resolve_clusters(
                op, left=_register_manifest(), right=_devices_manifest(gauges=True)
            )

        (finding,) = excinfo.value.findings
        assert finding.kind == "identity_disagreement"


# --------------------------------------------------------------------------- #
# Reusing names: any name from either side may be a name in the union.
# --------------------------------------------------------------------------- #


def _maintenance() -> GraphManifest:
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "maintenance", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "Asset",
                                "properties": ["asset_id", "serial_number"],
                                "identity": ["asset_id"],
                            },
                            {
                                "name": "WorkOrder",
                                "properties": ["work_order_id"],
                                "identity": ["work_order_id"],
                            },
                        ]
                    },
                    "edge_config": {
                        "edges": [
                            {
                                "source": "WorkOrder",
                                "target": "Asset",
                                "relation": "targets",
                            }
                        ]
                    },
                },
            },
        }
    )
    manifest.finish_init()
    return manifest


def _sensors() -> GraphManifest:
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "sensors", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "Device",
                                "properties": ["asset_id", "serial_number"],
                                "identity": ["asset_id"],
                            }
                        ]
                    },
                    "edge_config": {"edges": []},
                },
            },
        }
    )
    manifest.finish_init()
    return manifest


def _asset_device(**fields: object) -> VertexEquivalence:
    return VertexEquivalence.model_validate(
        {"left": "Asset", "right": "Device", **fields}
    )


class TestReusingNames:
    def test_into_reuses_a_name_the_vocabulary_vacates(self) -> None:
        op = MergeManifestsOp(
            vertex_equivalences=[_asset_device(into="WorkOrder")],
            canonical_maps={"left": CanonicalMap(vertices={"WorkOrder": "Ticket"})},
        )

        union = merge_manifests(_maintenance(), _sensors(), op)

        assert _vertex_set(union) == {"WorkOrder", "Ticket"}

    def test_renames_chain_simultaneously(self) -> None:
        op = MergeManifestsOp.model_validate(
            {
                "renames": {
                    "left": {"vertices": {"Asset": "WorkOrder", "WorkOrder": "Ticket"}}
                }
            }
        )

        union = merge_manifests(_maintenance(), _sensors(), op)

        assert _vertex_set(union) == {"WorkOrder", "Ticket", "Device"}

    def test_a_swap_through_the_vocabulary(self) -> None:
        op = MergeManifestsOp(
            vertex_equivalences=[_asset_device(into="WorkOrder")],
            canonical_maps={"left": CanonicalMap(vertices={"WorkOrder": "Asset"})},
        )

        union = merge_manifests(_maintenance(), _sensors(), op)

        assert _vertex_set(union) == {"WorkOrder", "Asset"}
        schema = union.graph_schema
        assert schema is not None
        (edge,) = schema.core_schema.edge_config.edges
        assert (edge.source, edge.target) == ("Asset", "WorkOrder")

    def test_into_is_never_translated_by_the_vocabulary(self) -> None:
        op = MergeManifestsOp(
            vertex_equivalences=[_asset_device(into="Asset")],
            canonical_maps={"left": CanonicalMap(vertices={"Asset": "Machine"})},
        )

        resolution = resolve_clusters(op, left=_maintenance(), right=_sensors())
        union = merge_manifests(_maintenance(), _sensors(), op)

        assert "Asset" in _vertex_set(union)
        assert "Machine" not in _vertex_set(union)
        assert _notes(resolution.findings, "vocabulary_override")

    def test_an_occupied_name_is_one_finding_with_the_whole_fiber(self) -> None:
        op = MergeManifestsOp(vertex_equivalences=[_asset_device(into="WorkOrder")])

        with pytest.raises(MergeNamingError) as excinfo:
            merge_manifests(_maintenance(), _sensors(), op)

        (finding,) = excinfo.value.findings
        assert finding.kind == "occupied_into"
        assert set(finding.subjects) >= {
            "left:Asset",
            "right:Device",
            "left:WorkOrder",
            "merged:WorkOrder",
        }
        assert finding.repairs[0].kind == "rename_away"
        assert finding.repairs[-1].kind == "extend_cluster"

    def test_a_rename_of_a_group_member_is_refused(self) -> None:
        op = MergeManifestsOp.model_validate(
            {
                "vertex_equivalences": [{"left": "Asset", "right": "Device"}],
                "renames": {"left": {"vertices": {"Asset": "Machine"}}},
            }
        )

        with pytest.raises(MergeNamingError) as excinfo:
            merge_manifests(_maintenance(), _sensors(), op)

        kinds = {f.kind for f in excinfo.value.findings}
        assert "double_home" in kinds

    def test_every_fault_is_reported_at_once(self) -> None:
        op = MergeManifestsOp.model_validate(
            {
                "vertex_equivalences": [
                    {"left": "Asset", "right": "Device", "into": "WorkOrder"},
                    {"left": "Assett", "right": "Device2", "into": "Spare"},
                ],
                "renames": {"left": {"vertices": {"Ghost": "Spectre"}}},
            }
        )

        with pytest.raises(MergeNamingError) as excinfo:
            merge_manifests(_maintenance(), _sensors(), op)

        kinds = sorted(f.kind for f in excinfo.value.findings if f.severity != "note")
        assert kinds == [
            "dangling",
            "occupied_into",
            "unknown_member",
            "unknown_member",
        ]
        for finding in excinfo.value.findings:
            assert finding.message in str(excinfo.value)


# --------------------------------------------------------------------------- #
# Showing the graph, and suggesting the declarations that settle it.
# --------------------------------------------------------------------------- #


def _customers(*names: str) -> GraphManifest:
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "-".join(names), "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {"name": n, "properties": ["id"], "identity": ["id"]}
                            for n in names
                        ]
                    },
                    "edge_config": {"edges": []},
                },
            }
        }
    )
    manifest.finish_init()
    return manifest


class TestTheNamingTable:
    def test_one_row_per_union_name_and_provenance(self) -> None:
        result = build_naming(
            _engineer_op(), left=_register_manifest(), right=_devices_manifest()
        )

        rows = result.graph.rows("vertex")

        assert rows == [
            ("Machine", ["Lathe", "Press"], ["Device"], "equivalence"),
            ("Machine", ["Bench"], [], "vocabulary"),
        ]

    def test_the_table_is_text_with_a_header(self) -> None:
        result = build_naming(
            _engineer_op(), left=_register_manifest(), right=_devices_manifest()
        )

        lines = naming_table(result.graph)

        assert lines[0].split() == ["merged", "left", "right", "via"]
        assert "Bench" in lines[2] and lines[2].rstrip().endswith("vocabulary")


class TestSuggestions:
    def test_a_scaffold_declares_every_shared_name_and_guesses_none(self) -> None:
        left = _customers("Customer", "OrderLine")
        right = _customers("Customer", "order_line")

        op = suggest_merge_op(left, right)

        assert [(e.left, e.right, e.into) for e in op.vertex_equivalences] == [
            ("Customer", "Customer", "Customer")
        ]

    def test_an_occupied_name_is_settled_by_renaming_it_away(self) -> None:
        op = MergeManifestsOp(vertex_equivalences=[_asset_device(into="WorkOrder")])

        suggested = suggest_merge_op(_maintenance(), _sensors(), op)
        union = merge_manifests(_maintenance(), _sensors(), suggested)

        assert suggested.renames.left.vertices == {"WorkOrder": "WorkOrder_left"}
        assert _vertex_set(union) == {"WorkOrder", "WorkOrder_left"}

    def test_an_automatic_key_is_written_out(self) -> None:
        suggested = suggest_merge_op(
            _register_manifest(), _devices_manifest(), _engineer_op()
        )

        (equivalence,) = suggested.vertex_equivalences
        local = equivalence.local_key_branch()
        assert local is not None
        register = local.local_key["register"]
        assert isinstance(register, dict)
        assert register["Bench"] == LocalKeySource(field="asset_id", tag="left:Bench")
        resolution = resolve_clusters(
            suggested, left=_register_manifest(), right=_devices_manifest()
        )
        assert not _notes(resolution.findings, "auto_local_key")


class TestThePreview:
    def test_every_problem_merge_lists_is_a_refusal(self) -> None:
        op = MergeManifestsOp.model_validate(
            {
                "vertex_equivalences": [
                    {"left": "Asset", "right": "Device", "into": "WorkOrder"},
                    {"left": "Assett", "right": "Device2", "into": "Spare"},
                ],
                "renames": {"left": {"vertices": {"Ghost": "Spectre"}}},
            }
        )

        preview = preview_merge(_maintenance(), _sensors(), op)

        refusals = sorted(f.kind for f in preview.findings if f.severity == "refusal")
        assert refusals == [
            "dangling",
            "occupied_into",
            "unknown_member",
            "unknown_member",
        ]

    def test_a_vocabulary_joined_member_is_drawn_as_a_map_edge(self) -> None:
        preview = preview_merge(
            _register_manifest(), _devices_manifest(), _engineer_op(), attempt=False
        )

        declared = preview.edges_between("left:Press", "merged:Machine")
        joined = preview.edges_between("left:Bench", "merged:Machine")
        assert [e.kind for e in declared] == ["member"]
        assert [(e.kind, e.label) for e in joined] == [("map", "vocabulary")]

    def test_a_demoted_key_lists_only_the_branches_its_member_can_reach(self) -> None:
        """Bench joins by the vocabulary alone: no derivation keys it, so its
        records complete only their own tagged local key."""
        preview = preview_merge(
            _register_manifest(), _devices_manifest(), _engineer_op()
        )

        notes = {
            f.message.split(" key on ")[0]: f.message
            for f in preview.findings
            if f.kind == "lookup_demotion"
        }
        assert "records of left:Bench" not in notes
        assert "['match_key', 'local_key']" in notes["records of left:Press"]
        assert "['match_key', 'local_key']" in notes["records of right:Device"]


class TestAVocabularyAcknowledgesItsOwnMerges:
    """A merge only the vocabulary makes has no equivalence to carry `allow`."""

    @staticmethod
    def _parts() -> GraphManifest:
        manifest = GraphManifest.from_config(
            {
                "schema": {
                    "metadata": {"name": "parts", "version": "1.0.0"},
                    "graph": {
                        "vertex_config": {
                            "vertices": [
                                {"name": n, "properties": ["id"], "identity": ["id"]}
                                for n in ("Engine", "Pump")
                            ]
                        },
                        "edge_config": {
                            "edges": [
                                {
                                    "source": "Engine",
                                    "target": "Pump",
                                    "relation": "drives",
                                }
                            ]
                        },
                    },
                }
            }
        )
        manifest.finish_init()
        return manifest

    def test_a_self_relation_needs_the_map_to_accept_it(self) -> None:
        def op(**flags: bool) -> MergeManifestsOp:
            return MergeManifestsOp(
                canonical_maps={
                    "left": CanonicalMap(
                        vertices={"Engine": "Part", "Pump": "Part"},
                        allow_merges=True,
                        **flags,
                    )
                }
            )

        with pytest.raises(ValueError, match="self-relation"):
            merge_manifests(self._parts(), _customers("Customer"), op())
        union = merge_manifests(
            self._parts(), _customers("Customer"), op(allow_self_relations=True)
        )
        assert _vertex_set(union) == {"Part", "Customer"}


# --------------------------------------------------------------------------- #
# An acknowledgement covers its own group, never another.
# --------------------------------------------------------------------------- #


_Allow = Literal["self_relations", "observation_fusion"]


def _two_pairs() -> GraphManifest:
    """Two pairs of types, each pair joined by an edge."""
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "pairs", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {"name": n, "properties": ["id"], "identity": ["id"]}
                            for n in ("Engine", "Pump", "Valve", "Pipe")
                        ]
                    },
                    "edge_config": {
                        "edges": [
                            {
                                "source": "Engine",
                                "target": "Pump",
                                "relation": "drives",
                            },
                            {"source": "Valve", "target": "Pipe", "relation": "seals"},
                        ]
                    },
                },
            }
        }
    )
    manifest.finish_init()
    return manifest


class TestAnAcknowledgementIsPerGroup:
    @staticmethod
    def _op(
        first: list[_Allow], second: list[_Allow], **map_flags: bool
    ) -> MergeManifestsOp:
        return MergeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(
                    left=["Engine", "Pump"],
                    right="Motor",
                    into="Motor",
                    allow=first,
                ),
                VertexEquivalence(
                    left=["Valve", "Pipe"],
                    right="Fitting",
                    into="Fitting",
                    allow=second,
                ),
            ],
            canonical_maps={"left": CanonicalMap(**map_flags)} if map_flags else {},
        )

    def test_one_groups_allow_does_not_license_the_others(self) -> None:
        with pytest.raises(MergeNamingError) as excinfo:
            merge_manifests(
                _two_pairs(),
                _customers("Motor", "Fitting"),
                self._op(["self_relations"], []),
            )

        (finding,) = excinfo.value.findings
        assert finding.kind == "self_relation"
        assert "merged:Fitting" in finding.subjects
        (repair,) = finding.repairs
        assert repair.kind == "acknowledge"
        assert repair.replaces == 1
        assert repair.vertex_equivalences[0]["allow"] == ["self_relations"]

    def test_each_group_accepting_its_own_merges(self) -> None:
        union = merge_manifests(
            _two_pairs(),
            _customers("Motor", "Fitting"),
            self._op(["self_relations"], ["self_relations"]),
        )

        assert _vertex_set(union) == {"Motor", "Fitting"}

    def test_a_map_flag_does_not_license_an_equivalences_merge(self) -> None:
        with pytest.raises(MergeNamingError, match="self-relation") as excinfo:
            merge_manifests(
                _two_pairs(),
                _customers("Motor", "Fitting"),
                self._op([], [], allow_self_relations=True),
            )

        assert {f.kind for f in excinfo.value.findings} == {"self_relation"}
        assert len(excinfo.value.findings) == 2

    def test_the_preview_reports_it_without_running_the_union(self) -> None:
        preview = preview_merge(
            _two_pairs(),
            _customers("Motor", "Fitting"),
            self._op(["self_relations"], []),
            attempt=False,
        )

        assert [f.kind for f in preview.blocking] == ["self_relation"]


# --------------------------------------------------------------------------- #
# An edge resource whose role routers reach a member the vocabulary joins.
# --------------------------------------------------------------------------- #


def _links_manifest() -> GraphManifest:
    """Presses and benches, plus a link table routing both ends by type.

    The link resource's two open routers reach every class of the side, so
    the pipeline says it produces ``Bench``; but one level has one transform
    buffer, which cannot hold a different derived value per role.
    """
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "plant", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": name,
                                "properties": ["asset_id", "name"],
                                "identity": ["asset_id"],
                            }
                            for name in ("Press", "Bench")
                        ]
                    },
                    "edge_config": {
                        "edges": [
                            {"source": "Press", "target": "Bench", "relation": "feeds"}
                        ]
                    },
                },
            },
            "ingestion_model": {
                "resources": [
                    {"name": "presses", "pipeline": [{"vertex": "Press"}]},
                    {"name": "benches", "pipeline": [{"vertex": "Bench"}]},
                    {
                        "name": "links",
                        "pipeline": [
                            {
                                "vertex_router": {
                                    "role": "source",
                                    "type_field": "source_type",
                                    "from": {"asset_id": "source_id"},
                                }
                            },
                            {
                                "vertex_router": {
                                    "role": "target",
                                    "type_field": "target_type",
                                    "from": {"asset_id": "target_id"},
                                }
                            },
                            {
                                "edge": {
                                    "source_role": "source",
                                    "target_role": "target",
                                    "relation_field": "relation",
                                }
                            },
                        ],
                    },
                ],
                "transforms": [],
            },
        }
    )
    manifest.finish_init()
    return manifest


_LINKS_VOCABULARY = CanonicalMap(
    vertices={"Press": "Machine", "Bench": "Machine"}, allow_merges=True
)


def _links_op(
    identity: list[object] | None = None,
    **local_key: LocalKeySource | dict[str, LocalKeySource],
) -> MergeManifestsOp:
    """``Press, Bench -> Machine``; ``[Press] ~ Device`` keyed on own keys."""
    if identity is None:
        identity = [
            LocalKeyBranch(
                local_key={
                    "presses": LocalKeySource(field="asset_id", tag="press"),
                    "devices": LocalKeySource(field="device_id", tag="device"),
                    **local_key,
                }
            )
        ]
    return MergeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence.model_validate(
                {
                    "left": ["Press"],
                    "right": "Device",
                    "into": "Machine",
                    "allow": ["self_relations"],
                    "identity": identity,
                }
            )
        ],
        canonical_maps={"left": _LINKS_VOCABULARY},
    )


def _links_steps(union: GraphManifest) -> list[dict]:
    resource = next(
        r for r in union.require_ingestion_model().resources if r.name == "links"
    )
    return [normalize_actor_step(dict(step)) for step in resource.pipeline]


class TestAnEdgeResourceReferencesAJoinedMember:
    def test_the_edge_resource_gets_no_automatic_key(self) -> None:
        resolution = resolve_clusters(
            _links_op(), left=_links_manifest(), right=_devices_manifest()
        )

        (cluster,) = resolution.index.vertices
        (branch,) = cluster.declaration.identity or []
        assert isinstance(branch, LocalKeyBranch)
        assert set(branch.local_key) == {"presses", "devices", "benches"}
        (note,) = _notes(resolution.findings, "reference_only")
        assert "left:Bench" in note.subjects
        assert "links" in note.message

    def test_the_union_looks_the_merged_class_up_from_both_roles(self) -> None:
        union = merge_manifests(_links_manifest(), _devices_manifest(), _links_op())

        source, target, edge = _links_steps(union)
        assert source["lookup_only"] == ["Machine"]
        assert target["lookup_only"] == ["Machine"]
        assert edge["source_match"] == {"Machine": "by_asset_id"}
        assert edge["target_match"] == {"Machine": "by_asset_id"}
        presses = _cast(union, "presses", [{"asset_id": "P1"}], "Machine")
        assert [p["local_key"] for p in presses] == ["press:P1"]

    def test_keying_the_edge_resource_by_member_is_refused(self) -> None:
        op = _links_op(links={"Bench": LocalKeySource(field="source_id", tag="link")})

        with pytest.raises(AlignmentConflictError, match="shared by every router"):
            merge_manifests(_links_manifest(), _devices_manifest(), op)

    def test_a_property_key_repair_does_not_name_the_edge_resource(self) -> None:
        result = build_naming(
            _links_op(identity=["device_id"]),
            left=_links_manifest(),
            right=_devices_manifest(),
        )

        finding = next(
            f
            for f in result.blocking
            if f.kind == "identity_coverage" and "left:Bench" in f.subjects
        )
        (repair,) = finding.repairs
        assert repair.vertex_equivalences == (
            {
                "local_key": {
                    "benches": {"Bench": {"field": "asset_id", "tag": "left:Bench"}}
                }
            },
        )


# --------------------------------------------------------------------------- #
# A property-only key and a member the vocabulary joins.
# --------------------------------------------------------------------------- #


class TestAPropertyKeyAndAVocabularyJoinedMember:
    @staticmethod
    def _op(identity: list[object]) -> MergeManifestsOp:
        return MergeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence.model_validate(
                    {
                        "left": ["Press", "Lathe"],
                        "right": "Device",
                        "properties": _SERIALS,
                        "identity": identity,
                    }
                )
            ],
            canonical_maps={"left": _VOCABULARY},
        )

    def test_a_member_that_cannot_fill_the_key_is_refused_with_its_own_key(
        self,
    ) -> None:
        result = build_naming(
            self._op(["serial"]), left=_register_manifest(), right=_devices_manifest()
        )

        (finding,) = result.blocking
        assert finding.kind == "identity_coverage"
        assert "left:Bench" in finding.subjects
        (repair,) = finding.repairs
        assert repair.kind == "add_key_source"
        assert repair.vertex_equivalences == (
            {
                "local_key": {
                    "register": {"Bench": {"field": "asset_id", "tag": "left:Bench"}}
                }
            },
        )

    def test_appending_the_repair_makes_the_key_a_funnel_that_keeps_it(
        self,
    ) -> None:
        local_key = {
            "local_key": {
                "register": {"Bench": {"field": "asset_id", "tag": "left:Bench"}}
            }
        }

        union = merge_manifests(
            _register_manifest(), _devices_manifest(), self._op(["serial", local_key])
        )

        benches = _cast(union, "register", _REGISTER_ROWS[2:], "Machine")
        assert [b["local_key"] for b in benches] == ["left:Bench:B1"]


# --------------------------------------------------------------------------- #
# The group's `into`, wherever it is written.
# --------------------------------------------------------------------------- #


def test_into_on_a_later_equivalence_of_the_group_is_the_declared_into() -> None:
    op = MergeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(left="Press", right="Device"),
            VertexEquivalence(left="Lathe", right="Gauge", into="Equipment"),
        ],
        canonical_maps={"left": _VOCABULARY},
    )

    result = build_naming(
        op, left=_register_manifest(), right=_devices_manifest(gauges=True)
    )

    (cluster,) = result.resolution.index.vertices
    assert cluster.into == "Equipment"
    assert cluster.declared_into == "Equipment"


# --------------------------------------------------------------------------- #
# Two classes of one side on one name.
# --------------------------------------------------------------------------- #


class TestTwoClassesOfOneSideOnOneName:
    @staticmethod
    def _op() -> MergeManifestsOp:
        return MergeManifestsOp(
            canonical_maps={"left": CanonicalMap(vertices={"Asset": "WorkOrder"})}
        )

    def test_the_class_the_vocabulary_moved_can_keep_its_own_name(self) -> None:
        result = build_naming(self._op(), left=_maintenance(), right=_sensors())

        (finding,) = result.blocking
        assert finding.kind == "occupied_into"
        keep, away = finding.repairs
        assert keep.renames == {"left": {"vertices": {"Asset": "Asset"}}}
        assert away.renames == {"left": {"vertices": {"WorkOrder": "WorkOrder_left"}}}

    def test_the_suggestion_keeps_it(self) -> None:
        suggested = suggest_merge_op(_maintenance(), _sensors(), self._op())

        assert suggested.renames.left.vertices == {"Asset": "Asset"}
        union = merge_manifests(_maintenance(), _sensors(), suggested)
        assert _vertex_set(union) == {"Asset", "WorkOrder", "Device"}


def test_merged_types_come_in_the_order_their_equivalences_are_declared() -> None:
    """Not alphabetically: Valve and Pipe are declared first, so Fitting leads."""
    op = MergeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(
                left=["Valve", "Pipe"],
                right="Fitting",
                into="Fitting",
                allow=["self_relations"],
            ),
            VertexEquivalence(
                left=["Engine", "Pump"],
                right="Motor",
                into="Motor",
                allow=["self_relations"],
            ),
        ]
    )

    union = merge_manifests(_two_pairs(), _customers("Motor", "Fitting"), op)

    schema = union.graph_schema
    assert schema is not None
    assert [v.name for v in schema.core_schema.vertex_config.vertices] == [
        "Fitting",
        "Motor",
    ]


def test_a_suggested_declaration_is_drawn_with_a_short_caption() -> None:
    preview = preview_merge(
        _maintenance(), _customers("WorkOrder"), MergeManifestsOp(), attempt=False
    )

    labels = {e.label for e in preview.edges if e.kind == "suggested"}
    assert labels == {"declare"}
