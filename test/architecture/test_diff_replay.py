"""The differ's replay invariant on the shapes a merge produces.

Each case is a change that used to make ``diff_manifests_verified`` report a
difference no op expressed -- so a merge commit over it was refused -- although
every one of them is expressible:

* two spellings of one pipeline step, which are not a change at all;
* indexes derived from secondary identities, which move with the identity;
* a registry transform arriving with the resource that uses it;
* transform steps appended to an existing pipeline;
* a joined vertex description;
* a class the merge renamed or folded, which the commit must record as the
  merge's own relabel rather than as an unrelated add and remove.
"""

from __future__ import annotations

from typing import Any

import pytest

from graflo.architecture.contract import GraphManifest
from graflo.architecture.contract.ingestion.steps import EdgeActorConfig
from graflo.architecture.evolution import apply_evolution
from graflo.architecture.evolution.autogenerate import diff_manifests_verified
from graflo.architecture.evolution.commit import build_root_commit
from graflo.architecture.evolution.hashing import manifest_hash
from graflo.architecture.evolution.history import History, checkout
from graflo.architecture.evolution.inverse import invert_op
from graflo.architecture.evolution.merge import merge_manifests
from graflo.architecture.evolution.merge3 import build_merge_recipe
from graflo.architecture.evolution.merge_commit import build_merge_commit
from graflo.architecture.evolution.ops import (
    MergeManifestsOp,
    RenameVerticesOp,
    SetEdgeDescriptionsOp,
    SetVertexDescriptionsOp,
)

PARTY: dict[str, Any] = {
    "name": "party",
    "properties": ["id", "name"],
    "identity": ["id"],
}
ORDER: dict[str, Any] = {"name": "order", "properties": ["oid"], "identity": ["oid"]}
PLACES: dict[str, Any] = {"source": "party", "target": "order", "relation": "places"}

TAGGER: dict[str, Any] = {
    "name": "tagger",
    "module": "graflo.util.transform",
    "foo": "tagged_key",
    "params": {"tag": "crm", "sep": ":"},
    "input": ["id"],
    "output": ["own_ref"],
}


def _manifest(
    vertices: list[dict[str, Any]],
    *,
    edges: list[dict[str, Any]] | None = None,
    resources: list[dict[str, Any]] | None = None,
    transforms: list[dict[str, Any]] | None = None,
) -> GraphManifest:
    payload: dict[str, Any] = {
        "schema": {
            "metadata": {"name": "replay", "version": "1.0.0"},
            "graph": {
                "vertex_config": {"vertices": vertices},
                "edge_config": {"edges": edges or []},
            },
        }
    }
    if resources is not None:
        payload["ingestion_model"] = {
            "resources": resources,
            "transforms": transforms or [],
        }
    manifest = GraphManifest.model_validate(payload)
    manifest.finish_init()
    return manifest


def _replays(base: GraphManifest, target: GraphManifest) -> list[Any]:
    ops, warnings = diff_manifests_verified(base, target)
    assert warnings == []
    replayed = apply_evolution(base, ops, bump_version=False, finish_init=False)
    assert manifest_hash(replayed) == manifest_hash(target)
    return ops


class TestStepSpelling:
    @pytest.mark.parametrize(
        ("authored", "respelled"),
        [
            ({"vertex": "party"}, {"vertex": "party", "type": "vertex"}),
            (
                {"from": "party", "to": "order", "relation": "places"},
                {"source": "party", "target": "order", "relation": "places"},
            ),
            (
                {"edge": {"from": "party", "to": "order", "relation": "places"}},
                {"from": "party", "to": "order", "relation": "places"},
            ),
        ],
        ids=["explicit-type", "from-to-vs-source-target", "edge-wrapper-vs-flat"],
    )
    def test_two_spellings_of_one_step_are_one_manifest(
        self, authored: dict[str, Any], respelled: dict[str, Any]
    ) -> None:
        def build(step: dict[str, Any]) -> GraphManifest:
            return _manifest(
                [PARTY, ORDER],
                edges=[PLACES],
                resources=[
                    {"name": "src", "pipeline": [{"vertex": "order"}, step]},
                ],
            )

        left, right = build(authored), build(respelled)

        assert manifest_hash(left) == manifest_hash(right)
        ops, warnings = diff_manifests_verified(left, right)
        assert (ops, warnings) == ([], [])


class TestDerivedIndexes:
    def test_removing_a_secondary_identity_does_not_emit_its_derived_index(
        self,
    ) -> None:
        keyed = {
            **PARTY,
            "secondary_identities": [{"name": "by_name", "fields": ["name"]}],
        }
        base, target = _manifest([keyed]), _manifest([PARTY])

        kinds = [op.op for op in _replays(base, target)]

        assert "remove_secondary_identities" in kinds
        assert "remove_vertex_indexes" not in kinds


class TestRegistryTransforms:
    def test_a_transform_arrives_with_the_resource_that_uses_it(self) -> None:
        base = _manifest([PARTY], resources=[])
        target = _manifest(
            [PARTY],
            resources=[
                {
                    "name": "crm",
                    "pipeline": [
                        {"transform": {"call": {"use": "tagger"}}},
                        {"vertex": "party"},
                    ],
                }
            ],
            transforms=[TAGGER],
        )

        [op] = _replays(base, target)

        assert op.op == "add_resources"
        assert [t.name for t in op.transforms] == ["tagger"]
        # remove_resources would leave the transform registered, and no op
        # withdraws one: this addition has no single-op inverse.
        assert invert_op(op, manifest=base) is None

    def test_appended_transform_steps_are_add_resource_transforms(self) -> None:
        pipeline = [{"vertex": "party"}]
        inline = {
            "transform": {
                "call": {
                    "module": "graflo.util.transform",
                    "foo": "tagged_key",
                    "params": {"tag": "crm", "sep": ":"},
                    "input": ["id"],
                    "output": ["own_ref"],
                }
            }
        }
        base = _manifest([PARTY], resources=[{"name": "crm", "pipeline": pipeline}])
        target = _manifest(
            [PARTY], resources=[{"name": "crm", "pipeline": [*pipeline, inline]}]
        )

        [op] = _replays(base, target)

        assert op.op == "add_resource_transforms"
        assert list(op.additions) == ["crm"]


class TestDescriptions:
    def test_a_changed_description_is_set_and_inverts(self) -> None:
        base = _manifest([PARTY])
        target = _manifest([{**PARTY, "description": "A trading party."}])

        [op] = _replays(base, target)

        assert isinstance(op, SetVertexDescriptionsOp)
        assert op.descriptions == {"party": "A trading party."}
        inverse = invert_op(op, manifest=base)
        assert isinstance(inverse, SetVertexDescriptionsOp)
        assert inverse.descriptions == {"party": None}

    def test_an_edge_given_a_description_is_set_and_inverts(self) -> None:
        """A reused relation that gains words: no longer a residual of the replay."""
        refers = {"source": "party", "target": "party", "relation": "refers"}
        base = _manifest([PARTY], edges=[refers])
        target = _manifest(
            [PARTY], edges=[{**refers, "description": "A party refers another."}]
        )

        [op] = _replays(base, target)

        assert isinstance(op, SetEdgeDescriptionsOp)
        assert [(e.edge_id(), e.description) for e in op.edges] == [
            (("party", "party", "refers"), "A party refers another.")
        ]
        inverse = invert_op(op, manifest=base)
        assert isinstance(inverse, SetEdgeDescriptionsOp)
        assert inverse.edges[0].description is None


class TestRenameReach:
    def test_a_vertex_rename_reaches_an_untyped_flat_edge_step(self) -> None:
        """``{from, to, relation}`` is an edge; a rename that skipped it stranded it."""
        manifest = _manifest(
            [PARTY, ORDER],
            edges=[PLACES],
            resources=[
                {
                    "name": "src",
                    "pipeline": [
                        {"vertex": "party"},
                        {"vertex": "order"},
                        {"from": "party", "to": "order", "relation": "places"},
                    ],
                }
            ],
        )

        renamed = apply_evolution(
            manifest,
            [RenameVerticesOp(renames={"order": "purchase"})],
            bump_version=False,
            finish_init=False,
        )

        assert renamed.ingestion_model is not None
        edge_step = renamed.ingestion_model.resources[0].pipeline[2]
        assert isinstance(edge_step, EdgeActorConfig)
        assert edge_step.target == "purchase"


class TestMergeCommitRelabel:
    @staticmethod
    def _sides() -> tuple[GraphManifest, GraphManifest, MergeManifestsOp]:
        left = _manifest(
            [{"name": "machine", "properties": ["id", "serial"], "identity": ["id"]}],
            resources=[{"name": "maintenance", "pipeline": [{"vertex": "machine"}]}],
        )
        right = _manifest(
            [{"name": "device", "properties": ["id", "sn"], "identity": ["id"]}],
            resources=[{"name": "sensors", "pipeline": [{"vertex": "device"}]}],
        )
        op = MergeManifestsOp.model_validate(
            {
                "vertex_equivalences": [
                    {
                        "left": "machine",
                        "right": "device",
                        "into": "equipment",
                        "properties": [
                            {
                                "left": "serial",
                                "right": "sn",
                                "into": "nameplate_serial",
                            }
                        ],
                    }
                ]
            }
        )
        return left, right, op

    def test_a_folding_merge_records_and_replays_from_its_first_parent(self) -> None:
        left, right, op = self._sides()
        merged = merge_manifests(left, right, op, bump_version=False)
        recipe = build_merge_recipe(left, right, op)
        roots = [
            build_root_commit(left, scope="l"),
            build_root_commit(right, scope="r"),
        ]

        entry = build_merge_commit(
            left,
            merged,
            parents=[roots[0].id, roots[1].id],
            recipe=recipe,
            right=right,
        )

        assert entry.ops[0].op == "canonicalize"
        replayed = checkout(left, History(commits=[*roots, entry]), entry.id)
        assert manifest_hash(replayed) == manifest_hash(merged)

    def test_without_the_right_side_the_fold_records_as_a_drop_and_an_add(
        self,
    ) -> None:
        """The relabel is resolved against both inputs; a plain diff cannot see it.

        It sees the class leave and another arrive, the way it reads any rename
        it is given no hint for.
        """
        left, right, op = self._sides()
        merged = merge_manifests(left, right, op, bump_version=False)

        entry = build_merge_commit(
            left,
            merged,
            parents=["a" * 12, "b" * 12],
            recipe=build_merge_recipe(left, right, op),
        )

        kinds = [o.op for o in entry.ops]
        assert "canonicalize" not in kinds
        assert {"add_vertices", "remove_vertices"} <= set(kinds)


def _schema(
    vertices: list[dict[str, Any]],
    *,
    edges: list[dict[str, Any]] | None = None,
    symmetric: list[str] | None = None,
    inverses: list[dict[str, str]] | None = None,
    profile: dict[str, Any] | None = None,
    resources: list[dict[str, Any]] | None = None,
) -> GraphManifest:
    schema: dict[str, Any] = {
        "metadata": {"name": "replay", "version": "1.0.0"},
        "graph": {
            "vertex_config": {"vertices": vertices},
            "edge_config": {
                "edges": edges or [],
                "symmetric": symmetric or [],
                "inverses": inverses or [],
            },
        },
    }
    if profile is not None:
        schema["db_profile"] = profile
    payload: dict[str, Any] = {"schema": schema}
    if resources is not None:
        payload["ingestion_model"] = {"resources": resources}
    return GraphManifest.model_validate(payload)


def _type(name: str, *fields: str, **extra: Any) -> dict[str, Any]:
    return {"name": name, "properties": ["id", *fields], "identity": ["id"], **extra}


EMAIL_KEYED = _type(
    "person", "email", secondary_identities=[{"name": "email_key", "fields": ["email"]}]
)
SELF_EMPLOYS = {"source": "person", "target": "person", "relation": "employs"}
WEIGHTED = {**SELF_EMPLOYS, "properties": ["weight"]}
KNOWS_PAIR = [
    {"source": "person", "target": "person", "relation": "knows"},
    {"source": "person", "target": "person", "relation": "known_by"},
]
MIRRORED = {
    "name": "feed",
    "pipeline": [
        {"vertex": "person"},
        {
            "edge": {
                "from": "person",
                "to": "person",
                "relation": "knows",
                "emit_inverse": True,
            }
        },
    ],
}
ROWS = {"name": "rows", "pipeline": [{"vertex": "person"}]}
INDEXED = {"edge_specs": [{**SELF_EMPLOYS, "indexes": [{"fields": ["weight"]}]}]}


class TestPhaseOrder:
    """Each op meets the manifest the ops before it produce."""

    @pytest.mark.parametrize(
        ("base", "target"),
        [
            pytest.param(
                _schema([EMAIL_KEYED]),
                _schema([_type("person")]),
                id="lookup-key-before-its-property",
            ),
            pytest.param(
                _schema(
                    [_type("person", "name")],
                    profile={"vertex_indexes": {"person": [{"fields": ["name"]}]}},
                ),
                _schema([_type("person")]),
                id="index-pruned-by-its-property",
            ),
            pytest.param(
                _schema(
                    [EMAIL_KEYED],
                    profile={"vertex_indexes": {"person": [{"fields": ["email"]}]}},
                ),
                _schema(
                    [_type("person", "email")],
                    profile={"vertex_indexes": {"person": [{"fields": ["email"]}]}},
                ),
                id="authored-index-shared-with-a-lookup-key",
            ),
            pytest.param(
                _schema(
                    [_type("person")],
                    edges=[{**SELF_EMPLOYS, "directed": False}],
                    symmetric=["employs"],
                ),
                _schema(
                    [_type("person"), _type("company")],
                    edges=[
                        {"source": "person", "target": "company", "relation": "employs"}
                    ],
                ),
                id="edge-removed-before-one-is-added",
            ),
            pytest.param(
                _schema(
                    [_type("person")],
                    edges=[
                        SELF_EMPLOYS,
                        {
                            "source": "person",
                            "target": "person",
                            "relation": "knows",
                            "subproperty_of": [{"edge": SELF_EMPLOYS}],
                        },
                    ],
                ),
                _schema(
                    [_type("person")],
                    edges=[
                        {
                            "source": "person",
                            "target": "person",
                            "relation": "knows",
                            "directed": False,
                        }
                    ],
                    symmetric=["knows"],
                ),
                id="parent-cleared-before-a-direction-change",
            ),
            pytest.param(
                _schema(
                    [_type("person"), _type("company")],
                    edges=[
                        {
                            "source": "person",
                            "target": "person",
                            "relation": "knows",
                            "properties": ["weight"],
                        },
                        {
                            "source": "company",
                            "target": "company",
                            "relation": "knows",
                            "properties": ["weight"],
                            "identities": [["source", "target", "weight"]],
                        },
                    ],
                ),
                _schema(
                    [_type("person")],
                    edges=[
                        {"source": "person", "target": "person", "relation": "knows"}
                    ],
                ),
                id="edge-removed-before-a-relation-wide-property-removal",
            ),
            pytest.param(
                _schema([_type("person")], edges=[WEIGHTED]),
                _schema([_type("person")], edges=[WEIGHTED], profile=INDEXED),
                id="first-edge-index-creates-its-spec",
            ),
            pytest.param(
                _schema([_type("person")], edges=[WEIGHTED], profile=INDEXED),
                _schema([_type("person")], edges=[WEIGHTED]),
                id="last-edge-index-drops-its-spec",
            ),
            pytest.param(
                _schema(
                    [_type("person"), _type("order")],
                    edges=[
                        {"source": "person", "target": "order", "relation": "ships"}
                    ],
                ),
                _schema(
                    [_type("person"), _type("order")],
                    edges=[
                        {
                            "source": "person",
                            "target": "order",
                            "relation": "ships",
                            "directed": False,
                        },
                        {
                            "source": "order",
                            "target": "person",
                            "relation": "ships",
                            "directed": False,
                        },
                    ],
                    symmetric=["ships"],
                ),
                id="direction-changed-before-an-edge-joins-the-relation",
            ),
            pytest.param(
                _schema(
                    [_type("person")],
                    edges=KNOWS_PAIR,
                    inverses=[{"relation": "knows", "inverse": "known_by"}],
                    resources=[ROWS, MIRRORED],
                ),
                _schema([_type("person")], resources=[ROWS]),
                id="dropped-resource-before-a-retraction-its-flag-holds-up",
            ),
            pytest.param(
                _schema(
                    [_type("person")],
                    edges=KNOWS_PAIR,
                    inverses=[{"relation": "knows", "inverse": "known_by"}],
                    resources=[MIRRORED],
                ),
                _schema(
                    [_type("person")],
                    edges=KNOWS_PAIR,
                    resources=[
                        {
                            "name": "feed",
                            "pipeline": [{"vertex": "person"}, {"vertex": "person"}],
                        }
                    ],
                ),
                id="edited-resource-flag-cleared-before-a-retraction",
            ),
        ],
    )
    def test_the_diff_replays(self, base: GraphManifest, target: GraphManifest) -> None:
        _replays(base, target)
