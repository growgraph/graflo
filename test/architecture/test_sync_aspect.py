"""The sync aspect and the field flag it rests on: ``set_by: sync``."""

from __future__ import annotations

import copy

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import apply_evolution, invert_ops
from graflo.architecture.evolution.aspects import (
    ASPECTS,
    DEFAULT_ASPECTS,
    LiftError,
    plan_lift,
)
from graflo.architecture.evolution.aspects.sync import SYNC_HASH, sync_field
from graflo.architecture.graph_types import GraphContainer
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.vertex import Field, FieldMergeConflict, merge_fields
from graflo.hq.db_writer import apply_stamp, check_stamp
from graflo.rdf import ManifestRdfDeserializer, ManifestRdfSerializer
from test.architecture.test_sync_ownership import MANIFEST

BASE_URI = "https://example.org/sync/"


def _manifest() -> GraphManifest:
    manifest = GraphManifest.from_config(copy.deepcopy(MANIFEST))
    manifest.finish_init()
    return manifest


def _lifted() -> GraphManifest:
    manifest = _manifest()
    lifted = apply_evolution(manifest, plan_lift(manifest, aspects=["sync"]))
    lifted.finish_init()
    return lifted


def _field(manifest: GraphManifest, vertex: str, name: str) -> Field:
    vc = manifest.require_schema().core_schema.vertex_config
    return next(f for f in vc.properties(vertex) if f.name == name)


def test_sync_is_registered_and_not_lifted_by_default() -> None:
    assert not ASPECTS["sync"].needs_spec
    assert "sync" not in DEFAULT_ASPECTS


def test_the_lift_gives_every_owned_type_a_hash_set_by_sync() -> None:
    lifted = _lifted()

    for owned in ("Work", "Author"):
        assert _field(lifted, owned, SYNC_HASH) == sync_field()
    vc = lifted.require_schema().core_schema.vertex_config
    assert SYNC_HASH not in {f.name for f in vc.properties("Place")}


def test_the_lift_inverts_and_a_second_lift_adds_nothing() -> None:
    manifest = _manifest()
    ops = plan_lift(manifest, aspects=["sync"])
    lifted = apply_evolution(manifest, ops)
    lifted.finish_init()

    assert plan_lift(lifted, aspects=["sync"]) == []
    inverses, reasons = invert_ops(ops, manifest=manifest)
    assert reasons == [], reasons
    restored = apply_evolution(lifted, inverses)

    def core(m: GraphManifest) -> dict:
        return m.require_schema().core_schema.to_minimal_canonical_dict()

    assert core(restored) == core(manifest)


def test_a_manifest_that_owns_nothing_cannot_be_lifted() -> None:
    config = copy.deepcopy(MANIFEST)
    for resource in config["ingestion_model"]["resources"]:
        del resource["sync"]
    manifest = GraphManifest.from_config(config)
    manifest.finish_init()

    with pytest.raises(LiftError, match="no resource declares one"):
        plan_lift(manifest, aspects=["sync"])


@pytest.mark.parametrize(
    "kwargs",
    [{"set_by": "sync"}, {"set_by": "sync", "reserved": True, "type": "LIST",
                          "item_type": "INT", "merge": "union"}],
)  # fmt: skip
def test_set_by_sync_needs_a_reserved_replace_field(kwargs) -> None:
    with pytest.raises(ValueError, match="set_by 'sync' needs a reserved field"):
        Field(name="h", **kwargs)


def test_set_by_sync_is_a_vertex_field() -> None:
    with pytest.raises(ValueError, match="set_by 'sync' is a vertex field"):
        Edge(source="A", target="B", properties=[sync_field()])


def test_the_default_is_left_out_of_a_dump_and_merges_refuse_a_change() -> None:
    assert "set_by" not in Field(name="z", reserved=True).to_dict()
    assert sync_field().to_dict()["set_by"] == "sync"
    with pytest.raises(FieldMergeConflict, match="set_by"):
        merge_fields(
            sync_field(),
            Field(name=SYNC_HASH, type="STRING", reserved=True),
            owner="vertex 'Work'",
        )


def test_no_stamp_sets_the_hash_and_an_upsert_writes_null() -> None:
    schema = _lifted().require_schema()
    gc = GraphContainer(vertices={"Work": [{"wid": "w1", SYNC_HASH: "stale"}]})

    check_stamp(gc, schema, None)
    apply_stamp(gc, schema, None)

    assert gc.vertices["Work"] == [{"wid": "w1", SYNC_HASH: None}]


def test_the_flag_round_trips_through_rdf() -> None:
    lifted = _lifted()

    turtle = ManifestRdfSerializer(include_ontology=False).to_turtle(lifted, BASE_URI)
    restored = ManifestRdfDeserializer().from_turtle(turtle, BASE_URI.rstrip("/"))
    restored.finish_init()

    assert _field(restored, "Work", SYNC_HASH).set_by == "sync"
