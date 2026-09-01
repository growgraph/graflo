"""The canonical form and its audit table.

The audit table in ``evolution/canonicalize.py`` is the one decision in the
version-control work that has to be right the first time: it decides which
manifests are the *same* world model, and every content hash and commit id is
downstream of it. These tests pin it three ways --

1. **Exhaustiveness.** Every sequence field reachable from ``GraphManifest`` is
   classified, and nothing classified is stale. A new list field cannot ship
   without a decision.
2. **Effect.** Reordering a ``SORTED`` field does not change the hash;
   reordering a ``PRESERVED`` one does.
3. **Stability.** Canonicalization is idempotent, order-independent, and does
   not disturb the manifest it reads.
"""

from __future__ import annotations

import copy
import glob
import json
import pathlib
import typing

import pytest

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution.canonicalize import (
    CANON_VERSION,
    CLASSIFIED_MAPPINGS,
    LIST_ORDER,
    ListOrder,
    UnclassifiedListField,
    canonical_payload,
)
from graflo.architecture.evolution.hashing import manifest_hash, schema_hash

EXAMPLES = sorted(
    glob.glob(
        str(pathlib.Path(__file__).parents[2] / "examples" / "*" / "manifest.yaml")
    )
)


# ── the model walk the exhaustiveness tests are built on ────────────────────


def _unwrap(annotation: typing.Any) -> typing.Iterator[typing.Any]:
    """Yield the concrete types inside an annotation, unions and all."""
    if typing.get_origin(annotation) is None:
        yield annotation
        return
    for argument in typing.get_args(annotation):
        yield from _unwrap(argument)


def _is_sequence(annotation: typing.Any) -> bool:
    """True for list- or tuple-typed fields, including inside a union.

    Tuples count: ``ProtoTransform.input`` is ``tuple[str, ...]`` and renders as
    a JSON array like any list, so it needs a classification exactly as much.
    """
    if typing.get_origin(annotation) in (list, tuple):
        return True
    return any(
        typing.get_origin(argument) in (list, tuple)
        for argument in typing.get_args(annotation)
    )


def _walk_contract() -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
    """Sequence fields and typed-mapping fields reachable from GraphManifest."""
    seen: set[type] = set()
    sequences: set[tuple[str, str]] = set()
    typed_mappings: set[tuple[str, str]] = set()

    def visit(model: typing.Any) -> None:
        if not (isinstance(model, type) and issubclass(model, ConfigBaseModel)):
            return
        if model in seen:
            return
        seen.add(model)
        for name, info in model.model_fields.items():
            annotation = info.annotation
            if _is_sequence(annotation):
                sequences.add((model.__name__, name))
            if typing.get_origin(annotation) is dict:
                arguments = typing.get_args(annotation)
                value_type = arguments[1] if len(arguments) == 2 else None
                if value_type is not None and any(
                    isinstance(t, type) and issubclass(t, ConfigBaseModel)
                    for t in _unwrap(value_type)
                ):
                    typed_mappings.add((model.__name__, name))
            for concrete in _unwrap(annotation):
                visit(concrete)

    visit(GraphManifest)
    return sequences, typed_mappings


# ── 1. exhaustiveness ───────────────────────────────────────────────────────


def test_every_sequence_field_is_classified() -> None:
    sequences, _ = _walk_contract()
    unclassified = sorted(sequences - set(LIST_ORDER))
    assert not unclassified, (
        "these sequence fields have no entry in LIST_ORDER: "
        f"{unclassified}. Classify each as SORTED (order carries no meaning -- "
        "state why in a comment) or PRESERVED (order is meaning, or is not "
        "settled), and bump CANON_VERSION if an existing entry moved."
    )


def test_no_stale_entries_in_the_audit_table() -> None:
    sequences, _ = _walk_contract()
    stale = sorted(set(LIST_ORDER) - sequences)
    assert not stale, (
        f"LIST_ORDER classifies fields that no longer exist: {stale}. "
        "Remove them so the table stays a description of the contract."
    )


def test_every_typed_mapping_is_accounted_for() -> None:
    """A mapping whose values are models needs a deliberate decision.

    Classification stops at a mapping by default, because nearly every dict in
    the contract is free-form user payload. A *typed* one is the exception, and
    a newly added one silently keeping its inner lists unsorted is a missed
    dedup that nothing else would surface.
    """
    _, typed_mappings = _walk_contract()
    undeclared = sorted(typed_mappings - CLASSIFIED_MAPPINGS)
    assert not undeclared, (
        f"typed mapping fields not listed in CLASSIFIED_MAPPINGS: {undeclared}. "
        "Add each one if its values are contract structure whose lists should "
        "be canonicalized, or leave it out deliberately and note why."
    )


def test_an_unclassified_field_raises_rather_than_guessing() -> None:
    manifest = GraphManifest.from_yaml(EXAMPLES[0])
    saved = LIST_ORDER.pop(("VertexConfig", "vertices"))
    try:
        with pytest.raises(UnclassifiedListField) as excinfo:
            canonical_payload(manifest)
        assert excinfo.value.model_name == "VertexConfig"
        assert excinfo.value.field_name == "vertices"
    finally:
        LIST_ORDER[("VertexConfig", "vertices")] = saved


# ── 2. effect ───────────────────────────────────────────────────────────────


def _minimal_manifest() -> GraphManifest:
    return GraphManifest.from_dict(
        {
            "schema": {
                "metadata": {"name": "audit", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "a",
                                "properties": [{"name": "x"}, {"name": "y"}],
                                "identity": ["x", "y"],
                            },
                            {
                                "name": "b",
                                "properties": [{"name": "k"}],
                                "identity": ["k"],
                            },
                        ]
                    },
                    "edge_config": {"edges": [{"source": "a", "target": "b"}]},
                },
            }
        }
    )


def _schema(manifest: GraphManifest):
    """The schema block, with the Optional narrowed for the type checker."""
    schema = manifest.graph_schema
    assert schema is not None
    return schema


def _core(manifest: GraphManifest):
    """The core schema, with the Optional narrowed for the type checker."""
    schema = manifest.graph_schema
    assert schema is not None
    return schema.core_schema


def _metadata(manifest: GraphManifest):
    schema = manifest.graph_schema
    assert schema is not None
    return schema.metadata


def test_reordering_vertices_does_not_change_the_hash() -> None:
    manifest = _minimal_manifest()
    before = manifest_hash(manifest)
    reordered = copy.deepcopy(manifest)
    config = _core(reordered).vertex_config
    config.vertices = list(reversed(config.vertices))
    assert manifest_hash(reordered) == before


def test_reordering_properties_does_not_change_the_hash() -> None:
    manifest = _minimal_manifest()
    before = manifest_hash(manifest)
    reordered = copy.deepcopy(manifest)
    vertex = _core(reordered).vertex_config.vertices[0]
    vertex.properties = list(reversed(vertex.properties))
    assert manifest_hash(reordered) == before


def test_reordering_identity_fields_does_change_the_hash() -> None:
    """The PRESERVED half of the table, asserted rather than assumed.

    A backend resolves an edge endpoint through the *first* identity field
    present, so `["x", "y"]` and `["y", "x"]` are different contracts and must
    not share a content address.
    """
    manifest = _minimal_manifest()
    before = manifest_hash(manifest)
    reordered = copy.deepcopy(manifest)
    _core(reordered).vertex_config.vertices[0].identity = ["y", "x"]
    assert manifest_hash(reordered) != before


def test_metadata_is_not_part_of_the_content_hash() -> None:
    """Content identity is content-based: a label change is not a new model.

    A hash that moved with the semver could never recognise that two registry
    versions carry identical content, which is the whole basis of dedup and of
    a lineage that can say "these two paths reached the same place".
    """
    manifest = _minimal_manifest()
    before = manifest_hash(manifest)
    schema_before = schema_hash(_schema(manifest))

    relabelled = copy.deepcopy(manifest)
    _metadata(relabelled).version = "9.9.9"
    _metadata(relabelled).description = "a different label entirely"

    assert manifest_hash(relabelled) == before
    assert schema_hash(_schema(relabelled)) == schema_before


def test_the_canon_version_is_part_of_the_hashed_bytes() -> None:
    """Changing the rules must change the hashes, not reinterpret them."""
    import graflo.architecture.evolution.hashing as hashing_module

    manifest = _minimal_manifest()
    before = manifest_hash(manifest)
    original = hashing_module.CANON_VERSION
    try:
        hashing_module.CANON_VERSION = "graflo/canon@test"
        assert manifest_hash(manifest) != before
    finally:
        hashing_module.CANON_VERSION = original
    assert manifest_hash(manifest) == before
    assert CANON_VERSION.startswith("graflo/canon@")


# ── 3. stability ────────────────────────────────────────────────────────────


@pytest.mark.parametrize("path", EXAMPLES, ids=lambda p: pathlib.Path(p).parent.name)
def test_examples_canonicalize_and_are_order_stable(path: str) -> None:
    """Every shipped example survives, and none depends on declaration order."""
    original = GraphManifest.from_yaml(path)
    before = manifest_hash(original)

    shuffled = GraphManifest.from_yaml(path)
    core = _core(shuffled)
    core.vertex_config.vertices = list(reversed(core.vertex_config.vertices))
    core.edge_config.edges = list(reversed(core.edge_config.edges))
    for vertex in core.vertex_config.vertices:
        vertex.properties = list(reversed(vertex.properties))
    if shuffled.ingestion_model is not None:
        shuffled.ingestion_model.resources = list(
            reversed(shuffled.ingestion_model.resources)
        )
        shuffled.ingestion_model.transforms = list(
            reversed(shuffled.ingestion_model.transforms)
        )

    assert manifest_hash(shuffled) == before


@pytest.mark.parametrize("path", EXAMPLES, ids=lambda p: pathlib.Path(p).parent.name)
def test_finish_init_does_not_move_the_hash(path: str) -> None:
    """An authored manifest and a finished one are the same world model.

    Replay produces finished manifests while a YAML load produces unfinished
    ones, so a hash that moved across `finish_init` would make every replayed
    chain fail verification against its own recorded hash.
    """
    authored = GraphManifest.from_yaml(path)
    before = manifest_hash(authored)
    finished = GraphManifest.from_yaml(path)
    finished.finish_init()
    assert manifest_hash(finished) == before


@pytest.mark.parametrize("path", EXAMPLES, ids=lambda p: pathlib.Path(p).parent.name)
def test_canonicalization_is_idempotent_and_non_mutating(path: str) -> None:
    manifest = GraphManifest.from_yaml(path)
    plain_before = manifest.to_minimal_canonical_dict()

    first = canonical_payload(manifest)
    second = canonical_payload(manifest)
    assert json.dumps(first, sort_keys=True) == json.dumps(second, sort_keys=True)

    # Canonicalization is a read: the model it walked is untouched, and so is
    # the ordinary dump, because YAML stays declaration-ordered for humans.
    assert manifest.to_minimal_canonical_dict() == plain_before


def test_the_table_only_holds_known_policies() -> None:
    assert set(LIST_ORDER.values()) <= {ListOrder.SORTED, ListOrder.PRESERVED}
