"""Laws of a lift, checked over generated manifests that carry part of a vocabulary.

A lift is a join with the aspect's vocabulary, so it must be idempotent, and
lifting must not depend on whether the manifests carrying parts of the
vocabulary were composed first or lifted first.
"""

from __future__ import annotations

from typing import Any

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import apply_evolution
from graflo.architecture.evolution.aspects.provenance import plan_provenance
from graflo.architecture.evolution.merge import merge_manifests
from graflo.architecture.evolution.ops import MergeManifestsOp
from graflo.architecture.schema.document import Schema
from test.hypothesis_budget import examples

LAWS = settings(
    max_examples=examples(100),
    deadline=None,
    derandomize=True,
    database=None,
    suppress_health_check=[HealthCheck.too_slow],
)

PROV = "http://www.w3.org/ns/prov#"

#: The aspect's properties a kit may or may not carry, beyond its key.
RECORD_FIELDS = {
    "source_uri": "STRING",
    "media_type": "STRING",
    "content_hash": "STRING",
    "retrieved_at": "DATETIME",
}
AGENT_FIELDS = {"label": "STRING", "agent_kind": "STRING"}


@st.composite
def kits(draw: Any, name: str) -> GraphManifest:
    """A kit's manifest: the vocabulary's keys, any part of the rest, its own fields."""

    def fields(pool: dict[str, str], own: str) -> list[dict[str, Any]]:
        chosen = draw(st.sets(st.sampled_from(sorted(pool))))
        out = [{"name": f, "type": pool[f]} for f in sorted(chosen)]
        if "retrieved_at" in chosen and draw(st.booleans()):
            out[sorted(chosen).index("retrieved_at")]["semantics"] = {
                "iri": f"{PROV}generatedAtTime"
            }
        if draw(st.booleans()):
            out.append({"name": own, "type": "STRING"})
        return out

    def grounding(iri: str) -> dict[str, Any] | None:
        return {"iri": iri} if draw(st.booleans()) else None

    record: dict[str, Any] = {
        "name": "SourceRecord",
        "properties": [
            {"name": "source_record_id", "type": "STRING"},
            *fields(RECORD_FIELDS, f"{name}_channel"),
        ],
        "identity": ["source_record_id"],
    }
    agent: dict[str, Any] = {
        "name": "Agent",
        "properties": [
            {"name": "agent_id", "type": "STRING"},
            *fields(AGENT_FIELDS, f"{name}_handle"),
        ],
        "identity": ["agent_id"],
    }
    for vertex, iri in ((record, f"{PROV}Entity"), (agent, f"{PROV}Agent")):
        if (semantics := grounding(iri)) is not None:
            vertex["semantics"] = semantics
    edges: list[dict[str, Any]] = []
    if draw(st.booleans()):
        edge: dict[str, Any] = {
            "source": "SourceRecord",
            "target": "Agent",
            "relation": "wasAttributedTo",
        }
        if draw(st.booleans()):
            edge["properties"] = [{"name": "role", "type": "STRING"}]
        edges.append(edge)
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": name, "version": "1.0.0"},
                "graph": {
                    "vertex_config": {"vertices": [record, agent]},
                    "edge_config": {"edges": edges},
                },
            }
        }
    )
    manifest.finish_init()
    return manifest


def _lift(manifest: GraphManifest) -> GraphManifest:
    return apply_evolution(manifest, plan_provenance(manifest), bump_version=False)


def _vocabulary(schema: Schema) -> dict[str, Any]:
    """What the vocabulary is about: types, keys, groundings and typed properties."""
    core = schema.core_schema

    def grounding(semantics: Any) -> tuple[Any, ...] | None:
        if semantics is None:
            return None
        return (semantics.iri, frozenset(semantics.exact_match))

    def properties(fields: Any) -> dict[str, Any]:
        return {f.name: (f.type, f.item_type, grounding(f.semantics)) for f in fields}

    return {
        "vertices": {
            v.name: (
                tuple(v.identity),
                grounding(v.semantics),
                properties(v.properties),
            )
            for v in core.vertex_config.vertices
        },
        "edges": {
            e.edge_id: (e.directed, grounding(e.semantics), properties(e.properties))
            for e in core.edge_config.edges
        },
    }


def _compose(left: GraphManifest, right: GraphManifest) -> GraphManifest:
    # The vocabulary's names are one name on both sides, so the union is by name.
    op = MergeManifestsOp(name_conflict="union_right")
    return merge_manifests(left, right, op, bump_version=False)


@LAWS
@given(kits("slack"))
def test_lifting_twice_is_lifting_once(kit: GraphManifest) -> None:
    once = _lift(kit)

    assert plan_provenance(once) == []
    assert _vocabulary(_lift(once).require_schema()) == _vocabulary(
        once.require_schema()
    )


@LAWS
@given(kits("slack"), kits("jira"))
def test_composing_then_lifting_is_lifting_then_composing(
    left: GraphManifest, right: GraphManifest
) -> None:
    composed_first = _lift(_compose(left, right))
    lifted_first = _compose(_lift(left), _lift(right))

    assert _vocabulary(composed_first.require_schema()) == _vocabulary(
        lifted_first.require_schema()
    )
