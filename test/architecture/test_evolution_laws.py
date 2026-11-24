"""Algebraic laws of the contract operations, checked over generated inputs.

The example-based suites pin each law on one hand-built manifest per op. That
shows a law *can* hold; it does not show the law. Here the manifests and the ops
are generated from a small closed vocabulary, so a law is exercised against
every shape that vocabulary can express -- including the ones nobody thought to
write down, which is where a law that is only nearly true gives way.

Each law is stated as it is meant, not as it is known to pass. A law that does
not hold yet is marked ``xfail(strict=True)`` with the mechanism that breaks it,
on the smallest case that shows it: the suite stays green, and turns red the
moment the case holds, so the marker cannot outlive the defect it records. A
per-kind law checks a marked kind's pinned case before drawing; a law over
several manifests is drawn in the :data:`SCOPES` it holds in, and names the
pinned cases that keep it out of the others.

A refusal is a result, not a failure: an op that does not apply raises
``ValueError``, and the laws are stated over outcomes (a manifest, or a
refusal). Anything else escaping a handler is a crash and fails the test.

The per-op laws are parametrized by op kind, so one kind breaking a law does not
hide another behind it.
"""

from __future__ import annotations

from collections.abc import Collection
from typing import Any

import pytest
from hypothesis import HealthCheck, assume, event, given, settings
from hypothesis import strategies as st
from hypothesis.control import currently_in_test_context
from hypothesis.errors import UnsatisfiedAssumption

from graflo.architecture.contract.bindings import FileConnector
from graflo.architecture.contract.ingestion.steps.models import DescendActorConfig
from graflo.architecture.contract.ingestion.steps.ref import iter_edge_steps
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import footprint
from graflo.architecture.evolution.apply import apply_evolution
from graflo.architecture.evolution.autogenerate import (
    diff_manifests,
    diff_manifests_verified,
)
from graflo.architecture.evolution.codec import op_from_dict, ops_to_dicts
from graflo.architecture.evolution.footprint import op_reads, op_slots, ops_independent
from graflo.architecture.evolution.hashing import manifest_hash
from graflo.architecture.evolution.inverse import IRREVERSIBLE, invert_op, invert_ops
from graflo.architecture.evolution.merge3 import (
    MergeError,
    merge_three_way,
    split_by_slot,
    take_left,
    take_right,
)
from graflo.architecture.evolution.ops import ManifestOp, MergeManifestsOp
from graflo.architecture.refusal import OpRefusal
from test.hypothesis_budget import examples

# Derandomized and database-free: a law suite that passes or fails depending on
# the seed is a flaky test, not a law.
LAWS = settings(
    max_examples=examples(200),
    deadline=None,
    derandomize=True,
    database=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much],
)
PER_KIND = settings(LAWS, max_examples=examples(60))

#: Where a generated manifest may reach: the plain schema (types, properties,
#: edges, hierarchies), and the widenings on top of it -- lookup keys, indexes,
#: semantics and declared inverses (``extras``), and an ingestion block with its
#: bindings (``blocks``). A law over several manifests names the scopes it is
#: checked in; a scope it does not hold in is left out with the pinned cases
#: that show why, and comes back once those hold.
SCOPES: dict[str, dict[str, bool]] = {
    "schema": {"blocks": False, "extras": False},
    "keys": {"blocks": False, "extras": True},
    "ingestion": {"blocks": True, "extras": False},
    "all": {"blocks": True, "extras": True},
}

# The closed vocabulary. No two names share a canonical key, so a collision in a
# generated manifest is always one the generator intended.
VERTEX_POOL = ["person", "company", "city", "order"]
FIELD_POOL = ["name", "email", "since"]
RELATION_POOL = ["knows", "employs", "ships"]
EDGE_FIELD_POOL = ["weight", "note"]
#: The partner a pairable relation is declared inverse to.
INVERSE_OF = {"knows": "known_by", "employs": "employed_by"}
RESOURCE_POOL = ["rows", "feed", "dump"]
FLAVOR_POOL = ["arango", "neo4j", "tigergraph"]
IRI_POOL = ["https://example.org/a", "https://example.org/b"]
#: The one registry transform; a step references it by name.
TAG = {
    "name": "tag",
    "module": "graflo.util.transform",
    "foo": "tagged_key",
    "params": {"tag": "a"},
    "input": ["id"],
    "output": ["name"],
}
#: An inline transform step, which no registry entry backs.
RENAME_STEP = {"transform": {"rename": {"mail": "email"}}}

#: Every op kind the generator can draw: every op of the vocabulary, some in
#: two forms. A ``:variant`` names a form that addresses or does something
#: different from the bare kind, so it has a footprint, an inverse or a set of
#: refusals of its own -- ``remove_edges`` by relation or by triple,
#: ``add_inverse_edges`` for named relations or for every declared pair, a
#: ``canonicalize`` that only renames or one that merges.
KINDS = [
    "add_vertices",
    "remove_vertices",
    "rename_vertices",
    "add_vertex_properties",
    "remove_vertex_properties",
    "rename_vertex_properties",
    "change_field_types",
    "set_vertex_descriptions",
    "set_vertex_subclass_of",
    "add_edges",
    "remove_edges:relation",
    "remove_edges:triple",
    "retarget_edges",
    "rename_relations",
    "set_edge_descriptions",
    "set_edge_subproperty_of",
    "add_edge_properties",
    "remove_edge_properties",
    "add_edge_properties:edge",
    "remove_edge_properties:edge",
    "rename_edge_properties:edge",
    "change_field_types:edge",
    # ingestion
    "add_resources",
    "remove_resources",
    "replace_resources",
    "rename_resources",
    "add_resource_transforms",
    "ensure_extracted_fields",
    "set_inverse_emission",
    # identities and indexes
    "replace_identity",
    "add_secondary_identities",
    "remove_secondary_identities",
    "replace_edge_identities",
    "add_vertex_indexes",
    "remove_vertex_indexes",
    "add_edge_indexes",
    "remove_edge_indexes",
    # declared inverses
    "declare_edge_inverses",
    "retract_edge_inverses",
    "add_inverse_edges:named",
    "add_inverse_edges:all",
    "set_native_inverses",
    "set_edge_directed",
    # semantics, profile, bindings
    "set_vertex_semantics",
    "set_edge_semantics",
    "set_field_semantics",
    "set_field_semantics:edge",
    "set_bindings",
    "set_db_profile",
    "sanitize",
    # whole structure
    "canonicalize",
    "canonicalize:merge",
    "merge_vertices",
    "merge_edges",
    "project_manifest",
    "merge_manifests",
]


def _base(kind: str) -> str:
    return kind.partition(":")[0]


#: Kinds ``apply`` refuses whatever the manifest: a law about what an applied op
#: does has nothing to check for them, so each such law states the refusal
#: instead (:func:`test_a_binary_op_is_refused_where_ops_apply`).
REFUSED_BY_APPLY = {
    "merge_manifests": (
        "binary: apply refuses it as invalid-op, merge_manifests(left, right, op) "
        "is its only entry point"
    ),
}

#: Kinds no inverse is ever offered for, so the inverse law has nothing to say
#: about them: the ops declared irreversible, and a ``canonicalize`` that merges,
#: which discards where each member of a group came from.
WITHOUT_INVERSE = {
    kind: IRREVERSIBLE[_base(kind)] for kind in KINDS if _base(kind) in IRREVERSIBLE
} | {"canonicalize:merge": "a merging canonicalize discards each member's origin"}

APPLIED_KINDS = [kind for kind in KINDS if kind not in REFUSED_BY_APPLY]
REVERSIBLE_KINDS = [kind for kind in KINDS if kind not in WITHOUT_INVERSE]


# ── generators ──────────────────────────────────────────────────────────────


def _subset(pool: list[str]) -> st.SearchStrategy[list[str]]:
    return st.lists(st.sampled_from(pool), unique=True)


Triple = tuple[str, str, str]


def _selector(edge: Triple) -> dict[str, str]:
    return {"source": edge[0], "target": edge[1], "relation": edge[2]}


@st.composite
def _vertices(draw: st.DrawFn, name: str, extras: bool) -> dict[str, Any]:
    """One vertex type; with *extras*, sometimes grounded, a unit, or a lookup key."""
    fields = ["id", *draw(_subset(FIELD_POOL))]
    properties: list[dict[str, Any]] = [{"name": f} for f in fields]
    vertex: dict[str, Any] = {
        "name": name,
        "properties": properties,
        "identity": ["id"],
    }
    if not extras:
        return vertex
    for prop in properties:
        if prop["name"] == "since" and draw(st.booleans()):
            prop["semantics"] = {"unit": "a"}
    if draw(st.booleans()):
        vertex["semantics"] = {"iri": draw(st.sampled_from(IRI_POOL))}
    if "email" in fields and draw(st.booleans()):
        vertex["secondary_identities"] = [{"name": "email_key", "fields": ["email"]}]
    return vertex


@st.composite
def _profiles(
    draw: st.DrawFn,
    fields: dict[str, list[str]],
    edge_fields: dict[Triple, list[str]],
    native: list[str],
) -> dict[str, Any]:
    """A database profile: a flavor, and sometimes indexes and native inverses."""
    flavor = draw(st.sampled_from(FLAVOR_POOL))
    profile: dict[str, Any] = {"db_flavor": flavor}
    indexable = [(v, f) for v, names in fields.items() for f in names if f != "id"]
    if indexable and draw(st.booleans()):
        vertex, field = draw(st.sampled_from(indexable))
        profile["vertex_indexes"] = {vertex: [{"fields": [field]}]}
    weighted = [(e, f) for e, names in edge_fields.items() for f in names]
    if weighted and draw(st.booleans()):
        edge, field = draw(st.sampled_from(weighted))
        profile["edge_specs"] = [{**_selector(edge), "indexes": [{"fields": [field]}]}]
    if flavor == "tigergraph" and native and draw(st.booleans()):
        profile["native_inverses"] = [draw(st.sampled_from(native))]
    return profile


@st.composite
def _pipelines(
    draw: st.DrawFn,
    vertices: list[str],
    edges: list[Triple],
    emitting: set[str],
    tagged: bool,
) -> list[dict[str, Any]]:
    """A small pipeline over *vertices* and the *edges* between what it produces.

    Vertex steps, sometimes projecting, sometimes a restricting router, the
    registry transform, edge steps (mirroring into an inverse where *emitting*
    has one) and a nested level -- so ops that address a step by position, a
    level or a router each have one to meet.
    """
    produced = draw(
        st.lists(st.sampled_from(vertices), unique=True, min_size=1, max_size=2)
    )
    steps: list[dict[str, Any]] = []
    for vertex in produced:
        step: dict[str, Any] = {"vertex": vertex}
        if draw(st.booleans()):
            step["keep_fields"] = ["id"]
        steps.append(step)
    if draw(st.booleans()):
        steps.append(
            {
                "vertex_router": {
                    "type_field": "kind",
                    "vertex_types": produced,
                    "extraction_scope": "mapped_only",
                    "role": "routed",
                }
            }
        )
    if tagged and draw(st.booleans()):
        steps.append({"transform": {"call": {"use": TAG["name"]}}})
    reachable = [e for e in edges if e[0] in produced and e[1] in produced]
    if reachable:
        for source, target, relation in draw(
            st.lists(st.sampled_from(reachable), unique=True, max_size=2)
        ):
            edge: dict[str, Any] = {"from": source, "to": target, "relation": relation}
            if relation in emitting and draw(st.booleans()):
                edge["emit_inverse"] = True
            steps.append({"edge": edge})
    if draw(st.booleans()):
        nested = draw(st.sampled_from(vertices))
        steps.append({"descend": {"key": "items", "pipeline": [{"vertex": nested}]}})
    return steps


def _bindings(resources: list[str]) -> dict[str, Any]:
    """A bindings block wiring each of *resources* to a file connector of its own."""
    return {
        "connectors": [
            FileConnector(
                name=f"{name}_files", regex=f"{name}.*", resource_name=name
            ).to_dict(skip_defaults=False)
            for name in resources
        ],
        "resource_connector": [
            {"resource": name, "connector": f"{name}_files"} for name in resources
        ],
    }


def _emitting(edges: list[Triple], pairs: dict[str, str]) -> set[str]:
    """Relations an edge step can mirror: paired, with the inverse edge declared."""
    present = set(edges)
    return {
        relation
        for source, target, relation in edges
        if relation in pairs and (target, source, pairs[relation]) in present
    }


@st.composite
def manifests(
    draw: st.DrawFn, min_vertices: int = 1, blocks: bool = True, extras: bool = True
) -> GraphManifest:
    """A manifest over the closed vocabulary.

    Always a schema of types, properties, edges and hierarchies. With *extras*,
    sometimes semantics, lookup keys, edge identities, a declared inverse pair
    or symmetric relation, and a database profile. With *blocks*, sometimes an
    ingestion block over the schema's types and a bindings block wiring some of
    its resources.
    """
    names = draw(
        st.lists(
            st.sampled_from(VERTEX_POOL), unique=True, min_size=min_vertices, max_size=3
        )
    )
    vertices = [draw(_vertices(name, extras)) for name in names]
    # Sometimes a hierarchy, so renames, removals and retargets meet references:
    # a vertex under an earlier one (acyclic by construction).
    for index, vertex in enumerate(vertices[1:], start=1):
        if draw(st.booleans()):
            parent = draw(st.sampled_from(names[:index]))
            vertex["subclass_of"] = [{"vertex": parent}]
    triples = draw(
        st.lists(
            st.tuples(
                st.sampled_from(names),
                st.sampled_from(names),
                st.sampled_from(RELATION_POOL),
            ),
            unique=True,
            max_size=3,
        )
    )
    edges: list[dict[str, Any]] = [
        {
            "source": source,
            "target": target,
            "relation": relation,
            "properties": [{"name": f} for f in draw(_subset(EDGE_FIELD_POOL))],
        }
        for source, target, relation in triples
    ]
    for edge in edges if extras else []:
        if draw(st.booleans()):
            edge["semantics"] = {"iri": draw(st.sampled_from(IRI_POOL))}
        if {"name": "weight"} in edge["properties"] and draw(st.booleans()):
            edge["identities"] = [["source", "target", "weight"]]
    # And an edge under an earlier one on the same endpoints, which subsume trivially.
    for index, edge in enumerate(edges):
        same_ends = [
            e
            for e in edges[:index]
            if (e["source"], e["target"]) == (edge["source"], edge["target"])
        ]
        if same_ends and draw(st.booleans()):
            parent = draw(st.sampled_from(same_ends))
            edge["subproperty_of"] = [
                {
                    "edge": {
                        "source": parent["source"],
                        "target": parent["target"],
                        "relation": parent["relation"],
                    }
                }
            ]
    # Sometimes a declared inverse pair, its inverse edges sometimes declared
    # too; or a symmetric relation, whose edges the schema requires undirected.
    relations = sorted({edge["relation"] for edge in edges})
    pairable = [r for r in relations if r in INVERSE_OF]
    declared = draw(st.sampled_from(["none", "pair", "symmetric"])) if extras else ""
    inverses: list[dict[str, str]] = []
    symmetric: list[str] = []
    pairs: dict[str, str] = {}
    if declared == "pair" and pairable:
        relation = draw(st.sampled_from(pairable))
        inverses = [{"relation": relation, "inverse": INVERSE_OF[relation]}]
        pairs = {relation: INVERSE_OF[relation], INVERSE_OF[relation]: relation}
        if draw(st.booleans()):
            edges += [
                {
                    "source": edge["target"],
                    "target": edge["source"],
                    "relation": INVERSE_OF[relation],
                }
                for edge in list(edges)
                if edge["relation"] == relation
            ]
    elif declared == "symmetric" and (
        # An undirected edge cannot be a subproperty of a directed one.
        unparented := sorted(
            {e["relation"] for e in edges}
            - {e["relation"] for e in edges if e.get("subproperty_of")}
        )
    ):
        relation = draw(st.sampled_from(unparented))
        symmetric = [relation]
        for edge in edges:
            if edge["relation"] == relation:
                edge["directed"] = False
    triples = [(e["source"], e["target"], e["relation"]) for e in edges]
    emitting = _emitting(triples, pairs)

    schema: dict[str, Any] = {
        "metadata": {"name": "laws"},
        "graph": {
            "vertex_config": {"vertices": vertices},
            "edge_config": {
                "edges": edges,
                "inverses": inverses,
                "symmetric": symmetric,
            },
        },
    }
    if extras and draw(st.booleans()):
        native = [pair["relation"] for pair in inverses if not emitting]
        schema["db_profile"] = draw(
            _profiles(
                {v["name"]: [p["name"] for p in v["properties"]] for v in vertices},
                {
                    (e["source"], e["target"], e["relation"]): [
                        p["name"] for p in e.get("properties", [])
                    ]
                    for e in edges
                },
                native,
            )
        )
    payload: dict[str, Any] = {"schema": schema}
    if blocks and draw(st.booleans()):
        resources = draw(
            st.lists(
                st.sampled_from(RESOURCE_POOL), unique=True, min_size=1, max_size=2
            )
        )
        tagged = draw(st.booleans())
        payload["ingestion_model"] = {
            "resources": [
                {
                    "name": name,
                    "pipeline": draw(_pipelines(names, triples, emitting, tagged)),
                }
                for name in resources
            ],
            "transforms": [TAG] if tagged else [],
        }
        wired = draw(_subset(resources))
        if wired:
            payload["bindings"] = _bindings(wired)
    return GraphManifest.from_dict(payload)


def _core(manifest: GraphManifest) -> Any:
    schema = manifest.graph_schema
    assert schema is not None
    return schema.core_schema


Payloads = dict[str, st.SearchStrategy[dict[str, Any]]]


def _payloads(manifest: GraphManifest) -> Payloads:
    """A payload strategy for each op kind that has something to address here.

    Mostly well-addressed, deliberately not always applicable: removing an
    identity field, renaming onto a name already taken, removing the last
    vertex, a resource the manifest does not have, a pair nothing declared are
    all drawn, because the laws are about refusals as much as about results.
    """
    view = _View(manifest)
    return (
        _element_payloads(manifest)
        | _ingestion_payloads(view)
        | _identity_payloads(view)
        | _inverse_payloads(view)
        | _semantics_payloads(view)
        | _structure_payloads(view)
    )


def _element_payloads(manifest: GraphManifest) -> Payloads:
    """Vertex and edge types, their properties and descriptions."""
    core = _core(manifest)
    present = sorted(core.vertex_config.vertex_set)
    absent = [name for name in VERTEX_POOL if name not in present]
    fields = {
        v.name: [f.name for f in v.properties] for v in core.vertex_config.vertices
    }
    edges = [(e.source, e.target, e.relation) for e in core.edge_config.edges]
    relations = sorted({relation for _, _, relation in edges})
    unused_relations = [r for r in RELATION_POOL if r not in relations]

    out: dict[str, st.SearchStrategy[dict[str, Any]]] = {}
    if absent:
        out["add_vertices"] = st.sampled_from(absent).map(
            lambda v: {
                "op": "add_vertices",
                "vertices": [{"name": v, "properties": ["id"], "identity": ["id"]}],
            }
        )
    if not present:
        return out

    vertex = st.sampled_from(present)
    field = st.sampled_from(FIELD_POOL)
    out["remove_vertices"] = vertex.map(
        lambda v: {"op": "remove_vertices", "names": [v]}
    )
    out["add_vertex_properties"] = st.tuples(vertex, field).map(
        lambda t: {"op": "add_vertex_properties", "additions": {t[0]: [t[1]]}}
    )
    out["remove_vertex_properties"] = st.tuples(
        vertex, st.sampled_from(["id", *FIELD_POOL])
    ).map(lambda t: {"op": "remove_vertex_properties", "removals": {t[0]: [t[1]]}})
    out["rename_vertex_properties"] = st.tuples(vertex, field, field).map(
        lambda t: {"op": "rename_vertex_properties", "renames": {t[0]: {t[1]: t[2]}}}
    )
    out["change_field_types"] = st.tuples(
        vertex, st.sampled_from(["STRING", "INT"])
    ).map(
        lambda t: {
            "op": "change_field_types",
            "vertices": {t[0]: {fields[t[0]][-1]: {"type": t[1]}}},
        }
    )
    out["set_vertex_descriptions"] = st.tuples(
        vertex, st.text("ab", min_size=1, max_size=3)
    ).map(lambda t: {"op": "set_vertex_descriptions", "descriptions": {t[0]: t[1]}})
    # A local parent (itself included, which is refused), an external one, or none.
    parent = st.one_of(
        st.sampled_from(present).map(lambda v: [{"vertex": v}]),
        st.just([{"iri": "https://example.org/Thing"}]),
        st.just([]),
    )
    out["set_vertex_subclass_of"] = st.tuples(vertex, parent).map(
        lambda t: {"op": "set_vertex_subclass_of", "subclass_of": {t[0]: t[1]}}
    )
    out["add_edges"] = st.tuples(vertex, vertex, st.sampled_from(RELATION_POOL)).map(
        lambda t: {
            "op": "add_edges",
            "edges": [{"source": t[0], "target": t[1], "relation": t[2]}],
        }
    )
    if absent:
        out["rename_vertices"] = st.tuples(vertex, st.sampled_from(absent)).map(
            lambda t: {"op": "rename_vertices", "renames": {t[0]: t[1]}}
        )
    if not edges:
        return out

    edge = st.sampled_from(edges)
    relation = st.sampled_from(relations)
    edge_field = st.sampled_from(EDGE_FIELD_POOL)
    out["remove_edges:relation"] = relation.map(
        lambda r: {"op": "remove_edges", "relations": [r]}
    )
    out["remove_edges:triple"] = edge.map(
        lambda e: {
            "op": "remove_edges",
            "edges": [{"source": e[0], "target": e[1], "relation": e[2]}],
        }
    )
    out["retarget_edges"] = st.tuples(edge, vertex).map(
        lambda t: {
            "op": "retarget_edges",
            "edges": [
                {
                    "source": t[0][0],
                    "target": t[0][1],
                    "relation": t[0][2],
                    "new_target": t[1],
                }
            ],
        }
    )
    out["set_edge_descriptions"] = st.tuples(
        edge, st.one_of(st.none(), st.text("ab", min_size=1, max_size=3))
    ).map(
        lambda t: {
            "op": "set_edge_descriptions",
            "edges": [
                {
                    "source": t[0][0],
                    "target": t[0][1],
                    "relation": t[0][2],
                    "description": t[1],
                }
            ],
        }
    )
    # A local parent (itself included, which is refused), an external one, or none.
    edge_parent = st.one_of(
        edge.map(
            lambda e: [{"edge": {"source": e[0], "target": e[1], "relation": e[2]}}]
        ),
        st.just([{"iri": "https://example.org/related"}]),
        st.just([]),
    )
    out["set_edge_subproperty_of"] = st.tuples(edge, edge_parent).map(
        lambda t: {
            "op": "set_edge_subproperty_of",
            "edges": [
                {
                    "source": t[0][0],
                    "target": t[0][1],
                    "relation": t[0][2],
                    "subproperty_of": t[1],
                }
            ],
        }
    )
    out["add_edge_properties"] = st.tuples(relation, edge_field).map(
        lambda t: {"op": "add_edge_properties", "additions": {t[0]: [t[1]]}}
    )
    out["remove_edge_properties"] = st.tuples(relation, edge_field).map(
        lambda t: {"op": "remove_edge_properties", "removals": {t[0]: [t[1]]}}
    )

    def _on(e: tuple[str, str, str | None], **entry: Any) -> list[dict[str, Any]]:
        return [{"source": e[0], "target": e[1], "relation": e[2], **entry}]

    out["add_edge_properties:edge"] = st.tuples(edge, edge_field).map(
        lambda t: {
            "op": "add_edge_properties",
            "per_edge": _on(t[0], properties=[t[1]]),
        }
    )
    out["remove_edge_properties:edge"] = st.tuples(edge, edge_field).map(
        lambda t: {"op": "remove_edge_properties", "per_edge": _on(t[0], fields=[t[1]])}
    )
    out["rename_edge_properties:edge"] = st.tuples(edge, edge_field, edge_field).map(
        lambda t: {
            "op": "rename_edge_properties",
            "per_edge": _on(t[0], renames={t[1]: t[2]}),
        }
    )
    out["change_field_types:edge"] = st.tuples(
        edge, edge_field, st.sampled_from(["STRING", "INT"])
    ).map(
        lambda t: {
            "op": "change_field_types",
            "per_edge": _on(t[0], fields={t[1]: {"type": t[2]}}),
        }
    )
    if unused_relations:
        out["rename_relations"] = st.tuples(
            relation, st.sampled_from(unused_relations)
        ).map(lambda t: {"op": "rename_relations", "renames": {t[0]: t[1]}})
    return out


class _View:
    """What a payload may address in one manifest, read once."""

    def __init__(self, manifest: GraphManifest) -> None:
        schema = manifest.graph_schema
        assert schema is not None
        core = schema.core_schema
        self.present = sorted(core.vertex_config.vertex_set)
        self.absent = [name for name in VERTEX_POOL if name not in self.present]
        self.fields = {
            v.name: [f.name for f in v.properties] for v in core.vertex_config.vertices
        }
        #: Each lookup key as a removal addresses it: by name, else by fields.
        self.secondaries: list[tuple[str, str | list[str]]] = [
            (v.name, s.name if s.name else list(s.fields))
            for v in core.vertex_config.vertices
            for s in v.secondary_identities
        ]
        self.edges: list[Triple] = [
            (e.source, e.target, e.relation)
            for e in core.edge_config.edges
            if e.relation is not None
        ]
        self.edge_fields: dict[Triple, list[str]] = {
            (e.source, e.target, e.relation): [f.name for f in e.properties]
            for e in core.edge_config.edges
            if e.relation is not None
        }
        self.relations = sorted({relation for _, _, relation in self.edges})
        self.unused_relations = [r for r in RELATION_POOL if r not in self.relations]
        edge_config = core.edge_config
        self.pairs: dict[str, str] = {}
        for pair in edge_config.inverses:
            self.pairs[pair.relation] = pair.inverse
            self.pairs[pair.inverse] = pair.relation
        self.declared = sorted(set(self.pairs) | set(edge_config.symmetric))
        profile = schema.db_profile
        self.vertex_indexes: list[tuple[str, list[str]]] = [
            (vertex, list(index.fields))
            for vertex, indexes in profile.vertex_indexes.items()
            for index in indexes
        ]
        #: Edges with a physical spec: the only ones an index op can address.
        self.spec_edges: list[Triple] = sorted(
            {
                (spec.source, spec.target, spec.relation)
                for spec in profile.edge_specs
                if spec.relation is not None
            }
        )
        self.edge_indexes: list[tuple[Triple, list[str]]] = [
            ((spec.source, spec.target, spec.relation), list(index.fields))
            for spec in profile.edge_specs
            if spec.relation is not None
            for index in spec.indexes
        ]
        ingestion = manifest.ingestion_model
        self.resources = (
            {r.name: r for r in ingestion.resources} if ingestion is not None else {}
        )
        self.unused_resources = [n for n in RESOURCE_POOL if n not in self.resources]
        #: Each resource's pipeline levels: the root, and each nested level.
        self.levels: dict[str, list[list[int]]] = {
            name: [[]]
            + [
                [index]
                for index, step in enumerate(resource.pipeline)
                if isinstance(step, DescendActorConfig)
            ]
            for name, resource in self.resources.items()
        }
        self.edge_steps: list[tuple[str, dict[str, Any]]] = [
            (name, view.ref.model_dump(exclude_none=True))
            for name, resource in self.resources.items()
            for view in iter_edge_steps(list(resource.pipeline))
        ]


def _ingestion_payloads(view: _View) -> Payloads:
    """Resources, their pipelines' transform steps, projections and flags."""
    out: Payloads = {}
    if not view.present:
        return out
    named = st.sampled_from(sorted(view.resources) or RESOURCE_POOL)
    pipeline = _pipelines(
        view.present, view.edges, _emitting(view.edges, view.pairs), tagged=True
    )
    # Whether the op registers the transform its steps may name: when it does
    # not and the manifest has not, the op is refused.
    brings = st.booleans()

    def resource_op(kind: str) -> Any:
        return lambda t: {
            "op": kind,
            "resources": [{"name": t[0], "pipeline": t[1]}],
            "transforms": [TAG] if t[2] else [],
        }

    out["add_resources"] = st.tuples(
        st.sampled_from(RESOURCE_POOL), pipeline, brings
    ).map(resource_op("add_resources"))
    out["replace_resources"] = st.tuples(named, pipeline, brings).map(
        resource_op("replace_resources")
    )
    out["remove_resources"] = named.map(
        lambda n: {"op": "remove_resources", "names": [n]}
    )
    if view.resources and view.unused_resources:
        out["rename_resources"] = st.tuples(
            st.sampled_from(sorted(view.resources)),
            st.sampled_from(view.unused_resources),
        ).map(lambda t: {"op": "rename_resources", "renames": {t[0]: t[1]}})
    # A level of the resource, or a path through a step that is no descend.
    level = named.flatmap(
        lambda n: st.tuples(st.just(n), st.sampled_from([*view.levels.get(n, []), [0]]))
    )
    step = st.sampled_from([{"transform": {"call": {"use": TAG["name"]}}}, RENAME_STEP])
    out["add_resource_transforms"] = st.tuples(level, step, brings).map(
        lambda t: {
            "op": "add_resource_transforms",
            "additions": {t[0][0]: [t[1]]},
            "at": {t[0][0]: t[0][1]} if t[0][1] else {},
            "transforms": [TAG] if t[2] else [],
        }
    )
    out["ensure_extracted_fields"] = st.tuples(
        level, st.sampled_from(view.present), st.sampled_from(["id", *FIELD_POOL])
    ).map(
        lambda t: {
            "op": "ensure_extracted_fields",
            "additions": {t[0][0]: [{"vertex": t[1], "fields": [t[2]], "at": t[0][1]}]},
        }
    )
    if view.edge_steps:
        out["set_inverse_emission"] = st.tuples(
            st.sampled_from(view.edge_steps), st.booleans()
        ).map(
            lambda t: {
                "op": "set_inverse_emission",
                "steps": {t[0][0]: [t[0][1]]},
                "enabled": t[1],
            }
        )
    return out


def _identity_payloads(view: _View) -> Payloads:
    """Primary and lookup identities, edge keys, and authored indexes."""
    out: Payloads = {}
    if not view.present:
        return out
    vertex = st.sampled_from(view.present)
    any_field = st.sampled_from(["id", *FIELD_POOL])
    target = vertex.flatmap(
        lambda v: st.tuples(
            st.just(v),
            st.one_of(
                st.sampled_from(view.fields[v]).map(
                    lambda f: {"mode": "natural", "identity": [f]}
                ),
                st.sampled_from(view.fields[v]).map(
                    lambda f: {"mode": "hash", "hash_from": [f]}
                ),
                st.sampled_from([{"mode": "assigned"}, {"mode": "blank"}]),
            ),
        )
    )
    retire = st.sampled_from(
        [
            ("demote", "follow_new"),
            ("demote", "pin_to_retired"),
            ("keep", "follow_new"),
            ("drop", "follow_new"),
        ]
    )
    out["replace_identity"] = st.tuples(target, retire).map(
        lambda t: {
            "op": "replace_identity",
            "replacements": {
                t[0][0]: {"to": t[0][1], "retire": t[1][0], "endpoints": t[1][1]}
            },
        }
    )
    out["add_secondary_identities"] = st.dictionaries(
        vertex, any_field, min_size=1, max_size=2
    ).map(
        lambda d: {
            "op": "add_secondary_identities",
            "additions": {
                v: [{"name": f"{f}_key", "fields": [f]}] for v, f in d.items()
            },
        }
    )
    out["remove_secondary_identities"] = st.one_of(
        st.sampled_from(view.secondaries) if view.secondaries else st.nothing(),
        st.tuples(vertex, st.just("email_key")),
    ).map(lambda t: {"op": "remove_secondary_identities", "removals": {t[0]: [t[1]]}})
    out["add_vertex_indexes"] = st.dictionaries(
        vertex, st.tuples(any_field, st.booleans()), min_size=1, max_size=2
    ).map(
        lambda d: {
            "op": "add_vertex_indexes",
            "indexes": {v: [{"fields": [f], "unique": u}] for v, (f, u) in d.items()},
        }
    )
    out["remove_vertex_indexes"] = st.one_of(
        st.sampled_from(view.vertex_indexes) if view.vertex_indexes else st.nothing(),
        st.tuples(vertex, any_field.map(lambda f: [f])),
    ).map(lambda t: {"op": "remove_vertex_indexes", "indexes": {t[0]: [t[1]]}})
    if not view.edges:
        return out

    edge = st.sampled_from(view.edges)
    edge_field = st.sampled_from(EDGE_FIELD_POOL)
    keys = st.one_of(
        st.just([]),
        st.just([["source", "target"]]),
        edge_field.map(lambda f: [["source", "target", f]]),
    )
    out["replace_edge_identities"] = st.tuples(edge, keys).map(
        lambda t: {
            "op": "replace_edge_identities",
            "edges": [{**_selector(t[0]), "identities": t[1]}],
        }
    )
    specified = (
        st.one_of(st.sampled_from(view.spec_edges), edge) if view.spec_edges else edge
    )
    out["add_edge_indexes"] = st.tuples(specified, edge_field).map(
        lambda t: {
            "op": "add_edge_indexes",
            "edges": [{**_selector(t[0]), "indexes": [{"fields": [t[1]]}]}],
        }
    )
    out["remove_edge_indexes"] = st.one_of(
        st.sampled_from(view.edge_indexes) if view.edge_indexes else st.nothing(),
        st.tuples(edge, edge_field.map(lambda f: [f])),
    ).map(
        lambda t: {
            "op": "remove_edge_indexes",
            "edges": [{**_selector(t[0]), "fields": [t[1]]}],
        }
    )
    return out


def _inverse_payloads(view: _View) -> Payloads:
    """Declared inverse pairs and symmetric relations, and their realization."""
    out: Payloads = {
        "declare_edge_inverses": st.one_of(
            st.tuples(
                st.sampled_from(RELATION_POOL),
                st.sampled_from([*RELATION_POOL, *INVERSE_OF.values()]),
            ).map(lambda t: {"op": "declare_edge_inverses", "inverses": {t[0]: t[1]}}),
            st.sampled_from(RELATION_POOL).map(
                lambda r: {"op": "declare_edge_inverses", "symmetric": [r]}
            ),
        ),
        "retract_edge_inverses": st.sampled_from(view.declared or RELATION_POOL).map(
            lambda r: {"op": "retract_edge_inverses", "relations": [r]}
        ),
        "add_inverse_edges:named": st.sampled_from(
            sorted(view.pairs) or RELATION_POOL
        ).map(lambda r: {"op": "add_inverse_edges", "relations": [r]}),
        "add_inverse_edges:all": st.just({"op": "add_inverse_edges"}),
    }
    if not view.edges:
        return out
    paired = [r for r in view.relations if r in view.pairs] or view.relations
    out["set_native_inverses"] = st.tuples(st.sampled_from(paired), st.booleans()).map(
        lambda t: {"op": "set_native_inverses", "relations": [t[0]], "enabled": t[1]}
    )
    out["set_edge_directed"] = st.tuples(
        st.lists(st.sampled_from(view.edges), unique=True, min_size=1, max_size=2),
        st.booleans(),
    ).map(
        lambda t: {
            "op": "set_edge_directed",
            "edges": [_selector(e) for e in t[0]],
            "directed": t[1],
        }
    )
    return out


def _semantics_payloads(view: _View) -> Payloads:
    """Grounding of types, relations and properties; the profile and bindings."""
    grounding = st.one_of(
        st.none(), st.sampled_from(IRI_POOL).map(lambda iri: {"iri": iri})
    )
    unit = st.one_of(st.none(), st.sampled_from(["a", "m"]).map(lambda u: {"unit": u}))
    out: Payloads = {
        "set_bindings": st.one_of(
            st.none(),
            st.lists(
                st.sampled_from(RESOURCE_POOL), unique=True, min_size=1, max_size=2
            ).map(_bindings),
        ).map(lambda b: {"op": "set_bindings", "bindings": b}),
        "set_db_profile": _profiles(
            view.fields,
            view.edge_fields,
            [r for r in view.relations if r in view.pairs],
        ).map(lambda p: {"op": "set_db_profile", "profile": p}),
        "sanitize": st.tuples(
            st.sampled_from(FLAVOR_POOL),
            st.one_of(
                st.none(),
                st.lists(
                    st.sampled_from([*VERTEX_POOL, *FIELD_POOL, *RELATION_POOL]),
                    unique=True,
                    min_size=1,
                    max_size=2,
                ),
            ),
        ).map(lambda t: {"op": "sanitize", "db_flavor": t[0], "reserved_words": t[1]}),
    }
    if view.present:
        vertex = st.sampled_from(view.present)
        out["set_vertex_semantics"] = st.dictionaries(
            vertex, grounding, min_size=1, max_size=2
        ).map(lambda d: {"op": "set_vertex_semantics", "semantics": d})
        property_of = vertex.flatmap(
            lambda v: st.tuples(
                st.just(v), st.sampled_from([*view.fields[v], *FIELD_POOL])
            )
        )
        out["set_field_semantics"] = st.tuples(property_of, unit).map(
            lambda t: {
                "op": "set_field_semantics",
                "targets": [{"vertex": t[0][0], "field": t[0][1], "semantics": t[1]}],
            }
        )
    if view.edges:
        edge = st.sampled_from(view.edges)
        out["set_edge_semantics"] = st.tuples(
            st.lists(edge, unique=True, min_size=1, max_size=2), grounding
        ).map(
            lambda t: {
                "op": "set_edge_semantics",
                "edges": [_selector(e) for e in t[0]],
                "semantics": t[1],
            }
        )
        out["set_field_semantics:edge"] = st.tuples(
            edge, st.sampled_from(EDGE_FIELD_POOL), unit
        ).map(
            lambda t: {
                "op": "set_field_semantics",
                "targets": [{**_selector(t[0]), "field": t[1], "semantics": t[2]}],
            }
        )
    return out


def _structure_payloads(view: _View) -> Payloads:
    """Ops over the whole structure: relabel, merge, project, merge manifests."""
    out: Payloads = {"merge_manifests": st.just({"op": "merge_manifests"})}
    if not view.present:
        return out
    vertex = st.sampled_from(view.present)
    renames: list[st.SearchStrategy[dict[str, Any]]] = [
        vertex.flatmap(
            lambda v: st.tuples(
                st.just(v), st.sampled_from(view.fields[v]), st.sampled_from(FIELD_POOL)
            )
        ).map(lambda t: {"properties": {t[0]: {t[1]: t[2]}}})
    ]
    if view.absent:
        renames.append(
            st.tuples(vertex, st.sampled_from(view.absent)).map(
                lambda t: {"vertices": {t[0]: t[1]}}
            )
        )
    if view.relations and view.unused_relations:
        renames.append(
            st.tuples(
                st.sampled_from(view.relations), st.sampled_from(view.unused_relations)
            ).map(lambda t: {"relations": {t[0]: t[1]}})
        )
    out["canonicalize"] = st.one_of(renames).map(lambda m: {"op": "canonicalize", **m})
    groups: list[st.SearchStrategy[dict[str, Any]]] = []
    if len(view.present) >= 2:
        groups.append(
            st.lists(vertex, min_size=2, max_size=2, unique=True).map(
                lambda g: {"vertices": {g[0]: g[1], g[1]: g[1]}}
            )
        )
    if len(view.relations) >= 2:
        groups.append(
            st.lists(
                st.sampled_from(view.relations), min_size=2, max_size=2, unique=True
            ).map(lambda g: {"relations": {g[0]: g[1], g[1]: g[1]}})
        )
    if groups:
        out["canonicalize:merge"] = st.tuples(
            st.one_of(groups), st.booleans(), st.booleans()
        ).map(
            lambda t: {
                "op": "canonicalize",
                **t[0],
                "allow_merges": True,
                "allow_self_relations": t[1],
                "allow_observation_fusion": t[2],
            }
        )
    out["merge_vertices"] = st.tuples(
        vertex, st.sampled_from(VERTEX_POOL), st.booleans(), st.booleans()
    ).map(
        lambda t: {
            "op": "merge_vertices",
            "sources": [t[0]],
            "into": t[1],
            "allow_self_relations": t[2],
            "allow_observation_fusion": t[3],
        }
    )
    if view.relations:
        out["merge_edges"] = st.tuples(
            st.sampled_from(view.relations), st.sampled_from(RELATION_POOL)
        ).map(lambda t: {"op": "merge_edges", "sources": [t[0]], "into": t[1]})
    keeps: list[st.SearchStrategy[dict[str, Any]]] = [
        st.tuples(
            st.lists(vertex, unique=True, min_size=1, max_size=2), st.integers(0, 1)
        ).map(lambda t: {"keep_vertices": t[0], "depth": t[1]})
    ]
    if view.edges:
        keeps.append(
            st.tuples(
                st.lists(
                    st.sampled_from(view.edges), unique=True, min_size=1, max_size=2
                ),
                st.booleans(),
            ).map(
                lambda t: {
                    "keep_edges": [_selector(e) for e in t[0]],
                    "keep_inverse_edges": t[1],
                }
            )
        )
    kept_resources = (
        st.one_of(st.none(), _subset(sorted(view.resources)))
        if view.resources
        else st.none()
    )
    out["project_manifest"] = st.tuples(
        st.one_of(keeps), kept_resources, st.sampled_from(["trim", "drop"])
    ).map(
        lambda t: {
            "op": "project_manifest",
            **t[0],
            "keep_resources": t[1],
            "partial_resources": t[2],
        }
    )
    return out


def _is_an_op(payload: dict[str, Any]) -> bool:
    """Whether the op model accepts *payload*.

    A rename onto its own name, a retarget that moves nothing: those are not
    ops, so they are not drawn -- unlike an op that is well formed and refused
    by the manifest it meets, which is a case the laws cover.
    """
    try:
        _op(payload)
    except ValueError:
        return False
    return True


def _op(payload: dict[str, Any]) -> ManifestOp:
    """The op *payload* spells -- ``merge_manifests`` too, which no revision holds."""
    if payload.get("op") == "merge_manifests":
        return MergeManifestsOp.model_validate(payload)
    return op_from_dict(payload)


def ops_for(
    manifest: GraphManifest,
    kind: str | None = None,
    without: Collection[str] = (),
) -> st.SearchStrategy[ManifestOp]:
    """Ops addressed at *manifest*: of one *kind*, or of any kind it admits.

    *without* names kinds a law leaves out of an any-kind draw; each law that
    does so says why in a named mapping beside it.
    """
    payloads = _payloads(manifest)
    if kind is not None:
        assume(kind in payloads)
        chosen = payloads[kind]
    else:
        applied = [
            s
            for k, s in payloads.items()
            if k not in REFUSED_BY_APPLY and k not in without
        ]
        assume(applied)
        chosen = st.one_of(applied)
    return chosen.filter(_is_an_op).map(_op)


@st.composite
def manifest_and_op(
    draw: st.DrawFn, kind: str | None = None
) -> tuple[GraphManifest, ManifestOp]:
    manifest = draw(manifests())
    return manifest, draw(ops_for(manifest, kind))


@st.composite
def manifest_and_two_ops(
    draw: st.DrawFn,
    blocks: bool = True,
    extras: bool = True,
    without: Collection[str] = (),
) -> tuple[GraphManifest, ManifestOp, ManifestOp]:
    """One base and two ops both drawn against it -- two branches, not a sequence."""
    manifest = draw(manifests(blocks=blocks, extras=extras))
    return (
        manifest,
        draw(ops_for(manifest, without=without)),
        draw(ops_for(manifest, without=without)),
    )


@st.composite
def manifest_and_sequence(draw: st.DrawFn) -> tuple[GraphManifest, list[ManifestOp]]:
    """A base and ops each drawn against the state the previous one produced."""
    manifest = draw(manifests())
    current, sequence = manifest, []
    for _ in range(draw(st.integers(min_value=2, max_value=3))):
        op = draw(ops_for(current))
        following = _apply(current, [op])
        assume(following is not None)
        assert following is not None
        current = following
        sequence.append(op)
    return manifest, sequence


# ── pinned cases ────────────────────────────────────────────────────────────


def _doc(manifest: dict[str, Any] | None = None, **schema: Any) -> dict[str, Any]:
    """A manifest payload for a pinned case: one schema graph, and other blocks."""
    profile = schema.pop("db_profile", None)
    graph = {
        "vertex_config": {"vertices": schema.pop("vertices")},
        "edge_config": {
            "edges": schema.pop("edges", []),
            "inverses": schema.pop("inverses", []),
            "symmetric": schema.pop("symmetric", []),
        },
    }
    assert not schema, schema
    doc: dict[str, Any] = {"schema": {"metadata": {"name": "laws"}, "graph": graph}}
    if profile is not None:
        doc["schema"]["db_profile"] = profile
    return {**doc, **(manifest or {})}


def _type(name: str, *fields: str, **extra: Any) -> dict[str, Any]:
    return {"name": name, "properties": ["id", *fields], "identity": ["id"], **extra}


def _edge(source: str, target: str, relation: str, **extra: Any) -> dict[str, Any]:
    return {"source": source, "target": target, "relation": relation, **extra}


def _kinds(
    kinds: list[str], defects: dict[str, tuple[str, dict[str, Any], dict[str, Any]]]
) -> list[Any]:
    """*kinds* as parameters, each in *defects* marked with its mechanism.

    A marked kind's law checks its pinned case first, so the marker fails the
    same way under every profile and turns red once the case holds.
    """
    return [
        pytest.param(
            kind,
            marks=pytest.mark.xfail(
                strict=True, raises=AssertionError, reason=defects[kind][0]
            )
            if kind in defects
            else (),
        )
        for kind in kinds
    ]


def _pinned_case(
    kind: str, defects: dict[str, tuple[str, dict[str, Any], dict[str, Any]]]
) -> tuple[GraphManifest, ManifestOp] | None:
    if kind not in defects:
        return None
    _reason, manifest, payload = defects[kind]
    return GraphManifest.from_dict(manifest), _op(payload)


# ── outcomes ────────────────────────────────────────────────────────────────


def _apply(manifest: GraphManifest, ops: list[ManifestOp]) -> GraphManifest | None:
    """The manifest *ops* produce, or ``None`` when one of them is refused.

    Only a refusal counts: any other exception is a crash, and fails the law.
    """
    try:
        return apply_evolution(manifest, ops, bump_version=False, finish_init=False)
    except OpRefusal:
        return None


def _outcome(manifest: GraphManifest, ops: list[ManifestOp]) -> str:
    applied = _apply(manifest, ops)
    return "refused" if applied is None else manifest_hash(applied)


def _merge_outcome(
    base: GraphManifest, left: GraphManifest, right: GraphManifest
) -> tuple[str, Any]:
    try:
        merged, result = merge_three_way(base, left, right)
    except MergeError:
        return ("error", None)
    if merged is None:
        return ("conflict", sorted(tuple(c.slot) for c in result.conflicts))
    return ("clean", manifest_hash(merged))


def _two_branches(
    case: tuple[GraphManifest, ManifestOp, ManifestOp],
) -> tuple[GraphManifest, GraphManifest, GraphManifest]:
    base, a, b = case
    left, right = _apply(base, [a]), _apply(base, [b])
    assume(left is not None and right is not None)
    assert left is not None and right is not None
    return base, left, right


# ── preservation ────────────────────────────────────────────────────────────


@pytest.mark.parametrize("kind", sorted(REFUSED_BY_APPLY))
@PER_KIND
@given(data=st.data())
def test_a_binary_op_is_refused_where_ops_apply(kind, data) -> None:
    """Whatever the manifest, ``apply`` refuses it as an op it does not run."""
    manifest, op = data.draw(manifest_and_op(kind))

    with pytest.raises(OpRefusal) as refused:
        apply_evolution(manifest, [op], bump_version=False, finish_init=False)
    assert refused.value.check == "invalid-op"


#: Kinds an applied op of which leaves a manifest ``finish_init`` changes or
#: refuses: the mechanism, and the smallest manifest and op that show it.
DOES_NOT_LOAD: dict[str, tuple[str, dict[str, Any], dict[str, Any]]] = {}


def _loads(manifest: GraphManifest, op: ManifestOp) -> None:
    raw = _apply(manifest, [op])
    assume(raw is not None)
    assert raw is not None
    try:
        finished = apply_evolution(manifest, [op], bump_version=False, finish_init=True)
    except OpRefusal as refusal:
        raise AssertionError(f"finish_init refuses what {op.op} left: {refusal}")

    assert manifest_hash(finished) == manifest_hash(raw)
    assert manifest_hash(GraphManifest.from_dict(finished.to_dict())) == manifest_hash(
        finished
    )


@pytest.mark.parametrize("kind", _kinds(APPLIED_KINDS, DOES_NOT_LOAD))
@PER_KIND
@given(data=st.data())
def test_an_op_that_applies_leaves_a_manifest_that_loads(kind, data) -> None:
    """Well-formed in, well-formed out -- and ``finish_init`` adds nothing.

    Inversion and diffing run with ``finish_init=False``, so they are only sound
    if an applied op never depends on that pass to repair its result.
    """
    if pinned := _pinned_case(kind, DOES_NOT_LOAD):
        _loads(*pinned)
    _loads(*data.draw(manifest_and_op(kind)))


# ── inversion ───────────────────────────────────────────────────────────────


@pytest.mark.parametrize("kind", REVERSIBLE_KINDS)
@PER_KIND
@given(data=st.data())
def test_an_inverse_restores_the_manifest(kind, data) -> None:
    """Exact or absent: an inverse that is offered lands back on the pre-state."""
    manifest, op = data.draw(manifest_and_op(kind))
    forward = _apply(manifest, [op])
    assume(forward is not None)
    assert forward is not None
    inverse = invert_op(op, manifest=manifest)
    assume(inverse is not None)
    assert inverse is not None

    restored = _apply(forward, [inverse])

    assert restored is not None, f"the inverse of {op.op} is refused"
    assert manifest_hash(restored) == manifest_hash(manifest)


@LAWS
@given(manifest_and_sequence())
def test_a_sequence_inverts_in_reverse(case) -> None:
    manifest, sequence = case
    inverses, blockers = invert_ops(sequence, manifest=manifest)
    assume(not blockers)
    forward = _apply(manifest, sequence)
    assert forward is not None

    restored = _apply(forward, inverses)

    assert restored is not None, "the inverse sequence is refused"
    assert manifest_hash(restored) == manifest_hash(manifest)


# ── diffing ─────────────────────────────────────────────────────────────────


def _registry(manifest: GraphManifest) -> list[str]:
    ingestion = manifest.ingestion_model
    return sorted(t.name or "" for t in ingestion.transforms) if ingestion else []


_EMAIL_KEYED = _type(
    "person", "email", secondary_identities=[{"name": "email_key", "fields": ["email"]}]
)

#: Pairs of manifests whose diff does not replay to the target, each with the
#: mechanism; the pinned test below turns red once a case holds.
DIFF_DEFECTS: dict[str, tuple[str, dict[str, Any], dict[str, Any]]] = {}

#: The scopes the diff law is drawn in.
DIFF_SCOPES = list(SCOPES)


def _diff_replays(base: GraphManifest, target: GraphManifest) -> None:
    """The diff replays to *target*, or says where no op can express the change.

    Two differences have no op: dropping the ingestion block whole, and a
    transform registry entry added or removed with no resource step bringing
    it. Outside those the diff replays exactly with no residue; inside them it
    either still replays or says that it does not -- it never passes an
    incomplete change set as complete.
    """
    ops, warnings = diff_manifests_verified(base, target)
    dropped = base.ingestion_model is not None and target.ingestion_model is None
    registry_moved = target.ingestion_model is not None and _registry(
        base
    ) != _registry(target)
    replayed = _outcome(base, ops) == manifest_hash(target)

    if dropped or registry_moved:
        assert warnings or replayed
    else:
        assert not warnings
        assert replayed


@pytest.mark.parametrize("scope", DIFF_SCOPES)
@LAWS
@given(data=st.data())
def test_a_diff_replays_to_its_target(scope, data) -> None:
    base = data.draw(manifests(**SCOPES[scope]))
    target = data.draw(manifests(**SCOPES[scope]))

    _diff_replays(base, target)


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(
            case,
            marks=pytest.mark.xfail(strict=True, raises=AssertionError, reason=reason),
            id=key,
        )
        for key, (reason, *case) in DIFF_DEFECTS.items()
    ],
)
def test_a_pinned_diff_replays_to_its_target(case) -> None:
    base, target = case
    _diff_replays(GraphManifest.from_dict(base), GraphManifest.from_dict(target))


@LAWS
@given(manifests())
def test_a_manifest_differs_from_itself_by_nothing(manifest) -> None:
    assert diff_manifests(manifest, manifest) == ([], [])


# ── commutation and merge ───────────────────────────────────────────────────


def _pinned(edges: list[dict[str, Any]], inverses: list[dict[str, str]] | None = None):
    """A two-type manifest for a pinned case the generator rarely reaches."""
    return GraphManifest.from_dict(
        {
            "schema": {
                "metadata": {"name": "laws"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": n,
                                "properties": [{"name": "id"}],
                                "identity": ["id"],
                            }
                            for n in ("person", "company")
                        ]
                    },
                    "edge_config": {"edges": edges, "inverses": inverses or []},
                },
            }
        }
    )


SELF_KNOWS = {"source": "company", "target": "company", "relation": "knows"}
PERSON_KNOWS = {"source": "person", "target": "person", "relation": "knows"}

#: A relation-wide edge property op and an edge added to that relation: the op
#: reaches the new edge in one order only.
GROWN_RELATION = (
    _pinned([PERSON_KNOWS]),
    op_from_dict({"op": "add_edge_properties", "additions": {"knows": ["weight"]}}),
    op_from_dict({"op": "add_edges", "edges": [SELF_KNOWS]}),
)

#: A whole-manifest op and a change to a type it drops: one order refuses.
PROJECTED_AWAY = (
    _pinned([PERSON_KNOWS]),
    op_from_dict({"op": "project_manifest", "keep_vertices": ["person"]}),
    op_from_dict({"op": "add_vertex_properties", "additions": {"company": ["name"]}}),
)

#: Realizing every declared inverse and an edge added to a paired relation:
#: the new edge gets its inverse in one order only.
REALIZED_ALL = (
    _pinned(
        [{"source": "person", "target": "company", "relation": "knows"}],
        [{"relation": "knows", "inverse": "known_by"}],
    ),
    op_from_dict({"op": "add_inverse_edges"}),
    op_from_dict({"op": "add_edges", "edges": [SELF_KNOWS]}),
)


# Pairs of ops drawn against one base that once broke a law over them, each
# checked as a regression case beside the drawn ones.

#: A profile naming an edge, set on one side, and the edge removed on the other.
PROFILE_OVER_A_REMOVED_EDGE = (
    GraphManifest.from_dict(
        _doc(
            vertices=[_type("person")],
            edges=[{**PERSON_KNOWS, "properties": ["weight"]}],
        )
    ),
    op_from_dict({"op": "remove_vertices", "names": ["person"]}),
    op_from_dict(
        {
            "op": "set_db_profile",
            "profile": {
                "db_flavor": "neo4j",
                "edge_specs": [{**PERSON_KNOWS, "indexes": [{"fields": ["weight"]}]}],
            },
        }
    ),
)
#: An index named by a profile, and the type it indexes renamed.
PROFILE_BESIDE_A_RENAME = (
    GraphManifest.from_dict(_doc(vertices=[_type("person", "name")])),
    op_from_dict({"op": "rename_vertices", "renames": {"person": "company"}}),
    op_from_dict(
        {
            "op": "set_db_profile",
            "profile": {"vertex_indexes": {"person": [{"fields": ["name"]}]}},
        }
    ),
)

#: A lookup key withdrawn, and an index added on its fields.
INDEX_OVER_A_WITHDRAWN_KEY = (
    GraphManifest.from_dict(_doc(vertices=[_EMAIL_KEYED])),
    op_from_dict(
        {"op": "remove_secondary_identities", "removals": {"person": ["email_key"]}}
    ),
    op_from_dict(
        {"op": "add_vertex_indexes", "indexes": {"person": [{"fields": ["email"]}]}}
    ),
)

#: Bindings wiring a resource the manifest lacks, and an unrelated removal.
BINDINGS_BESIDE_A_REMOVAL = (
    GraphManifest.from_dict(
        _doc(
            {
                "ingestion_model": {
                    "resources": [{"name": "rows", "pipeline": [{"vertex": "person"}]}]
                }
            },
            vertices=[_type("person"), _type("company")],
        )
    ),
    op_from_dict({"op": "remove_vertices", "names": ["company"]}),
    _op({"op": "set_bindings", "bindings": _bindings(["rows", "feed"])}),
)

#: Two removals whose cascades each trim one pipeline.
TWO_CASCADES_INTO_ONE_PIPELINE = (
    GraphManifest.from_dict(
        _doc(
            {
                "ingestion_model": {
                    "resources": [
                        {"name": "rows", "pipeline": [{"vertex": "person"}]},
                        {
                            "name": "feed",
                            "pipeline": [{"vertex": "person"}, {"vertex": "company"}],
                        },
                    ]
                }
            },
            vertices=[_type("person"), _type("company")],
        )
    ),
    op_from_dict({"op": "remove_vertices", "names": ["company"]}),
    op_from_dict({"op": "remove_vertices", "names": ["person"]}),
)


#: A type added on one side, and its names recorded for a flavor on the other.
SANITIZED_BESIDE_A_NEW_TYPE = (
    GraphManifest.from_dict(
        _doc(vertices=[_type("person"), _type("company"), _type("city")])
    ),
    op_from_dict({"op": "add_vertices", "vertices": [_type("order")]}),
    op_from_dict({"op": "sanitize", "db_flavor": "tigergraph"}),
)

#: A type added on one side, and a relation's edges made undirected on the other.
UNDIRECTED_BESIDE_A_NEW_TYPE = (
    GraphManifest.from_dict(
        _doc(
            vertices=[_type("person"), _type("company"), _type("city")],
            edges=[
                _edge("person", "person", "employs"),
                _edge("person", "company", "employs"),
            ],
        )
    ),
    op_from_dict({"op": "add_vertices", "vertices": [_type("order")]}),
    op_from_dict(
        {
            "op": "set_edge_directed",
            "edges": [
                _edge("person", "person", "employs"),
                _edge("person", "company", "employs"),
            ],
            "directed": False,
        }
    ),
)

#: An edge property added relation-wide, and its grounding set on one edge.
GROUNDING_BESIDE_ITS_PROPERTY = (
    GraphManifest.from_dict(
        _doc(
            vertices=[_type("person"), _type("company")],
            edges=[_edge("person", "person", "knows")],
        )
    ),
    op_from_dict({"op": "add_edge_properties", "additions": {"knows": ["weight"]}}),
    op_from_dict(
        {
            "op": "set_field_semantics",
            "targets": [{**_edge("person", "person", "knows"), "field": "weight"}],
        }
    ),
)

#: A relation's first edge added, and a pair declared for that relation.
DECLARED_BESIDE_THE_FIRST_EDGE = (
    GraphManifest.from_dict(_doc(vertices=[_type("person")])),
    op_from_dict({"op": "add_edges", "edges": [_edge("person", "person", "knows")]}),
    op_from_dict({"op": "declare_edge_inverses", "inverses": {"knows": "employs"}}),
)

#: Kinds left out of the commute law's draw, with the mechanism; each returns
#: once a pinned case for it holds.
COMMUTE_WITHOUT: dict[str, str] = {}
#: Kinds left out of the clean-merge law's draw, likewise.
CLEAN_MERGE_WITHOUT: dict[str, str] = {}


def _pinned_pairs(*cases: tuple[str, Any, str]) -> list[Any]:
    """``(id, case, mechanism)`` as strict-xfail parameters."""
    return [
        pytest.param(
            case,
            marks=pytest.mark.xfail(strict=True, raises=AssertionError, reason=reason),
            id=key,
        )
        for key, case, reason in cases
    ]


def _cases(*cases: tuple[str, Any]) -> list[Any]:
    """``(id, case)`` as parameters."""
    return [pytest.param(case, id=key) for key, case in cases]


def _holds(law: Any, case: Any) -> None:
    """*law* on one case: it holds, or the case falls outside its premise."""
    try:
        law(case)
    except UnsatisfiedAssumption:
        pass


#: The scopes the commute, merge-symmetry and clean-merge laws are drawn in.
MERGE_SCOPES = ["schema", "keys", "ingestion", "all"]


def _two_ops(
    scope: str, without: Collection[str] = ()
) -> st.SearchStrategy[tuple[GraphManifest, ManifestOp, ManifestOp]]:
    reach = SCOPES[scope]
    return manifest_and_two_ops(
        blocks=reach["blocks"], extras=reach["extras"], without=without
    )


def _commutes(case: tuple[GraphManifest, ManifestOp, ManifestOp]) -> None:
    manifest, a, b = case
    assume(ops_independent(a, b, manifest))

    assert _outcome(manifest, [a, b]) == _outcome(manifest, [b, a])


@pytest.mark.parametrize("scope", MERGE_SCOPES)
@LAWS
@given(data=st.data())
def test_independent_ops_commute(scope, data) -> None:
    """Neither writes where the other writes or reads: one outcome, either order."""
    _commutes(data.draw(_two_ops(scope, COMMUTE_WITHOUT)))


@pytest.mark.parametrize(
    "case",
    _cases(
        ("grown relation", GROWN_RELATION),
        ("projected away", PROJECTED_AWAY),
        ("realized all", REALIZED_ALL),
        ("index over a withdrawn key", INDEX_OVER_A_WITHDRAWN_KEY),
        ("bindings beside a removal", BINDINGS_BESIDE_A_REMOVAL),
        ("declared beside the first edge", DECLARED_BESIDE_THE_FIRST_EDGE),
        ("profile beside a rename", PROFILE_BESIDE_A_RENAME),
        ("sanitized beside a new type", SANITIZED_BESIDE_A_NEW_TYPE),
        ("grounding beside its property", GROUNDING_BESIDE_ITS_PROPERTY),
    ),
)
def test_pinned_independent_ops_commute(case) -> None:
    _holds(_commutes, case)


def _footprints_are_stable(case: tuple[GraphManifest, ManifestOp, ManifestOp]) -> None:
    manifest, a, b = case
    assume(ops_independent(a, b, manifest))
    checked = 0
    for first, second in ((a, b), (b, a)):
        after = _apply(manifest, [first])
        if after is None:
            continue
        checked += 1
        assert op_slots(second, after) == op_slots(second, manifest), (
            f"{first.op} moved what {second.op} writes"
        )
        assert op_reads(second, after) == op_reads(second, manifest), (
            f"{first.op} moved what {second.op} reads"
        )
    if currently_in_test_context():
        event(f"orders checked={checked}")


@pytest.mark.parametrize("scope", MERGE_SCOPES)
@LAWS
@given(data=st.data())
def test_an_independent_op_leaves_the_others_footprint_alone(scope, data) -> None:
    """Footprints taken against the base hold after an independent op runs.

    Independence is judged on each op's footprint against the base; that is
    sound only if neither op moves the other's footprint, so the judgement
    still holds at the state the second op actually meets.
    """
    _footprints_are_stable(data.draw(_two_ops(scope, COMMUTE_WITHOUT)))


def _merge_is_symmetric(case: tuple[GraphManifest, ManifestOp, ManifestOp]) -> None:
    base, left, right = _two_branches(case)

    assert _merge_outcome(base, left, right) == _merge_outcome(base, right, left)


@pytest.mark.parametrize("scope", MERGE_SCOPES)
@LAWS
@given(data=st.data())
def test_merge_does_not_depend_on_which_side_is_left(scope, data) -> None:
    _merge_is_symmetric(data.draw(_two_ops(scope)))


@pytest.mark.parametrize(
    "case",
    _cases(
        ("grown relation", GROWN_RELATION),
        ("profile over a removed edge", PROFILE_OVER_A_REMOVED_EDGE),
        ("bindings beside a removal", BINDINGS_BESIDE_A_REMOVAL),
        ("two cascades into one pipeline", TWO_CASCADES_INTO_ONE_PIPELINE),
    ),
)
def test_pinned_merge_does_not_depend_on_which_side_is_left(case) -> None:
    _holds(_merge_is_symmetric, case)


def _clean_merge_loses_nothing(
    case: tuple[GraphManifest, ManifestOp, ManifestOp],
) -> None:
    """A clean merge is each side's change applied on top of the other.

    That is what "nothing was lost" means for a merge, and it is checkable: what
    the left changed and the right did not must apply to the right and land on
    the merged manifest, and the other way round. A change both sides made is
    agreement and is already in both. A merge that is clean and fails this
    dropped something.
    """
    base, left, right = _two_branches(case)
    left_ops, left_residue = diff_manifests_verified(base, left)
    right_ops, right_residue = diff_manifests_verified(base, right)
    # A change no op expresses cannot be merged, and the merge says so.
    assume(not left_residue and not right_residue)
    kind, merged_hash = _merge_outcome(base, left, right)
    assert kind != "error", "two expressible descendants must merge or conflict"
    assume(kind == "clean")

    # Agreement is per slot, as the merge decides it: the differ spells each
    # side as one op per kind, so a change both made sits inside ops that differ.
    left_ops = [piece for op in left_ops for piece in split_by_slot(op)]
    right_ops = [piece for op in right_ops for piece in split_by_slot(op)]
    left_dicts, right_dicts = ops_to_dicts(left_ops), ops_to_dicts(right_ops)
    left_only = [op for op, d in zip(left_ops, left_dicts) if d not in right_dicts]
    right_only = [op for op, d in zip(right_ops, right_dicts) if d not in left_dicts]

    assert _outcome(right, left_only) == merged_hash
    assert _outcome(left, right_only) == merged_hash


@pytest.mark.parametrize("scope", MERGE_SCOPES)
@LAWS
@given(data=st.data())
def test_a_clean_merge_loses_neither_side(scope, data) -> None:
    _clean_merge_loses_nothing(data.draw(_two_ops(scope, CLEAN_MERGE_WITHOUT)))


@pytest.mark.parametrize(
    "case",
    _cases(
        ("grown relation", GROWN_RELATION),
        ("profile over a removed edge", PROFILE_OVER_A_REMOVED_EDGE),
        ("two cascades into one pipeline", TWO_CASCADES_INTO_ONE_PIPELINE),
        ("undirected beside a new type", UNDIRECTED_BESIDE_A_NEW_TYPE),
    ),
)
def test_a_pinned_clean_merge_loses_neither_side(case) -> None:
    _holds(_clean_merge_loses_nothing, case)


# ── a decision is local ─────────────────────────────────────────────────────


#: Kinds whose pieces do not land where the whole op lands.
SPLIT_APART: dict[str, tuple[str, dict[str, Any], dict[str, Any]]] = {}

#: Cases whose pieces once did not land where the whole op lands, checked
#: before the drawn ones: the manifest, and the op.
SPLIT_CASES: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {
    # Every edge of one relation must agree on `directed`.
    "set_edge_directed": (
        _doc(
            vertices=[_type("person"), _type("company")],
            edges=[
                _edge("person", "person", "knows"),
                _edge("person", "company", "knows"),
            ],
        ),
        {
            "op": "set_edge_directed",
            "edges": [
                _edge("person", "person", "knows"),
                _edge("person", "company", "knows"),
            ],
            "directed": False,
        },
    ),
}


def _split_is_the_op(manifest: GraphManifest, op: ManifestOp) -> None:
    assert _outcome(manifest, split_by_slot(op)) == _outcome(manifest, [op])


@pytest.mark.parametrize("kind", _kinds(KINDS, SPLIT_APART))
@PER_KIND
@given(data=st.data())
def test_an_op_split_by_slot_is_the_op(kind, data) -> None:
    """Its pieces, applied in order, land where the op lands -- or are refused."""
    if pinned := _pinned_case(kind, SPLIT_APART):
        _split_is_the_op(*pinned)
    if kind in SPLIT_CASES:
        manifest, payload = SPLIT_CASES[kind]
        _split_is_the_op(GraphManifest.from_dict(manifest), _op(payload))
    _split_is_the_op(*data.draw(manifest_and_op(kind)))


def _describe_vertex(vertex: str, text: str) -> dict[str, Any]:
    return {"op": "set_vertex_descriptions", "descriptions": {vertex: text}}


def _retype(vertex: str, field: str, type_: str) -> dict[str, Any]:
    return {"op": "change_field_types", "vertices": {vertex: {field: {"type": type_}}}}


def _triple(edge: tuple[str, str, str]) -> dict[str, str]:
    return {"source": edge[0], "target": edge[1], "relation": edge[2]}


def _describe_edge(edge: tuple[str, str, str], text: str) -> dict[str, Any]:
    return {
        "op": "set_edge_descriptions",
        "edges": [{**_triple(edge), "description": text}],
    }


def _contention(
    manifest: GraphManifest, element: tuple[str, str]
) -> st.SearchStrategy[tuple[dict[str, Any], dict[str, Any]]]:
    """Two payloads that write one slot of *element* differently, in either order.

    Only pairs that collide whenever both apply: a rename or an edit of another
    field never meets its counterpart, so it would not make a conflict.
    """
    core = _core(manifest)
    kind, name = element
    texts = st.lists(
        st.text("ab", min_size=1, max_size=3), min_size=2, max_size=2, unique=True
    )
    if kind == "vertex":
        pairs = [
            texts.map(
                lambda t: (_describe_vertex(name, t[0]), _describe_vertex(name, t[1]))
            ),
            texts.map(
                lambda t: (
                    {"op": "remove_vertices", "names": [name]},
                    _describe_vertex(name, t[0]),
                )
            ),
        ]
        fields = [
            f.name
            for v in core.vertex_config.vertices
            if v.name == name
            for f in v.properties
            if f.name != "id"
        ]
        if fields:
            field = st.sampled_from(fields)
            pairs += [
                field.map(
                    lambda f: (_retype(name, f, "STRING"), _retype(name, f, "INT"))
                ),
                st.tuples(field, st.sampled_from(["STRING", "INT"])).map(
                    lambda t: (
                        {"op": "remove_vertex_properties", "removals": {name: [t[0]]}},
                        _retype(name, t[0], t[1]),
                    )
                ),
            ]
    else:
        edge = st.sampled_from(
            [
                (e.source, e.target, e.relation)
                for e in core.edge_config.edges
                if e.relation == name
            ]
        )
        pairs = [
            st.tuples(edge, texts).map(
                lambda t: (_describe_edge(t[0], t[1][0]), _describe_edge(t[0], t[1][1]))
            ),
            st.tuples(edge, texts).map(
                lambda t: (
                    {"op": "remove_edges", "edges": [_triple(t[0])]},
                    _describe_edge(t[0], t[1][0]),
                )
            ),
        ]
    return st.tuples(st.one_of(pairs), st.booleans()).map(
        lambda t: (t[0][1], t[0][0]) if t[1] else t[0]
    )


def _pipeline_names(resource: Any) -> set[footprint.Slot]:
    """The vertex and relation slots a resource's pipeline produces into."""
    return {footprint.vertex_slot(name) for name in resource.collect_vertex_names()} | {
        footprint.relation_slot(relation)
        for view in iter_edge_steps(list(resource.pipeline))
        for relation in view.relations_written() or set()
    }


def _step(manifest: GraphManifest, payload: dict[str, Any]) -> GraphManifest:
    """*manifest* after *payload*; itself when the op is malformed or refused."""
    if not _is_an_op(payload):
        return manifest
    return _apply(manifest, [_op(payload)]) or manifest


@st.composite
def manifest_and_two_contending_branches(
    draw: st.DrawFn, blocks: bool = True, extras: bool = True
) -> tuple[GraphManifest, GraphManifest, GraphManifest]:
    """A base and two descendants that collide on two or three elements.

    The collisions are built, not filtered for. Each branch also takes up to one
    unrelated op: that is what a decision must not carry along with it.
    """
    base = draw(manifests(min_vertices=2, blocks=blocks, extras=extras))
    core = _core(base)
    elements = [("vertex", v) for v in sorted(core.vertex_config.vertex_set)] + [
        ("relation", r)
        for r in sorted({e.relation for e in core.edge_config.edges if e.relation})
    ]
    hot = draw(st.lists(st.sampled_from(elements), min_size=2, max_size=3, unique=True))
    left = right = base
    for element in hot:
        a, b = draw(_contention(base, element))
        left, right = _step(left, a), _step(right, b)

    def unrelated(branch: GraphManifest) -> GraphManifest:
        payloads = [
            s for k, s in _payloads(branch).items() if k not in REFUSED_BY_APPLY
        ]
        if not payloads or not draw(st.booleans()):
            return branch
        return _step(branch, draw(st.one_of(payloads)))

    return base, unrelated(left), unrelated(right)


def _pipeline_of(name: str, *steps: dict[str, Any]) -> dict[str, Any]:
    return {"name": name, "pipeline": list(steps)}


_FROM_EMAIL = {"vertex": "person", "from": {"name": "email"}}

_ROWS = _pipeline_of("rows", {"vertex": "person"})
_FEED_CITY = _pipeline_of("feed", {"vertex": "person"}, {"vertex": "city"})
_FEED_COMPANY = _pipeline_of("feed", {"vertex": "person"}, {"vertex": "company"})
_EMAIL_FED = {
    "ingestion_model": {
        "resources": [
            _pipeline_of("rows", _FROM_EMAIL),
            _pipeline_of("feed", _FROM_EMAIL, {"vertex": "city"}),
        ]
    }
}

#: Three-way merges where flipping one decision once moved something outside
#: what that conflict reaches: a base, a left and a right.
DECISION_CASES: dict[str, tuple[dict[str, Any], dict[str, Any], dict[str, Any]]] = {
    # A conflict on one resource held the side's other resources as well.
    "resources replaced together": (
        _doc(
            {"ingestion_model": {"resources": [_ROWS, _FEED_CITY]}},
            vertices=[_type("person", "email"), _type("company"), _type("city")],
        ),
        _doc(
            {"ingestion_model": {"resources": [_ROWS, _FEED_COMPANY]}},
            vertices=[
                _type("person", "email"),
                _type("company", description="a"),
                _type("city"),
            ],
        ),
        _doc(
            _EMAIL_FED,
            vertices=[
                _type("person", "name"),
                _type("company", description="aa"),
                _type("city", description="b"),
            ],
        ),
    ),
    # A superclass set under a contested ancestor followed that ancestor's
    # decision, not its own.
    "one op held by two conflicts": (
        _doc(
            vertices=[
                _type("order", "name"),
                _type("city", "name"),
                _type("company", subclass_of=[{"vertex": "city"}]),
            ]
        ),
        _doc(
            vertices=[
                {
                    **_type("order"),
                    "properties": ["id", {"name": "name", "type": "STRING"}],
                },
                _type("company"),
            ]
        ),
        _doc(
            vertices=[
                {
                    **_type("order", subclass_of=[{"vertex": "company"}]),
                    "properties": ["id", {"name": "name", "type": "INT"}],
                },
                {
                    **_type("city"),
                    "properties": ["id", {"name": "name", "type": "INT"}],
                },
                _type("company", subclass_of=[{"vertex": "city"}]),
            ]
        ),
    ),
    # A resource conflict holds a type removal whose cascade trims it; the
    # conflict reaches the type, so withholding the removal stays inside it.
    "removal held by a resource conflict": (
        _doc(
            {"ingestion_model": {"resources": [_ROWS, _FEED_CITY]}},
            vertices=[_type("person", "email"), _type("company"), _type("city")],
        ),
        _doc(
            {"ingestion_model": {"resources": [_ROWS, _FEED_COMPANY]}},
            vertices=[_type("person", "email"), _type("company", description="a")],
        ),
        _doc(
            _EMAIL_FED,
            vertices=[
                _type("person", "name"),
                _type("company", description="aa"),
                _type("city", description="b"),
            ],
        ),
    ),
}

#: Three-way merges where flipping one decision moves something outside what
#: that conflict reaches, each with the mechanism.
DECISION_DEFECTS: dict[
    str, tuple[str, dict[str, Any], dict[str, Any], dict[str, Any]]
] = {}

#: The scopes the decision law is drawn in.
DECISION_SCOPES = list(SCOPES)


@pytest.mark.parametrize("scope", DECISION_SCOPES)
@LAWS
@given(data=st.data())
def test_a_decision_changes_nothing_outside_its_conflict(scope, data) -> None:
    """Flipping one conflict's decision moves only what that conflict reaches.

    Each side's change set is one op per kind for the whole manifest; a decision
    that held a whole op would take the side's unrelated edits along with it.
    """
    _decision_is_local(
        *data.draw(manifest_and_two_contending_branches(**SCOPES[scope]))
    )


@pytest.mark.parametrize(
    "case",
    [pytest.param(case, id=key) for key, case in DECISION_CASES.items()]
    + [
        pytest.param(
            case,
            marks=pytest.mark.xfail(strict=True, raises=AssertionError, reason=reason),
            id=key,
        )
        for key, (reason, *case) in DECISION_DEFECTS.items()
    ],
)
def test_a_pinned_decision_changes_nothing_outside_its_conflict(case) -> None:
    _holds(
        lambda docs: _decision_is_local(*(GraphManifest.from_dict(d) for d in docs)),
        case,
    )


def _decision_is_local(
    base: GraphManifest, left: GraphManifest, right: GraphManifest
) -> None:
    try:
        _none, result = merge_three_way(base, left, right)
    except MergeError:
        assume(False)
        return
    if currently_in_test_context():
        event(f"conflicts={min(len(result.conflicts), 3)}")
    assume(len(result.conflicts) >= 2)

    def merged(flip: int | None) -> GraphManifest | None:
        decisions = [
            take_right(c) if i == flip else take_left(c)
            for i, c in enumerate(result.conflicts)
        ]
        try:
            return merge_three_way(base, left, right, resolutions=decisions)[0]
        except MergeError:
            return None

    all_left = merged(None)
    assume(all_left is not None)
    assert all_left is not None
    for flip, conflict in enumerate(result.conflicts):
        flipped = merged(flip)
        if flipped is None:
            continue
        # Stated per element (a vertex, a relation): the diff may spell what
        # moved with a different op than the one that moved it -- an edge added
        # with a property reads back as that property -- but never on another
        # element. A conflict reaches every element an op it holds writes,
        # footprints taken against the base: the decision is what applies or
        # withholds those ops, and the conflict shows them. A vertex reaches the
        # relations incident to it, since removing it removes their edges.
        reach = {tuple(conflict.slot)[:2]} | {
            slot[:2]
            for op in [*conflict.left_ops, *conflict.right_ops]
            for slot in op_slots(op, base)
        }
        manifests = (base, left, right, all_left, flipped)
        # Removing a vertex or an edge splices it out of the hierarchy, so it
        # also reaches what was directly under it.
        reach |= {
            footprint.vertex_slot(vertex.name)
            for manifest in manifests
            for vertex in _core(manifest).vertex_config.vertices
            if any(
                p.vertex is not None and footprint.vertex_slot(p.vertex) in reach
                for p in vertex.subclass_of
            )
        }
        reach |= {
            footprint.relation_slot(edge.relation)
            for manifest in manifests
            for edge in _core(manifest).edge_config.edges
            if edge.relation is not None
            and {footprint.vertex_slot(edge.source), footprint.vertex_slot(edge.target)}
            & reach
        }
        reach |= {
            footprint.relation_slot(edge.relation)
            for manifest in manifests
            for edge in _core(manifest).edge_config.edges
            if edge.relation is not None
            and any(
                p.edge is not None
                and p.edge.relation is not None
                and footprint.relation_slot(p.edge.relation) in reach
                for p in edge.subproperty_of
            )
        }
        # A declared inverse pair is one fact about two relations: restoring or
        # dropping it with one side reaches the other.
        reach |= {
            footprint.relation_slot(name)
            for manifest in manifests
            for pair in _core(manifest).edge_config.inverses
            if {
                footprint.relation_slot(pair.relation),
                footprint.relation_slot(pair.inverse),
            }
            & reach
            for name in (pair.relation, pair.inverse)
        }
        # A pipeline names the types and relations it produces, and removing
        # one trims every pipeline naming it.
        reach |= {
            footprint.resource_slot(resource.name)
            for manifest in manifests
            if manifest.ingestion_model is not None
            for resource in manifest.ingestion_model.resources
            if _pipeline_names(resource) & reach
        }
        moved, _residue = diff_manifests(all_left, flipped)
        for op in moved:
            assert {slot[:2] for slot in op_slots(op, all_left)} <= reach, (
                f"flipping {conflict.slot} moved {op.op} outside it"
            )
