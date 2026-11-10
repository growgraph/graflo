"""The vocabulary layer: labels and SKOS links, class and relation hierarchies, base IRI and prefixes."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from graflo.architecture.contract import GraphManifest
from graflo.architecture.evolution import apply_evolution
from graflo.architecture.evolution.autogenerate import diff_manifests_verified
from graflo.architecture.evolution.codec import op_from_dict
from graflo.architecture.evolution.hashing import manifest_hash
from graflo.architecture.evolution.inverse import invert_op
from graflo.architecture.evolution.merge import merge_manifests
from graflo.architecture.evolution.merge3 import merge_three_way
from graflo.architecture.evolution.ops import (
    MergeManifestsOp,
    SetEdgeSubpropertyOfOp,
    SetVertexSubclassOfOp,
)
from graflo.architecture.schema.edge import EdgeRef, SuperProperty
from graflo.architecture.schema.hierarchy import (
    Hierarchy,
    new_subsumptions,
    vertex_iri,
)
from graflo.architecture.schema.metadata import GraphMetadata
from graflo.architecture.schema.projection import build_subschema, select_induced
from graflo.architecture.schema.semantics import Semantics, merge_semantics
from graflo.architecture.schema.vertex import SuperClass

AGENT = "http://xmlns.com/foaf/0.1/Agent"
RELATED = "http://www.w3.org/2004/02/skos/core#related"


def _vertex(name: str, *parents: dict[str, str], **extra: Any) -> dict[str, Any]:
    vertex: dict[str, Any] = {
        "name": name,
        "properties": ["id"],
        "identity": ["id"],
        **extra,
    }
    if parents:
        vertex["subclass_of"] = list(parents)
    return vertex


def _edge(
    source: str, target: str, relation: str, *parents: dict[str, Any], **extra: Any
) -> dict[str, Any]:
    edge: dict[str, Any] = {
        "source": source,
        "target": target,
        "relation": relation,
        **extra,
    }
    if parents:
        edge["subproperty_of"] = list(parents)
    return edge


def _under(source: str, target: str, relation: str) -> dict[str, Any]:
    return {"edge": {"source": source, "target": target, "relation": relation}}


def _manifest(
    *vertices: dict[str, Any],
    edges: list[dict[str, Any]] | None = None,
    **metadata: Any,
) -> GraphManifest:
    manifest = GraphManifest.model_validate(
        {
            "schema": {
                "metadata": {"name": "vocab", "version": "1.0.0", **metadata},
                "graph": {
                    "vertex_config": {"vertices": list(vertices)},
                    "edge_config": {"edges": edges or []},
                },
            }
        }
    )
    manifest.finish_init()
    return manifest


def _apply(manifest: GraphManifest, *ops: dict[str, Any]) -> GraphManifest:
    return apply_evolution(
        manifest, [op_from_dict(op) for op in ops], bump_version=False
    )


def _core(manifest: GraphManifest) -> Any:
    assert manifest.graph_schema is not None
    return manifest.graph_schema.core_schema


def _parents(manifest: GraphManifest, name: str) -> list[SuperClass]:
    return next(
        v for v in _core(manifest).vertex_config.vertices if v.name == name
    ).subclass_of


def _edge_parents(
    manifest: GraphManifest, edge_id: tuple[str, str, str]
) -> list[SuperProperty]:
    return _core(manifest).edge_config.edge_for(edge_id).subproperty_of


def _ref(source: str, target: str, relation: str) -> SuperProperty:
    return SuperProperty(edge=EdgeRef(source=source, target=target, relation=relation))


# A small hierarchy in which classes and relations both have parents.
ASSETS = [
    _vertex("asset"),
    _vertex("machine", {"vertex": "asset"}),
    _vertex("press", {"vertex": "machine"}, {"iri": AGENT}),
    _vertex("controller", {"vertex": "asset"}),
]
RELATIONS = [
    _edge("asset", "asset", "depends_on"),
    _edge("controller", "press", "mounted_on", _under("asset", "asset", "depends_on")),
]


# -- contract ---------------------------------------------------------------------


def test_a_parent_names_exactly_one_target_and_an_iri_is_absolute() -> None:
    assert SuperClass(vertex="party").vertex == "party"
    assert SuperClass(iri=AGENT).iri == AGENT
    for bad in ({}, {"vertex": "party", "iri": AGENT}, {"iri": "foaf:Agent x"}):
        with pytest.raises(ValidationError):
            SuperClass.model_validate(bad)
    with pytest.raises(ValidationError):
        SuperProperty.model_validate(
            {"edge": _under("a", "b", "r")["edge"], "iri": RELATED}
        )


@pytest.mark.parametrize(
    "build",
    [
        lambda: SuperClass(iri="foaf:Agent"),
        lambda: SuperProperty(iri="skos:related"),
        lambda: Semantics(iri="schema:Person"),
        lambda: Semantics(close_match=["https://ok/1", "owl:Thing"]),
    ],
    ids=["superclass", "superproperty", "iri", "link"],
)
def test_a_prefixed_name_is_not_an_iri(build: Any) -> None:
    with pytest.raises(ValidationError, match="not an absolute IRI"):
        build()


def test_a_declared_prefix_is_refused_as_a_scheme() -> None:
    with pytest.raises(ValueError, match="declared prefix 'hr'"):
        _manifest(
            _vertex("person", semantics={"iri": "hr:Person"}),
            namespaces={"hr": "https://example.org/hr/"},
        )


@pytest.mark.parametrize(
    ("vertices", "message"),
    [
        ([_vertex("person", {"vertex": "nobody"})], "does not declare"),
        ([_vertex("person", {"vertex": "person"})], "subclass of itself"),
        (
            [
                _vertex("a", {"vertex": "b"}),
                _vertex("b", {"vertex": "c"}),
                _vertex("c", {"vertex": "a"}),
            ],
            "cycle",
        ),
    ],
    ids=["unknown", "self", "cycle"],
)
def test_finish_init_refuses_a_broken_class_hierarchy(
    vertices: list[dict[str, Any]], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        _manifest(*vertices)


@pytest.mark.parametrize(
    ("edges", "message"),
    [
        (
            [_edge("asset", "asset", "r", _under("asset", "asset", "nothing"))],
            "does not declare",
        ),
        (
            [_edge("asset", "asset", "r", _under("asset", "asset", "r"))],
            "subproperty of itself",
        ),
        (
            [
                _edge("asset", "asset", "r", _under("asset", "asset", "s")),
                _edge("asset", "asset", "s", _under("asset", "asset", "r")),
            ],
            "cycle",
        ),
        (
            [
                _edge("press", "press", "r"),
                _edge("asset", "asset", "s", _under("press", "press", "r")),
            ],
            "endpoints are not subclasses",
        ),
        (
            [
                _edge("asset", "asset", "r"),
                _edge(
                    "press", "press", "s", _under("asset", "asset", "r"), directed=False
                ),
            ],
            "undirected edge",
        ),
    ],
    ids=["unknown", "self", "cycle", "endpoints", "directedness"],
)
def test_finish_init_refuses_a_broken_relation_hierarchy(
    edges: list[dict[str, Any]], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        _manifest(*ASSETS, edges=edges)


def test_an_undirected_parent_subsumes_either_orientation() -> None:
    manifest = _manifest(
        *ASSETS,
        edges=[
            _edge("asset", "controller", "linked", directed=False),
            _edge(
                "press",
                "controller",
                "carries",
                _under("asset", "controller", "linked"),
            ),
            _edge(
                "controller", "press", "uses", _under("asset", "controller", "linked")
            ),
        ],
    )
    assert Hierarchy.of(_core(manifest)).is_subproperty(
        ("controller", "press", "uses"), ("asset", "controller", "linked")
    )


def test_a_local_element_named_by_iri_is_refused() -> None:
    with pytest.raises(ValueError, match=r"write \{vertex: party\}"):
        _manifest(
            _vertex("party", semantics={"iri": "https://schema.org/Organization"}),
            _vertex("shop", {"iri": "https://schema.org/Organization"}),
        )
    # The IRI the schema mints names it as well.
    with pytest.raises(ValueError, match=r"write \{vertex: party\}"):
        _manifest(
            _vertex("party"),
            _vertex("shop", {"iri": "https://example.org/hr/party"}),
            base_iri="https://example.org/hr/",
        )
    with pytest.raises(ValueError, match="superproperty"):
        _manifest(
            *ASSETS,
            edges=[
                _edge("asset", "asset", "depends_on", semantics={"iri": RELATED}),
                _edge("press", "press", "mirrors", {"iri": RELATED}),
            ],
        )


def test_base_iri_and_prefixes_are_checked() -> None:
    meta = GraphMetadata(
        name="m",
        base_iri="https://example.org/hr/",
        namespaces={"schema": "https://schema.org/"},
    )
    assert meta.base_iri == "https://example.org/hr/"
    for bad in (
        {"base_iri": "https://example.org/hr"},
        {"base_iri": "hr/"},
        {"namespaces": {"1bad": "https://schema.org/"}},
        {"namespaces": {"schema": "schema.org"}},
    ):
        with pytest.raises(ValidationError):
            GraphMetadata.model_validate({"name": "m", **bad})


def test_merged_semantics_union_the_links_and_clear_a_disputed_label() -> None:
    left = Semantics(
        label="Person", close_match=["https://a/1"], broader=["https://a/T"]
    )
    right = Semantics(
        label="Human", close_match=["https://b/1"], broader=["https://a/T"]
    )
    merged = merge_semantics(left, right)
    assert merged is not None
    assert merged.label is None
    assert merged.close_match == ["https://a/1", "https://b/1"]
    assert merged.broader == ["https://a/T"]
    same = merge_semantics(left, left.model_copy())
    assert same is not None and same.label == "Person"


# -- what the hierarchy entails ---------------------------------------------------


def test_the_hierarchy_is_reflexive_transitive_and_reaches_external_parents() -> None:
    hierarchy = Hierarchy.of(_core(_manifest(*ASSETS, edges=RELATIONS)))
    assert hierarchy.superclasses("press") == ["machine", "asset"]
    assert hierarchy.superclasses("press", transitive=False) == ["machine"]
    assert hierarchy.subclasses("asset") == ["machine", "controller", "press"]
    assert hierarchy.is_subclass("press", "asset") and hierarchy.is_subclass(
        "asset", "asset"
    )
    assert not hierarchy.is_subclass("asset", "press")
    assert hierarchy.external_superclasses("press") == [AGENT]
    assert hierarchy.external_superclasses("machine") == []
    mounted_on, depends_on = (
        ("controller", "press", "mounted_on"),
        ("asset", "asset", "depends_on"),
    )
    assert hierarchy.superproperties(mounted_on) == [depends_on]
    assert hierarchy.subproperties(depends_on) == [mounted_on]
    assert hierarchy.subproperty_pairs() == {(mounted_on, depends_on)}


def test_a_new_subsumption_between_base_classes_is_reported() -> None:
    base = _manifest(*ASSETS)
    narrower = _manifest(*ASSETS, _vertex("tool", {"vertex": "asset"}))
    assert not new_subsumptions(_core(base), _core(narrower))
    collapsed = _manifest(
        _vertex("asset"),
        _vertex("machine", {"vertex": "asset"}),
        _vertex("press", {"vertex": "machine"}, {"iri": AGENT}),
        _vertex("controller", {"vertex": "press"}),
    )
    found = new_subsumptions(_core(base), _core(collapsed))
    assert found.classes == {("controller", "press"), ("controller", "machine")}


def test_a_vertex_iri_is_its_grounding_else_the_minted_one() -> None:
    meta = GraphMetadata(name="m", base_iri="https://example.org/hr/")
    manifest = _manifest(
        _vertex("person", semantics={"iri": "https://schema.org/Person"}),
        _vertex("team"),
    )
    person, team = _core(manifest).vertex_config.vertices
    assert vertex_iri(meta, person) == "https://schema.org/Person"
    assert vertex_iri(meta, team) == "https://example.org/hr/team"
    assert vertex_iri(GraphMetadata(name="m"), team) is None


# -- evolution --------------------------------------------------------------------


def test_a_changed_superclass_is_diffed_replayed_and_inverted() -> None:
    base = _manifest(_vertex("person"), _vertex("party"))
    target = _manifest(_vertex("person", {"vertex": "party"}), _vertex("party"))
    ops, warnings = diff_manifests_verified(base, target)
    assert warnings == []
    (op,) = ops
    assert isinstance(op, SetVertexSubclassOfOp)
    replayed = apply_evolution(base, ops, bump_version=False, finish_init=False)
    assert manifest_hash(replayed) == manifest_hash(target)
    inverse = invert_op(op, manifest=base)
    assert isinstance(inverse, SetVertexSubclassOfOp)
    assert inverse.subclass_of == {"person": []}


def test_a_changed_superproperty_is_diffed_replayed_and_inverted() -> None:
    base = _manifest(
        *ASSETS, edges=[RELATIONS[0], _edge("controller", "press", "mounted_on")]
    )
    target = _manifest(*ASSETS, edges=RELATIONS)
    ops, warnings = diff_manifests_verified(base, target)
    assert warnings == []
    (op,) = ops
    assert isinstance(op, SetEdgeSubpropertyOfOp)
    replayed = apply_evolution(base, ops, bump_version=False, finish_init=False)
    assert manifest_hash(replayed) == manifest_hash(target)
    inverse = invert_op(op, manifest=base)
    assert isinstance(inverse, SetEdgeSubpropertyOfOp)
    assert inverse.edges[0].subproperty_of == []


def test_a_rename_follows_every_reference() -> None:
    base = _manifest(*ASSETS, edges=RELATIONS)
    renamed = _apply(
        base,
        {"op": "rename_vertices", "renames": {"asset": "item"}},
        {"op": "rename_relations", "renames": {"depends_on": "needs"}},
    )
    assert _parents(renamed, "machine") == [SuperClass(vertex="item")]
    assert _edge_parents(renamed, ("controller", "press", "mounted_on")) == [
        _ref("item", "item", "needs")
    ]


def test_removing_a_parent_splices_it_out() -> None:
    """What was under the removed class or relation moves under its parents."""
    base = _manifest(
        *ASSETS,
        edges=[
            _edge("asset", "asset", "relates", {"iri": RELATED}),
            _edge("asset", "asset", "depends_on", _under("asset", "asset", "relates")),
            _edge(
                "controller",
                "press",
                "mounted_on",
                _under("asset", "asset", "depends_on"),
            ),
        ],
    )
    spliced = _apply(base, {"op": "remove_vertices", "names": ["machine"]})
    assert _parents(spliced, "press") == [
        SuperClass(vertex="asset"),
        SuperClass(iri=AGENT),
    ]
    spliced = _apply(base, {"op": "remove_edges", "relations": ["depends_on"]})
    assert _edge_parents(spliced, ("controller", "press", "mounted_on")) == [
        _ref("asset", "asset", "relates")
    ]
    # In any order, one at a time or together, the survivors end the same.
    together = _apply(
        base, {"op": "remove_edges", "relations": ["relates", "depends_on"]}
    )
    one_by_one = _apply(
        base,
        {"op": "remove_edges", "relations": ["relates"]},
        {"op": "remove_edges", "relations": ["depends_on"]},
    )
    assert manifest_hash(together) == manifest_hash(one_by_one)
    assert _edge_parents(together, ("controller", "press", "mounted_on")) == [
        SuperProperty(iri=RELATED)
    ]


def test_merging_vertices_follows_every_reference_to_a_source() -> None:
    base = _manifest(
        _vertex("person", {"vertex": "party"}, {"iri": AGENT}),
        _vertex("party", {"iri": "https://schema.org/Thing"}),
        _vertex("employee", {"vertex": "person"}),
    )
    merged = _apply(
        base, {"op": "merge_vertices", "sources": ["person"], "into": "party"}
    )
    assert _parents(merged, "party") == [
        SuperClass(iri="https://schema.org/Thing"),
        SuperClass(iri=AGENT),
    ]
    assert _parents(merged, "employee") == [SuperClass(vertex="party")]


def test_merging_relations_drops_a_parent_folded_into_its_child() -> None:
    base = _manifest(
        *ASSETS,
        edges=[
            _edge("asset", "asset", "r"),
            _edge("asset", "asset", "s", _under("asset", "asset", "r")),
        ],
    )
    merged = _apply(base, {"op": "merge_edges", "sources": ["s"], "into": "r"})
    assert _edge_parents(merged, ("asset", "asset", "r")) == []


def test_retargeting_an_edge_follows_its_subproperties() -> None:
    base = _manifest(
        *ASSETS,
        edges=[
            _edge("machine", "asset", "on"),
            _edge("press", "asset", "installed_at", _under("machine", "asset", "on")),
        ],
    )
    moved = _apply(
        base,
        {
            "op": "retarget_edges",
            "edges": [
                {
                    "source": "machine",
                    "target": "asset",
                    "relation": "on",
                    "new_source": "asset",
                }
            ],
        },
    )
    assert _edge_parents(moved, ("press", "asset", "installed_at")) == [
        _ref("asset", "asset", "on")
    ]


def test_merged_manifests_make_an_iri_parent_that_landed_local() -> None:
    left = _manifest(
        _vertex("party", semantics={"iri": "https://schema.org/Organization"})
    )
    right = _manifest(_vertex("shop", {"iri": "https://schema.org/Organization"}))
    merged = merge_manifests(left, right, MergeManifestsOp())
    assert _parents(merged, "shop") == [SuperClass(vertex="party")]


def test_merged_manifests_keep_an_agreed_base_iri_and_refuse_a_rebound_prefix() -> None:
    left = _manifest(
        _vertex("person"),
        base_iri="https://example.org/hr/",
        namespaces={"s": "https://schema.org/"},
    )
    right = _manifest(
        _vertex("party"),
        base_iri="https://example.org/hr/",
        namespaces={"f": "http://xmlns.com/foaf/0.1/"},
    )
    merged = merge_manifests(left, right, MergeManifestsOp())
    assert merged.graph_schema is not None
    metadata = merged.graph_schema.metadata
    assert metadata.base_iri == "https://example.org/hr/"
    assert metadata.namespaces == {
        "s": "https://schema.org/",
        "f": "http://xmlns.com/foaf/0.1/",
    }

    other = _manifest(_vertex("party"), base_iri="https://example.org/ops/")
    disputed = merge_manifests(left, other, MergeManifestsOp()).graph_schema
    assert disputed is not None and disputed.metadata.base_iri is None

    clash = _manifest(_vertex("party"), namespaces={"s": "https://example.org/s/"})
    with pytest.raises(ValueError, match="Conflicting namespace prefixes"):
        merge_manifests(left, clash, MergeManifestsOp())


def test_a_link_cut_on_one_branch_conflicts_with_a_parent_relying_on_it() -> None:
    """The edge's parent holds only through ``machine``'s chain, which the other side cuts."""
    base = _manifest(
        *ASSETS,
        edges=[_edge("asset", "asset", "depends_on"), _edge("press", "asset", "feeds")],
    )
    left = _apply(
        base,
        {
            "op": "set_edge_subproperty_of",
            "edges": [
                {
                    "source": "press",
                    "target": "asset",
                    "relation": "feeds",
                    "subproperty_of": [_under("asset", "asset", "depends_on")],
                }
            ],
        },
    )
    right = _apply(
        base, {"op": "set_vertex_subclass_of", "subclass_of": {"machine": []}}
    )
    merged, result = merge_three_way(base, left, right)
    assert merged is None
    assert [list(c.slot) for c in result.conflicts] == [
        ["vertex", "machine", "subclass_of"]
    ]


# -- slices -----------------------------------------------------------------------


def test_a_slice_keeps_the_parents_of_what_it_keeps() -> None:
    manifest = _manifest(*ASSETS, edges=RELATIONS)
    assert manifest.graph_schema is not None
    selection = select_induced(_core(manifest), keep_vertices=["controller", "press"])
    assert selection.surviving_vertices == {"controller", "press", "machine", "asset"}
    assert ("asset", "asset", "depends_on") in selection.surviving_edge_ids
    sliced = build_subschema(manifest.graph_schema, selection)
    assert Hierarchy.of(sliced.core_schema).is_subclass("press", "asset")


def test_removing_disconnected_vertices_keeps_a_superclass() -> None:
    manifest = _manifest(
        *ASSETS, _vertex("orphan"), edges=[_edge("controller", "press", "mounted_on")]
    )
    assert manifest.graph_schema is not None
    removed = manifest.graph_schema.remove_disconnected_vertices()
    assert removed == {"orphan"}
