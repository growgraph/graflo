"""Tests for vertex role slots and multi-link edge steps.

Covers:
  - VertexActor with role stores at lindex.(role, 0) not bare lindex
  - VertexActor without role stores at bare lindex
  - Two vertex+role steps for the same vertex type accumulate at distinct slots
  - Passthrough does not mutate shared doc when role is set (sibling steps see same col)
  - source_role / target_role on EdgeActorConfig resolve to source_type_field / target_type_field
  - EdgeActor with links emits N intents per row
  - Full pipeline: 3 role-vertex steps + 2-link edge → 2 correct edge intents
  - Validation: links + from raises; source_role + source_type_field raises; link missing source raises
"""

from __future__ import annotations

import pytest

from graflo.architecture.graph_types import (
    ExtractionContext,
    LocationIndex,
    TransformPayload,
    VertexRep,
)
from graflo.architecture.pipeline.runtime.actor.base import ActorInitContext
from graflo.architecture.pipeline.runtime.actor.config import (
    EdgeActorConfig,
    EdgeLinkConfig,
    VertexActorConfig,
)
from graflo.architecture.pipeline.runtime.actor.edge import EdgeActor
from graflo.architecture.pipeline.runtime.actor.vertex import VertexActor
from graflo.architecture.schema.edge import EdgeConfig
from graflo.architecture.schema.vertex import VertexConfig


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _vc(*names: str) -> VertexConfig:
    return VertexConfig.model_validate(
        {
            "vertices": [
                {"name": n, "properties": ["id", "name"], "identity": ["id"]}
                for n in names
            ]
        }
    )


def _init(vc: VertexConfig, ec: EdgeConfig | None = None) -> ActorInitContext:
    return ActorInitContext(
        vertex_config=vc,
        edge_config=ec or EdgeConfig(),
        transforms={},
    )


def _lindex(*path) -> LocationIndex:
    return LocationIndex(tuple(path))


def _make_vertex_actor(
    name: str,
    *,
    role: str | None = None,
    from_doc: dict | None = None,
    keep_fields: list[str] | None = None,
    extraction_scope: str | None = None,
) -> VertexActor:
    cfg = VertexActorConfig.model_validate(
        {
            "vertex": name,
            **({"role": role} if role else {}),
            **({"from": from_doc} if from_doc else {}),
            **({"keep_fields": keep_fields} if keep_fields else {}),
            **({"extraction_scope": extraction_scope} if extraction_scope else {}),
        }
    )
    return VertexActor.from_config(cfg)


def _make_edge_actor_links(links: list[dict]) -> EdgeActor:
    cfg = EdgeActorConfig.model_validate({"type": "edge", "links": links})
    return EdgeActor.from_config(cfg)


# ---------------------------------------------------------------------------
# 1. VertexActor role — storage location
# ---------------------------------------------------------------------------


def test_vertex_actor_with_role_stores_at_role_slot() -> None:
    """VertexActor with role stores vertex at lindex.(role, 0), not bare lindex."""
    vc = _vc("person")
    va = _make_vertex_actor("person", role="self")
    va.finish_init(_init(vc))

    ctx = ExtractionContext()
    base = _lindex(0)
    ctx = va(ctx, base, doc={"id": "12", "name": "Bob"})

    slot_lindex = base.extend(("self", 0))
    assert slot_lindex in ctx.acc_vertex["person"]
    assert ctx.acc_vertex["person"][slot_lindex][0].vertex["id"] == "12"
    # Bare lindex must be empty.
    assert base not in ctx.acc_vertex["person"]


def test_vertex_actor_without_role_stores_at_bare_lindex() -> None:
    """VertexActor without role stores vertex at bare lindex."""
    vc = _vc("person")
    va = _make_vertex_actor("person")
    va.finish_init(_init(vc))

    ctx = ExtractionContext()
    base = _lindex(0)
    ctx = va(ctx, base, doc={"id": "12", "name": "Bob"})

    assert base in ctx.acc_vertex["person"]
    assert ctx.acc_vertex["person"][base][0].vertex["id"] == "12"


def test_two_role_steps_same_type_occupy_distinct_slots() -> None:
    """Two vertex: person steps with different roles accumulate at distinct slots."""
    vc = _vc("person")
    # from_doc is {vertex_field: doc_field}: vertex field 'id' comes from doc field 'person'
    va_self = _make_vertex_actor("person", role="self", from_doc={"id": "person"})
    va_parent = _make_vertex_actor("person", role="parent", from_doc={"id": "parent"})
    init = _init(vc)
    va_self.finish_init(init)
    va_parent.finish_init(init)

    ctx = ExtractionContext()
    base = _lindex(0)
    doc = {"person": "12", "parent": "13", "name": "Bob"}
    ctx = va_self(ctx, base, doc=doc)
    ctx = va_parent(ctx, base, doc=doc)

    slot_self = base.extend(("self", 0))
    slot_parent = base.extend(("parent", 0))
    assert ctx.acc_vertex["person"][slot_self][0].vertex["id"] == "12"
    assert ctx.acc_vertex["person"][slot_parent][0].vertex["id"] == "13"


# ---------------------------------------------------------------------------
# 2. Passthrough safety (non-mutating effective observation reads)
# ---------------------------------------------------------------------------


def test_passthrough_does_not_mutate_doc_when_role_is_set() -> None:
    """Role-bearing vertex step keeps the shared doc unchanged for sibling steps."""
    vc = _vc("person")
    # from_doc: {vertex_field: doc_field}
    va_self = _make_vertex_actor("person", role="self", from_doc={"id": "person"})
    va_parent = _make_vertex_actor("person", role="parent", from_doc={"id": "parent"})
    init = _init(vc)
    va_self.finish_init(init)
    va_parent.finish_init(init)

    ctx = ExtractionContext()
    base = _lindex(0)
    # 'name' is a vertex property — va_self should pick it up via passthrough.
    # 'name' must still be present in the doc for va_parent to read if it needed it.
    doc = {"person": "12", "parent": "13", "name": "Bob"}
    doc_before = dict(doc)
    ctx = va_self(ctx, base, doc=doc)

    # After va_self the doc is unmodified.
    assert doc == doc_before, "role-bearing VertexActor must not mutate the shared doc"

    # va_parent can still run on the same doc without missing anything.
    ctx = va_parent(ctx, base, doc=doc)
    slot_parent = base.extend(("parent", 0))
    assert ctx.acc_vertex["person"][slot_parent][0].vertex["id"] == "13"


def test_keep_fields_restricts_passthrough_for_role_vertex() -> None:
    """keep_fields on a role-vertex step prevents other schema properties from leaking in."""
    vc = _vc("person")
    # parent step: only take id; name must not be absorbed from the shared doc
    va_parent = _make_vertex_actor(
        "person", role="parent", from_doc={"id": "parent"}, keep_fields=["id"]
    )
    va_parent.finish_init(_init(vc))

    ctx = ExtractionContext()
    base = _lindex(0)
    doc = {"person": "12", "parent": "13", "name": "Bob"}
    ctx = va_parent(ctx, base, doc=doc)

    slot_parent = base.extend(("parent", 0))
    vertex = ctx.acc_vertex["person"][slot_parent][0].vertex
    assert vertex["id"] == "13"
    assert "name" not in vertex


def test_passthrough_without_role_does_not_mutate_doc() -> None:
    """Without role, passthrough reads non-destructively from effective doc."""
    vc = _vc("person")
    va = _make_vertex_actor("person")  # no role
    va.finish_init(_init(vc))

    ctx = ExtractionContext()
    base = _lindex(0)
    doc = {"id": "12", "name": "Bob"}
    ctx = va(ctx, base, doc=doc)

    assert doc == {"id": "12", "name": "Bob"}


def test_from_doc_prefers_transform_value_over_raw_doc() -> None:
    """from_doc reads merged observation with transform priority over raw doc."""
    vc = _vc("person")
    va = _make_vertex_actor("person", from_doc={"id": "external_id"})
    va.finish_init(_init(vc))

    ctx = ExtractionContext()
    base = _lindex(0)
    ctx.transform_buffer[base].append(
        TransformPayload(named={"external_id": "from_tf"})
    )
    doc = {"external_id": "from_doc", "name": "Bob"}
    ctx = va(ctx, base, doc=doc)

    vertex = ctx.acc_vertex["person"][base][0].vertex
    assert vertex["id"] == "from_tf"
    assert doc == {"external_id": "from_doc", "name": "Bob"}


def test_mapped_only_role_extracts_only_from_mapping() -> None:
    """mapped_only limits extraction to explicit from mappings for role vertices."""
    vc = _vc("person")
    va_parent = _make_vertex_actor(
        "person",
        role="parent",
        from_doc={"id": "parent"},
        extraction_scope="mapped_only",
    )
    va_parent.finish_init(_init(vc))

    ctx = ExtractionContext()
    base = _lindex(0)
    doc = {"person": "12", "parent": "13", "name": "Bob"}
    doc_before = dict(doc)
    ctx = va_parent(ctx, base, doc=doc)

    slot_parent = base.extend(("parent", 0))
    vertex = ctx.acc_vertex["person"][slot_parent][0].vertex
    assert vertex == {"id": "13"}
    assert doc == doc_before


def test_mapped_only_without_role_does_not_pop_unmapped() -> None:
    """mapped_only disables passthrough, so unmapped keys remain on doc."""
    vc = _vc("person")
    va = _make_vertex_actor(
        "person",
        from_doc={"id": "person"},
        extraction_scope="mapped_only",
    )
    va.finish_init(_init(vc))

    ctx = ExtractionContext()
    base = _lindex(0)
    doc = {"person": "12", "name": "Bob"}
    ctx = va(ctx, base, doc=doc)

    vertex = ctx.acc_vertex["person"][base][0].vertex
    assert vertex == {"id": "12"}
    assert doc == {"person": "12", "name": "Bob"}


# ---------------------------------------------------------------------------
# 3. source_role / target_role on EdgeActorConfig
# ---------------------------------------------------------------------------


def test_source_role_is_canonical_slot_key() -> None:
    """source_role is the canonical dynamic slot key."""
    cfg = EdgeActorConfig.model_validate(
        {"type": "edge", "source_role": "buyer", "to": "company"}
    )
    assert cfg.source_role == "buyer"
    assert cfg.source_type_field is None


def test_target_role_is_canonical_slot_key() -> None:
    """target_role is the canonical dynamic slot key."""
    cfg = EdgeActorConfig.model_validate(
        {"type": "edge", "from": "company", "target_role": "seller"}
    )
    assert cfg.target_role == "seller"
    assert cfg.target_type_field is None


def test_source_type_field_legacy_alias_canonicalizes_to_source_role() -> None:
    cfg = EdgeActorConfig.model_validate(
        {"type": "edge", "source_type_field": "buyer", "to": "company"}
    )
    assert cfg.source_role == "buyer"
    assert cfg.source_type_field is None


def test_target_type_field_legacy_alias_canonicalizes_to_target_role() -> None:
    cfg = EdgeActorConfig.model_validate(
        {"type": "edge", "from": "company", "target_type_field": "seller"}
    )
    assert cfg.target_role == "seller"
    assert cfg.target_type_field is None


def test_source_role_and_source_type_field_equal_values_canonicalize() -> None:
    cfg = EdgeActorConfig.model_validate(
        {
            "type": "edge",
            "source_role": "buyer",
            "source_type_field": "buyer",
            "to": "company",
        }
    )
    assert cfg.source_role == "buyer"
    assert cfg.source_type_field is None


def test_target_role_and_target_type_field_equal_values_canonicalize() -> None:
    cfg = EdgeActorConfig.model_validate(
        {
            "type": "edge",
            "from": "company",
            "target_role": "seller",
            "target_type_field": "seller",
        }
    )
    assert cfg.target_role == "seller"
    assert cfg.target_type_field is None


def test_source_role_and_source_type_field_mismatch_raises() -> None:
    with pytest.raises(Exception, match="must match"):
        EdgeActorConfig.model_validate(
            {
                "type": "edge",
                "source_role": "buyer",
                "source_type_field": "seller",
                "to": "company",
            }
        )


def test_target_role_and_target_type_field_mismatch_raises() -> None:
    with pytest.raises(Exception, match="must match"):
        EdgeActorConfig.model_validate(
            {
                "type": "edge",
                "from": "company",
                "target_role": "seller",
                "target_type_field": "buyer",
            }
        )


def test_edge_actor_config_roundtrip_does_not_retrigger_alias_conflict() -> None:
    cfg = EdgeActorConfig.model_validate(
        {"type": "edge", "source_role": "buyer", "target_role": "seller"}
    )
    payload = cfg.to_dict()
    reparsed = EdgeActorConfig.model_validate(payload)
    assert reparsed.source_role == "buyer"
    assert reparsed.target_role == "seller"
    assert reparsed.source_type_field is None
    assert reparsed.target_type_field is None


# ---------------------------------------------------------------------------
# 4. EdgeLinkConfig validation
# ---------------------------------------------------------------------------


def test_edge_link_config_source_role_resolved() -> None:
    lk = EdgeLinkConfig.model_validate(
        {"source_role": "self", "target_role": "parent", "relation": "is_child_of"}
    )
    assert lk.source_role == "self"
    assert lk.target_role == "parent"
    assert lk.source_type_field is None
    assert lk.target_type_field is None


def test_edge_link_config_missing_source_raises() -> None:
    with pytest.raises(Exception, match="source"):
        EdgeLinkConfig.model_validate({"target_role": "parent"})


def test_edge_link_config_missing_target_raises() -> None:
    with pytest.raises(Exception, match="target"):
        EdgeLinkConfig.model_validate({"source_role": "self"})


def test_edge_link_config_legacy_alias_canonicalizes_to_role() -> None:
    lk = EdgeLinkConfig.model_validate(
        {"source_type_field": "self", "target_type_field": "parent"}
    )
    assert lk.source_role == "self"
    assert lk.target_role == "parent"
    assert lk.source_type_field is None
    assert lk.target_type_field is None


def test_edge_link_config_role_and_type_field_mismatch_raises() -> None:
    with pytest.raises(Exception, match="must match"):
        EdgeLinkConfig.model_validate(
            {
                "source_role": "self",
                "source_type_field": "other",
                "target_role": "parent",
            }
        )


# ---------------------------------------------------------------------------
# 5. EdgeActorConfig links validation
# ---------------------------------------------------------------------------


def test_links_and_from_are_exclusive() -> None:
    with pytest.raises(Exception, match="mutually exclusive"):
        EdgeActorConfig.model_validate(
            {
                "type": "edge",
                "from": "person",
                "to": "person",
                "links": [
                    {"source_role": "self", "target_role": "parent", "relation": "x"}
                ],
            }
        )


def test_links_and_source_type_field_are_exclusive() -> None:
    with pytest.raises(Exception, match="mutually exclusive"):
        EdgeActorConfig.model_validate(
            {
                "type": "edge",
                "source_type_field": "self",
                "links": [
                    {"source_role": "self", "target_role": "parent", "relation": "x"}
                ],
            }
        )


# ---------------------------------------------------------------------------
# 6. EdgeActor multi-link mode — emits N intents per row
# ---------------------------------------------------------------------------


def _populate_slot(
    ctx: ExtractionContext,
    base: LocationIndex,
    slot: str,
    vertex_type: str,
    vertex_doc: dict,
) -> None:
    """Simulate a role-vertex step having stored a vertex at lindex.(slot, 0)."""
    slot_lindex = base.extend((slot, 0))
    ctx.acc_vertex[vertex_type][slot_lindex].append(VertexRep(vertex=vertex_doc))


def test_edge_actor_links_emits_two_intents() -> None:
    """EdgeActor with links emits one intent per link per row."""
    vc = _vc("person")
    ec = EdgeConfig()

    ea = _make_edge_actor_links(
        [
            {"source_role": "self", "target_role": "parent", "relation": "is_child_of"},
            {"source_role": "self", "target_role": "child", "relation": "is_parent_of"},
        ]
    )
    ea.finish_init(_init(vc, ec))

    ctx = ExtractionContext()
    base = _lindex(0)
    _populate_slot(ctx, base, "self", "person", {"id": "12"})
    _populate_slot(ctx, base, "parent", "person", {"id": "13"})
    _populate_slot(ctx, base, "child", "person", {"id": "21"})

    ea(ctx, base, doc={})

    assert len(ctx.edge_intents) == 2
    relations = {i.edge.relation for i in ctx.edge_intents}
    assert "is_child_of" in relations
    assert "is_parent_of" in relations


def test_edge_actor_links_correct_source_target() -> None:
    """Each link's edge intent has the correct source and target vertex types."""
    vc = _vc("person")
    ec = EdgeConfig()

    ea = _make_edge_actor_links(
        [
            {"source_role": "self", "target_role": "parent", "relation": "is_child_of"},
            {"source_role": "self", "target_role": "child", "relation": "is_parent_of"},
        ]
    )
    ea.finish_init(_init(vc, ec))

    ctx = ExtractionContext()
    base = _lindex(0)
    _populate_slot(ctx, base, "self", "person", {"id": "12"})
    _populate_slot(ctx, base, "parent", "person", {"id": "13"})
    _populate_slot(ctx, base, "child", "person", {"id": "21"})

    ea(ctx, base, doc={})

    for intent in ctx.edge_intents:
        assert intent.edge.source == "person"
        assert intent.edge.target == "person"


def test_edge_actor_links_skips_when_slot_missing() -> None:
    """A link whose required slot is empty emits no intent for that link."""
    vc = _vc("person")
    ec = EdgeConfig()

    ea = _make_edge_actor_links(
        [
            {"source_role": "self", "target_role": "parent", "relation": "is_child_of"},
            {"source_role": "self", "target_role": "child", "relation": "is_parent_of"},
        ]
    )
    ea.finish_init(_init(vc, ec))

    ctx = ExtractionContext()
    base = _lindex(0)
    _populate_slot(ctx, base, "self", "person", {"id": "12"})
    _populate_slot(ctx, base, "parent", "person", {"id": "13"})
    # "child" slot intentionally empty

    ea(ctx, base, doc={})

    # Only the is_child_of link should fire.
    assert len(ctx.edge_intents) == 1
    assert ctx.edge_intents[0].edge.relation == "is_child_of"


def test_edge_actor_references_vertices_multi_link() -> None:
    """references_vertices returns the union of all link actors' vertex references."""
    vc = _vc("person", "company")
    ea = _make_edge_actor_links(
        [
            {"source_role": "self", "target_role": "parent", "relation": "is_child_of"},
            {"from": "person", "to": "company", "relation": "works_at"},
        ]
    )
    ea.finish_init(_init(vc))
    refs = ea.references_vertices()
    assert "person" in refs
    assert "company" in refs


# ---------------------------------------------------------------------------
# 7. Full integration: 3 role-vertex steps + 2-link edge
# ---------------------------------------------------------------------------


def test_full_pipeline_role_vertices_and_links() -> None:
    """End-to-end: family row → 2 edges with correct source/target identity docs."""
    vc = _vc("person")
    ec = EdgeConfig()

    va_self = _make_vertex_actor("person", role="self", from_doc={"id": "person"})
    va_parent = _make_vertex_actor(
        "person", role="parent", from_doc={"id": "parent"}, keep_fields=["id"]
    )
    va_child = _make_vertex_actor(
        "person", role="child", from_doc={"id": "child"}, keep_fields=["id"]
    )
    ea = _make_edge_actor_links(
        [
            {"source_role": "self", "target_role": "parent", "relation": "is_child_of"},
            {"source_role": "self", "target_role": "child", "relation": "is_parent_of"},
        ]
    )

    init = _init(vc, ec)
    va_self.finish_init(init)
    va_parent.finish_init(init)
    va_child.finish_init(init)
    ea.finish_init(init)

    ctx = ExtractionContext()
    base = _lindex(0)
    doc = {"person": "12", "parent": "13", "child": "21", "name": "Bob"}

    ctx = va_self(ctx, base, doc=doc)
    ctx = va_parent(ctx, base, doc=doc)
    ctx = va_child(ctx, base, doc=doc)
    ctx = ea(ctx, base, doc=doc)

    # 2 edge intents
    assert len(ctx.edge_intents) == 2

    # Correct vertex identity docs extracted
    slot_self = base.extend(("self", 0))
    slot_parent = base.extend(("parent", 0))
    slot_child = base.extend(("child", 0))

    assert ctx.acc_vertex["person"][slot_self][0].vertex["id"] == "12"
    assert ctx.acc_vertex["person"][slot_parent][0].vertex["id"] == "13"
    assert ctx.acc_vertex["person"][slot_child][0].vertex["id"] == "21"

    # 'name' was picked up by va_self via passthrough; parent/child have only id
    assert ctx.acc_vertex["person"][slot_self][0].vertex.get("name") == "Bob"
    assert "name" not in ctx.acc_vertex["person"][slot_parent][0].vertex
    assert "name" not in ctx.acc_vertex["person"][slot_child][0].vertex

    relations = {i.edge.relation for i in ctx.edge_intents}
    assert relations == {"is_child_of", "is_parent_of"}
