"""Tests for dynamic-mode EdgeActor (slot-based type resolution).

Covers all scenario dimensions:
  - VRA slots by role (role inferred from type_field when omitted)
  - Static types with static relation
  - Static types with dynamic relation via relation_field
  - Dynamic types via VRA type_field → EdgeActor source/target_type_field
  - Strict vs permissive edge type registration
  - relation_map normalisation
  - Multi-edge flat-row (vertex B as source and target in different edges)
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
    VertexRouterActorConfig,
)
from graflo.architecture.pipeline.runtime.actor.edge import EdgeActor
from graflo.architecture.pipeline.runtime.actor.vertex_router import VertexRouterActor
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.vertex import VertexConfig


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _vc(*names: str) -> VertexConfig:
    return VertexConfig.model_validate(
        {"vertices": [{"name": n, "properties": ["id"]} for n in names]}
    )


def _init(vc: VertexConfig, ec: EdgeConfig | None = None) -> ActorInitContext:
    return ActorInitContext(
        vertex_config=vc,
        edge_config=ec or EdgeConfig(),
        transforms={},
    )


def _lindex(*path) -> LocationIndex:
    return LocationIndex(tuple(path))


def _make_static_ea(
    source: str,
    target: str,
    *,
    relation: str | None = None,
    relation_field: str | None = None,
    match_source: str | None = None,
    match_target: str | None = None,
) -> EdgeActor:
    cfg = EdgeActorConfig.model_validate(
        {
            "type": "edge",
            "from": source,
            "to": target,
            **({"relation": relation} if relation else {}),
            **({"relation_field": relation_field} if relation_field else {}),
            **({"match_source": match_source} if match_source else {}),
            **({"match_target": match_target} if match_target else {}),
        }
    )
    return EdgeActor.from_config(cfg)


def _make_dynamic_ea(
    source_type_field: str,
    target_type_field: str,
    *,
    relation: str | None = None,
    relation_field: str | None = None,
    relation_map: dict[str, str] | None = None,
    strict_edge_types: bool = False,
) -> EdgeActor:
    cfg = EdgeActorConfig.model_validate(
        {
            "type": "edge",
            "source_type_field": source_type_field,
            "target_type_field": target_type_field,
            **({"relation": relation} if relation else {}),
            **({"relation_field": relation_field} if relation_field else {}),
            **({"relation_map": relation_map} if relation_map else {}),
            "strict_edge_types": strict_edge_types,
        }
    )
    return EdgeActor.from_config(cfg)


def _populate_slot(
    ctx: ExtractionContext,
    base_lindex: LocationIndex,
    type_field: str,
    vertex_type: str,
    vertex_doc: dict,
) -> None:
    """Simulate VRA storing a vertex rep at lindex.(type_field, 0)."""
    slot_lindex = base_lindex.extend((type_field, 0))
    ctx.acc_vertex[vertex_type][slot_lindex].append(
        VertexRep(vertex=vertex_doc, ctx={})
    )


# ---------------------------------------------------------------------------
# 1. VRA stores at lindex.(role, 0), with role inferred from type_field
# ---------------------------------------------------------------------------


def test_vra_config_infers_role_from_type_field() -> None:
    cfg = VertexRouterActorConfig(type_field="vtype")
    assert cfg.role == "vtype"


def test_vra_stores_at_type_field_slot_lindex() -> None:
    """VertexRouterActor stores at inferred role slot when role is unset."""
    vc = _vc("server", "database")
    cfg = VertexRouterActorConfig(type_field="vtype")
    vra = VertexRouterActor(cfg)
    vra.finish_init(_init(vc))

    ctx = ExtractionContext()
    base = _lindex(0)
    vra(ctx, base, doc={"vtype": "server", "id": "s1"})

    slot_lindex = base.extend(("vtype", 0))
    assert slot_lindex in ctx.acc_vertex["server"]
    assert ctx.acc_vertex["server"][slot_lindex][0].vertex["id"] == "s1"


def test_vra_stores_at_role_slot_when_role_set() -> None:
    """VertexRouterActor uses role as the accumulator slot segment when role is set."""
    vc = _vc("server", "database")
    cfg = VertexRouterActorConfig(type_field="vtype", role="src")
    vra = VertexRouterActor(cfg)
    vra.finish_init(_init(vc))

    ctx = ExtractionContext()
    base = _lindex(0)
    vra(ctx, base, doc={"vtype": "server", "id": "s1"})

    slot_lindex = base.extend(("src", 0))
    assert slot_lindex in ctx.acc_vertex["server"]
    assert ctx.acc_vertex["server"][slot_lindex][0].vertex["id"] == "s1"


def test_vra_from_doc_used_when_vertex_from_map_missing_type() -> None:
    """Router-level from_doc applies when resolved type has no vertex_from_map entry."""
    vc = _vc("server")
    cfg = VertexRouterActorConfig(
        type_field="vtype",
        from_doc={"id": "row_id"},
    )
    vra = VertexRouterActor(cfg)
    vra.finish_init(_init(vc))

    ctx = ExtractionContext()
    base = _lindex(0)
    vra(ctx, base, doc={"vtype": "server", "row_id": "r9"})

    slot_lindex = base.extend(("vtype", 0))
    assert ctx.acc_vertex["server"][slot_lindex][0].vertex["id"] == "r9"


def test_vra_vertex_from_map_overrides_from_doc() -> None:
    """Per-type vertex_from_map replaces router from_doc for that type."""
    vc = _vc("server", "database")
    cfg = VertexRouterActorConfig(
        type_field="vtype",
        from_doc={"id": "fallback_id"},
        vertex_from_map={"server": {"id": "sid"}},
        type_map={"s": "server", "d": "database"},
    )
    vra = VertexRouterActor(cfg)
    vra.finish_init(_init(vc))

    ctx = ExtractionContext()
    base = _lindex(0)
    vra(
        ctx,
        base,
        doc={"vtype": "s", "sid": "one", "fallback_id": "ignored_for_server"},
    )
    slot = base.extend(("vtype", 0))
    assert ctx.acc_vertex["server"][slot][0].vertex["id"] == "one"

    ctx2 = ExtractionContext()
    vra(ctx2, base, doc={"vtype": "d", "fallback_id": "two"})
    slot2 = base.extend(("vtype", 0))
    assert ctx2.acc_vertex["database"][slot2][0].vertex["id"] == "two"


def test_two_vras_with_different_type_fields_use_separate_slots() -> None:
    """Two VRAs with different inferred roles accumulate into separate slots."""
    vc = _vc("server", "database")
    vra_src = VertexRouterActor(VertexRouterActorConfig(type_field="source_type"))
    vra_tgt = VertexRouterActor(VertexRouterActorConfig(type_field="target_type"))
    init = _init(vc)
    vra_src.finish_init(init)
    vra_tgt.finish_init(init)

    ctx = ExtractionContext()
    base = _lindex(0)
    row = {"source_type": "server", "target_type": "database", "id": "1"}
    ctx = vra_src(ctx, base, doc=row)
    ctx = vra_tgt(ctx, base, doc=row)

    src_slot = base.extend(("source_type", 0))
    tgt_slot = base.extend(("target_type", 0))
    assert src_slot in ctx.acc_vertex["server"]
    assert tgt_slot in ctx.acc_vertex["database"]


# ---------------------------------------------------------------------------
# 2. Static-mode EdgeActor (unchanged behavior)
# ---------------------------------------------------------------------------


def test_static_types_static_relation() -> None:
    """Static EdgeActor records an edge intent with the pre-built schema Edge."""
    vc = _vc("server", "database")
    ea = _make_static_ea("server", "database", relation="connects")
    ea.finish_init(_init(vc))

    ctx = ExtractionContext()
    loc = _lindex(0)
    ea(ctx, loc)

    assert len(ctx.edge_intents) == 1
    intent = ctx.edge_intents[0]
    assert intent.edge.source == "server"
    assert intent.edge.target == "database"
    assert intent.edge.relation == "connects"
    assert intent.location == loc


def test_static_types_dynamic_relation_field() -> None:
    """Static EdgeActor passes relation_field through derivation for assembly-time lookup."""
    vc = _vc("server", "database")
    ea = _make_static_ea("server", "database", relation_field="rel_col")
    ea.finish_init(_init(vc))

    ctx = ExtractionContext()
    ea(ctx, _lindex(0))

    assert len(ctx.edge_intents) == 1
    derivation = ctx.edge_intents[0].derivation
    assert derivation is not None
    assert derivation.relation_field == "rel_col"


# ---------------------------------------------------------------------------
# 3. Dynamic slot-mode EdgeActor
# ---------------------------------------------------------------------------


def test_dynamic_both_types_static_relation() -> None:
    """Dynamic EdgeActor resolves types from VRA slots; static relation."""
    vc = _vc("server", "database")
    ea = _make_dynamic_ea("S", "T", relation="uses")
    ea.finish_init(_init(vc))

    ctx = ExtractionContext()
    base = _lindex(0)
    _populate_slot(ctx, base, "S", "server", {"id": "s1"})
    _populate_slot(ctx, base, "T", "database", {"id": "d1"})

    ea(ctx, base, doc={})

    assert len(ctx.edge_intents) == 1
    intent = ctx.edge_intents[0]
    assert intent.edge.source == "server"
    assert intent.edge.target == "database"
    assert intent.edge.relation == "uses"
    assert intent.derivation is not None
    assert intent.derivation.match_source == "S"
    assert intent.derivation.match_target == "T"


def test_dynamic_both_types_dynamic_relation() -> None:
    """Dynamic EdgeActor reads relation from merged doc (relation_field)."""
    vc = _vc("server", "database")
    ea = _make_dynamic_ea("S", "T", relation_field="rel")
    ea.finish_init(_init(vc))

    ctx = ExtractionContext()
    base = _lindex(0)
    _populate_slot(ctx, base, "S", "server", {"id": "s1"})
    _populate_slot(ctx, base, "T", "database", {"id": "d1"})

    ea(ctx, base, doc={"rel": "runs_on"})

    assert len(ctx.edge_intents) == 1
    assert ctx.edge_intents[0].edge.relation == "runs_on"


def test_dynamic_relation_from_transform_buffer() -> None:
    """Dynamic EdgeActor reads relation_field from transform buffer (not just raw doc)."""
    vc = _vc("server", "database")
    ea = _make_dynamic_ea("S", "T", relation_field="rel")
    ea.finish_init(_init(vc))

    ctx = ExtractionContext()
    base = _lindex(0)
    _populate_slot(ctx, base, "S", "server", {"id": "s1"})
    _populate_slot(ctx, base, "T", "database", {"id": "d1"})

    ctx.buffer_transforms[base].append(TransformPayload(named={"rel": "from_buffer"}))
    ea(ctx, base, doc={"rel": "from_doc"})  # buffer overrides doc

    assert ctx.edge_intents[0].edge.relation == "from_buffer"


def test_dynamic_with_relation_map() -> None:
    """Dynamic EdgeActor applies relation_map to normalise raw relation values."""
    vc = _vc("server", "database")
    ea = _make_dynamic_ea(
        "S", "T", relation_field="rt", relation_map={"raw_rel": "canonical"}
    )
    ea.finish_init(_init(vc))

    ctx = ExtractionContext()
    base = _lindex(0)
    _populate_slot(ctx, base, "S", "server", {"id": "s1"})
    _populate_slot(ctx, base, "T", "database", {"id": "d1"})

    ea(ctx, base, doc={"rt": "raw_rel"})

    assert ctx.edge_intents[0].edge.relation == "canonical"


def test_dynamic_skips_when_slot_empty() -> None:
    """Dynamic EdgeActor skips when a required slot has no vertex data."""
    vc = _vc("server", "database")
    ea = _make_dynamic_ea("S", "T")
    ea.finish_init(_init(vc))

    ctx = ExtractionContext()
    base = _lindex(0)
    _populate_slot(ctx, base, "S", "server", {"id": "s1"})
    # T slot is deliberately empty

    ea(ctx, base, doc={})
    assert len(ctx.edge_intents) == 0


def test_dynamic_skips_unknown_vertex_type() -> None:
    """Dynamic EdgeActor skips if the resolved type is not in vertex_set."""
    vc = _vc("server")  # "database" not in vertex_set
    ea = _make_dynamic_ea("S", "T")
    ea.finish_init(_init(vc))

    ctx = ExtractionContext()
    base = _lindex(0)
    _populate_slot(ctx, base, "S", "server", {"id": "s1"})
    _populate_slot(ctx, base, "T", "database", {"id": "d1"})  # not in vc

    ea(ctx, base, doc={})
    assert len(ctx.edge_intents) == 0


# ---------------------------------------------------------------------------
# 4. Vertex B as source in one edge and target in another (no confusion)
# ---------------------------------------------------------------------------


def test_vertex_b_is_source_and_target_in_different_edges() -> None:
    """A vertex accumulated at slot B can be source of E(B→C) and target of E(A→B)."""
    vc = _vc("car", "truck", "trailer")

    ea_ab = _make_dynamic_ea("A", "B")
    ea_bc = _make_dynamic_ea("B", "C")

    init = _init(vc)
    ea_ab.finish_init(init)
    ea_bc.finish_init(init)

    ctx = ExtractionContext()
    base = _lindex(0)
    _populate_slot(ctx, base, "A", "car", {"id": "a1"})
    _populate_slot(ctx, base, "B", "truck", {"id": "b1"})
    _populate_slot(ctx, base, "C", "trailer", {"id": "c1"})

    ea_ab(ctx, base, doc={})
    ea_bc(ctx, base, doc={})

    assert len(ctx.edge_intents) == 2
    sources = {i.edge.source for i in ctx.edge_intents}
    targets = {i.edge.target for i in ctx.edge_intents}
    assert "car" in sources and "truck" in sources
    assert "truck" in targets and "trailer" in targets


# ---------------------------------------------------------------------------
# 5. strict_edge_types
# ---------------------------------------------------------------------------


def test_strict_edge_types_skips_unknown() -> None:
    """With strict_edge_types=True, undeclared (source_type, target_type) is skipped."""
    vc = _vc("server", "database")
    ec = EdgeConfig()  # No edges pre-declared
    ea = _make_dynamic_ea("S", "T", strict_edge_types=True)
    ea.finish_init(_init(vc, ec))

    ctx = ExtractionContext()
    base = _lindex(0)
    _populate_slot(ctx, base, "S", "server", {"id": "s1"})
    _populate_slot(ctx, base, "T", "database", {"id": "d1"})

    ea(ctx, base, doc={})
    assert len(ctx.edge_intents) == 0


def test_strict_edge_types_allows_known() -> None:
    """With strict_edge_types=True, a pre-declared edge pair passes through."""
    vc = _vc("server", "database")
    pre_edge = Edge(source="server", target="database")
    pre_edge.finish_init(vertex_config=vc)
    ec = EdgeConfig(edges=[pre_edge])

    ea = _make_dynamic_ea("S", "T", strict_edge_types=True)
    ea.finish_init(_init(vc, ec))

    ctx = ExtractionContext()
    base = _lindex(0)
    _populate_slot(ctx, base, "S", "server", {"id": "s1"})
    _populate_slot(ctx, base, "T", "database", {"id": "d1"})

    ea(ctx, base, doc={})
    assert len(ctx.edge_intents) == 1
    assert ctx.edge_intents[0].edge.source == "server"


# ---------------------------------------------------------------------------
# 6. EdgeActorConfig validation
# ---------------------------------------------------------------------------


def test_edge_config_requires_source() -> None:
    with pytest.raises(Exception, match="source"):
        EdgeActorConfig.model_validate({"type": "edge", "to": "database"})


def test_edge_config_requires_target() -> None:
    with pytest.raises(Exception, match="target"):
        EdgeActorConfig.model_validate({"type": "edge", "from": "server"})


def test_edge_config_from_and_source_type_field_exclusive() -> None:
    with pytest.raises(Exception, match="mutually exclusive"):
        EdgeActorConfig.model_validate(
            {
                "type": "edge",
                "from": "server",
                "source_type_field": "S",
                "to": "database",
            }
        )


def test_edge_config_mixed_mode_source_type_field_static_target_is_valid() -> None:
    """source_type_field + static 'to' (mixed mode) is now a valid configuration."""
    cfg = EdgeActorConfig.model_validate(
        {
            "type": "edge",
            "source_type_field": "S",
            "to": "database",
        }
    )
    assert cfg.source_type_field == "S"
    assert cfg.target == "database"


def test_edge_config_mixed_mode_static_source_target_type_field_is_valid() -> None:
    """Static 'from' + target_type_field (mixed mode) is a valid configuration."""
    cfg = EdgeActorConfig.model_validate(
        {
            "type": "edge",
            "from": "server",
            "target_type_field": "T",
        }
    )
    assert cfg.source == "server"
    assert cfg.target_type_field == "T"
