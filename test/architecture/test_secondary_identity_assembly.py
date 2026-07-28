"""Assembly behaviour for endpoints matched on a secondary identity.

The assembly path keyed everything on the primary identity. For an edge-only
source that is exactly the data it does not have, so each of these tests pins
down a step that used to silently misbehave.
"""

from __future__ import annotations

import pytest

from graflo.architecture.graph_types import (
    AssemblyContext,
    ExtractionContext,
    LocationIndex,
    VertexRep,
)
from graflo.architecture.pipeline.runtime.actor.edge_render import render_edge
from graflo.architecture.pipeline.runtime.assemble import _merge_vertices_for_edge
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.vertex import Vertex, VertexConfig
from graflo.util.merge import merge_doc_basis


@pytest.fixture
def vertex_config() -> VertexConfig:
    return VertexConfig(
        vertices=[
            Vertex.model_validate(
                {
                    "name": "instrument",
                    "properties": ["sid", "isin"],
                    "identity": ["sid"],
                    "secondary_identities": [{"name": "by_isin", "fields": ["isin"]}],
                }
            ),
            Vertex.model_validate(
                {
                    "name": "issuer",
                    "properties": ["iid", "lei"],
                    "identity": ["iid"],
                    "secondary_identities": [{"name": "by_lei", "fields": ["lei"]}],
                }
            ),
        ]
    )


def _empty_context() -> AssemblyContext:
    return AssemblyContext.from_extraction(ExtractionContext())


def _context(vertex_config: VertexConfig, source_docs, target_docs) -> AssemblyContext:
    ctx = _empty_context()
    lindex = LocationIndex()
    for doc in source_docs:
        ctx.acc_vertex["instrument"][lindex].append(VertexRep(vertex=doc))
    for doc in target_docs:
        ctx.acc_vertex["issuer"][lindex].append(VertexRep(vertex=doc))
    return ctx


class TestMergeBasis:
    def test_secondary_only_docs_would_collapse_under_primary_basis(self) -> None:
        """Documents the regression: merging keyless docs folds them into one."""
        docs = [{"isin": "US001"}, {"isin": "US002"}, {"isin": "US003"}]
        assert len(merge_doc_basis(list(docs), ("sid",))) == 1

    def test_merge_on_selected_fields_keeps_rows_distinct(
        self, vertex_config: VertexConfig
    ) -> None:
        ctx = _context(
            vertex_config,
            [{"isin": "US001"}, {"isin": "US002"}, {"isin": "US003"}],
            [{"lei": "L1"}],
        )
        _merge_vertices_for_edge(
            ctx,
            vertex_config,
            "instrument",
            "issuer",
            source_fields=["isin"],
            target_fields=["lei"],
        )
        merged = ctx.acc_vertex["instrument"][LocationIndex()]
        assert [rep.vertex["isin"] for rep in merged] == ["US001", "US002", "US003"]

    def test_same_endpoint_type_merges_once_on_the_union(
        self, vertex_config: VertexConfig
    ) -> None:
        """A self-edge shares one bucket, so both field-sets must be honoured."""
        ctx = _empty_context()
        lindex = LocationIndex()
        for doc in ({"sid": "S1", "isin": "US001"}, {"sid": "S2", "isin": "US002"}):
            ctx.acc_vertex["instrument"][lindex].append(VertexRep(vertex=doc))
        _merge_vertices_for_edge(
            ctx,
            vertex_config,
            "instrument",
            "instrument",
            source_fields=["sid"],
            target_fields=["isin"],
        )
        merged = ctx.acc_vertex["instrument"][lindex]
        assert [rep.vertex["sid"] for rep in merged] == ["S1", "S2"]


class TestRenderProjection:
    def test_endpoints_project_the_selected_fields(
        self, vertex_config: VertexConfig
    ) -> None:
        ctx = _context(vertex_config, [{"isin": "US001"}], [{"lei": "L1"}])
        edge = Edge.from_dict({"source": "instrument", "target": "issuer"})
        rendered = render_edge(
            edge=edge,
            vertex_config=vertex_config,
            ctx=ctx,
            source_match_fields=["isin"],
            target_match_fields=["lei"],
        )
        (source_proj, target_proj, _), = rendered[None]
        assert source_proj == {"isin": "US001"}
        assert target_proj == {"lei": "L1"}

    def test_asymmetric_projection(self, vertex_config: VertexConfig) -> None:
        """Source on the primary identity, target on a secondary one."""
        ctx = _context(
            vertex_config, [{"sid": "S1", "isin": "US001"}], [{"lei": "L1"}]
        )
        edge = Edge.from_dict({"source": "instrument", "target": "issuer"})
        rendered = render_edge(
            edge=edge,
            vertex_config=vertex_config,
            ctx=ctx,
            source_match_fields=["sid"],
            target_match_fields=["lei"],
        )
        (source_proj, target_proj, _), = rendered[None]
        assert source_proj == {"sid": "S1"}
        assert target_proj == {"lei": "L1"}

    def test_defaults_to_primary_identity(self, vertex_config: VertexConfig) -> None:
        """Omitting the field-sets keeps the historical behaviour exactly."""
        ctx = _context(
            vertex_config, [{"sid": "S1", "isin": "US001"}], [{"iid": "I1"}]
        )
        edge = Edge.from_dict({"source": "instrument", "target": "issuer"})
        rendered = render_edge(edge=edge, vertex_config=vertex_config, ctx=ctx)
        (source_proj, target_proj, _), = rendered[None]
        assert source_proj == {"sid": "S1"}
        assert target_proj == {"iid": "I1"}

    def test_endpoints_without_the_selected_fields_are_dropped(
        self, vertex_config: VertexConfig
    ) -> None:
        """An endpoint carrying none of the match fields cannot be located."""
        ctx = _context(vertex_config, [{"sid": "S1"}], [{"lei": "L1"}])
        edge = Edge.from_dict({"source": "instrument", "target": "issuer"})
        rendered = render_edge(
            edge=edge,
            vertex_config=vertex_config,
            ctx=ctx,
            source_match_fields=["isin"],
            target_match_fields=["lei"],
        )
        assert not rendered[None]


class TestLookupOnlyObservations:
    def test_default_is_written(self) -> None:
        assert VertexRep(vertex={"sid": "S1"}).lookup_only is False

    def test_flag_survives_merge(self) -> None:
        """merge_doc_basis rebuilds VertexReps; the tag must not be lost."""
        reps = [
            VertexRep(vertex={"isin": "US001"}, lookup_only=True),
            VertexRep(vertex={"isin": "US002"}, lookup_only=True),
        ]
        merged = merge_doc_basis(reps, ("isin",))
        assert all(rep.lookup_only for rep in merged)
