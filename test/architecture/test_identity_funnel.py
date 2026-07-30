"""Identity funnel: model validation, digest semantics, and cast-path materialization.

The cast-path tests are regression cover for a defect fixed here: digest
identities used to be computed only in ``DBWriter``, so at assemble time a
hash-mode doc still had an empty ``id``. ``merge_doc_basis`` then folded the
whole batch into one document and ``drop_empty_identity_docs`` deleted what was
left, edges included.
"""

from __future__ import annotations

import pytest

from graflo.architecture.graph_types import (
    ExtractionContext,
    GraphContainer,
    LocationIndex,
    VertexRep,
)
from graflo.architecture.pipeline.runtime.actor.wrapper import ActorWrapper
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.identity_digest import (
    compute_funnel_identity,
    compute_hash_identity,
    compute_vertex_identity,
    ensure_digest_identities_in_acc_vertex,
)
from graflo.architecture.schema.identity_funnel import IdentityBranch, IdentityFunnel
from graflo.architecture.schema.vertex import Field, FieldType, Vertex, VertexConfig
from graflo.hq.document_caster import filter_graph_container_drop_empty_identity_inplace

PARTY_PROPERTIES = ["name", "email", "phone", "country", "dob"]


def _funnel() -> IdentityFunnel:
    return IdentityFunnel(
        branches=[
            IdentityBranch(id="email", fields=["email"]),
            IdentityBranch(
                id="phone",
                when_all_present=["phone", "country"],
                fields=["phone", "country"],
            ),
            IdentityBranch(id="weak", fields=["name", "dob"]),
        ]
    )


def _party(**kwargs) -> Vertex:
    payload: dict = {
        "name": "party",
        "properties": list(PARTY_PROPERTIES),
        "identity_funnel": _funnel(),
    }
    payload.update(kwargs)
    return Vertex.model_validate(payload)


# ---------------------------------------------------------------- model


class TestFunnelModel:
    def test_branch_defaults_condition_to_its_fields(self) -> None:
        branch = IdentityBranch(id="email", fields=["email"])
        assert branch.when_all_present is None
        assert branch.required_fields == ["email"]

    def test_explicit_condition_is_used_as_required_fields(self) -> None:
        branch = IdentityBranch(
            id="phone", when_all_present=["phone"], fields=["phone", "country"]
        )
        assert branch.required_fields == ["phone"]

    def test_duplicate_branch_ids_rejected(self) -> None:
        with pytest.raises(ValueError, match="duplicate branch ids"):
            IdentityFunnel(
                branches=[
                    IdentityBranch(id="a", fields=["email"]),
                    IdentityBranch(id="a", fields=["phone"]),
                ]
            )

    def test_condition_outside_branch_fields_rejected(self) -> None:
        with pytest.raises(ValueError, match="when_all_present names"):
            IdentityBranch(id="a", when_all_present=["zip"], fields=["email"])

    def test_empty_branch_list_rejected(self) -> None:
        with pytest.raises(ValueError):
            IdentityFunnel(branches=[])

    def test_field_names_are_deduped_in_declaration_order(self) -> None:
        assert _funnel().field_names == ["email", "phone", "country", "name", "dob"]

    def test_yaml_round_trip(self) -> None:
        vertex = _party()
        restored = Vertex.from_dict(vertex.to_dict(skip_defaults=False))
        assert restored.identity_funnel == vertex.identity_funnel


class TestFunnelOnVertex:
    def test_funnel_resolves_to_hash_mode_and_synthetic_identity(self) -> None:
        vertex = _party()
        config = VertexConfig(vertices=[vertex])
        assert vertex.identity_mode == "hash"
        assert vertex.has_identity_funnel is True
        assert vertex.identity == ["id"]
        assert config.hash_identity_vertices == ["party"]
        assert config.identity_funnel_vertices == ["party"]

    def test_funnel_fields_are_synthesized_as_properties(self) -> None:
        vertex = Vertex(name="party", properties=["name"], identity_funnel=_funnel())
        assert set(vertex.property_names) >= set(_funnel().field_names)

    def test_funnel_and_hash_properties_are_mutually_exclusive(self) -> None:
        with pytest.raises(ValueError, match="mutually exclusive"):
            _party(hash_identity_properties=["email"])

    @pytest.mark.parametrize("flag", ["blank", "assigned"])
    def test_funnel_excludes_minted_modes(self, flag: str) -> None:
        with pytest.raises(ValueError, match="mutually exclusive"):
            _party(**{flag: True})

    def test_list_typed_funnel_field_rejected(self) -> None:
        with pytest.raises(ValueError, match="cannot be used in identity_funnel"):
            Vertex(
                name="party",
                properties=[
                    Field(name="email", type=FieldType.LIST, item_type=FieldType.STRING)
                ],
                identity_funnel=IdentityFunnel(
                    branches=[IdentityBranch(id="email", fields=["email"])]
                ),
            )


# ---------------------------------------------------------------- digest


class TestDigest:
    def test_first_complete_branch_wins(self) -> None:
        funnel = _funnel()
        doc = {"email": "a@b.c", "phone": "123", "country": "FR"}
        assert compute_funnel_identity(doc, funnel) == compute_funnel_identity(
            {"email": "a@b.c"}, funnel
        )

    def test_later_branch_fires_when_earlier_is_incomplete(self) -> None:
        funnel = _funnel()
        by_phone = compute_funnel_identity({"phone": "123", "country": "FR"}, funnel)
        assert by_phone is not None
        assert by_phone != compute_funnel_identity({"email": "a@b.c"}, funnel)

    def test_partial_branch_condition_falls_through(self) -> None:
        """phone without country must not fire the phone branch."""
        funnel = _funnel()
        doc = {"phone": "123", "name": "Ada", "dob": "1815-12-10"}
        assert compute_funnel_identity(doc, funnel) == compute_funnel_identity(
            {"name": "Ada", "dob": "1815-12-10"}, funnel
        )

    def test_empty_string_counts_as_absent(self) -> None:
        funnel = _funnel()
        doc = {"email": "", "phone": "123", "country": "FR"}
        assert compute_funnel_identity(doc, funnel) == compute_funnel_identity(
            {"phone": "123", "country": "FR"}, funnel
        )

    def test_no_complete_branch_yields_none(self) -> None:
        assert compute_funnel_identity({"name": "Ada"}, _funnel()) is None

    def test_branch_id_separates_equal_payloads(self) -> None:
        """Two branches over the same value must not collide."""
        funnel = IdentityFunnel(
            branches=[
                IdentityBranch(id="primary", fields=["email"]),
                IdentityBranch(id="secondary", fields=["email"]),
            ]
        )
        with_branch = compute_funnel_identity({"email": "a@b.c"}, funnel)
        without_branch = compute_funnel_identity(
            {"email": "a@b.c"},
            funnel.model_copy(update={"include_branch_id": False}),
        )
        assert with_branch != without_branch

    def test_legacy_flat_digest_is_untouched_by_branch_marker(self) -> None:
        """A one-branch funnel is not byte-compatible with a flat hash by design."""
        flat = compute_hash_identity({"email": "a@b.c"}, ["email"])
        single_branch = IdentityFunnel(
            branches=[IdentityBranch(id="email", fields=["email"])],
            include_branch_id=False,
        )
        assert compute_funnel_identity({"email": "a@b.c"}, single_branch) == flat

    def test_all_empty_flat_sources_yield_no_identity(self) -> None:
        """Otherwise every empty doc digests to one shared key and merges."""
        vertex = Vertex(
            name="party",
            properties=["email", "country"],
            hash_identity_properties=["email", "country"],
        )
        assert compute_vertex_identity({"email": None, "country": ""}, vertex) is None
        assert compute_vertex_identity({"email": "a@b.c"}, vertex) is not None

    def test_natural_vertex_has_no_digest_identity(self) -> None:
        vertex = Vertex(name="user", properties=["id"], identity=["id"])
        assert compute_vertex_identity({"id": "u1"}, vertex) is None


# ------------------------------------------------------------ cast path


def _hash_party_config() -> VertexConfig:
    return VertexConfig(
        vertices=[
            Vertex(
                name="party",
                properties=["email", "country"],
                hash_identity_properties=["email", "country"],
            )
        ]
    )


class TestCastPathMaterialization:
    """Digest identities must exist before dedup, edge assembly and the drop filter."""

    def test_acc_vertex_hook_keys_every_distinct_doc(self) -> None:
        config = VertexConfig(vertices=[_party()])
        ext = ExtractionContext()
        loc = LocationIndex(())
        ext.acc_vertex["party"][loc] = [
            VertexRep(vertex={"email": "a@b.c"}),
            VertexRep(vertex={"phone": "123", "country": "FR"}),
            VertexRep(vertex={"name": "Ada"}),
        ]

        ensure_digest_identities_in_acc_vertex(ext.acc_vertex, config)

        ids = [rep.vertex.get("id") for rep in ext.acc_vertex["party"][loc]]
        assert ids[0] and ids[1] and ids[0] != ids[1]
        assert ids[2] is None, "a funnel miss must not invent a key"

    def test_hash_docs_survive_assemble_as_distinct_documents(self) -> None:
        """The core of the defect: the batch used to collapse into one doc."""
        from graflo.architecture.contract.ingestion.steps import VertexActorConfig
        from graflo.architecture.pipeline.runtime.actor.base import ActorInitContext

        config = _hash_party_config()
        wrapper = ActorWrapper.from_config(
            VertexActorConfig(type="vertex", vertex="party")
        )
        wrapper.finish_init(
            ActorInitContext(
                vertex_config=config,
                edge_config=EdgeConfig(edges=[]),
                transforms={},
                infer_edges=False,
            )
        )

        ext = ExtractionContext()
        loc = LocationIndex(())
        ext.acc_vertex["party"][loc] = [
            VertexRep(vertex={"email": "a@b.c", "country": "FR"}),
            VertexRep(vertex={"email": "d@e.f", "country": "FR"}),
            VertexRep(vertex={"email": "g@h.i", "country": "DE"}),
        ]

        docs = wrapper.assemble(ext).get("party", [])

        assert len(docs) == 3
        assert len({doc["id"] for doc in docs}) == 3

    def test_edges_to_a_hash_endpoint_keep_their_key(self) -> None:
        from graflo.architecture.contract.ingestion.steps import VertexActorConfig
        from graflo.architecture.graph_types import EdgeIntent
        from graflo.architecture.pipeline.runtime.actor.base import ActorInitContext

        config = VertexConfig(
            vertices=[
                Vertex(
                    name="party",
                    properties=["email", "country"],
                    hash_identity_properties=["email", "country"],
                ),
                Vertex(name="order", properties=[Field(name="id")], identity=["id"]),
            ]
        )
        edge = Edge(source="party", target="order", relation="placed")
        edge.finish_init(config)

        wrapper = ActorWrapper.from_config(
            VertexActorConfig(type="vertex", vertex="party")
        )
        wrapper.finish_init(
            ActorInitContext(
                vertex_config=config,
                edge_config=EdgeConfig(edges=[edge]),
                transforms={},
                infer_edges=False,
            )
        )

        ext = ExtractionContext()
        loc = LocationIndex(())
        ext.acc_vertex["party"][loc] = [
            VertexRep(vertex={"email": "a@b.c", "country": "FR"})
        ]
        ext.acc_vertex["order"][loc] = [VertexRep(vertex={"id": "o1"})]
        ext.edge_intents.append(EdgeIntent(edge=edge, location=loc, derivation=None))

        result = wrapper.assemble(ext)

        party_id = result["party"][0]["id"]
        assert party_id
        edge_docs = result.get(("party", "order", "placed"), [])
        assert edge_docs, "edge to a hash endpoint was dropped"
        assert edge_docs[0][0]["id"] == party_id

    def test_drop_filter_keeps_keyed_hash_docs_and_drops_misses(self) -> None:
        config = VertexConfig(vertices=[_party()])
        vertex = config._get_vertex_by_name("party")
        keyed = {"email": "a@b.c"}
        keyed["id"] = compute_vertex_identity(keyed, vertex)
        graph = GraphContainer(
            vertices={"party": [keyed, {"name": "Ada"}]}, edges={}, linear=[]
        )

        filter_graph_container_drop_empty_identity_inplace(graph, vertex_config=config)

        assert graph.vertices["party"] == [keyed]
