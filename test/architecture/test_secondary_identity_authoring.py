"""Authored surface for secondary-identity edge endpoints.

Covers the YAML an author writes — ``source_match`` / ``target_match`` on an
edge step, ``lookup_only`` on a vertex step, and ``endpoints_on_ambiguous`` on
the ingestion model — and the runtime state it produces.
"""

from __future__ import annotations

import pytest

from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.contract.runtime.edge_derivation import (
    EdgeDerivationRegistry,
    EndpointMatch,
)
from graflo.architecture.pipeline.runtime.actor.base import ActorInitContext
from graflo.architecture.pipeline.runtime.actor.config import EdgeActorConfig
from graflo.architecture.pipeline.runtime.actor.wrapper import ActorWrapper
from graflo.architecture.schema.edge import EdgeConfig
from graflo.architecture.schema.vertex import Vertex, VertexConfig


@pytest.fixture
def vertex_config() -> VertexConfig:
    return VertexConfig(
        vertices=[
            Vertex.model_validate(
                {
                    "name": "instrument",
                    "properties": ["sid", "isin", "org", "local_code"],
                    "identity": ["sid"],
                    "secondary_identities": [
                        {"name": "by_isin", "fields": ["isin"]},
                        ["org", "local_code"],
                    ],
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


def _init(
    step: dict, vertex_config: VertexConfig
) -> tuple[ActorWrapper, ActorInitContext]:
    wrapper = ActorWrapper(**step)
    ctx = ActorInitContext(
        vertex_config=vertex_config,
        edge_config=EdgeConfig(),
        transforms={},
        edge_derivation=EdgeDerivationRegistry(),
    )
    wrapper.finish_init(init_ctx=ctx)
    return wrapper, ctx


class TestEdgeStepSelectors:
    def test_default_records_nothing(self, vertex_config: VertexConfig) -> None:
        """An edge step without selectors leaves the write path untouched."""
        _, ctx = _init({"edge": {"from": "instrument", "to": "issuer"}}, vertex_config)
        assert (
            ctx.edge_derivation.endpoint_match_for(("instrument", "issuer", None))
            is None
        )

    def test_asymmetric_selection_is_recorded(
        self, vertex_config: VertexConfig
    ) -> None:
        """Source and target may pick different identities independently."""
        _, ctx = _init(
            {
                "edge": {
                    "from": "instrument",
                    "to": "issuer",
                    "source_match": "by_isin",
                    "target_match": "identity",
                }
            },
            vertex_config,
        )
        match = ctx.edge_derivation.endpoint_match_for(("instrument", "issuer", None))
        assert match == EndpointMatch(
            source="by_isin", target="identity", on_ambiguous=None
        )

    def test_explicit_field_list_is_recorded(self, vertex_config: VertexConfig) -> None:
        _, ctx = _init(
            {
                "edge": {
                    "from": "instrument",
                    "to": "issuer",
                    "source_match": ["org", "local_code"],
                }
            },
            vertex_config,
        )
        match = ctx.edge_derivation.endpoint_match_for(("instrument", "issuer", None))
        assert match is not None and match.source == ["org", "local_code"]

    def test_unknown_selector_fails_at_load(self, vertex_config: VertexConfig) -> None:
        """Fail fast at manifest load, naming the declared alternatives."""
        with pytest.raises(ValueError, match="by_isin"):
            _init(
                {
                    "edge": {
                        "from": "instrument",
                        "to": "issuer",
                        "source_match": "by_cusip",
                    }
                },
                vertex_config,
            )

    def test_per_step_ambiguity_override_is_recorded(
        self, vertex_config: VertexConfig
    ) -> None:
        _, ctx = _init(
            {
                "edge": {
                    "from": "instrument",
                    "to": "issuer",
                    "source_match": "by_isin",
                    "on_ambiguous": "skip",
                }
            },
            vertex_config,
        )
        match = ctx.edge_derivation.endpoint_match_for(("instrument", "issuer", None))
        assert match is not None and match.on_ambiguous == "skip"

    def test_invalid_policy_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            EdgeActorConfig.model_validate(
                {"type": "edge", "from": "a", "to": "b", "on_ambiguous": "sometimes"}
            )

    def test_links_carry_their_own_selectors(self, vertex_config: VertexConfig) -> None:
        """Multi-link mode must not silently drop endpoint selection."""
        _, ctx = _init(
            {
                "edge": {
                    "links": [
                        {
                            "from": "instrument",
                            "to": "issuer",
                            "relation": "issued_by",
                            "source_match": "by_isin",
                            "on_ambiguous": "first",
                        },
                        {
                            "from": "instrument",
                            "to": "issuer",
                            "relation": "guaranteed_by",
                        },
                    ]
                }
            },
            vertex_config,
        )
        selected = ctx.edge_derivation.endpoint_match_for(
            ("instrument", "issuer", "issued_by")
        )
        assert selected is not None
        assert selected.source == "by_isin"
        assert selected.on_ambiguous == "first"
        # The second link keeps the default and records nothing.
        assert (
            ctx.edge_derivation.endpoint_match_for(
                ("instrument", "issuer", "guaranteed_by")
            )
            is None
        )


class TestEndpointMatch:
    @pytest.mark.parametrize(
        "match",
        [
            EndpointMatch(),
            EndpointMatch(source="identity", target="identity"),
        ],
    )
    def test_primary_selection_is_default(self, match: EndpointMatch) -> None:
        assert match.is_default()

    def test_secondary_selection_is_not_default(self) -> None:
        assert not EndpointMatch(source="by_isin").is_default()

    def test_policy_alone_is_not_default(self) -> None:
        assert not EndpointMatch(on_ambiguous="skip").is_default()


class TestRegistry:
    def test_merge_carries_endpoint_match(self) -> None:
        left, right = EdgeDerivationRegistry(), EdgeDerivationRegistry()
        edge_id = ("a", "b", "r")
        right.set_endpoint_match(edge_id, EndpointMatch(source="by_isin"))
        left.merge_from(right)
        assert left.endpoint_match_for(edge_id) == EndpointMatch(source="by_isin")

    def test_copy_carries_endpoint_match(self) -> None:
        registry = EdgeDerivationRegistry()
        edge_id = ("a", "b", "r")
        registry.set_endpoint_match(edge_id, EndpointMatch(target="by_lei"))
        assert registry.copy().endpoint_match_for(edge_id) == EndpointMatch(
            target="by_lei"
        )


class TestIngestionModelPolicy:
    def test_defaults_to_attach_all(self) -> None:
        assert IngestionModel().endpoints_on_ambiguous == "all"

    @pytest.mark.parametrize("policy", ["all", "first", "skip", "error"])
    def test_accepts_every_policy(self, policy: str) -> None:
        model = IngestionModel.model_validate({"endpoints_on_ambiguous": policy})
        assert model.endpoints_on_ambiguous == policy

    def test_rejects_unknown_policy(self) -> None:
        with pytest.raises(ValueError):
            IngestionModel.model_validate({"endpoints_on_ambiguous": "maybe"})


class TestLookupOnlyStep:
    def test_defaults_to_false(self, vertex_config: VertexConfig) -> None:
        wrapper, _ = _init({"vertex": "instrument"}, vertex_config)
        assert wrapper.actor.lookup_only is False

    def test_can_be_declared(self, vertex_config: VertexConfig) -> None:
        wrapper, _ = _init({"vertex": "instrument", "lookup_only": True}, vertex_config)
        assert wrapper.actor.lookup_only is True
