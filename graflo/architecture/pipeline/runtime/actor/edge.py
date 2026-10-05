"""Edge actor for processing edge data."""

from __future__ import annotations

import logging
import threading
from typing import Any

from graflo.architecture.contract.ingestion.steps import EdgeActorConfig, EdgeLinkConfig
from graflo.architecture.graph_types import (
    EdgeId,
    ExtractionContext,
    LocationIndex,
    Weight,
    merge_observation_with_transform_buffer,
)
from graflo.architecture.graph_types.edge_derivation import (
    EdgeDerivation,
    EndpointMatch,
    EndpointRule,
    EndpointSelector,
    selector_for,
)
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.inverse_realization import inverse_emission_refusal
from graflo.architecture.schema.vertex import VertexConfig, VertexName
from graflo.onto import PRIMARY_IDENTITY_SELECTOR

from .base import Actor, ActorInitContext

logger = logging.getLogger(__name__)

# Serializes the check-create-register of an edge named per document. Cast
# workers share one runtime, so two documents naming the same new edge would
# otherwise both create it. Module-level for the reason the vertex router's
# lock is: actors are deep-copied, and a held lock cannot cross a copy or pickle.
_EDGE_REGISTRATION_LOCK = threading.Lock()


def _link_to_edge_actor_config(link: EdgeLinkConfig) -> EdgeActorConfig:
    """Convert an EdgeLinkConfig item into a standalone EdgeActorConfig for delegation."""
    data: dict[str, Any] = {"type": "edge"}
    # source/target role keys are canonicalized in EdgeLinkConfig.resolve_and_validate
    if link.source is not None:
        data["from"] = link.source
    if link.target is not None:
        data["to"] = link.target
    if link.source_role is not None:
        data["source_role"] = link.source_role
    if link.target_role is not None:
        data["target_role"] = link.target_role
    if link.relation is not None:
        data["relation"] = link.relation
    if link.relation_field is not None:
        data["relation_field"] = link.relation_field
    if link.match_source is not None:
        data["match_source"] = link.match_source
    if link.match_target is not None:
        data["match_target"] = link.match_target
    if link.source_match is not None:
        data["source_match"] = link.source_match
    if link.target_match is not None:
        data["target_match"] = link.target_match
    if link.on_ambiguous is not None:
        data["on_ambiguous"] = link.on_ambiguous
    if link.emit_inverse:
        data["emit_inverse"] = True
    return EdgeActorConfig.model_validate(data)


def _held(
    roles: dict[str, frozenset[str] | None], role: str | None
) -> frozenset[str] | None:
    """What *role* can hold, ``None`` when any class or unknown."""
    if role is None:
        return None
    return roles.get(role)


class EdgeActor(Actor):
    """Actor for processing edge data.

    Operates in three modes determined by configuration:

    **Static mode** (``from``/``to`` set): both vertex types are declared at config
    time.  The schema ``Edge`` is created during ``finish_init`` and the
    ``__call__`` path is unchanged from the original implementation.

    **Dynamic mode** (at least one of ``source_role``/``target_role`` set, with
    ``source_type_field``/``target_type_field`` accepted as legacy aliases):
    vertex types for the dynamic side(s) are
    resolved at extraction time by looking up accumulator slots populated by an
    upstream ``VertexRouterActor`` (slot segment = ``role`` or ``type_field``) or a
    ``VertexActor`` with a matching ``role``.
    The schema ``Edge`` is created—or retrieved from cache—per unique
    ``(source_type, target_type, relation)`` triple encountered.

    **Multi-link mode** (``links`` list set): each item in ``links`` becomes a
    dedicated sub-``EdgeActor`` that runs in sequence per document, emitting one edge
    intent each.  Use when one flat document encodes multiple distinct relationships.
    """

    def __init__(self, config: EdgeActorConfig):
        # Multi-link mode: delegate each link to its own EdgeActor.
        if config.links:
            self._link_actors: list[EdgeActor] = [
                EdgeActor(_link_to_edge_actor_config(lk)) for lk in config.links
            ]
            # Null-out all single-intent state so the dispatch is unambiguous.
            self._source_slot_key = None
            self._target_slot_key = None
            self._static_source = None
            self._static_target = None
            self._relation_map: dict[str, str] = {}
            self._relation_map_only = False
            self._strict_edge_types = False
            self._rejected_edges: set[tuple[str, str, str | None]] = set()
            self._edge_cache: dict[tuple[str, str, str | None], Edge] = {}
            self._init_ctx: ActorInitContext | None = None
            self.derivation: EdgeDerivation = EdgeDerivation()
            self._pending_vertex_weights: list[Weight] = []
            self._static_relation = None
            self.edge: Edge | None = None
            self.vertex_config: VertexConfig | None = None
            self.edge_config: EdgeConfig | None = None
            self.allowed_vertex_names: set[VertexName] | None = None
            return

        self._link_actors = []

        self._source_slot_key = config.source_role
        self._target_slot_key = config.target_role
        # Static fallback for whichever side is not dynamic.
        self._static_source = config.source
        self._static_target = config.target
        self._relation_map = config.relation_map or {}
        self._relation_map_only = config.relation_map_only
        self._strict_edge_types = config.strict_edge_types
        self._rejected_edges = set()
        self._edge_cache = {}
        self._init_ctx = None

        self.derivation = config.derivation
        self._pending_vertex_weights = []

        # In dynamic/mixed mode the static relation (if set) is used as a fallback
        # when relation_field yields nothing.
        self._static_relation = None

        # Dynamic mode: at least one side is resolved at extraction time.
        # Static mode: both sides are fixed at config time.
        is_dynamic = (
            self._source_slot_key is not None or self._target_slot_key is not None
        )
        if not is_dynamic:
            payload: dict[str, Any] = {
                "source": config.source,
                "target": config.target,
            }
            if config.relation is not None:
                payload["relation"] = config.relation
            if config.description is not None:
                payload["description"] = config.description
            if config.properties:
                payload["properties"] = config.properties
            for item in config.vertex_weights:
                self._pending_vertex_weights.append(Weight.model_validate(item))
            self.edge: Edge | None = Edge.from_dict(payload)
        else:
            self.edge = None
            self._static_relation = config.relation

        self.vertex_config: VertexConfig | None = None
        self.edge_config: EdgeConfig | None = None
        self.allowed_vertex_names: set[VertexName] | None = None

    @property
    def relation_field(self) -> str | None:
        """Alias for tooling (e.g. plot labels)."""
        return self.derivation.relation_field

    @property
    def is_dynamic(self) -> bool:
        """Whether this step names its edges per document, and so registers them mid-cast."""
        if self._link_actors:
            return any(link.is_dynamic for link in self._link_actors)
        return self._source_slot_key is not None or self._target_slot_key is not None

    @classmethod
    def from_config(cls, config: EdgeActorConfig) -> EdgeActor:
        return cls(config)

    def fetch_important_items(self) -> dict[str, Any]:
        if self._link_actors:
            return {"links": str(len(self._link_actors))}
        items: dict[str, Any] = {}
        if self.edge is not None:
            items["source"] = self.edge.source
            items["target"] = self.edge.target
        else:
            if self._source_slot_key is not None:
                items["source_role"] = self._source_slot_key
            elif self._static_source is not None:
                items["source"] = self._static_source
            if self._target_slot_key is not None:
                items["target_role"] = self._target_slot_key
            elif self._static_target is not None:
                items["target"] = self._static_target
        for k in ("match_source", "match_target"):
            v = getattr(self.derivation, k)
            if v is not None:
                items[k] = v
        return items

    def finish_init(self, init_ctx: ActorInitContext) -> None:
        self._init_ctx = init_ctx
        self.vertex_config = init_ctx.vertex_config
        self.edge_config = init_ctx.edge_config
        self.allowed_vertex_names = init_ctx.allowed_vertex_names

        if self._link_actors:
            # Multi-link mode: delegate finish_init to each sub-actor.
            for la in self._link_actors:
                la.finish_init(init_ctx)
            return

        if init_ctx.strict_references:
            # Strict references close the schema: a relation found in the data
            # must be declared too.
            self._strict_edge_types = True

        if self.edge is not None:
            # Static mode: register schema Edge now.
            self._adopt_declared_relation()
            edge_id = self.edge.edge_id
            if init_ctx.strict_references:
                self._refuse_undeclared(edge_id)
            init_ctx.edge_config.update_edges(
                self.edge, vertex_config=self.vertex_config
            )
            if self.derivation.relation_from_key:
                init_ctx.edge_derivation.mark_relation_from_key(edge_id)
            if self._pending_vertex_weights:
                init_ctx.edge_derivation.merge_vertex_weights(
                    edge_id, self._pending_vertex_weights
                )
            self._register_endpoint_match(init_ctx, edge_id)
            self.edge = init_ctx.edge_config.edge_for(edge_id)
            self._check_inverse_emission(init_ctx, edge_id)
        else:
            # Dynamic mode: cache will be populated per-document.
            self._edge_cache.clear()
            self._register_endpoint_rule(
                init_ctx, self._static_source, self._static_target
            )
            self._check_inverse_emission(init_ctx, None)

    def _refuse_undeclared(self, edge_id: EdgeId) -> None:
        """Refuse a static step whose edge the schema does not declare.

        Checked before the step registers its edge, so the resource's edge
        config still holds only what the schema declares.
        """
        if self.edge_config is None or self.edge_config.declared(edge_id) is not None:
            return
        source, target, relation = edge_id
        declared = sorted(
            str(r) for r in self._declared_relations(source, target) if r is not None
        )
        raise ValueError(
            f"edge step {source} -> {target} (relation {relation!r}) writes an edge "
            "the schema does not declare"
            + (f"; declared between them: {declared}" if declared else "")
            + ". Declare it in edge_config, or name a declared relation"
        )

    def _declared_relations(self, source: str, target: str) -> set[str | None]:
        """Relations the edge config declares from *source* to *target*."""
        if self.edge_config is None:
            return set()
        return {
            edge.relation
            for edge in self.edge_config.edges
            if edge.source == source and edge.target == target
        }

    def _adopt_declared_relation(self) -> None:
        """Give a static step that names no relation the one its endpoints declare.

        Otherwise the step registers a relation-less edge beside the declared
        one, and the writer drops every edge it renders. Several declared
        relations cannot be chosen between, so the step is refused.
        """
        if (
            self.edge is None
            or self.edge.relation is not None
            or self._relation_from_data()
        ):
            return
        declared = self._declared_relations(self.edge.source, self.edge.target)
        if not declared or None in declared:
            return
        if len(declared) > 1:
            raise ValueError(
                f"edge step {self.edge.source} -> {self.edge.target} names no "
                f"relation, and {sorted(r for r in declared if r)} are declared "
                "between them; name one with `relation`"
            )
        (relation,) = declared
        payload = self.edge.to_dict(skip_defaults=True)
        payload["relation"] = relation
        self.edge = Edge.from_dict(payload)

    def _check_inverse_emission(
        self, init_ctx: ActorInitContext, edge_id: EdgeId | None
    ) -> None:
        """Tie ``emit_inverse`` to the declared inverses, as far as the step is known.

        A step whose endpoints and relation are all fixed names exactly one edge,
        so everything can be checked now: the relation must have a declared pair
        (a symmetric relation has no inverse edge -- its edges are undirected),
        and the inverse edge must be declared, because only a *materialized*
        inverse is written. A step whose relation or endpoints come from the data
        is checked per document at assembly instead.

        Raises:
            ValueError: naming the step's edge and what to declare.
        """
        if not self.derivation.emit_inverse:
            return
        derivation = self.derivation
        data_driven = (
            edge_id is None
            or edge_id[2] is None
            or derivation.relation_field is not None
            or derivation.relation_from_key
        )
        if data_driven:
            if derivation.uses_secondary_identity():
                # The writer learns which identity to match an endpoint on per
                # edge id, at load time; an inverse known only per document has
                # no id to register the swapped selectors under.
                raise ValueError(
                    "emit_inverse cannot be combined with source_match / "
                    "target_match on an edge step whose relation or endpoints come "
                    "from the data; write the inverse with a step of its own"
                )
            return

        assert edge_id is not None
        edge_config = init_ctx.edge_config
        refusal = inverse_emission_refusal(edge_config, edge_id)
        if refusal is not None:
            raise ValueError(f"emit_inverse on edge step {edge_id}: {refusal}")
        source, target, relation = edge_id
        inverse_id = (target, source, edge_config.inverse_of(relation))
        if inverse_id in edge_config and (
            derivation.uses_secondary_identity() or derivation.on_ambiguous is not None
        ):
            # The mirror's endpoints are this step's endpoints, swapped.
            init_ctx.edge_derivation.set_endpoint_match(
                inverse_id,
                EndpointMatch(
                    source=selector_for(derivation.target_match, target),
                    target=selector_for(derivation.source_match, source),
                    on_ambiguous=derivation.on_ambiguous,
                ),
            )

    def _register_endpoint_match(
        self, init_ctx: ActorInitContext, edge_id: EdgeId
    ) -> None:
        """Validate endpoint identity selectors and record them for the writer.

        Resolving here fails fast at manifest load with the vertex name and the
        declared alternatives, rather than mid-ingest on the first batch. A
        relation read from the data leaves the edge id unknown until then, so
        the selectors are recorded as a rule over every relation.
        """
        source_type, target_type, _ = edge_id
        self._validate_selectors(init_ctx, source_type, target_type)
        if not self._selects_endpoints():
            return
        if self._relation_from_data():
            self._add_endpoint_rule(init_ctx, source_type, target_type)
            return
        derivation = self.derivation
        init_ctx.edge_derivation.set_endpoint_match(
            edge_id,
            EndpointMatch(
                source=selector_for(derivation.source_match, source_type),
                target=selector_for(derivation.target_match, target_type),
                on_ambiguous=derivation.on_ambiguous,
            ),
        )

    def _register_endpoint_rule(
        self,
        init_ctx: ActorInitContext,
        source: str | None,
        target: str | None,
    ) -> None:
        """Validate and record the selectors of a step whose edges are named per document.

        *source* / *target* are the endpoints fixed at config time, ``None``
        for one a router role fills.
        """
        self._validate_selectors(init_ctx, source, target)
        if self._selects_endpoints():
            self._add_endpoint_rule(init_ctx, source, target)

    def _selects_endpoints(self) -> bool:
        """Whether the writer needs to know how this step matches its endpoints."""
        derivation = self.derivation
        return (
            derivation.uses_secondary_identity() or derivation.on_ambiguous is not None
        )

    def _relation_from_data(self) -> bool:
        derivation = self.derivation
        return derivation.relation_field is not None or derivation.relation_from_key

    def _add_endpoint_rule(
        self, init_ctx: ActorInitContext, source: str | None, target: str | None
    ) -> None:
        """Record a rule over the edges this step can write.

        The relation is fixed only when the step names one and reads none from
        the data.
        """
        derivation = self.derivation
        if self._relation_from_data():
            relation = None
        elif self.edge is not None:
            relation = self.edge.relation
        else:
            relation = self._static_relation
        init_ctx.edge_derivation.add_endpoint_rule(
            EndpointRule(
                source=source,
                target=target,
                relation=relation,
                source_match=derivation.source_match,
                target_match=derivation.target_match,
                on_ambiguous=derivation.on_ambiguous,
            )
        )

    def _validate_selectors(
        self, init_ctx: ActorInitContext, source: str | None, target: str | None
    ) -> None:
        roles = init_ctx.role_reach
        self._validate_selector(
            "source_match",
            self.derivation.source_match,
            source,
            _held(roles, self._source_slot_key),
        )
        self._validate_selector(
            "target_match",
            self.derivation.target_match,
            target,
            _held(roles, self._target_slot_key),
        )

    def _validate_selector(
        self,
        name: str,
        selector: EndpointSelector | None,
        endpoint: str | None,
        held: frozenset[str] | None,
    ) -> None:
        """Refuse a selector no class this endpoint can be declares.

        *endpoint* is the class fixed at config time, ``None`` for one a router
        role fills; *held* is what that role can hold, ``None`` for any class.
        A plain selector on such an endpoint is resolved against each routed
        class when the edge is rendered; a per-class one is checked here, entry
        by entry: on a fixed endpoint it may name only that class, on a role
        only the classes the role can hold.

        Raises:
            ValueError: naming the selector, the class and what it declares.
        """
        vertex_config = self.vertex_config
        if vertex_config is None:
            return
        if isinstance(selector, dict):
            unknown = sorted(set(selector) - vertex_config.vertex_set)
            if unknown:
                raise ValueError(
                    f"{name} names {unknown}, which are not among the classes "
                    "this resource can produce"
                )
            if endpoint is not None:
                foreign = sorted(set(selector) - {endpoint})
                if foreign:
                    raise ValueError(
                        f"{name} names {foreign}, but the endpoint is always "
                        f"{endpoint!r}"
                    )
            elif held is not None:
                foreign = sorted(set(selector) - held)
                if foreign:
                    raise ValueError(
                        f"{name} names {foreign}, which the role can never hold; "
                        f"it holds {sorted(held)}"
                    )
            for vertex, plain in selector.items():
                # Raises with the declared alternatives when a selector is unknown.
                vertex_config.match_fields(vertex, plain)
        elif endpoint is not None and selector not in (None, PRIMARY_IDENTITY_SELECTOR):
            vertex_config.match_fields(endpoint, selector)

    # ------------------------------------------------------------------
    # Dynamic-mode helpers
    # ------------------------------------------------------------------

    def _get_or_create_edge(
        self, source: str, target: str, relation: str | None
    ) -> Edge | None:
        key = (source, target, relation)
        cached = self._edge_cache.get(key)
        if cached is not None:
            return cached
        if key in self._rejected_edges:
            return None
        with _EDGE_REGISTRATION_LOCK:
            return self._create_edge_locked(key)

    def _create_edge_locked(self, key: tuple[str, str, str | None]) -> Edge | None:
        """Create and register the edge for *key*; the registration lock is held."""
        source, target, relation = key
        cached = self._edge_cache.get(key)
        if cached is not None:
            return cached
        # Skip what the schema does not declare, by the writer's rule: the edge
        # itself, or the relation-less template between its endpoints.
        if (
            self._strict_edge_types
            and self.edge_config is not None
            and self.edge_config.declared(key) is None
        ):
            if key not in self._rejected_edges:
                self._rejected_edges.add(key)
                logger.warning(
                    "Edge (%s, %s, %s) is skipped: the schema does not declare it "
                    "(reported once per step).",
                    source,
                    target,
                    relation,
                )
            return None
        edge = Edge(
            source=source,
            target=target,
            relation=relation,
            # Edges of one relation agree on `directed`; a relation named only
            # at ingest time follows the ones already declared.
            directed=(
                self.edge_config.directed_for(relation)
                if self.edge_config is not None
                else True
            ),
        )
        if self.vertex_config is not None:
            edge.finish_init(vertex_config=self.vertex_config)
        if self.edge_config is not None and self.vertex_config is not None:
            self.edge_config.update_edges(edge, vertex_config=self.vertex_config)
        self._edge_cache[key] = edge
        logger.debug(
            "EdgeActor: registered dynamic edge (%s, %s, %s)", source, target, relation
        )
        return edge

    def _find_type_at_slot(
        self, ctx: ExtractionContext, slot_lindex: LocationIndex
    ) -> str | None:
        """Scan acc_vertex to find which vertex type has data at *slot_lindex*."""
        for vtype, by_loc in ctx.acc_vertex.items():
            if by_loc.get(slot_lindex):
                return vtype
        return None

    # ------------------------------------------------------------------
    # Main dispatch
    # ------------------------------------------------------------------

    def __call__(
        self, ctx: ExtractionContext, lindex: LocationIndex, *nargs: Any, **kwargs: Any
    ) -> ExtractionContext:
        if self._link_actors:
            # Multi-link mode: run each sub-actor in sequence.
            for la in self._link_actors:
                ctx = la(ctx, lindex, *nargs, **kwargs)
            return ctx
        if self._source_slot_key is not None or self._target_slot_key is not None:
            return self._call_dynamic(ctx, lindex, **kwargs)
        return self._call_static(ctx, lindex, **kwargs)

    def _call_static(
        self, ctx: ExtractionContext, lindex: LocationIndex, **kwargs: Any
    ) -> ExtractionContext:
        """Static mode: unchanged behavior from original EdgeActor."""
        assert self.edge is not None
        if self.allowed_vertex_names is not None and (
            self.edge.source not in self.allowed_vertex_names
            or self.edge.target not in self.allowed_vertex_names
        ):
            return ctx
        if (
            self.allowed_vertex_names is None
            and self.vertex_config is not None
            and (
                self.edge.source not in self.vertex_config.vertex_set
                or self.edge.target not in self.vertex_config.vertex_set
            )
        ):
            return ctx

        der = None if self.derivation.is_empty() else self.derivation
        ctx.record_edge_intent(
            edge=self.edge,
            location=lindex,
            derivation=der,
        )
        return ctx

    def _call_dynamic(
        self, ctx: ExtractionContext, lindex: LocationIndex, **kwargs: Any
    ) -> ExtractionContext:
        """Dynamic / mixed mode: resolve dynamic side(s) from VRA accumulator slots.

        Source or target (but not both) may be statically declared; that side's
        type is taken directly from config rather than looked up in the accumulator.
        """
        raw_observation = kwargs.get("doc", {})
        if not isinstance(raw_observation, dict):
            logger.debug(
                "EdgeActor: expected dict observation, got %s, skipping",
                type(raw_observation).__name__,
            )
            return ctx

        buffer_items: list[Any] = list(ctx.transform_buffer.get(lindex, []))
        doc = merge_observation_with_transform_buffer(raw_observation, buffer_items)
        ctx.obs_buffer[lindex] = dict(doc)

        # --- source type ---
        if self._source_slot_key is not None:
            source_slot_lindex = lindex.extend((self._source_slot_key, 0))
            source_type = self._find_type_at_slot(ctx, source_slot_lindex)
            if source_type is None:
                logger.debug(
                    "EdgeActor: no vertex data at source slot '%s', skipping",
                    self._source_slot_key,
                )
                return ctx
        else:
            # Mixed mode: static source
            assert self._static_source is not None
            source_type = self._static_source

        if (
            self.vertex_config is not None
            and source_type not in self.vertex_config.vertex_set
        ):
            logger.debug(
                "EdgeActor: source type '%s' not in vertex_set, skipping", source_type
            )
            return ctx

        # --- target type ---
        if self._target_slot_key is not None:
            target_slot_lindex = lindex.extend((self._target_slot_key, 0))
            target_type = self._find_type_at_slot(ctx, target_slot_lindex)
            if target_type is None:
                logger.debug(
                    "EdgeActor: no vertex data at target slot '%s', skipping",
                    self._target_slot_key,
                )
                return ctx
        else:
            # Mixed mode: static target
            assert self._static_target is not None
            target_type = self._static_target

        if (
            self.vertex_config is not None
            and target_type not in self.vertex_config.vertex_set
        ):
            logger.debug(
                "EdgeActor: target type '%s' not in vertex_set, skipping", target_type
            )
            return ctx

        # allowed_vertex_names early-exit
        if self.allowed_vertex_names is not None and (
            source_type not in self.allowed_vertex_names
            or target_type not in self.allowed_vertex_names
        ):
            return ctx

        # --- relation ---
        raw_relation: str | None
        if self.derivation.relation_field:
            raw_relation = doc.get(self.derivation.relation_field)
        else:
            raw_relation = None

        if raw_relation is not None:
            if self._relation_map_only and raw_relation not in self._relation_map:
                return ctx
            relation: str | None = self._relation_map.get(raw_relation, raw_relation)
        else:
            relation = self._static_relation
        if relation is None and not self._relation_from_data():
            # The per-document counterpart of `_adopt_declared_relation`; with
            # several declared relations the edge stays relation-less.
            declared = self._declared_relations(source_type, target_type)
            if len(declared) == 1:
                (relation,) = declared

        # Create / retrieve cached schema Edge.
        edge = self._get_or_create_edge(source_type, target_type, relation)
        if edge is None:
            return ctx

        # Build derivation: slot names for dynamic sides so render_edge can filter,
        # and the endpoint selectors, which render reduces to the concrete classes.
        derivation = EdgeDerivation(
            match_source=self._source_slot_key,
            match_target=self._target_slot_key,
            source_match=self.derivation.source_match,
            target_match=self.derivation.target_match,
            on_ambiguous=self.derivation.on_ambiguous,
            emit_inverse=self.derivation.emit_inverse,
        )
        ctx.record_edge_intent(edge=edge, location=lindex, derivation=derivation)
        return ctx

    def references_vertices(self) -> set[VertexName]:
        if self._link_actors:
            result: set[str] = set()
            for la in self._link_actors:
                result |= la.references_vertices()
            return result
        if self.edge is not None:
            return {self.edge.source, self.edge.target}
        static: set[str] = set()
        if self._static_source:
            static.add(self._static_source)
        if self._static_target:
            static.add(self._static_target)
        return (
            static
            | {s for s, _, _ in self._edge_cache}
            | {t for _, t, _ in self._edge_cache}
        )
