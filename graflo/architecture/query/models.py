"""Read-request models for the DB-agnostic graph query surface.

Four questions an agent asks of a live graph — *which nodes*, *what is adjacent
to this one*, *what is reachable from these*, *how many* — expressed once, in
logical schema names, and answered identically by every backend.

Filters are :class:`FilterExpression`, which already renders to AQL, Cypher,
nGQL, GSQL, SQL and Python. There is no new query language here, and there is
deliberately no way to pass a backend query through: ``Connection.execute`` must
never be reachable from an agent-facing path, so nothing in this module carries
a raw query string.

Validation raises rather than clamps. Silently reducing ``hops=99`` to ``3``
hands the caller a partial answer it believes is complete — the failure mode
this codebase already rejected for TigerGraph edge direction.
"""

from __future__ import annotations

from typing import Any, Self

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.graph_types import EdgeDirection
from graflo.architecture.query.caps import HARD_CAPS, CapExceededError, QueryCaps
from graflo.filter.onto import FilterExpression
from graflo.onto import AggregationType


class GraphQuery(ConfigBaseModel):
    """Shared envelope for every read request.

    Note:
        There is intentionally no ``caps`` field. Caps come from
        :data:`HARD_CAPS` and are lowered only through :meth:`narrowed`, so a
        request body cannot raise its own ceiling.
    """

    limit: int = PydanticField(
        default=100, ge=1, description="Rows to return, within `max_rows`."
    )
    timeout_s: float = PydanticField(
        default=10.0, gt=0, description="Seconds to allow, within `timeout_s`."
    )
    projection: list[str] | None = PydanticField(
        default=None,
        description=(
            "Property names to return. None returns whatever the backend "
            "stores, subject to the cap's allow-list."
        ),
    )

    def finish_init(self, caps: QueryCaps | None = None) -> Self:
        """Validate against *caps*, defaulting to the core ceiling.

        Returns:
            Self, so the call composes: ``NodeQuery(...).finish_init()``.

        Raises:
            CapExceededError: Naming the cap that was exceeded.
        """
        effective = caps or HARD_CAPS
        if self.limit > effective.max_rows:
            raise CapExceededError("max_rows", self.limit, effective.max_rows)
        if self.timeout_s > effective.timeout_s:
            raise CapExceededError("timeout_s", self.timeout_s, effective.timeout_s)
        if self.projection is not None and effective.projection_allow_list is not None:
            permitted = set(effective.projection_allow_list)
            denied = sorted(n for n in self.projection if n not in permitted)
            if denied:
                raise CapExceededError(
                    "projection_allow_list", denied, effective.projection_allow_list
                )
        self._validate_specific(effective)
        return self

    def _validate_specific(self, caps: QueryCaps) -> None:
        """Per-query-kind checks. Overridden by subclasses."""

    def narrowed(self, caps: QueryCaps) -> Self:
        """Return a copy fitted to *caps*, raising on anything explicitly asked for.

        The split is what makes caps both enforceable and usable:

        - A value the caller **explicitly set** above a cap raises
          :class:`CapExceededError`. Silently clamping it would hand back a
          partial answer the caller believes is complete — the failure mode this
          codebase already rejected for TigerGraph edge direction.
        - A value the caller **left at its default** is clamped. A policy of
          ``max_rows=5`` must not 422 every request that simply did not mention
          a limit; that would make strict policies unusable rather than safe.

        ``model_fields_set`` is what distinguishes the two, which is precisely
        why it must be consulted rather than comparing against the default.

        Projection is always intersected rather than raising, since an
        allow-list exists to *hide* properties: refusing the request would tell
        the caller which forbidden property they guessed correctly.
        """
        effective = HARD_CAPS.narrow(caps)
        explicit = self.model_fields_set
        narrowed = self.model_copy(deep=True)

        if "limit" in explicit and self.limit > effective.max_rows:
            raise CapExceededError("max_rows", self.limit, effective.max_rows)
        narrowed.limit = min(self.limit, effective.max_rows)

        if "timeout_s" in explicit and self.timeout_s > effective.timeout_s:
            raise CapExceededError("timeout_s", self.timeout_s, effective.timeout_s)
        narrowed.timeout_s = min(self.timeout_s, effective.timeout_s)

        if (
            narrowed.projection is not None
            and effective.projection_allow_list is not None
        ):
            permitted = set(effective.projection_allow_list)
            narrowed.projection = [n for n in narrowed.projection if n in permitted]

        narrowed._narrow_specific(effective, explicit)
        return narrowed

    def _narrow_specific(self, caps: QueryCaps, explicit: set[str]) -> None:
        """Per-query-kind fitting. Overridden by subclasses.

        Args:
            caps: The effective ceiling.
            explicit: Field names the caller actually set, so an explicit
                over-ask can raise while a default is clamped.
        """


class NodeQuery(GraphQuery):
    """List vertices of one type, optionally filtered."""

    vertex_type: str = PydanticField(..., description="Logical vertex type name.")
    filters: FilterExpression | None = PydanticField(
        default=None, description="Predicate applied before limiting."
    )


class NeighborQuery(GraphQuery):
    """What is adjacent to one anchor vertex.

    The instance-plane counterpart of ``SchemaGraph.schema_neighbors``. They are
    different questions and must never share a name: this one asks which *rows*
    are adjacent, the other which *types* can be.
    """

    vertex_type: str = PydanticField(..., description="Logical type of the anchor.")
    key: str | dict[str, Any] = PydanticField(
        ..., description="Anchor identity value, or a single-field mapping."
    )
    hops: int = PydanticField(
        default=1, ge=1, description="Hop distance, within `max_hops`."
    )
    direction: EdgeDirection = PydanticField(
        default=EdgeDirection.OUT,
        description=(
            "Orientation followed from the anchor. Defaults to OUT, matching "
            "`Connection.fetch_edges`; an edge declared `directed: false` is "
            "followed both ways regardless."
        ),
    )
    edge_relations: list[str] | None = PydanticField(
        default=None, description="Restrict to these relations. None means all."
    )
    filters: FilterExpression | None = PydanticField(default=None)

    @property
    def edge_direction(self) -> EdgeDirection:
        """`direction` as an enum, for handing to a driver.

        `ConfigBaseModel` sets ``use_enum_values=True``, so the stored value is
        the bare string. Backends compare against `EdgeDirection` members, and a
        string silently matches none of them — which each backend then resolves
        differently, so the same query returns different neighbourhoods
        depending on where it runs. Always cross the boundary through this.
        """
        return EdgeDirection(self.direction)

    def _validate_specific(self, caps: QueryCaps) -> None:
        if self.hops > caps.max_hops:
            raise CapExceededError("max_hops", self.hops, caps.max_hops)
        if self.edge_relations and len(self.edge_relations) > caps.max_edge_types:
            raise CapExceededError(
                "max_edge_types", len(self.edge_relations), caps.max_edge_types
            )

    def _narrow_specific(self, caps: QueryCaps, explicit: set[str]) -> None:
        if "hops" in explicit and self.hops > caps.max_hops:
            raise CapExceededError("max_hops", self.hops, caps.max_hops)
        self.hops = min(self.hops, caps.max_hops)
        if self.edge_relations and len(self.edge_relations) > caps.max_edge_types:
            raise CapExceededError(
                "max_edge_types", len(self.edge_relations), caps.max_edge_types
            )


class TraverseQuery(GraphQuery):
    """What is reachable from a set of anchors."""

    seeds: list[dict[str, Any]] = PydanticField(
        ...,
        min_length=1,
        description=(
            "Anchors, each `{vertex_type, key}`. Bounded by `max_seeds`: a "
            "traversal fans out per seed, so seed count multiplies cost."
        ),
    )
    max_hops: int = PydanticField(default=2, ge=1)
    direction: EdgeDirection = PydanticField(default=EdgeDirection.ANY)
    edge_relations: list[str] | None = PydanticField(default=None)
    filters: FilterExpression | None = PydanticField(default=None)

    @property
    def edge_direction(self) -> EdgeDirection:
        """`direction` as an enum. See `NeighborQuery.edge_direction`."""
        return EdgeDirection(self.direction)

    def _validate_specific(self, caps: QueryCaps) -> None:
        if self.max_hops > caps.max_hops:
            raise CapExceededError("max_hops", self.max_hops, caps.max_hops)
        if len(self.seeds) > caps.max_seeds:
            raise CapExceededError("max_seeds", len(self.seeds), caps.max_seeds)
        if self.edge_relations and len(self.edge_relations) > caps.max_edge_types:
            raise CapExceededError(
                "max_edge_types", len(self.edge_relations), caps.max_edge_types
            )
        for seed in self.seeds:
            missing = {"vertex_type", "key"} - set(seed)
            if missing:
                raise ValueError(
                    f"seed {seed!r} is missing {sorted(missing)}; each seed needs "
                    "a vertex_type and a key"
                )

    def _narrow_specific(self, caps: QueryCaps, explicit: set[str]) -> None:
        if "max_hops" in explicit and self.max_hops > caps.max_hops:
            raise CapExceededError("max_hops", self.max_hops, caps.max_hops)
        self.max_hops = min(self.max_hops, caps.max_hops)
        # Seeds are always explicit — there is no default set of anchors — so
        # dropping any of them would silently answer a different question.
        if len(self.seeds) > caps.max_seeds:
            raise CapExceededError("max_seeds", len(self.seeds), caps.max_seeds)
        if self.edge_relations and len(self.edge_relations) > caps.max_edge_types:
            raise CapExceededError(
                "max_edge_types", len(self.edge_relations), caps.max_edge_types
            )


class AggregateQuery(GraphQuery):
    """Count or summarise one vertex type."""

    vertex_type: str = PydanticField(...)
    function: AggregationType = PydanticField(
        default=AggregationType.COUNT, description="Aggregation to apply."
    )
    group_by: str | None = PydanticField(
        default=None, description="Property to group by. COUNT only."
    )
    aggregated_field: str | None = PydanticField(
        default=None,
        description="Property to aggregate. Required for everything but COUNT.",
    )
    filters: FilterExpression | None = PydanticField(default=None)

    @property
    def aggregation(self) -> AggregationType:
        """`function` as an enum. See `NeighborQuery.edge_direction`."""
        return AggregationType(self.function)

    def _validate_specific(self, caps: QueryCaps) -> None:
        if self.function != AggregationType.COUNT and not self.aggregated_field:
            raise ValueError(
                f"aggregated_field is required for {self.function}; only COUNT "
                "can aggregate without naming a property"
            )
        if self.group_by and self.function != AggregationType.COUNT:
            raise ValueError(
                f"group_by is only supported for COUNT, not {self.function}"
            )
