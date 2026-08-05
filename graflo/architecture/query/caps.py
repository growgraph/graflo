"""Hard limits on what a read request may ask for.

Caps live in core, not in route handlers, so that no surface can opt out of them
by forgetting to check. A handler that skips a check is a silent hole; a model
that will not construct is not.

The central decision here is that :class:`QueryCaps` is **not** a field on any
query model. If it were, a request body could carry ``{"caps": {"max_hops": 99}}``
and "enforced in core" would be false by construction. Queries validate against
the module-level :data:`HARD_CAPS`, and :meth:`GraphQuery.narrowed` is the only
way to change a limit — downward.
"""

from __future__ import annotations

from typing import Final

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel


class QueryCaps(ConfigBaseModel):
    """Ceilings a read request must fit inside.

    Defaults are the core ceiling. A deployment narrows them per connection via
    :meth:`GraphQuery.narrowed`; nothing widens them.
    """

    max_hops: int = PydanticField(
        default=3, ge=1, description="Deepest traversal permitted."
    )
    max_rows: int = PydanticField(
        default=1000, ge=1, description="Most rows a single query may return."
    )
    max_elements: int = PydanticField(
        default=5000,
        ge=1,
        description="Most vertices plus edges a response may carry.",
    )
    timeout_s: float = PydanticField(
        default=30.0, gt=0, description="Longest a query may run."
    )
    max_edge_types: int = PydanticField(
        default=20,
        ge=1,
        description="Most distinct relations one request may name.",
    )
    max_seeds: int = PydanticField(
        default=10,
        ge=1,
        description="Most anchor vertices a traversal may start from.",
    )
    projection_allow_list: list[str] | None = PydanticField(
        default=None,
        description=(
            "Property names a response may include. None means unrestricted; "
            "an empty list means nothing may be projected, which is not the "
            "same thing."
        ),
    )

    def narrow(self, other: QueryCaps) -> QueryCaps:
        """Combine two cap sets, taking the stricter of each.

        Narrowing is a lattice meet, not an override: a policy that tried to
        raise a ceiling silently becomes a no-op rather than a privilege
        escalation.
        """
        allow: list[str] | None
        if self.projection_allow_list is None:
            allow = other.projection_allow_list
        elif other.projection_allow_list is None:
            allow = self.projection_allow_list
        else:
            # Intersection, order fixed for reproducibility.
            permitted = set(other.projection_allow_list)
            allow = [n for n in self.projection_allow_list if n in permitted]
        return QueryCaps(
            max_hops=min(self.max_hops, other.max_hops),
            max_rows=min(self.max_rows, other.max_rows),
            max_elements=min(self.max_elements, other.max_elements),
            timeout_s=min(self.timeout_s, other.timeout_s),
            max_edge_types=min(self.max_edge_types, other.max_edge_types),
            max_seeds=min(self.max_seeds, other.max_seeds),
            projection_allow_list=allow,
        )


#: The core ceiling. Every query validates against this at construction; a
#: deployment can only go lower.
HARD_CAPS: Final[QueryCaps] = QueryCaps()


class CapExceededError(ValueError):
    """A query asked for more than a cap allows.

    Carries the cap's name so the surface can say *which* limit was hit rather
    than returning a generic validation failure — an agent that is told "too
    many hops, max is 3" can retry; one told "invalid request" cannot.
    """

    def __init__(self, cap: str, requested: object, allowed: object) -> None:
        self.cap = cap
        self.requested = requested
        self.allowed = allowed
        super().__init__(
            f"{cap} exceeded: requested {requested!r}, maximum is {allowed!r}"
        )
