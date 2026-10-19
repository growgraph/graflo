"""The access aspect: integer zone labels on declared types.

The lift adds one reserved field, ``access_zones: list[int]``, to each vertex
type and edge relation the spec names. What a zone is and who opens it is not
part of the graph: a caller stamps writes with zone ids and reads with a guard
built from ``ANY_IN`` over the zones a reader opens.
"""

from __future__ import annotations

from .plan import ACCESS_ZONES, access_field, plan_access

__all__ = ["ACCESS_ZONES", "access_field", "plan_access"]
