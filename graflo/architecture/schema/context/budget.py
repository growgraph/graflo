"""Budget accounting for schema context payloads.

Deliberately dependency-free: no tokenizer, no model. The estimate is a
documented characters-per-token ratio over the *same* compact serialization the
transport layer will send, and every result also reports the exact character
count so a caller holding a real tokenizer can re-estimate without core changing.
"""

from __future__ import annotations

import json
import math
from typing import Any, Final, Literal

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel

#: Characters per token. A documented estimate, not a tokenizer. English JSON
#: sits near 4; the exact ``serialized_chars`` is reported alongside every
#: estimate so a caller can substitute a real count.
CHARS_PER_TOKEN: Final[float] = 4.0


class Budget(ConfigBaseModel):
    """Caller-requested ceilings on a schema context payload."""

    max_elements: int | None = PydanticField(
        default=60,
        description="Maximum vertices + edges in the slice. None disables the cap.",
        ge=1,
    )
    max_tokens: int | None = PydanticField(
        default=4000,
        description="Maximum estimated tokens for the serialized slice. None disables the cap.",
        ge=1,
    )
    max_properties_per_vertex: int | None = PydanticField(
        default=None,
        description=(
            "Maximum properties retained per vertex. Identity-bearing fields are "
            "never counted against this and never dropped."
        ),
        ge=1,
    )


class BudgetAccounting(ConfigBaseModel):
    """What the budget actually cost, measured rather than assumed."""

    requested: Budget = PydanticField(..., description="The budget as asked for.")
    elements_used: int = PydanticField(
        ..., description="Vertices + edges admitted into the slice."
    )
    estimated_tokens: int = PydanticField(
        ..., description="Token estimate for the assembled slice."
    )
    serialized_chars: int = PydanticField(
        ...,
        description=(
            "Exact character count of the compact serialization. Lets a caller "
            "re-estimate with a real tokenizer without trusting CHARS_PER_TOKEN."
        ),
    )
    exhausted_by: Literal["elements", "tokens", "none"] = PydanticField(
        ..., description="Which ceiling stopped admission, if any."
    )


def serialize_compact(payload: Any) -> str:
    """Serialize *payload* the way the transport layer will: compact and stable."""
    return json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str)


def estimate_tokens(payload: Any) -> int:
    """Estimate token count for *payload*.

    Runs over the compact serialization — estimating over a pretty-printed or
    defaults-included dump overcounts by a factor of two or more, which would make
    every budget silently pessimistic.
    """
    return math.ceil(len(serialize_compact(payload)) / CHARS_PER_TOKEN)
