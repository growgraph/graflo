"""What every aspect shares: the refusal, and the entry that names it."""

from __future__ import annotations

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel


class LiftError(ValueError):
    """The lift cannot be planned, and the message says which declaration is missing.

    Raised rather than guessed. Every case here is one where the planner can see
    that something is wrong but not what the right answer is -- an ungrounded
    type, a name it would have to overwrite, a property it was told to move that
    is part of the key.
    """


class Aspect(ConfigBaseModel):
    """One cross-cutting vocabulary a manifest can be lifted with.

    An aspect is domain-free: it adds the types and relations that let a
    schema say something every domain needs to say -- where a fact came from,
    how a fact changes over time -- grounded in a standard vocabulary.

    Attributes:
        name: How the CLI and a caller address it (``--aspect NAME``).
        description: One line, for listings.
        needs_spec: Whether planning it needs declarations only a
            :class:`~graflo.architecture.evolution.aspects.spec.LiftSpec` carries.
    """

    name: str = PydanticField(..., min_length=1)
    description: str
    needs_spec: bool


__all__ = ["Aspect", "LiftError"]
