"""One record per evolution op: how it applies, what it writes and reads, how it is undone.

Each behaviour lives beside the code it belongs to -- apply functions in
:mod:`.apply` (:func:`.apply.appliers`), footprints in :mod:`.footprint`
(``WRITES``, ``READS``), inverses in :mod:`.inverse` (``INVERTERS``,
``IRREVERSIBLE``). This module joins them into one :class:`OpLaws` per op type,
and is what laws are tested against generically: every op in the vocabulary
has exactly one record, and every record is complete. A new op costs one entry
in each table; a missing one fails the totality test, not a merge.
"""

from __future__ import annotations

import typing
from collections.abc import Callable
from dataclasses import dataclass
from functools import cache
from typing import Any

from pydantic import BaseModel

from graflo.architecture.contract.manifest import GraphManifest

from . import ops
from .footprint import RelationAliases, Slot


@dataclass(frozen=True)
class OpLaws:
    """What one op type does, as the laws see it.

    Attributes:
        kind: The op's ``op`` literal.
        apply: Applies the op to a manifest in place; refuses with
            :class:`~graflo.architecture.refusal.OpRefusal`.
        writes: The slots the op changes (:func:`.footprint.op_slots`).
        reads: The slots the op needs and does not change, given the base it
            applies to (:func:`.footprint.op_reads`).
        invert: Builds the op undoing it from the pre-state, or ``None`` when
            this pre-state admits none; ``None`` for an irreversible op.
        irreversible: Why the op cannot be undone; ``None`` for a reversible op.
            Exactly one of *invert* and *irreversible* is set.
    """

    kind: str
    apply: Callable[[GraphManifest, Any], None]
    writes: Callable[[Any], set[Slot]]
    reads: Callable[[Any, GraphManifest | None, RelationAliases | None], set[Slot]]
    invert: Callable[[Any, GraphManifest], Any] | None
    irreversible: str | None

    def __post_init__(self) -> None:
        if (self.invert is None) == (self.irreversible is None):
            raise ValueError(
                f"{self.kind}: an op is either invertible or irreversible, not "
                "both or neither"
            )


def op_models() -> tuple[type[BaseModel], ...]:
    """Every op model in the vocabulary (the members of ``ManifestOp``)."""
    return typing.get_args(typing.get_args(ops.ManifestOp)[0])


def op_kind(model: type[BaseModel]) -> str:
    """The ``op`` literal of an op model."""
    return model.model_fields["op"].default


@cache
def op_laws() -> dict[type, OpLaws]:
    """One :class:`OpLaws` per op model.

    Raises:
        LookupError: when a table lacks an op, naming the table and the op.
    """
    from .apply import appliers
    from .footprint import READS, WRITES
    from .inverse import INVERTERS, IRREVERSIBLE

    apply_table = appliers()
    laws: dict[type, OpLaws] = {}
    for model in op_models():
        kind = op_kind(model)
        for table, key, name in (
            (apply_table, model, "appliers"),
            (WRITES, model, "footprint.WRITES"),
            (READS, model, "footprint.READS"),
        ):
            if key not in table:
                raise LookupError(f"{name} has no entry for {model.__name__}")
        laws[model] = OpLaws(
            kind=kind,
            apply=apply_table[model],
            writes=WRITES[model],
            reads=READS[model],
            invert=INVERTERS.get(kind),
            irreversible=IRREVERSIBLE.get(kind),
        )
    return laws


def laws_for(op: Any) -> OpLaws:
    """The :class:`OpLaws` of *op*'s type.

    Raises:
        TypeError: for an object that is not an op of the vocabulary.
    """
    laws = op_laws().get(type(op))
    if laws is None:
        raise TypeError(f"not an evolution op: {type(op)!r}")
    return laws


__all__ = ["OpLaws", "laws_for", "op_kind", "op_laws", "op_models"]
