"""One source per value combination of a templated connector, read in order."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from pydantic import Field, PrivateAttr

from graflo.architecture.base import ConfigBaseModel
from graflo.data_source.base import (
    AbstractDataSource,
    DataSourceType,
    later_watermark,
)


class EnumeratedMember(ConfigBaseModel):
    """One request of an enumerated connector: its variables and its source."""

    values: dict[str, Any]
    source: AbstractDataSource


class EnumeratedDataSource(AbstractDataSource):
    """Read each member in order; every row carries its member's variables.

    A row field of the same name as a variable wins over it. ``limit`` caps the
    rows over all members together, and :attr:`high_water` is the largest
    watermark any member reached in the current iteration.
    """

    members: list[EnumeratedMember] = Field(..., min_length=1)
    source_type: DataSourceType
    _started: int = PrivateAttr(default=0)

    @property
    def high_water(self) -> Any:
        mark: Any = None
        for member in self.members[: self._started]:
            value = member.source.high_water
            if value is not None and (mark is None or later_watermark(value, mark)):
                mark = value
        return mark

    def iter_batches(
        self, batch_size: int = 1000, limit: int | None = None
    ) -> Iterator[list[dict]]:
        total = 0
        self._started = 0
        for member in self.members:
            if limit is not None and total >= limit:
                return
            self._started += 1
            remaining = None if limit is None else limit - total
            for batch in member.source.iter_batches(
                batch_size=batch_size, limit=remaining
            ):
                rows = [{**member.values, **row} for row in batch]
                total += len(rows)
                yield rows

    def close(self) -> None:
        for member in self.members:
            member.source.close()
