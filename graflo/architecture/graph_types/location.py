"""Location indexing for nested graph traversal."""

from __future__ import annotations

from typing import Any

from pydantic import ConfigDict, Field, model_validator

from graflo.architecture.base import ConfigBaseModel


class LocationIndex(ConfigBaseModel):
    """Immutable location index for nested graph traversal."""

    model_config = ConfigDict(frozen=True)

    path: tuple[str | int | None, ...] = Field(default_factory=tuple)

    @model_validator(mode="before")
    @classmethod
    def accept_tuple(cls, data: Any) -> Any:
        """Accept a single tuple as positional path (e.g. LocationIndex((0,)))."""
        if isinstance(data, tuple):
            return {"path": data}
        return data

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Allow LocationIndex((0,)) or LocationIndex(path=(0,))."""
        if args and len(args) == 1 and isinstance(args[0], tuple) and not kwargs:
            kwargs = {"path": args[0]}
        super().__init__(**kwargs)

    def extend(self, extension: tuple[str | int | None, ...]) -> LocationIndex:
        return LocationIndex(path=(*self.path, *extension))

    def parent(self) -> LocationIndex | None:
        if not self.path:
            return None
        return LocationIndex(path=self.path[:-1])

    def depth(self) -> int:
        return len(self.path)

    def congruence_measure(self, other: LocationIndex) -> int:
        neq_position = 0
        for step_a, step_b in zip(self.path, other.path):
            if step_a != step_b:
                break
            neq_position += 1
        return neq_position

    def filter(self, lindex_list: list[LocationIndex]) -> list[LocationIndex]:
        return [
            t
            for t in lindex_list
            if t.depth() >= self.depth() and t.path[: self.depth()] == self.path
        ]

    def __lt__(self, other: LocationIndex) -> bool:
        return len(self.path) < len(other.path)

    def __contains__(self, item: object) -> bool:
        return item in self.path

    def __len__(self) -> int:
        return len(self.path)

    def __iter__(self):
        return iter(self.path)

    def __getitem__(self, item: int | slice):
        return self.path[item]


class ProvenancePath(ConfigBaseModel):
    """Explicit provenance path for extracted observations."""

    path: tuple[str | int | None, ...] = Field(default_factory=tuple)

    @classmethod
    def from_lindex(cls, lindex: LocationIndex) -> ProvenancePath:
        return cls(path=lindex.path)
