"""Base actor classes and shared context."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from graflo.architecture.schema.edge import EdgeConfig
from graflo.architecture.graph_types import EdgeId, ExtractionContext, LocationIndex
from graflo.architecture.contract.declarations.transform import ProtoTransform
from graflo.architecture.schema.vertex import VertexConfig


class ActorConstants:
    """Constants used throughout the actor system."""

    DESCEND_KEY: str = "key"
    DRESSING_TRANSFORMED_VALUE_KEY: str = "__value__"


@dataclass(slots=True)
class ActorInitContext:
    """Typed initialization state shared across actor tree."""

    vertex_config: VertexConfig
    edge_config: EdgeConfig
    transforms: dict[str, ProtoTransform]
    infer_edges: bool = True
    infer_edge_only: set[EdgeId] = field(default_factory=set)
    infer_edge_except: set[EdgeId] = field(default_factory=set)
    strict_references: bool = False


class Actor(ABC):
    """Abstract base class for all actors in the system."""

    @abstractmethod
    def __call__(
        self, ctx: ExtractionContext, lindex: LocationIndex, *nargs, **kwargs
    ) -> ExtractionContext:
        """Execute the actor's main processing logic."""
        pass

    def fetch_important_items(self) -> dict[str, object]:
        """Get a dictionary of important items for string representation."""
        return {}

    def finish_init(self, init_ctx: ActorInitContext) -> None:
        """Complete initialization of the actor."""
        pass

    def init_transforms(self, init_ctx: ActorInitContext) -> None:
        """Initialize transformations for the actor."""
        pass

    def count(self) -> int:
        """Get the count of items processed by this actor."""
        return 1

    def references_vertices(self) -> set[str]:
        """Return vertex names this actor references."""
        return set()

    def _filter_items(self, items: dict[str, object]) -> dict[str, object]:
        """Filter out None and empty items."""
        return {k: v for k, v in items.items() if v is not None and v}

    def _stringify_items(self, items: dict[str, object]) -> dict[str, str]:
        """Convert items to string representation."""
        return {
            k: ", ".join(list(v)) if isinstance(v, (tuple, list)) else str(v)
            for k, v in items.items()
        }

    def _fetch_items_from_dict(self, keys: tuple[str, ...]) -> dict[str, object]:
        """Helper method to extract items from instance dict for string representation."""
        return {k: self.__dict__[k] for k in keys if k in self.__dict__}

    def __str__(self) -> str:
        """Get string representation of the actor."""
        d = self.fetch_important_items()
        d = self._filter_items(d)
        d = self._stringify_items(d)
        d_list = [[k, d[k]] for k in sorted(d)]
        d_list_b = [type(self).__name__] + [": ".join(x) for x in d_list]
        return "\n".join(d_list_b)

    __repr__ = __str__

    def fetch_actors(self, level: int, edges: list) -> tuple[int, type, str, list]:
        """Fetch actor information for tree representation."""
        return level, type(self), str(self), edges
