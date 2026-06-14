"""Transform payloads and observation merging."""

from __future__ import annotations

from typing import Any

from pydantic import ConfigDict, Field

from graflo.architecture.base import ConfigBaseModel


class VertexRep(ConfigBaseModel):
    """Context for graph transformation actions.

    Attributes:
        vertex: doc representing a vertex
    """

    model_config = ConfigDict(kw_only=True)  # type: ignore[assignment]

    vertex: dict[str, Any]


class TransformPayload(ConfigBaseModel):
    """Typed transform output shared between extraction and assembly phases."""

    named: dict[str, Any] = Field(default_factory=dict)
    positional: tuple[Any, ...] = Field(default_factory=tuple)
    removed_keys: frozenset[str] = Field(default_factory=frozenset)

    @classmethod
    def from_result(cls, result: Any) -> TransformPayload:
        if isinstance(result, dict):
            return cls(named=dict(result))
        if isinstance(result, tuple):
            return cls(positional=tuple(result))
        return cls(positional=(result,))

    def context_doc(self) -> dict[str, Any]:
        """Named values available for weight and relation extraction."""
        return dict(self.named)


def context_dict_from_transform_buffer_item(item: Any) -> dict[str, Any]:
    """Map one ``transform_buffer`` entry to a flat context dict (named keys only)."""
    if isinstance(item, TransformPayload):
        return item.context_doc()
    if isinstance(item, dict):
        return dict(item)
    return {}


def merge_observation_with_transform_buffer(
    observation: dict[str, Any],
    buffer_items: list[Any],
) -> dict[str, Any]:
    """Merge a JSON observation slice with transform outputs at the same location.

    ``observation`` is the current dict-shaped fragment of the nested document
    passed into actors (often a child object under a :class:`DescendActor`).
    ``buffer_items`` are the entries in ``ExtractionContext.transform_buffer``
    for the same :class:`LocationIndex`.

    Starts from a shallow copy of ``observation``; each buffer entry (in pipeline
    order) updates the merged view, so later transforms override earlier keys
    and transform output overrides the raw JSON on key conflicts.
    """
    merged: dict[str, Any] = dict(observation)
    for item in buffer_items:
        merged.update(context_dict_from_transform_buffer_item(item))
        if isinstance(item, TransformPayload) and item.removed_keys:
            for k in item.removed_keys:
                merged.pop(k, None)
    return merged


def merge_row_doc_with_transform_buffer(
    doc: dict[str, Any],
    buffer_items: list[Any],
) -> dict[str, Any]:
    """Backward-compatible alias for :func:`merge_observation_with_transform_buffer`."""
    return merge_observation_with_transform_buffer(doc, buffer_items)
