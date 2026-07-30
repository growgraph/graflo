"""Resource sampling contract — pure-JSON samples and their derived profiles.

Samples are the raw material every schema inferencer consumes, whether it reasons
algorithmically (:mod:`graflo.db.identity_inference`) or with a language model
(ScheWea). Two ideas, deliberately kept apart:

* **Sampling** pulls documents from a connector. :class:`ResourceSample` holds
  them **verbatim as JSON** — tabular sources yield flat ``list[dict]`` rows,
  API sources yield arbitrarily nested documents. Nothing is flattened at this
  boundary, so a hierarchical response survives intact.
* **Profiling** describes those documents. :func:`profile_sample` derives the
  flat, path-keyed, typed view (:class:`ResourceProfile`) used for prompting,
  studio previews and identity inference.

:attr:`ResourceSample.connector` records *where* the documents came from. That
relation is what later becomes a resource plus its ``resource_connector``
binding, so it must survive the round trip — a sample that has lost its
provenance cannot be turned back into an ingestion model.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from datetime import date, datetime, time
from typing import Any

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.schema.identity_uuid import UUID_PATTERN
from graflo.architecture.schema.vertex import FieldType

#: Path segment appended when descending into a list of objects, e.g. ``items[].sku``.
LIST_MARKER = "[]"

_ISO_DATETIME_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)?$"
)
_LONG_TEXT_THRESHOLD = 256

DEFAULT_MAX_DOCS = 100
DEFAULT_MAX_PATHS = 200
DEFAULT_MAX_EXAMPLES = 3


class ForeignKeyHint(ConfigBaseModel):
    """A declared reference from one resource to another.

    Populated only when the source *declares* it (a SQL foreign key, an RDF
    range). This is ground truth for edge inference and must not be confused
    with the name-suffix guessing an inferencer falls back to.
    """

    field: str
    references_resource: str
    references_field: str | None = None


class ResourceSample(ConfigBaseModel):
    """Documents sampled from one resource, plus what the source declared about it."""

    resource_name: str
    """Logical resource name; becomes ``ResourceConfig.name``."""

    connector: str | None = None
    """Name of the connector the documents came from; becomes the
    ``resource_connector`` binding. ``None`` when sampled without bindings."""

    docs: list[dict[str, Any]] = PydanticField(default_factory=list)
    """Sampled documents, verbatim JSON. Flat rows for tables, nested for APIs."""

    description: str | None = None
    primary_key: list[str] = PydanticField(default_factory=list)
    """Declared primary key, when the source has one."""

    foreign_keys: list[ForeignKeyHint] = PydanticField(default_factory=list)
    """Declared outbound references, when the source has them."""

    truncated: bool = False
    """True when documents were dropped or values clipped to respect caps."""

    total_estimate: int | None = None
    """Approximate total document count at the source, when cheaply available."""


class SourceSample(ConfigBaseModel):
    """A set of resource samples drawn from one logical source."""

    source_name: str
    description: str | None = None
    samples: list[ResourceSample] = PydanticField(min_length=1)

    @property
    def samples_by_resource(self) -> dict[str, list[dict[str, Any]]]:
        """Documents keyed by resource name.

        This is the input shape consumed by cross-resource identity inference,
        so no adapter is needed between sampling and inference.
        """
        return {sample.resource_name: sample.docs for sample in self.samples}

    def get(self, resource_name: str) -> ResourceSample | None:
        """Return the sample for *resource_name*, or ``None``."""
        for sample in self.samples:
            if sample.resource_name == resource_name:
                return sample
        return None


class FieldProfile(ConfigBaseModel):
    """Derived description of one field path within a resource sample."""

    path: str
    """Dotted path to the value, e.g. ``address.city`` or ``items[].sku``."""

    type: FieldType = FieldType.STRING
    item_type: FieldType | None = None
    """Element type; set only when ``type`` is ``LIST``."""

    depth: int = 0
    """Nesting depth of the path. ``0`` for a top-level scalar."""

    present: int = 0
    """Documents in which the path occurred."""

    null_count: int = 0
    distinct: int = 0
    examples: list[str] = PydanticField(default_factory=list)

    @property
    def null_ratio(self) -> float:
        """Fraction of occurrences whose value was null."""
        return self.null_count / self.present if self.present else 0.0

    @property
    def unique(self) -> bool:
        """True when every non-null occurrence was distinct."""
        non_null = self.present - self.null_count
        return non_null > 0 and self.distinct == non_null


class ResourceProfile(ConfigBaseModel):
    """Derived, flat, typed view of a :class:`ResourceSample`."""

    resource_name: str
    connector: str | None = None
    doc_count: int = 0
    max_depth: int = 0
    """Deepest nesting observed. ``> 0`` means ingestion needs ``descend`` steps."""

    fields: list[FieldProfile] = PydanticField(default_factory=list)
    primary_key: list[str] = PydanticField(default_factory=list)
    foreign_keys: list[ForeignKeyHint] = PydanticField(default_factory=list)
    truncated: bool = False

    @property
    def field_paths(self) -> list[str]:
        return [field.path for field in self.fields]

    @property
    def nested(self) -> bool:
        return self.max_depth > 0

    def flat_docs(self, docs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Project *docs* onto this profile's paths as flat records.

        Identity inference operates on flat records; this is how a nested source
        becomes eligible for it.
        """
        paths = set(self.field_paths)
        flattened: list[dict[str, Any]] = []
        for doc in docs:
            record: dict[str, Any] = {}
            for path, _depth, value in iter_paths(doc):
                if path in paths and path not in record:
                    record[path] = value
            flattened.append(record)
        return flattened


def iter_paths(
    doc: dict[str, Any], prefix: str = "", depth: int = 0
) -> Iterator[tuple[str, int, Any]]:
    """Yield ``(path, depth, value)`` for every leaf in a JSON document.

    Nested objects extend the path with ``.``; lists of objects extend it with
    ``[]``. Lists of scalars are yielded whole so they can be typed as ``LIST``.
    """
    for key, value in doc.items():
        path = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            yield from iter_paths(value, path, depth + 1)
        elif isinstance(value, list) and any(isinstance(item, dict) for item in value):
            nested_path = f"{path}{LIST_MARKER}"
            for item in value:
                if isinstance(item, dict):
                    yield from iter_paths(item, nested_path, depth + 1)
        else:
            yield path, depth, value


def infer_field_type(values: list[Any]) -> tuple[FieldType, FieldType | None]:
    """Infer a ``FieldType`` (and ``item_type`` for lists) from observed values.

    Checks ``bool`` before ``int`` deliberately — ``bool`` is an ``int`` subclass
    in Python, so the naive order mistypes every boolean column as ``INT``.
    """
    non_null = [value for value in values if value is not None]
    if not non_null:
        return FieldType.STRING, None

    if all(isinstance(value, list) for value in non_null):
        items = [item for value in non_null for item in value]
        item_type, _ = infer_field_type(items) if items else (FieldType.STRING, None)
        return FieldType.LIST, item_type

    if all(isinstance(value, bool) for value in non_null):
        return FieldType.BOOL, None
    if all(
        isinstance(value, int) and not isinstance(value, bool) for value in non_null
    ):
        return FieldType.INT, None
    if all(
        isinstance(value, (int, float)) and not isinstance(value, bool)
        for value in non_null
    ):
        return FieldType.FLOAT, None
    if all(isinstance(value, (datetime, date, time)) for value in non_null):
        return FieldType.DATETIME, None

    if all(isinstance(value, str) for value in non_null):
        if all(UUID_PATTERN.match(value) for value in non_null):
            return FieldType.UUID, None
        if all(_ISO_DATETIME_PATTERN.match(value) for value in non_null):
            return FieldType.DATETIME, None

    return FieldType.STRING, None


def _example(value: Any) -> str:
    text = str(value)
    if len(text) > _LONG_TEXT_THRESHOLD:
        return text[:_LONG_TEXT_THRESHOLD] + "…"
    return text


def profile_sample(
    sample: ResourceSample,
    *,
    max_paths: int = DEFAULT_MAX_PATHS,
    max_examples: int = DEFAULT_MAX_EXAMPLES,
) -> ResourceProfile:
    """Derive a :class:`ResourceProfile` from a sample's documents.

    Handles tabular and hierarchical documents through one code path: a flat row
    is simply the depth-0 case.
    """
    observed: dict[str, list[Any]] = {}
    depths: dict[str, int] = {}

    for doc in sample.docs:
        for path, depth, value in iter_paths(doc):
            if path not in observed and len(observed) >= max_paths:
                continue
            observed.setdefault(path, []).append(value)
            depths[path] = max(depths.get(path, depth), depth)

    truncated = sample.truncated or len(observed) >= max_paths

    fields: list[FieldProfile] = []
    for path, values in observed.items():
        field_type, item_type = infer_field_type(values)
        non_null = [value for value in values if value is not None]
        hashable = {
            value if isinstance(value, (str, int, float, bool)) else repr(value)
            for value in non_null
        }
        fields.append(
            FieldProfile(
                path=path,
                type=field_type,
                item_type=item_type,
                depth=depths.get(path, 0),
                present=len(values),
                null_count=len(values) - len(non_null),
                distinct=len(hashable),
                examples=[_example(value) for value in non_null[:max_examples]],
            )
        )

    return ResourceProfile(
        resource_name=sample.resource_name,
        connector=sample.connector,
        doc_count=len(sample.docs),
        max_depth=max(depths.values(), default=0),
        fields=fields,
        primary_key=list(sample.primary_key),
        foreign_keys=list(sample.foreign_keys),
        truncated=truncated,
    )


def profile_source(
    source_sample: SourceSample,
    *,
    max_paths: int = DEFAULT_MAX_PATHS,
    max_examples: int = DEFAULT_MAX_EXAMPLES,
) -> list[ResourceProfile]:
    """Profile every resource in a :class:`SourceSample`."""
    return [
        profile_sample(sample, max_paths=max_paths, max_examples=max_examples)
        for sample in source_sample.samples
    ]


__all__ = [
    "DEFAULT_MAX_DOCS",
    "DEFAULT_MAX_EXAMPLES",
    "DEFAULT_MAX_PATHS",
    "LIST_MARKER",
    "FieldProfile",
    "ForeignKeyHint",
    "ResourceProfile",
    "ResourceSample",
    "SourceSample",
    "infer_field_type",
    "iter_paths",
    "profile_sample",
    "profile_source",
]
