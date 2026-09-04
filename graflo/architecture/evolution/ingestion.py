"""Ingestion-side evolution ops: mutations whose primary effect is the pipeline.

Every other op touches ``ingestion_model`` only as a cascade of a schema
change. These are the fundamentals the other direction, leaving
``graph_schema`` untouched: :func:`apply_add_resource_transforms` appends
transform steps to a named level of named resources (and optionally registers
named transforms), and :func:`apply_ensure_extracted_fields` widens a
producing step's projection so named fields survive extraction.
"""

from __future__ import annotations

import copy

from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.contract.ingestion.steps.models import TransformActorConfig
from graflo.architecture.contract.ingestion.steps.normalize import (
    normalize_actor_step,
)
from graflo.architecture.contract.ingestion.transform import ProtoTransform
from graflo.architecture.contract.manifest import GraphManifest

from .ops import AddResourceTransformsOp, EnsureExtractedFieldsOp


def _merged_registry(
    existing: list[ProtoTransform], added: list[ProtoTransform]
) -> list[ProtoTransform]:
    """Union registries by name: identical bodies dedupe, divergent ones raise.

    Mirrors ``compose_manifests`` transform-union semantics so an op collides
    exactly as loudly as a compose would.
    """
    by_name: dict[str, ProtoTransform] = {}
    out: list[ProtoTransform] = []
    for t in list(existing) + list(added):
        name = t.name
        if name is None:
            out.append(t)
            continue
        if name in by_name:
            existing_t = by_name[name]
            if existing_t.to_dict(skip_defaults=False) != t.to_dict(
                skip_defaults=False
            ):
                raise ValueError(
                    f"add_resource_transforms: incompatible transform "
                    f"definitions for {name!r}"
                )
            continue
        by_name[name] = t
        out.append(t)
    return out


def _step_use_name(step: dict) -> str | None:
    config = TransformActorConfig.model_validate(step)
    return config.call.use if config.call is not None else None


def apply_add_resource_transforms(
    manifest: GraphManifest, op: AddResourceTransformsOp
) -> None:
    """Append transform steps to named resources' pipelines, in place.

    Raises when the manifest carries no ``ingestion_model`` (an ingestion-only
    op silently dropped would be a lie in the revision log), when a named
    resource does not exist, when a registry name collides with a different
    body, when a step's ``call.use`` resolves against neither the existing
    registry nor ``op.transforms``, or when an ``op.at`` path does not resolve
    to a ``descend`` level. Steps are appended at the end of the level
    ``op.at`` names (the root pipeline by default); actor type-priority
    ordering runs them before vertex extraction at that level.
    """
    im = manifest.ingestion_model
    if im is None:
        raise ValueError(
            "add_resource_transforms requires ingestion_model — the op's "
            "entire effect is the pipeline"
        )

    known = {resource.name for resource in im.resources}
    missing = sorted(set(op.additions) - known)
    if missing:
        raise ValueError(
            f"add_resource_transforms: unknown resources {missing}; "
            f"manifest defines {sorted(known)}"
        )

    registry = _merged_registry(list(im.transforms), list(op.transforms))
    registry_names = {t.name for t in registry if t.name}
    for resource_name, steps in op.additions.items():
        for step in steps:
            use = _step_use_name(step)
            if use is not None and use not in registry_names:
                raise ValueError(
                    f"add_resource_transforms: step for resource "
                    f"{resource_name!r} references unknown transform {use!r}; "
                    f"registered: {sorted(registry_names)}"
                )

    from graflo.architecture.contract.ingestion.resource import (
        Resource,
        resolve_pipeline_level,
    )

    resources: list[Resource] = []
    for resource in im.resources:
        payload = resource.to_dict(skip_defaults=False)
        added = op.additions.get(resource.name)
        if added:
            pipeline = copy.deepcopy(list(payload.get("pipeline") or []))
            path = op.at.get(resource.name) or []
            try:
                level = resolve_pipeline_level(pipeline, path)
            except ValueError as exc:
                raise ValueError(
                    f"add_resource_transforms: resource {resource.name!r}: {exc}"
                ) from exc
            level.extend(copy.deepcopy(step) for step in added)
            payload["pipeline"] = pipeline
        resources.append(Resource.model_validate(payload))

    im.resources = resources
    im.transforms = registry
    # Full round-trip re-validation: rebuilds the name maps and runtimes, and
    # ProtoTransform's eager module import fails loudly here on a bad foo.
    manifest.ingestion_model = IngestionModel.model_validate(
        im.to_dict(skip_defaults=False)
    )


def _widen_router_projection(step: dict, vertex: str, fields: list[str]) -> dict | None:
    """Return *step* widened so *fields* survive extraction for *vertex*.

    ``None`` when nothing needs widening — a plain ``vertex`` step (it reads the
    transform buffer directly), or a router that restricts neither
    ``keep_fields`` nor ``extraction_scope``.
    """
    normalized = normalize_actor_step(dict(step))
    if normalized.get("type") != "vertex_router":
        return None

    produced = set((normalized.get("type_map") or {}).values()) | set(
        normalized.get("vertex_from_map") or {}
    )
    if vertex not in produced:
        return None

    keep_fields = normalized.get("keep_fields")
    mapped_only = normalized.get("extraction_scope") == "mapped_only"
    if keep_fields is None and not mapped_only:
        return None

    out = copy.deepcopy(normalized)
    if keep_fields is not None:
        out["keep_fields"] = list(keep_fields) + [
            field for field in fields if field not in keep_fields
        ]
    if mapped_only:
        vertex_from_map = dict(out.get("vertex_from_map") or {})
        # Seed from the router-level `from` so creating the entry extends the
        # author's projection instead of replacing it: the router falls back to
        # `from_doc` only for types absent from `vertex_from_map`.
        per_type = dict(vertex_from_map.get(vertex) or out.get("from") or {})
        for field in fields:
            per_type.setdefault(field, field)
        vertex_from_map[vertex] = per_type
        out["vertex_from_map"] = vertex_from_map
    return out


def apply_ensure_extracted_fields(
    manifest: GraphManifest, op: EnsureExtractedFieldsOp
) -> None:
    """Widen producing steps' projections so named fields survive, in place.

    Raises when the manifest carries no ``ingestion_model``, when a named
    resource does not exist, or when an entry's ``at`` path does not resolve.
    An entry naming a level that produces the vertex through a plain ``vertex``
    step, or through an unrestricted router, applies cleanly and changes
    nothing — the fields already survive there.
    """
    im = manifest.ingestion_model
    if im is None:
        raise ValueError(
            "ensure_extracted_fields requires ingestion_model — the op's "
            "entire effect is the pipeline"
        )

    known = {resource.name for resource in im.resources}
    missing = sorted(set(op.additions) - known)
    if missing:
        raise ValueError(
            f"ensure_extracted_fields: unknown resources {missing}; "
            f"manifest defines {sorted(known)}"
        )

    from graflo.architecture.contract.ingestion.resource import (
        Resource,
        resolve_pipeline_level,
    )

    resources: list[Resource] = []
    for resource in im.resources:
        payload = resource.to_dict(skip_defaults=False)
        entries = op.additions.get(resource.name)
        if entries:
            pipeline = copy.deepcopy(list(payload.get("pipeline") or []))
            for entry in entries:
                try:
                    level = resolve_pipeline_level(pipeline, entry.at)
                except ValueError as exc:
                    raise ValueError(
                        f"ensure_extracted_fields: resource {resource.name!r}: {exc}"
                    ) from exc
                for index, step in enumerate(level):
                    if not isinstance(step, dict):
                        continue
                    widened = _widen_router_projection(
                        step, entry.vertex, list(entry.fields)
                    )
                    if widened is not None:
                        level[index] = widened
            payload["pipeline"] = pipeline
        resources.append(Resource.model_validate(payload))

    im.resources = resources
    manifest.ingestion_model = IngestionModel.model_validate(
        im.to_dict(skip_defaults=False)
    )
