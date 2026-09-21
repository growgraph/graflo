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
from graflo.architecture.contract.ingestion.steps.ref import (
    EdgeStepRef,
    find_edge_step,
    iter_edge_steps,
    with_emit_inverse,
)
from graflo.architecture.contract.ingestion.transform import ProtoTransform
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.graph_types import EdgeId
from graflo.architecture.schema.edge import EdgeConfig, inverse_map
from graflo.architecture.schema.inverse_realization import (
    inverse_emission_refusal,
    materialized_inverse_id,
)

from .ops import (
    AddResourcesOp,
    AddResourceTransformsOp,
    EnsureExtractedFieldsOp,
    RemoveResourcesOp,
    SetInverseEmissionOp,
)


def apply_add_resources(manifest: GraphManifest, op: AddResourcesOp) -> None:
    """Append resources to ``ingestion_model``, creating the block if absent.

    A name already present raises: two definitions under one name would
    shadow each other in the name-keyed lookup, and ``rename_resources`` or
    ``remove_resources`` exist to make room explicitly.
    """
    im = manifest.ingestion_model
    existing = {resource.name for resource in im.resources} if im is not None else set()
    collisions = sorted(existing & {resource.name for resource in op.resources})
    if collisions:
        raise ValueError(f"add_resources: resources already exist: {collisions}")
    payload = im.to_dict(skip_defaults=False) if im is not None else {"resources": []}
    payload["resources"] = [
        *payload.get("resources", []),
        *(resource.to_dict(skip_defaults=False) for resource in op.resources),
    ]
    if op.transforms:
        registry = _union_transforms(
            list(im.transforms) if im is not None else [], list(op.transforms)
        )
        payload["transforms"] = [t.to_dict(skip_defaults=False) for t in registry]
    manifest.ingestion_model = IngestionModel.model_validate(payload)


def apply_remove_resources(manifest: GraphManifest, op: RemoveResourcesOp) -> None:
    """Drop resources and the ``resource_connector`` entries that wired them."""
    from .apply import _filter_bindings_for_resources

    im = manifest.ingestion_model
    if im is None:
        raise ValueError("remove_resources requires ingestion_model")
    existing = {resource.name for resource in im.resources}
    unknown = sorted(set(op.names) - existing)
    if unknown:
        raise ValueError(f"remove_resources: unknown resources: {unknown}")
    removed = set(op.names)
    payload = im.to_dict(skip_defaults=False)
    payload["resources"] = [
        resource
        for resource in payload.get("resources", [])
        if resource.get("name") not in removed
    ]
    manifest.ingestion_model = IngestionModel.model_validate(payload)
    _filter_bindings_for_resources(manifest, existing - removed)


def _union_transforms(
    existing: list[ProtoTransform], added: list[ProtoTransform]
) -> list[ProtoTransform]:
    """Union registries by name: identical bodies dedupe, divergent ones raise.

    Mirrors ``merge_manifests`` transform-union semantics so an op collides
    exactly as loudly as a merge would.
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

    registry = _union_transforms(list(im.transforms), list(op.transforms))
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
    ``keep_fields`` nor ``extraction_scope``. Every router at the level is
    widened, not only one whose table names *vertex*: a router routes an
    unmapped discriminator value as the class name, so one without an entry
    for *vertex* still produces it, and a derived field it does not keep is
    written and then discarded without a word.
    """
    normalized = normalize_actor_step(dict(step))
    if normalized.get("type") != "vertex_router":
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


def set_emit_inverse_flags(
    manifest: GraphManifest, steps: dict[str, list[EdgeStepRef]], enabled: bool
) -> None:
    """Set or clear ``emit_inverse`` on the addressed steps, in place.

    The mechanical half of :func:`apply_set_inverse_emission`, shared with the
    ops that move flags as a consequence of a schema change.

    Raises:
        ValueError: naming an unknown resource, or a ref that is not an edge step.
    """
    ingestion = manifest.ingestion_model
    if ingestion is None:
        raise ValueError("set_inverse_emission requires ingestion_model")
    by_name = {resource.name: resource for resource in ingestion.resources}
    unknown = sorted(set(steps) - set(by_name))
    if unknown:
        raise ValueError(f"set_inverse_emission: unknown resources: {unknown}")
    payload = ingestion.to_dict(skip_defaults=False)
    for resource_payload in payload.get("resources", []):
        refs = steps.get(resource_payload.get("name"))
        if not refs:
            continue
        pipeline = resource_payload.get("pipeline") or []
        for ref in refs:
            try:
                pipeline = with_emit_inverse(pipeline, ref, enabled)
            except ValueError as exc:
                raise ValueError(
                    f"set_inverse_emission: resource {resource_payload['name']!r}: {exc}"
                ) from exc
        resource_payload["pipeline"] = pipeline
    manifest.ingestion_model = IngestionModel.model_validate(payload)


def apply_set_inverse_emission(
    manifest: GraphManifest, op: SetInverseEmissionOp
) -> None:
    """Set or clear ``emit_inverse`` on edge steps addressed by position.

    Enabling a step that names exactly one edge is checked against the schema
    here, in the op's own words, rather than left to the next manifest load: the
    relation needs a declared pair, must not be symmetric, and its inverse edge
    must be declared.
    """
    ingestion = manifest.ingestion_model
    if ingestion is None:
        raise ValueError("set_inverse_emission requires ingestion_model")
    schema = manifest.graph_schema
    if op.enabled and schema is not None:
        edge_config = schema.core_schema.edge_config
        by_name = {resource.name: resource for resource in ingestion.resources}
        for name, refs in op.steps.items():
            resource = by_name.get(name)
            if resource is None:
                continue
            for ref in refs:
                view = find_edge_step(resource.pipeline, ref)
                static = view.static_edge_id if view is not None else None
                if static is None:
                    continue
                refusal = inverse_emission_refusal(edge_config, static)
                if refusal is not None:
                    raise ValueError(
                        f"set_inverse_emission: resource {name!r} step {ref} "
                        f"writes {static}: {refusal}"
                    )
    set_emit_inverse_flags(manifest, op.steps, op.enabled)


def _flag_is_live(
    view_static: EdgeId | None, written: set[str] | None, edge_config: EdgeConfig
) -> bool:
    """Whether a flagged step can mirror into anything under ``edge_config``."""
    if view_static is not None:
        return materialized_inverse_id(edge_config, view_static) is not None
    paired = inverse_map(edge_config.inverses)
    materialized = {
        edge.relation
        for edge in edge_config.edges
        if edge.relation is not None
        and materialized_inverse_id(edge_config, edge.edge_id) is not None
    }
    if written is None:
        return bool(materialized)
    return any(name in paired and name in materialized for name in written)


def stranded_emission_flags(
    manifest: GraphManifest, before: EdgeConfig, after: EdgeConfig
) -> dict[str, list[EdgeStepRef]]:
    """Flagged steps that mirrored into something under *before* and into nothing under *after*.

    Removing an inverse edge strands the flags that fed it. A flag that was
    already idle before the change is not the change's to clear.
    """
    ingestion = manifest.ingestion_model
    if ingestion is None:
        return {}
    stranded: dict[str, list[EdgeStepRef]] = {}
    for resource in ingestion.resources:
        refs = [
            view.ref
            for view in iter_edge_steps(resource.pipeline)
            if view.emit_inverse
            and _flag_is_live(view.static_edge_id, view.relations_written(), before)
            and not _flag_is_live(view.static_edge_id, view.relations_written(), after)
        ]
        if refs:
            stranded[resource.name] = refs
    return stranded
