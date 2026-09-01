"""Ingestion-side evolution ops: mutations whose primary effect is the pipeline.

Every other op touches ``ingestion_model`` only as a cascade of a schema
change. :func:`apply_add_resource_transforms` is the fundamental the other
direction: it appends transform steps to named resources (and optionally
registers named transforms), leaving ``graph_schema`` untouched.
"""

from __future__ import annotations

import copy

from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.contract.ingestion.steps.models import TransformActorConfig
from graflo.architecture.contract.ingestion.transform import ProtoTransform
from graflo.architecture.contract.manifest import GraphManifest

from .ops import AddResourceTransformsOp


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
    body, or when a step's ``call.use`` resolves against neither the existing
    registry nor ``op.transforms``. Steps are appended at the end of the root
    pipeline; actor type-priority ordering runs them before vertex extraction
    at that level.
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

    from graflo.architecture.contract.ingestion.resource import Resource

    resources: list[Resource] = []
    for resource in im.resources:
        payload = resource.to_dict(skip_defaults=False)
        added = op.additions.get(resource.name)
        if added:
            pipeline = list(payload.get("pipeline") or [])
            pipeline.extend(copy.deepcopy(step) for step in added)
            payload["pipeline"] = pipeline
        resources.append(Resource.model_validate(payload))

    im.resources = resources
    im.transforms = registry
    # Full round-trip re-validation: rebuilds the name maps and runtimes, and
    # ProtoTransform's eager module import fails loudly here on a bad foo.
    manifest.ingestion_model = IngestionModel.model_validate(
        im.to_dict(skip_defaults=False)
    )
