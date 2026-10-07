"""Ops that rewrite ingestion resources."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import AliasChoices, model_validator
from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.ingestion.resource import ResourceConfig
from graflo.architecture.contract.ingestion.transform import ProtoTransform

from .validation import validate_rename_map_is_injective


class RenameResourcesOp(ConfigBaseModel):
    """Rename ingestion resource names and bindings references."""

    op: Literal["rename_resources"] = "rename_resources"
    renames: dict[str, str] = PydanticField(
        ...,
        validation_alias=AliasChoices("renames", "resources"),
        description=(
            "Ingestion resource rename map: ``{old_resource: new_resource}``. Must "
            "be injective. ``resources`` is accepted as a legacy alias."
        ),
        min_length=1,
    )

    @model_validator(mode="after")
    def _reject_collapsing_map(self) -> RenameResourcesOp:
        # IngestionModel already rejects duplicate resource names, so a collapsing
        # map fails downstream anyway — but with a message about the model rather
        # than about the op the author actually wrote.
        validate_rename_map_is_injective(
            self.renames,
            kind="rename_resources",
            merge_hint="MergeManifestsOp renames.<side>.resources",
        )
        return self


class AddResourceTransformsOp(ConfigBaseModel):
    """Append transform steps to named resources' pipelines.

    The first op whose primary effect is ingestion: ``graph_schema`` is
    untouched. Steps land at the root level of each pipeline unless ``at``
    names a deeper one; at whichever level they land, actor type-priority
    sorting (transform runs before vertex extraction at the same level) makes
    the position safe.

    The level is load-bearing rather than cosmetic. An actor reads its
    transform buffer at its own ``LocationIndex`` with no ancestor fallback,
    and a ``descend`` subtree runs *before* its own level's transforms, so a
    step appended at the root is invisible to a vertex produced under a
    ``descend`` — and a transform whose declared inputs are missing skips
    silently by default. Target the level that produces the vertex.

    Steps may reference a registry transform via ``call.use`` (resolved
    against the manifest's existing ``ingestion_model.transforms`` union the
    op's own ``transforms``) or carry a fully inline ``call``
    (``module`` + ``foo`` + ``params``), which cannot collide by name.
    """

    op: Literal["add_resource_transforms"] = "add_resource_transforms"
    additions: dict[str, list[dict[str, Any]]] = PydanticField(
        ...,
        description=(
            "Per-resource transform steps to append: "
            "``{resource_name: [step_dict, ...]}``."
        ),
        min_length=1,
    )
    at: dict[str, list[int]] = PydanticField(
        default_factory=dict,
        description=(
            "Per-resource pipeline level to append into: "
            "``{resource_name: [step_index, ...]}``. Each index must address a "
            "``descend`` step, descending one level per element; an omitted or "
            "empty path means the root level."
        ),
    )
    transforms: list[ProtoTransform] = PydanticField(
        default_factory=list,
        description=(
            "Named transforms to register in ``ingestion_model.transforms`` "
            "for steps that reference them via ``call.use``. A name already "
            "registered with a different body is an error at apply time."
        ),
    )

    @model_validator(mode="after")
    def _validate_steps(self) -> AddResourceTransformsOp:
        from graflo.architecture.contract.ingestion.steps.models import (
            TransformActorConfig,
        )
        from graflo.architecture.contract.ingestion.steps.normalize import (
            normalize_actor_step,
        )

        for resource_name, steps in self.additions.items():
            if not steps:
                raise ValueError(
                    f"add_resource_transforms: empty step list for resource "
                    f"{resource_name!r}"
                )
            for step in steps:
                normalized = normalize_actor_step(step)
                if (
                    not isinstance(normalized, dict)
                    or normalized.get("type") != "transform"
                ):
                    raise ValueError(
                        f"add_resource_transforms: step for resource "
                        f"{resource_name!r} is not a transform step: {step!r}"
                    )
                TransformActorConfig.model_validate(normalized)
        stray = sorted(set(self.at) - set(self.additions))
        if stray:
            raise ValueError(
                f"add_resource_transforms: `at` names resources {stray} with no "
                "steps to append"
            )
        for resource_name, path in self.at.items():
            if any(index < 0 for index in path):
                raise ValueError(
                    f"add_resource_transforms: `at` path for resource "
                    f"{resource_name!r} has a negative index: {path}"
                )
        for proto in self.transforms:
            if not proto.name:
                raise ValueError(
                    "add_resource_transforms: registry transforms must define "
                    "a non-empty name"
                )
        return self


class EnsureExtractedFields(ConfigBaseModel):
    """Fields that must survive extraction for one vertex type at one level."""

    vertex: str = PydanticField(
        ...,
        description="The vertex type whose extraction must keep ``fields``.",
    )
    fields: list[str] = PydanticField(
        ...,
        min_length=1,
        description="Property names that must reach the extracted vertex document.",
    )
    at: list[int] = PydanticField(
        default_factory=list,
        description=(
            "Pipeline level holding the producing step, as ``descend`` step "
            "indices. Empty means the root level."
        ),
    )


class EnsureExtractedFieldsOp(ConfigBaseModel):
    """Widen a producing step's projection so named fields are not dropped.

    Needed because a ``vertex_router`` delivers differently from a ``vertex``
    step. The router builds its child ``VertexActor`` at
    ``lindex.extend((role, 0))``, where the transform buffer is empty, so
    derived fields reach the child only through the merged observation — that
    is, through passthrough or ``from``. A plain ``vertex`` step instead reads
    the buffer directly, which bypasses ``keep_fields`` and
    ``extraction_scope`` entirely.

    So on a router, ``extraction_scope: mapped_only`` or a ``keep_fields`` list
    that does not name the fields drops them silently. This op restores them:
    ``keep_fields`` gains the names, and under ``mapped_only`` the per-type
    ``vertex_from_map`` entry gains identity mappings — seeded from the
    router-level ``from`` when the entry does not exist yet, since creating it
    otherwise replaces the author's projection rather than extending it.

    A plain ``vertex`` step, or a router that restricts nothing, is a no-op:
    the fields already survive.
    """

    op: Literal["ensure_extracted_fields"] = "ensure_extracted_fields"
    additions: dict[str, list[EnsureExtractedFields]] = PydanticField(
        ...,
        description=(
            "Per-resource extraction guarantees: "
            "``{resource_name: [{vertex, fields, at}, ...]}``."
        ),
        min_length=1,
    )

    @model_validator(mode="after")
    def _validate_entries(self) -> EnsureExtractedFieldsOp:
        for resource_name, entries in self.additions.items():
            if not entries:
                raise ValueError(
                    f"ensure_extracted_fields: empty entry list for resource "
                    f"{resource_name!r}"
                )
            for entry in entries:
                if any(index < 0 for index in entry.at):
                    raise ValueError(
                        f"ensure_extracted_fields: `at` path for resource "
                        f"{resource_name!r} has a negative index: {entry.at}"
                    )
        return self


class AddResourcesOp(ConfigBaseModel):
    """Introduce ingestion resources, in the shape the ingestion block accepts.

    The unary way to grow ``ingestion_model``: without it a change set could
    only ever rename or narrow the resources it started with, and the differ
    had to report an added resource as inexpressible.
    """

    op: Literal["add_resources"] = "add_resources"
    resources: list[ResourceConfig] = PydanticField(
        ...,
        description="Full resource definitions.",
        min_length=1,
    )
    transforms: list[ProtoTransform] = PydanticField(
        default_factory=list,
        description=(
            "Named transforms to register in ``ingestion_model.transforms`` for "
            "steps of the new resources that reference them via ``call.use``. "
            "Unioned by name exactly as ``add_resource_transforms`` does: an "
            "identical body already registered dedupes, a different one is an "
            "error at apply time."
        ),
    )

    @model_validator(mode="after")
    def _validate_unique_names(self) -> AddResourcesOp:
        names = [resource.name for resource in self.resources]
        if len(names) != len(set(names)):
            raise ValueError("add_resources entries must be unique by name")
        return self


class RemoveResourcesOp(ConfigBaseModel):
    """Remove ingestion resources and the bindings that wired them."""

    op: Literal["remove_resources"] = "remove_resources"
    names: list[str] = PydanticField(
        ...,
        description="Resource names to remove.",
        min_length=1,
    )


class ReplaceResourcesOp(ConfigBaseModel):
    """Replace the definitions of existing resources, matched by name.

    Each resource keeps its position in ``ingestion_model.resources`` and its
    bindings; only its definition changes. This is how an edited pipeline is
    expressed, since a pipeline is an ordered program no finer op can patch.
    """

    op: Literal["replace_resources"] = "replace_resources"
    resources: list[ResourceConfig] = PydanticField(
        ...,
        description="Full new definitions; each name must already exist.",
        min_length=1,
    )
    transforms: list[ProtoTransform] = PydanticField(
        default_factory=list,
        description=(
            "Named transforms the new definitions reference via ``call.use``, "
            "registered as ``add_resources`` registers them."
        ),
    )

    @model_validator(mode="after")
    def _validate_unique_names(self) -> ReplaceResourcesOp:
        names = [resource.name for resource in self.resources]
        if len(names) != len(set(names)):
            raise ValueError("replace_resources entries must be unique by name")
        return self
