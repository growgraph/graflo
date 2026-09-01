"""Identity alignment: compose an equivalence identity from fundamental ops.

An :class:`IdentityAlignment` states, for one canonical class, which canonical
attributes carry cross-source entity equivalence and how each resource derives
them. It is a *composer*, not a mechanism: :func:`alignment_to_ops` emits only
fundamental ops —

1. ``AddVertexPropertiesOp`` — declare the canonical attributes on the class;
2. ``AddResourceTransformsOp`` — per-resource derivation steps (gating,
   normalization, local-key namespacing) appended to the pipelines;
3. ``ReplaceIdentityOp`` — a priority funnel over the canonical attributes,
   in row order, with the namespaced ``local_key`` as the last branch;
4. ``AddSecondaryIdentitiesOp`` — the retired side keys as lookup-only
   secondary identities.

The division of labor is deliberate: **a primary identity is a property of the
class**, so the funnel references only canonical attributes; *how* a given
source populates them is resource knowledge and lives in that resource's
pipeline. Derivation inputs are RAW source-doc field names — property renames
rewrite ``vertex.from`` maps so documents keep their original keys, and
``transform.call.input`` is never rewritten.
"""

from __future__ import annotations

import logging
from typing import Any

from pydantic import Field as PydanticField
from pydantic import model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.schema.identity_funnel import IdentityBranch, IdentityFunnel
from graflo.architecture.schema.vertex import SecondaryIdentity

from .canonical import CanonicalMap
from .ops import (
    AddResourceTransformsOp,
    AddSecondaryIdentitiesOp,
    AddVertexPropertiesOp,
    FunnelIdentityTarget,
    IdentityReplacement,
    ManifestOp,
    ReplaceIdentityOp,
)

logger = logging.getLogger(__name__)


class AlignmentConflictError(ValueError):
    """An identity alignment contradicts the union manifest or canonical maps."""


class DerivationSpec(ConfigBaseModel):
    """How one resource derives a canonical attribute from its raw doc fields."""

    input: list[str] = PydanticField(
        ...,
        min_length=1,
        description=(
            "RAW source-doc field names fed to the function, in order. "
            "Documents keep their original keys after property renames, so "
            "canonical property names are usually wrong here."
        ),
    )
    module: str = PydanticField(
        default="graflo.util.transform",
        description="Module holding the derivation function.",
    )
    foo: str = PydanticField(
        default="gated_normalized_key",
        description="Function name; called as ``foo(*values, **params)``.",
    )
    params: dict[str, Any] = PydanticField(
        default_factory=dict,
        description="Keyword parameters for the function (gate prefix, ...).",
    )


class AlignmentRow(ConfigBaseModel):
    """One aligned canonical attribute; list position = funnel priority."""

    into: str = PydanticField(
        ...,
        description="Canonical attribute name on the class; funnel branch id.",
    )
    sources: dict[str, DerivationSpec] = PydanticField(
        ...,
        min_length=1,
        description="Per-resource derivation: ``{resource_name: spec}``.",
    )


class LocalKeySource(ConfigBaseModel):
    """Where one resource's side-local key comes from, and its namespace tag."""

    field: str = PydanticField(
        ...,
        description="RAW doc field carrying the side-local key.",
    )
    tag: str = PydanticField(
        ...,
        description="Namespace tag: tag 'a' turns 'f2' into 'a:f2'.",
    )


class LocalKeySpec(ConfigBaseModel):
    """The canonical fallback identity attribute for non-aligned records."""

    into: str = PydanticField(
        default="local_key",
        description="Canonical fallback property name on the class.",
    )
    sep: str = PydanticField(
        default=":",
        description="Separator between tag and key.",
    )
    sources: dict[str, LocalKeySource] = PydanticField(
        ...,
        min_length=1,
        description="Per-resource local-key wiring: ``{resource_name: source}``.",
    )


class IdentityAlignment(ConfigBaseModel):
    """Cross-source identity alignment for one canonical class.

    ``rows`` order is funnel priority: a record keys by the highest-priority
    aligned attribute it carries. Two records fuse when their strongest
    present attribute coincides — a match on a lower-priority attribute does
    NOT fuse records when one of them also carries a higher-priority one.
    """

    vertex: str = PydanticField(
        ...,
        description="The canonical class whose identity is being aligned.",
    )
    rows: list[AlignmentRow] = PydanticField(
        default_factory=list,
        description="Aligned canonical attributes, in priority order.",
    )
    local_key: LocalKeySpec | None = PydanticField(
        default=None,
        description=(
            "Fallback identity for records carrying no aligned attribute. "
            "Without it such records get no identity and are dropped."
        ),
    )
    secondary_identities: dict[str, list[str]] = PydanticField(
        default_factory=dict,
        description=(
            "Retired side keys kept as lookup-only secondary identities: "
            "``{name: [field, ...]}``."
        ),
    )

    @model_validator(mode="after")
    def _validate_shape(self) -> IdentityAlignment:
        if not self.rows and self.local_key is None:
            raise ValueError(
                "IdentityAlignment requires at least one row or a local_key"
            )
        into_names = [row.into for row in self.rows]
        if self.local_key is not None:
            into_names.append(self.local_key.into)
        duplicates = {n for n in into_names if into_names.count(n) > 1}
        if duplicates:
            raise ValueError(
                f"IdentityAlignment: duplicate target attributes {sorted(duplicates)}"
            )
        return self


def _conflict(check: str, detail: str, hint: str) -> AlignmentConflictError:
    return AlignmentConflictError(
        f"identity alignment conflict ({check}): {detail}. {hint}"
    )


def _canonical_rename_targets(canonical_maps: tuple[CanonicalMap, ...]) -> set[str]:
    """Property names that exist only post-rename — absent from raw documents."""
    targets: set[str] = set()
    for cm in canonical_maps:
        for attr_map in cm.properties.values():
            targets.update(new for old, new in attr_map.items() if old != new)
    return targets


def validate_alignment(
    alignment: IdentityAlignment,
    manifest: GraphManifest,
    *,
    canonical_maps: tuple[CanonicalMap, ...] | list[CanonicalMap] = (),
) -> None:
    """Fail loudly when *alignment* contradicts *manifest* or the canonical maps.

    *manifest* is the composed union the alignment ops will be applied to.
    Pass the :class:`CanonicalMap`\\ s used to canonicalize the sides to catch
    derivation inputs written in canonical vocabulary: renamed documents still
    carry their raw field names, so a rename *target* used as a derivation
    input reads an absent field and silently derives nothing.
    """
    schema = manifest.graph_schema
    if schema is None:
        raise AlignmentConflictError("identity alignment requires graph_schema")
    vertex_config = schema.core_schema.vertex_config
    if alignment.vertex not in vertex_config.vertex_set:
        raise _conflict(
            "unknown vertex",
            f"{alignment.vertex!r} is not defined in the manifest",
            f"Defined: {sorted(vertex_config.vertex_set)}.",
        )
    if manifest.ingestion_model is None:
        raise AlignmentConflictError(
            "identity alignment requires ingestion_model — derivations are "
            "resource pipeline steps"
        )
    known_resources = {r.name for r in manifest.ingestion_model.resources}

    resource_refs: set[str] = set()
    raw_inputs: dict[str, list[str]] = {}
    for row in alignment.rows:
        resource_refs.update(row.sources)
        for resource, spec in row.sources.items():
            raw_inputs.setdefault(resource, []).extend(spec.input)
    if alignment.local_key is not None:
        resource_refs.update(alignment.local_key.sources)
        for resource, src in alignment.local_key.sources.items():
            raw_inputs.setdefault(resource, []).append(src.field)

    missing = sorted(resource_refs - known_resources)
    if missing:
        raise _conflict(
            "unknown resources",
            f"{missing} are not defined in the manifest",
            f"Defined: {sorted(known_resources)}.",
        )

    current_identity = set(vertex_config.identity_fields(alignment.vertex))
    into_names = [row.into for row in alignment.rows]
    if alignment.local_key is not None:
        into_names.append(alignment.local_key.into)
    colliding = sorted(set(into_names) & current_identity)
    if colliding:
        raise _conflict(
            "identity collision",
            f"target attributes {colliding} are already primary-identity "
            f"fields of {alignment.vertex!r}",
            "Pick canonical attribute names distinct from the current key; "
            "the alignment replaces the identity wholesale.",
        )

    declared = set(vertex_config.property_names(alignment.vertex))
    for name, fields in alignment.secondary_identities.items():
        undeclared = sorted(set(fields) - declared)
        if undeclared:
            raise _conflict(
                "undeclared secondary fields",
                f"secondary identity {name!r} references {undeclared}, not "
                f"declared on {alignment.vertex!r}",
                "Secondary identities index existing properties.",
            )

    rename_targets = _canonical_rename_targets(tuple(canonical_maps))
    if rename_targets:
        for resource, fields in raw_inputs.items():
            canonical_used = sorted(set(fields) & rename_targets)
            if canonical_used:
                raise _conflict(
                    "canonical name as derivation input",
                    f"resource {resource!r} derivations read {canonical_used}, "
                    "which are canonical rename targets — documents still "
                    "carry the RAW source field names",
                    "Use the raw field names the source documents actually "
                    "carry (property renames rewrite vertex.from maps, not "
                    "transform inputs).",
                )

    if alignment.local_key is None:
        logger.warning(
            "identity alignment for %r has no local_key: records matching no "
            "aligned attribute complete no funnel branch and are dropped",
            alignment.vertex,
        )


def alignment_to_ops(
    alignment: IdentityAlignment,
    *,
    manifest: GraphManifest | None = None,
    canonical_maps: tuple[CanonicalMap, ...] | list[CanonicalMap] = (),
) -> list[ManifestOp]:
    """Compose the alignment into an ordered list of fundamental ops.

    Apply the result to the composed union with
    :func:`~graflo.architecture.evolution.apply.apply_evolution`. When
    *manifest* is given, :func:`validate_alignment` runs first.
    """
    if manifest is not None:
        validate_alignment(alignment, manifest, canonical_maps=canonical_maps)

    ops: list[ManifestOp] = []

    into_names = [row.into for row in alignment.rows]
    if alignment.local_key is not None:
        into_names.append(alignment.local_key.into)
    ops.append(AddVertexPropertiesOp(additions={alignment.vertex: list(into_names)}))

    additions: dict[str, list[dict[str, Any]]] = {}
    for row in alignment.rows:
        for resource, spec in row.sources.items():
            additions.setdefault(resource, []).append(
                {
                    "transform": {
                        "call": {
                            "module": spec.module,
                            "foo": spec.foo,
                            "params": dict(spec.params),
                            "input": list(spec.input),
                            "output": [row.into],
                        }
                    }
                }
            )
    if alignment.local_key is not None:
        for resource, src in alignment.local_key.sources.items():
            additions.setdefault(resource, []).append(
                {
                    "transform": {
                        "call": {
                            "module": "graflo.util.transform",
                            "foo": "tagged_key",
                            "params": {
                                "tag": src.tag,
                                "sep": alignment.local_key.sep,
                            },
                            "input": [src.field],
                            "output": [alignment.local_key.into],
                        }
                    }
                }
            )
    ops.append(AddResourceTransformsOp(additions=additions))

    # Always a funnel, even for a single row: gating means presence is never
    # guaranteed, and include_branch_id keeps branches collision-free.
    branches = [IdentityBranch(id=name, fields=[name]) for name in into_names]
    ops.append(
        ReplaceIdentityOp(
            vertices={
                alignment.vertex: IdentityReplacement(
                    to=FunnelIdentityTarget(funnel=IdentityFunnel(branches=branches)),
                    # The pre-alignment identity on a composed class is the
                    # merged union of the side keys — a field-set no record
                    # carries. Demoting it would index nothing; the per-side
                    # keys are demoted explicitly below instead.
                    retire="keep",
                )
            }
        )
    )

    if alignment.secondary_identities:
        ops.append(
            AddSecondaryIdentitiesOp(
                additions={
                    alignment.vertex: [
                        SecondaryIdentity(name=name, fields=list(fields))
                        for name, fields in sorted(
                            alignment.secondary_identities.items()
                        )
                    ]
                }
            )
        )

    return ops
