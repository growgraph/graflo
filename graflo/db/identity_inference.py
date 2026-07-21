"""Algorithmic identity inference from vertex record samples."""

from __future__ import annotations

import hashlib
import json
import logging
import random
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.schema.graflo_output import GraFloOutput
from graflo.architecture.schema.vertex import Vertex, VertexConfig
from graflo.db.graph_introspection import strip_internal_properties

logger = logging.getLogger(__name__)

_SYNTHETIC_ID_FIELD = "id"
_SEMANTIC_PATTERN = re.compile(r"(?i)(id|uuid|key|code|pk)$")
_UUID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
_ISO_DATETIME_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)?$"
)
_LONG_TEXT_THRESHOLD = 256
_NONE_RATIO_REJECT = 0.5

IdentityStrategy = Literal["unary", "composite", "hash_fallback", "no_viable_identity"]

DEFAULT_MIN_SAMPLE_SIZE = 100


class IdentityInferenceConfig(ConfigBaseModel):
    """External configuration for :class:`IdentityInferencer`."""

    max_key_width: int = PydanticField(
        default=3,
        description="Maximum natural composite identity width before hash fallback.",
    )
    min_sample_size: int = PydanticField(
        default=DEFAULT_MIN_SAMPLE_SIZE,
        ge=1,
        description=(
            "Minimum number of records required before inference runs. "
            "Typical production values are around 100 or more."
        ),
    )
    max_sample_size: int | None = PydanticField(
        default=None,
        ge=1,
        description=(
            "When set, randomly subsample at most this many records for inference. "
            "Useful when snapshots contain thousands of rows."
        ),
    )
    type_cost_weight: float = PydanticField(default=0.2, ge=0.0)
    semantic_weight: float = PydanticField(default=0.5, ge=0.0)
    n_boots: int = PydanticField(default=5, ge=1)
    subsample_ratio: float = PydanticField(default=0.8, gt=0.0, le=1.0)


class IdentityInferenceResult(ConfigBaseModel):
    """Outcome of identity inference for one vertex type."""

    identity: list[str] = PydanticField(default_factory=list)
    hash_identity_properties: list[str] = PydanticField(default_factory=list)
    confidence: float = PydanticField(default=0.0, ge=0.0, le=1.0)
    strategy: IdentityStrategy = "no_viable_identity"
    warning: str | None = None


def infer_column_type_cost(values: list[Any]) -> float | None:
    """Return per-column type suitability cost, or ``None`` when disqualified."""
    if not values:
        return None

    non_null = [value for value in values if value is not None]
    if not non_null:
        return None
    if len(non_null) / len(values) < (1.0 - _NONE_RATIO_REJECT):
        return None

    costs: list[float] = []
    for value in non_null:
        if isinstance(value, bool):
            costs.append(0.1)
        elif isinstance(value, int):
            costs.append(0.0)
        elif isinstance(value, float):
            costs.append(1.0)
        elif isinstance(value, datetime):
            costs.append(0.5)
        elif isinstance(value, bytes):
            return None
        elif isinstance(value, list):
            return None
        elif isinstance(value, str):
            if _UUID_PATTERN.match(value):
                costs.append(0.0)
            elif len(value) > _LONG_TEXT_THRESHOLD:
                return None
            elif _ISO_DATETIME_PATTERN.match(value):
                costs.append(0.5)
            else:
                costs.append(0.1)
        else:
            return None

    return sum(costs) / len(costs)


def score_candidate(
    fields: list[str],
    type_costs: dict[str, float],
    *,
    type_cost_weight: float = 0.2,
    semantic_weight: float = 0.5,
) -> float:
    """Lower scores are better (parsimony + type cost - semantic bonus)."""
    parsimony = float(len(fields) ** 2)
    type_cost = sum(type_costs[field] for field in fields)
    semantic_bonus = (
        semantic_weight
        if any(_SEMANTIC_PATTERN.search(field) for field in fields)
        else 0.0
    )
    return parsimony + type_cost_weight * type_cost - semantic_bonus


def uniqueness_ratio(samples: list[dict], key_fields: list[str]) -> float:
    """Fraction of rows with distinct key tuples."""
    if not samples or not key_fields:
        return 0.0
    tuples = [tuple(sample.get(field) for field in key_fields) for sample in samples]
    return len(set(tuples)) / len(samples)


def bootstrap_pass_rate(
    samples: list[dict],
    key_fields: list[str],
    *,
    n_boots: int = 5,
    subsample_ratio: float = 0.8,
    min_sample_size: int = 10,
    rng: random.Random | None = None,
) -> float:
    """Return the fraction of bootstrap sub-samples that remain fully unique."""
    sample_count = len(samples)
    if sample_count < min_sample_size:
        return 0.0

    randomizer = rng or random.Random()
    subsample_size = max(1, int(sample_count * subsample_ratio))
    passes = 0
    for _ in range(n_boots):
        subsample = randomizer.sample(samples, k=subsample_size)
        if uniqueness_ratio(subsample, key_fields) >= 1.0:
            passes += 1
    return passes / n_boots


def bootstrap_is_stable(
    samples: list[dict],
    key_fields: list[str],
    *,
    n_boots: int = 5,
    subsample_ratio: float = 0.8,
    min_sample_size: int = 10,
    rng: random.Random | None = None,
) -> bool:
    """Return whether bootstrap validation passes on every sub-sample."""
    return (
        bootstrap_pass_rate(
            samples,
            key_fields,
            n_boots=n_boots,
            subsample_ratio=subsample_ratio,
            min_sample_size=min_sample_size,
            rng=rng,
        )
        == 1.0
    )


def compute_hash_identity(
    doc: dict[str, Any],
    source_fields: list[str],
) -> str:
    """Compute a deterministic SHA256 hex digest from source field values."""
    payload = {field: doc.get(field) for field in source_fields}
    source = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(source.encode()).hexdigest()


def _column_values(samples: list[dict], field: str) -> list[Any]:
    return [sample.get(field) for sample in samples]


def _eligible_columns(
    samples: list[dict],
    property_names: list[str],
) -> tuple[list[str], dict[str, float]]:
    eligible: list[str] = []
    type_costs: dict[str, float] = {}
    for field in property_names:
        cost = infer_column_type_cost(_column_values(samples, field))
        if cost is None:
            continue
        eligible.append(field)
        type_costs[field] = cost
    return eligible, type_costs


def _minimize_key_fields(samples: list[dict], key_fields: list[str]) -> list[str]:
    minimal = list(key_fields)
    changed = True
    while changed:
        changed = False
        for index in range(len(minimal)):
            subset = minimal[:index] + minimal[index + 1 :]
            if subset and uniqueness_ratio(samples, subset) >= 1.0:
                minimal = subset
                changed = True
                break
    return minimal


def _greedy_unique_key(
    samples: list[dict],
    ranked_fields: list[str],
) -> list[str] | None:
    if not ranked_fields:
        return None

    selected = [ranked_fields[0]]
    if uniqueness_ratio(samples, selected) >= 1.0:
        return selected

    for field in ranked_fields[1:]:
        selected.append(field)
        if uniqueness_ratio(samples, selected) >= 1.0:
            return selected
    return None


def _no_viable_result(warning: str) -> IdentityInferenceResult:
    return IdentityInferenceResult(
        identity=[],
        hash_identity_properties=[],
        confidence=0.0,
        strategy="no_viable_identity",
        warning=warning,
    )


def _prepare_inference_samples(
    samples: list[dict],
    config: IdentityInferenceConfig,
    rng: random.Random | None,
) -> list[dict]:
    """Strip internal fields and optionally subsample to ``max_sample_size``."""
    cleaned = [strip_internal_properties(sample) for sample in samples]
    if config.max_sample_size is None or len(cleaned) <= config.max_sample_size:
        return cleaned

    randomizer = rng or random.Random()
    return randomizer.sample(cleaned, k=config.max_sample_size)


class IdentityInferencer:
    """Infer vertex identity fields from flat record samples."""

    def __init__(
        self,
        config: IdentityInferenceConfig | None = None,
        *,
        rng: random.Random | None = None,
    ) -> None:
        self.config = config or IdentityInferenceConfig()
        self.rng = rng

    def infer(
        self,
        samples: list[dict],
        property_names: list[str] | None = None,
    ) -> IdentityInferenceResult:
        cleaned_samples = _prepare_inference_samples(samples, self.config, self.rng)
        if len(cleaned_samples) < self.config.min_sample_size:
            return _no_viable_result("sample too small")

        resolved_properties = property_names or sorted(
            {key for sample in cleaned_samples for key in sample}
        )
        eligible, type_costs = _eligible_columns(cleaned_samples, resolved_properties)
        if not eligible:
            return _no_viable_result("all columns disqualified")

        ranked_fields = sorted(
            eligible,
            key=lambda field: score_candidate(
                [field],
                type_costs,
                type_cost_weight=self.config.type_cost_weight,
                semantic_weight=self.config.semantic_weight,
            ),
        )

        unary_candidates = [
            field
            for field in ranked_fields
            if uniqueness_ratio(cleaned_samples, [field]) >= 1.0
        ]
        if unary_candidates:
            best_unary = min(
                unary_candidates,
                key=lambda field: score_candidate(
                    [field],
                    type_costs,
                    type_cost_weight=self.config.type_cost_weight,
                    semantic_weight=self.config.semantic_weight,
                ),
            )
            if bootstrap_is_stable(
                cleaned_samples,
                [best_unary],
                n_boots=self.config.n_boots,
                subsample_ratio=self.config.subsample_ratio,
                min_sample_size=self.config.min_sample_size,
                rng=self.rng,
            ):
                return IdentityInferenceResult(
                    identity=[best_unary],
                    hash_identity_properties=[],
                    confidence=1.0,
                    strategy="unary",
                )

        composite_key = _greedy_unique_key(cleaned_samples, ranked_fields)
        if composite_key is None:
            return _no_viable_result("no unique combination found")

        minimal_key = _minimize_key_fields(cleaned_samples, composite_key)
        pass_rate = bootstrap_pass_rate(
            cleaned_samples,
            minimal_key,
            n_boots=self.config.n_boots,
            subsample_ratio=self.config.subsample_ratio,
            min_sample_size=self.config.min_sample_size,
            rng=self.rng,
        )

        if pass_rate == 1.0 and len(minimal_key) <= self.config.max_key_width:
            return IdentityInferenceResult(
                identity=minimal_key,
                hash_identity_properties=[],
                confidence=pass_rate,
                strategy="composite",
            )

        warning = (
            "identity key exceeds max_key_width"
            if len(minimal_key) > self.config.max_key_width
            else "bootstrap validation failed"
        )
        return IdentityInferenceResult(
            identity=[_SYNTHETIC_ID_FIELD],
            hash_identity_properties=minimal_key,
            confidence=max(pass_rate * 0.5, 0.1),
            strategy="hash_fallback",
            warning=warning,
        )


def apply_identity_inference_to_vertices(
    vertices: list[Vertex],
    samples_by_name: dict[str, list[dict]],
    config: IdentityInferenceConfig | None = None,
) -> tuple[list[Vertex], dict[str, IdentityInferenceResult]]:
    """Infer and apply identity fields for each vertex type (immutable copy)."""
    inferencer = IdentityInferencer(config=config or IdentityInferenceConfig())
    updated_vertices: list[Vertex] = []
    results: dict[str, IdentityInferenceResult] = {}

    for vertex in vertices:
        samples = samples_by_name.get(vertex.name, [])
        result = inferencer.infer(samples, property_names=vertex.property_names)
        results[vertex.name] = result

        if result.strategy == "no_viable_identity":
            logger.error(
                "No viable identity for vertex '%s': %s",
                vertex.name,
                result.warning,
            )
            updated_vertices.append(vertex)
            continue

        if result.warning:
            log = logger.warning if result.strategy == "hash_fallback" else logger.info
            log(
                "Identity inference for vertex '%s' (%s): %s",
                vertex.name,
                result.strategy,
                result.warning,
            )

        updated_vertices.append(
            vertex.model_copy(
                update={
                    "identity": result.identity,
                    "hash_identity_properties": result.hash_identity_properties,
                }
            )
        )

    return updated_vertices, results


def infer_identities_from_snapshot(
    snapshot_path: str | Path,
    output_path: str | Path | None = None,
    *,
    config: IdentityInferenceConfig | None = None,
) -> GraFloOutput:
    """Infer vertex identities from a GraFlo output snapshot and return an updated copy."""
    output = GraFloOutput.from_yaml(str(snapshot_path))
    vertex_config = output.core_schema.vertex_config
    samples_by_name = {name: docs for name, docs in output.data.vertices.items()}
    updated_vertices, _results = apply_identity_inference_to_vertices(
        list(vertex_config.vertices),
        samples_by_name,
        config=config,
    )

    updated_vertex_config = VertexConfig(
        vertices=updated_vertices,
        force_types=vertex_config.force_types,
        identity_from_all_properties=vertex_config.identity_from_all_properties,
    )
    updated_core_schema = output.core_schema.model_copy(
        update={"vertex_config": updated_vertex_config}
    )
    updated_schema = output.graph_schema.model_copy(
        update={"core_schema": updated_core_schema}
    )
    updated_output = output.model_copy(update={"graph_schema": updated_schema})

    if output_path is not None:
        updated_output.to_yaml(str(output_path))

    return updated_output
