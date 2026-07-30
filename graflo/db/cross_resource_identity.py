"""Algorithmic cross-resource vertex identity discovery.

Given two or more sampled resources that may describe the same vertex, propose a
shared identity policy — a natural key, a composite key, a flat hash, or an
:class:`~graflo.architecture.schema.identity_funnel.IdentityFunnel` — together
with the per-resource field maps and the evidence behind the choice.

**Proposal only.** Nothing here runs at write time and nothing mutates a
manifest. Fuzzy signals (column-name similarity, value overlap) are used to
*align columns*; a key is only ever proven by exact equality after
normalization. That line is deliberate: soft matching in the write path silently
merges distinct entities, and the damage is unbounded and hard to reverse.

Typical use::

    sample = engine.sample_resources(bindings)
    proposal = CrossResourceIdentityInferencer().infer(
        sample.samples_by_resource, vertex_name="party"
    )
    vertex = apply_proposal_to_vertex(vertex, proposal)   # after human review
"""

from __future__ import annotations

import logging
import random
import re
from difflib import SequenceMatcher
from typing import Any, Literal

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.onto_sample import ForeignKeyHint, ResourceSample, SourceSample
from graflo.architecture.schema.identity_funnel import IdentityBranch, IdentityFunnel
from graflo.architecture.schema.vertex import Vertex
from graflo.db.identity_inference import (
    DEFAULT_MIN_SAMPLE_SIZE,
    IdentityInferenceConfig,
    bootstrap_pass_rate,
    column_values,
    eligible_columns,
    greedy_unique_key,
    infer_column_type_cost,
    minimize_key_fields,
    score_candidate,
    uniqueness_ratio,
)

logger = logging.getLogger(__name__)

_TOKEN_SPLIT = re.compile(r"[^a-z0-9]+")
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
)
_DIGITS_RE = re.compile(r"\D+")
_PHONE_HINT = re.compile(r"(?i)(phone|tel|mobile|msisdn)")

#: Strategy of a cross-resource proposal.
#:
#: Deliberately *not* :data:`~graflo.db.identity_inference.IdentityStrategy`:
#: that literal is shipped and tested with ``unary``, and renaming it to
#: ``natural`` to fit this module would be a breaking change for cosmetic
#: reasons. ``unary`` is mapped to ``natural`` at the proposal boundary.
CrossResourceStrategy = Literal[
    "natural",
    "composite",
    "hash_fallback",
    "funnel",
    "no_viable_identity",
]


def normalize_for_match(value: Any, *, digits_only: bool = False) -> str | None:
    """Canonical string for equality comparison, or ``None`` when unusable.

    Used **only** for value-overlap scoring and join projection — never to
    decide that two entities are the same. Trims and lowercases strings,
    normalizes UUID case, and optionally reduces a value to its digits (for
    phone-like columns, where formatting varies by source).
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if digits_only:
        text = _DIGITS_RE.sub("", text)
        return text or None
    if _UUID_RE.match(text):
        return text.lower()
    return text.lower()


def _field_tokens(name: str) -> set[str]:
    return {token for token in _TOKEN_SPLIT.split(name.lower()) if token}


def name_similarity(left: str, right: str) -> float:
    """Similarity of two column names in ``[0, 1]``.

    Token overlap catches ``customer_email`` vs ``email_address``; the character
    ratio catches ``phone`` vs ``phone_no``. The better of the two wins, so
    neither spelling convention is privileged.
    """
    if left == right:
        return 1.0
    left_tokens, right_tokens = _field_tokens(left), _field_tokens(right)
    token_score = 0.0
    if left_tokens and right_tokens:
        token_score = len(left_tokens & right_tokens) / len(left_tokens | right_tokens)
    ratio = SequenceMatcher(None, left.lower(), right.lower()).ratio()
    return max(token_score, ratio)


def value_jaccard(
    left_values: list[Any], right_values: list[Any], *, digits_only: bool = False
) -> float:
    """Jaccard overlap of two columns' normalized non-empty values."""
    left_set = {
        norm
        for norm in (
            normalize_for_match(v, digits_only=digits_only) for v in left_values
        )
        if norm is not None
    }
    right_set = {
        norm
        for norm in (
            normalize_for_match(v, digits_only=digits_only) for v in right_values
        )
        if norm is not None
    }
    if not left_set or not right_set:
        return 0.0
    return len(left_set & right_set) / len(left_set | right_set)


class ColumnAlignment(ConfigBaseModel):
    """A candidate correspondence between two resources' columns."""

    left_resource: str
    left_field: str
    right_resource: str
    right_field: str
    name_score: float = PydanticField(ge=0.0, le=1.0)
    value_jaccard: float = PydanticField(ge=0.0, le=1.0)
    declared: bool = PydanticField(
        default=False,
        description=(
            "True when this pairing comes from a declared primary/foreign key "
            "rather than from name and value heuristics."
        ),
    )

    @property
    def score(self) -> float:
        """Combined confidence; a declared key is ground truth."""
        if self.declared:
            return 1.0
        return 0.5 * self.name_score + 0.5 * self.value_jaccard


class CrossResourceIdentityConfig(ConfigBaseModel):
    """Knobs for :class:`CrossResourceIdentityInferencer`. Defaults are conservative."""

    min_sample_size: int = PydanticField(default=DEFAULT_MIN_SAMPLE_SIZE, ge=1)
    max_sample_size: int | None = PydanticField(default=None, ge=1)
    max_key_width: int = PydanticField(default=3, ge=1)
    min_value_jaccard: float = PydanticField(
        default=0.1,
        ge=0.0,
        le=1.0,
        description=(
            "Mandatory floor on value overlap. Two columns that share no values "
            "cannot be the same column, however alike their names read."
        ),
    )
    min_pair_score: float = PydanticField(
        default=0.5,
        ge=0.0,
        le=1.0,
        description=(
            "Floor on the combined name/value score. Name similarity is a weak "
            "prior and must not veto strong value evidence on its own: "
            "``email`` vs ``email_address`` scores 0.55 on names while sharing "
            "every value."
        ),
    )
    max_alignments: int = PydanticField(default=20, ge=1)
    type_cost_weight: float = PydanticField(default=0.2, ge=0.0)
    semantic_weight: float = PydanticField(default=0.5, ge=0.0)
    n_boots: int = PydanticField(default=5, ge=1)
    subsample_ratio: float = PydanticField(default=0.8, gt=0.0, le=1.0)

    def to_identity_config(self) -> IdentityInferenceConfig:
        """Per-resource inference config sharing these thresholds."""
        return IdentityInferenceConfig(
            max_key_width=self.max_key_width,
            min_sample_size=self.min_sample_size,
            max_sample_size=self.max_sample_size,
            type_cost_weight=self.type_cost_weight,
            semantic_weight=self.semantic_weight,
            n_boots=self.n_boots,
            subsample_ratio=self.subsample_ratio,
        )


class CrossResourceIdentityProposal(ConfigBaseModel):
    """A reviewable identity policy for one vertex across several resources."""

    vertex_name: str
    identity: list[str] = PydanticField(default_factory=list)
    hash_identity_properties: list[str] = PydanticField(default_factory=list)
    identity_funnel: IdentityFunnel | None = None
    assigned: bool = False
    strategy: CrossResourceStrategy = "no_viable_identity"
    confidence: float = PydanticField(default=0.0, ge=0.0, le=1.0)
    alignments: list[ColumnAlignment] = PydanticField(default_factory=list)
    resource_field_maps: dict[str, dict[str, str]] = PydanticField(
        default_factory=dict,
        description="Per resource: ``{source_field: canonical_field}``.",
    )
    suggested_transforms: list[dict[str, Any]] = PydanticField(
        default_factory=list,
        description="Pipeline step dicts splicing into a resource's ``pipeline``.",
    )
    warning: str | None = None
    evidence: dict[str, Any] = PydanticField(default_factory=dict)


def _no_viable(vertex_name: str, warning: str) -> CrossResourceIdentityProposal:
    return CrossResourceIdentityProposal(
        vertex_name=vertex_name, strategy="no_viable_identity", warning=warning
    )


class CrossResourceIdentityInferencer:
    """Propose a shared identity for a vertex described by several resources."""

    def __init__(
        self,
        config: CrossResourceIdentityConfig | None = None,
        *,
        rng: random.Random | None = None,
    ) -> None:
        self.config = config or CrossResourceIdentityConfig()
        self.rng = rng

    # -- public ---------------------------------------------------------

    def infer(
        self,
        samples_by_resource: dict[str, list[dict]],
        *,
        vertex_name: str = "entity",
        declared_keys: dict[str, ResourceSample] | None = None,
        config: CrossResourceIdentityConfig | None = None,
    ) -> CrossResourceIdentityProposal:
        """Propose an identity policy from per-resource document samples.

        Args:
            samples_by_resource: ``{resource_name: [doc, ...]}`` — exactly
                :attr:`SourceSample.samples_by_resource`.
            vertex_name: Name of the vertex the proposal is for.
            declared_keys: Optional per-resource samples carrying declared
                ``primary_key`` / ``foreign_keys``. When present these are ground
                truth and short-circuit heuristic alignment.
            config: Overrides the instance config for this call.
        """
        cfg = config or self.config
        usable = {
            name: docs for name, docs in samples_by_resource.items() if len(docs) > 0
        }
        if len(usable) < 2:
            return _no_viable(
                vertex_name,
                "cross-resource inference needs at least two non-empty resources; "
                f"got {len(usable)}",
            )

        too_small = {
            name: len(docs)
            for name, docs in usable.items()
            if len(docs) < cfg.min_sample_size
        }
        if too_small:
            return _no_viable(
                vertex_name,
                f"resources below min_sample_size={cfg.min_sample_size}: {too_small}. "
                "Uniqueness on a small sample is not evidence of a key.",
            )

        eligible_by_resource = {
            name: eligible_columns(docs, sorted(_all_field_names(docs)))[0]
            for name, docs in usable.items()
        }
        alignments = self._align(usable, eligible_by_resource, cfg, declared_keys)
        if not alignments:
            return _no_viable(
                vertex_name,
                "no column pairs cleared the alignment thresholds, so the "
                "resources share no comparable key material",
            )

        field_maps = self._canonical_field_maps(alignments)
        projected = {
            name: _project(docs, field_maps.get(name, {}))
            for name, docs in usable.items()
        }
        shared_fields = sorted(
            set.intersection(*(set(_all_field_names(d)) for d in projected.values()))
        )
        if not shared_fields:
            return _no_viable(
                vertex_name, "column alignment produced no shared canonical fields"
            )

        key = self._search_shared_key(projected, shared_fields, cfg)

        evidence: dict[str, Any] = {
            "resources": sorted(usable),
            "doc_counts": {name: len(docs) for name, docs in usable.items()},
            "shared_fields": shared_fields,
        }

        if key is not None:
            return self._natural_proposal(
                vertex_name, key, projected, field_maps, alignments, evidence, cfg
            )
        return self._fallback_proposal(
            vertex_name, projected, shared_fields, field_maps, alignments, evidence, cfg
        )

    # -- alignment ------------------------------------------------------

    def _align(
        self,
        samples: dict[str, list[dict]],
        eligible_by_resource: dict[str, list[str]],
        cfg: CrossResourceIdentityConfig,
        declared_keys: dict[str, ResourceSample] | None,
    ) -> list[ColumnAlignment]:
        """Pair columns across resources, declared keys first."""
        names = sorted(samples)
        alignments: list[ColumnAlignment] = []
        declared_pairs = _declared_alignments(declared_keys or {})

        for index, left in enumerate(names):
            for right in names[index + 1 :]:
                alignments.extend(
                    self._align_pair(
                        left,
                        right,
                        samples,
                        eligible_by_resource,
                        cfg,
                        declared_pairs,
                    )
                )

        alignments.sort(key=lambda a: (-a.score, a.left_field, a.right_field))
        return alignments[: cfg.max_alignments]

    def _align_pair(
        self,
        left: str,
        right: str,
        samples: dict[str, list[dict]],
        eligible_by_resource: dict[str, list[str]],
        cfg: CrossResourceIdentityConfig,
        declared_pairs: set[tuple[str, str, str, str]],
    ) -> list[ColumnAlignment]:
        pairs: list[ColumnAlignment] = []
        left_fields = eligible_by_resource.get(left, [])
        right_fields = eligible_by_resource.get(right, [])

        for left_field in left_fields:
            for right_field in right_fields:
                declared = (
                    left,
                    left_field,
                    right,
                    right_field,
                ) in declared_pairs or (
                    right,
                    right_field,
                    left,
                    left_field,
                ) in declared_pairs
                name_score = name_similarity(left_field, right_field)
                digits_only = bool(
                    _PHONE_HINT.search(left_field) or _PHONE_HINT.search(right_field)
                )
                jaccard = value_jaccard(
                    column_values(samples[left], left_field),
                    column_values(samples[right], right_field),
                    digits_only=digits_only,
                )
                candidate = ColumnAlignment(
                    left_resource=left,
                    left_field=left_field,
                    right_resource=right,
                    right_field=right_field,
                    name_score=name_score,
                    value_jaccard=jaccard,
                    declared=declared,
                )
                if not declared and (
                    jaccard < cfg.min_value_jaccard
                    or candidate.score < cfg.min_pair_score
                ):
                    continue
                pairs.append(candidate)
        return pairs

    @staticmethod
    def _canonical_field_maps(
        alignments: list[ColumnAlignment],
    ) -> dict[str, dict[str, str]]:
        """Map each aligned source column to a shared canonical name.

        The canonical name is the alphabetically first field in the alignment
        group, so the choice is stable across runs regardless of resource order.
        """
        maps: dict[str, dict[str, str]] = {}
        for alignment in alignments:
            canonical = min(alignment.left_field, alignment.right_field)
            maps.setdefault(alignment.left_resource, {})[alignment.left_field] = (
                canonical
            )
            maps.setdefault(alignment.right_resource, {})[alignment.right_field] = (
                canonical
            )
        return maps

    # -- key search -----------------------------------------------------

    def _search_shared_key(
        self,
        projected: dict[str, list[dict]],
        shared_fields: list[str],
        cfg: CrossResourceIdentityConfig,
    ) -> list[str] | None:
        """Smallest shared field tuple that keys **every** resource.

        Uniqueness is evaluated *within* each resource, never over the pooled
        rows: the entities described by two resources are supposed to overlap,
        so a good key necessarily repeats across them. Requiring pooled
        uniqueness would reject exactly the keys this module exists to find.

        Scores *tuples*, not columns — a pair may key the rows while neither
        field is unique alone.
        """
        pooled = [doc for docs in projected.values() for doc in docs]
        eligible, type_costs = eligible_columns(pooled, shared_fields)
        eligible = [
            field
            for field in eligible
            if all(
                infer_column_type_cost(column_values(docs, field)) is not None
                for docs in projected.values()
            )
        ]
        if not eligible:
            return None

        ranked = sorted(
            eligible,
            key=lambda field: score_candidate(
                [field],
                type_costs,
                type_cost_weight=cfg.type_cost_weight,
                semantic_weight=cfg.semantic_weight,
            ),
        )

        def keys_every_resource(fields: list[str]) -> bool:
            return all(
                uniqueness_ratio(docs, fields) >= 1.0 for docs in projected.values()
            )

        selected: list[str] = []
        for field in ranked:
            selected.append(field)
            if keys_every_resource(selected):
                break
        else:
            return None

        minimal = _minimize(selected, keys_every_resource)
        if len(minimal) > cfg.max_key_width:
            return None
        if not self._bootstrap_holds(projected, minimal, cfg):
            return None
        return minimal

    def _search_local_key(
        self,
        docs: list[dict],
        cfg: CrossResourceIdentityConfig,
    ) -> list[str] | None:
        """Smallest key for one resource, for the per-resource funnel branches."""
        fields = sorted(_all_field_names(docs))
        eligible, type_costs = eligible_columns(docs, fields)
        if not eligible:
            return None
        ranked = sorted(
            eligible,
            key=lambda field: score_candidate(
                [field],
                type_costs,
                type_cost_weight=cfg.type_cost_weight,
                semantic_weight=cfg.semantic_weight,
            ),
        )
        candidate = greedy_unique_key(docs, ranked)
        if candidate is None:
            return None
        minimal = minimize_key_fields(docs, candidate)
        if len(minimal) > cfg.max_key_width:
            return None
        return minimal

    def _bootstrap_holds(
        self,
        projected: dict[str, list[dict]],
        key: list[str],
        cfg: CrossResourceIdentityConfig,
    ) -> bool:
        """Uniqueness must survive resampling in every resource, not just one."""
        return all(
            bootstrap_pass_rate(
                docs,
                key,
                n_boots=cfg.n_boots,
                subsample_ratio=cfg.subsample_ratio,
                min_sample_size=min(cfg.min_sample_size, len(docs)),
                rng=self.rng,
            )
            >= 1.0
            for docs in projected.values()
        )

    # -- proposals ------------------------------------------------------

    def _natural_proposal(
        self,
        vertex_name: str,
        key: list[str],
        projected: dict[str, list[dict]],
        field_maps: dict[str, dict[str, str]],
        alignments: list[ColumnAlignment],
        evidence: dict[str, Any],
        cfg: CrossResourceIdentityConfig,
    ) -> CrossResourceIdentityProposal:
        overlap = _key_overlap(projected, key)
        evidence = {
            **evidence,
            "uniqueness_by_resource": {
                name: uniqueness_ratio(docs, key) for name, docs in projected.items()
            },
            "shared_key_values": overlap,
            "key_width": len(key),
        }
        return CrossResourceIdentityProposal(
            vertex_name=vertex_name,
            identity=key,
            # ``unary`` in the single-resource inferencer; ``natural`` here.
            strategy="natural" if len(key) == 1 else "composite",
            confidence=_confidence(alignments),
            alignments=alignments,
            resource_field_maps=field_maps,
            suggested_transforms=_rename_steps(field_maps),
            evidence=evidence,
        )

    def _fallback_proposal(
        self,
        vertex_name: str,
        projected: dict[str, list[dict]],
        shared_fields: list[str],
        field_maps: dict[str, dict[str, str]],
        alignments: list[ColumnAlignment],
        evidence: dict[str, Any],
        cfg: CrossResourceIdentityConfig,
    ) -> CrossResourceIdentityProposal:
        """No shared key: propose per-resource branches, or a flat hash.

        A funnel is the honest answer when each resource keys itself well but no
        single field-set spans them: each branch records how *that* source
        identifies the entity, in descending order of evidence strength.
        """
        branches: list[IdentityBranch] = []
        per_resource: dict[str, list[str]] = {}
        for name in sorted(projected):
            local = self._search_local_key(projected[name], cfg)
            if local is None:
                continue
            per_resource[name] = local
            if not any(set(branch.fields) == set(local) for branch in branches):
                branches.append(IdentityBranch(id=name, fields=local))

        evidence = {**evidence, "per_resource_keys": per_resource}

        if len(branches) >= 2:
            return CrossResourceIdentityProposal(
                vertex_name=vertex_name,
                identity=["id"],
                identity_funnel=IdentityFunnel(branches=branches),
                strategy="funnel",
                confidence=_confidence(alignments) * 0.8,
                alignments=alignments,
                resource_field_maps=field_maps,
                suggested_transforms=_rename_steps(field_maps),
                warning=(
                    "No field-set keys every resource, so each source keys itself "
                    "through its own branch. Rows that only one source describes "
                    "will not converge — review before accepting."
                ),
                evidence=evidence,
            )

        hash_fields = sorted(shared_fields)[: cfg.max_key_width]
        if not hash_fields:
            return _no_viable(
                vertex_name, "no shared fields survived alignment for a hash fallback"
            )
        return CrossResourceIdentityProposal(
            vertex_name=vertex_name,
            identity=["id"],
            hash_identity_properties=hash_fields,
            strategy="hash_fallback",
            confidence=_confidence(alignments) * 0.5,
            alignments=alignments,
            resource_field_maps=field_maps,
            suggested_transforms=_rename_steps(field_maps),
            warning=(
                "No unique key was proven; the digest over "
                f"{hash_fields} may collide. Verify before ingesting."
            ),
            evidence=evidence,
        )


def infer_from_source_sample(
    source_sample: SourceSample,
    *,
    vertex_name: str = "entity",
    config: CrossResourceIdentityConfig | None = None,
    rng: random.Random | None = None,
) -> CrossResourceIdentityProposal:
    """Infer directly from a :class:`SourceSample`, using its declared keys."""
    inferencer = CrossResourceIdentityInferencer(config, rng=rng)
    return inferencer.infer(
        source_sample.samples_by_resource,
        vertex_name=vertex_name,
        declared_keys={s.resource_name: s for s in source_sample.samples},
    )


def apply_proposal_to_vertex(
    vertex: Vertex,
    proposal: CrossResourceIdentityProposal,
) -> Vertex:
    """Return *vertex* with *proposal*'s identity policy applied.

    Rebuilt through ``model_validate`` rather than field assignment so
    ``Vertex.set_identity`` runs: the identity flags are mutually constrained and
    a piecewise assignment can trip validation on an intermediate state.
    """
    if proposal.strategy == "no_viable_identity":
        raise ValueError(
            f"cannot apply a no_viable_identity proposal for '{proposal.vertex_name}'"
            + (f": {proposal.warning}" if proposal.warning else "")
        )

    payload: dict[str, Any] = vertex.to_dict(skip_defaults=False)
    payload["identity"] = list(proposal.identity)
    payload["hash_identity_properties"] = list(proposal.hash_identity_properties)
    payload["identity_funnel"] = (
        proposal.identity_funnel.to_dict(skip_defaults=False)
        if proposal.identity_funnel is not None
        else None
    )
    payload["assigned"] = proposal.assigned

    known = {field.get("name") for field in payload.get("properties", [])}
    for name in _proposal_field_names(proposal):
        if name not in known:
            payload.setdefault("properties", []).append({"name": name, "type": None})
            known.add(name)
    return Vertex.model_validate(payload)


# -- helpers ------------------------------------------------------------


def _all_field_names(docs: list[dict]) -> set[str]:
    return {key for doc in docs for key in doc}


def _minimize(fields: list[str], holds: Any) -> list[str]:
    """Drop fields while *holds* still accepts the remainder."""
    minimal = list(fields)
    changed = True
    while changed:
        changed = False
        for index in range(len(minimal)):
            subset = minimal[:index] + minimal[index + 1 :]
            if subset and holds(subset):
                minimal = subset
                changed = True
                break
    return minimal


def _key_overlap(projected: dict[str, list[dict]], key: list[str]) -> int:
    """Key tuples present in every resource — the evidence they describe one entity."""
    per_resource = [
        {
            tuple(normalize_for_match(doc.get(field)) for field in key)
            for doc in docs
            if all(doc.get(field) not in (None, "") for field in key)
        }
        for docs in projected.values()
    ]
    if not per_resource:
        return 0
    return len(set.intersection(*per_resource))


def _project(docs: list[dict], field_map: dict[str, str]) -> list[dict]:
    """Rename aligned columns to their canonical names, leaving the rest alone."""
    if not field_map:
        return [dict(doc) for doc in docs]
    projected: list[dict] = []
    for doc in docs:
        out = {field_map.get(key, key): value for key, value in doc.items()}
        projected.append(out)
    return projected


def _declared_alignments(
    declared: dict[str, ResourceSample],
) -> set[tuple[str, str, str, str]]:
    """Column pairs implied by declared foreign keys — ground truth, not a guess."""
    pairs: set[tuple[str, str, str, str]] = set()
    for resource_name, sample in declared.items():
        for hint in sample.foreign_keys:
            if not isinstance(hint, ForeignKeyHint):
                continue
            if hint.references_field is None:
                continue
            pairs.add(
                (
                    resource_name,
                    hint.field,
                    hint.references_resource,
                    hint.references_field,
                )
            )
    return pairs


def _confidence(alignments: list[ColumnAlignment]) -> float:
    if not alignments:
        return 0.0
    return min(1.0, max(alignment.score for alignment in alignments))


def _rename_steps(field_maps: dict[str, dict[str, str]]) -> list[dict[str, Any]]:
    """Transform steps renaming each resource's columns to canonical names.

    Emitted as raw step dicts so they splice straight into a resource's
    ``pipeline`` and validate as ``TransformActorConfig``.
    """
    steps: list[dict[str, Any]] = []
    for resource_name in sorted(field_maps):
        renames = {
            source: canonical
            for source, canonical in sorted(field_maps[resource_name].items())
            if source != canonical
        }
        if renames:
            steps.append({"resource": resource_name, "transform": {"rename": renames}})
    return steps


def _proposal_field_names(proposal: CrossResourceIdentityProposal) -> list[str]:
    names = list(proposal.identity) + list(proposal.hash_identity_properties)
    if proposal.identity_funnel is not None:
        names += proposal.identity_funnel.field_names
    seen: dict[str, None] = {}
    for name in names:
        seen.setdefault(name, None)
    return list(seen)


__all__ = [
    "ColumnAlignment",
    "CrossResourceIdentityConfig",
    "CrossResourceIdentityInferencer",
    "CrossResourceIdentityProposal",
    "CrossResourceStrategy",
    "apply_proposal_to_vertex",
    "infer_from_source_sample",
    "name_similarity",
    "normalize_for_match",
    "value_jaccard",
]
