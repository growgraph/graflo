"""Deterministic digest identity for hash-mode and funnel-mode vertices.

Identity is materialized at **assemble time**, before edges are assembled and
before documents are deduplicated on their identity fields. That ordering is not
incidental: a hash-mode vertex resolves to ``identity: ["id"]``, so a document
whose ``id`` is still empty has no dedup basis (``fuse_doc_basis`` folds the
whole batch into one document) and no endpoint key for its edges.

This module sits in ``architecture.schema`` (L2) rather than ``db`` (L5) because
``architecture.pipeline`` (L4) calls into it — see ``test_layering``. It mirrors
:mod:`~graflo.architecture.schema.identity_uuid`, which plays the same role for
``assigned`` mode.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from graflo.architecture.graph_types.transform import VertexRep
from graflo.architecture.schema.derive import complete_derived, derive_fields
from graflo.architecture.schema.identity_funnel import (
    BRANCH_PAYLOAD_KEY,
    IdentityFunnel,
)
from graflo.architecture.schema.vertex import Vertex, VertexConfig


def _identity_value_is_empty(value: Any) -> bool:
    return value is None or value == ""


def _digest(payload: dict[str, Any]) -> str:
    """SHA256 hex digest over a canonical JSON rendering of *payload*."""
    source = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(source.encode()).hexdigest()


def compute_hash_identity(doc: dict[str, Any], source_fields: list[str]) -> str:
    """Compute a deterministic SHA256 hex digest from source field values.

    The payload is ``{field: value}`` with no branch marker, which is what makes
    a legacy ``hash_identity_properties`` digest byte-identical to the values
    produced before funnels existed. Do not add keys to this payload.
    """
    payload = {field: doc.get(field) for field in source_fields}
    return _digest(payload)


def compute_derived_hash_identity(
    doc: Mapping[str, Any], source_fields: list[str], normalizers: dict[str, str]
) -> str:
    """Digest of *source_fields* when some are derived, naming their normalizers.

    The payload is ``{field: value, ..., "normalizers": {field: name}}``, so a new
    normalizer version gives new ids. Kept apart from
    :func:`compute_hash_identity`, whose payload is frozen.
    """
    payload: dict[str, Any] = {field: doc.get(field) for field in source_fields}
    payload["normalizers"] = dict(sorted(normalizers.items()))
    return _digest(payload)


def _derived_normalizers(vertex: Vertex, source_fields: list[str]) -> dict[str, str]:
    """The normalizer behind each derived field among *source_fields*."""
    by_name = {f.name: f for f in vertex.properties}
    out: dict[str, str] = {}
    for name in source_fields:
        field = by_name.get(name)
        if field is None or field.derived is None:
            continue
        match = by_name[field.derived.source].match
        if match is not None and match.normalizer is not None:
            out[name] = match.normalizer
    return out


def compute_funnel_identity(
    doc: Mapping[str, Any], funnel: IdentityFunnel
) -> str | None:
    """Digest the first complete branch of *funnel*, or ``None`` if none fires.

    A branch fires when every field in its ``when_all_present`` (defaulting to
    its ``fields``) is present and non-empty. ``None`` means the document has no
    identity under this policy — callers must leave the identity field empty
    rather than inventing a key, so the caster can drop the document.
    """
    for branch in funnel.branches:
        if any(
            _identity_value_is_empty(doc.get(field)) for field in branch.required_fields
        ):
            continue
        payload: dict[str, Any] = {field: doc.get(field) for field in branch.fields}
        if funnel.include_branch_id:
            payload[BRANCH_PAYLOAD_KEY] = branch.id
        return _digest(payload)
    return None


def compute_vertex_identity(doc: Mapping[str, Any], vertex: Vertex) -> str | None:
    """Synthetic identity for *doc* under *vertex*'s digest policy.

    Returns ``None`` when the vertex has no digest policy, when no funnel branch
    fires, or when every flat source field is empty — the last case would
    otherwise digest ``{field: None}`` and fold every empty document onto one
    shared key.
    """
    if vertex.identity_funnel is not None:
        return compute_funnel_identity(doc, vertex.identity_funnel)
    source_fields = vertex.hash_identity_properties
    if not source_fields:
        return None
    normalizers = _derived_normalizers(vertex, source_fields)
    if normalizers:
        doc = complete_derived(vertex, doc)
    if all(_identity_value_is_empty(doc.get(field)) for field in source_fields):
        return None
    if normalizers:
        return compute_derived_hash_identity(doc, source_fields, normalizers)
    return compute_hash_identity(dict(doc), source_fields)


def ensure_digest_identity_on_doc(
    doc: dict[str, Any], vertex: Vertex, *, field: str
) -> None:
    """Fill an empty *field* on *doc* with its digest identity, in place.

    Idempotent: a document that already carries a value is left untouched, so
    the writer-side safety net cannot overwrite an assemble-time key. Derived
    fields are set first (:func:`~graflo.architecture.schema.derive.derive_fields`),
    so identity is known before edges are built.
    """
    derive_fields(vertex, doc)
    if not _identity_value_is_empty(doc.get(field)):
        return
    generated = compute_vertex_identity(doc, vertex)
    if generated is not None:
        doc[field] = generated


def ensure_digest_identities_in_acc_vertex(
    acc_vertex: Mapping[str, Any],
    vertex_config: VertexConfig,
) -> None:
    """Materialize digest identities on ``acc_vertex`` docs before edge assembly.

    ``acc_vertex`` maps vertex name -> location -> list of ``VertexRep`` (or
    objects exposing a ``.vertex`` dict). Shaped like
    :func:`~graflo.architecture.schema.identity_uuid.ensure_assigned_uuids_in_acc_vertex`.
    """
    for vname, by_loc in acc_vertex.items():
        if not by_loc or not vertex_config.is_hash_identity(vname):
            continue
        vertex = vertex_config._get_vertex_by_name(vname)
        identity_fields = vertex_config.identity_fields(vname)
        preferred = identity_fields[0] if identity_fields else "id"
        for reps in by_loc.values():
            for rep in reps:
                if isinstance(rep, VertexRep) and rep.find is not None:
                    # Found by a secondary identity: it must not get a key of
                    # its own, which would make it a new vertex.
                    continue
                doc = rep.vertex if hasattr(rep, "vertex") else rep
                if isinstance(doc, dict):
                    ensure_digest_identity_on_doc(doc, vertex, field=preferred)


def ensure_digest_identities_on_docs(
    data: list[dict[str, Any]],
    vertex: Vertex,
    *,
    preferred_field: str,
) -> None:
    """Idempotent digest-identity ensure for a flat doc list (writer safety net)."""
    for doc in data:
        ensure_digest_identity_on_doc(doc, vertex, field=preferred_field)
