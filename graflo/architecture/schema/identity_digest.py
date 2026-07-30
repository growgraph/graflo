"""Deterministic digest identity for hash-mode and funnel-mode vertices.

Identity is materialized at **assemble time**, before edges are assembled and
before documents are deduplicated on their identity fields. That ordering is not
incidental: a hash-mode vertex resolves to ``identity: ["id"]``, so a document
whose ``id`` is still empty has no dedup basis (``merge_doc_basis`` folds the
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
    if all(_identity_value_is_empty(doc.get(field)) for field in source_fields):
        return None
    return compute_hash_identity(dict(doc), source_fields)


def ensure_digest_identity_on_doc(
    doc: dict[str, Any], vertex: Vertex, *, field: str
) -> None:
    """Fill an empty *field* on *doc* with its digest identity, in place.

    Idempotent: a document that already carries a value is left untouched, so
    the writer-side safety net cannot overwrite an assemble-time key.
    """
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
    for vname in vertex_config.hash_identity_vertices:
        by_loc = acc_vertex.get(vname)
        if not by_loc:
            continue
        vertex = vertex_config._get_vertex_by_name(vname)
        identity_fields = vertex_config.identity_fields(vname)
        preferred = identity_fields[0] if identity_fields else "id"
        for reps in by_loc.values():
            for rep in reps:
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
