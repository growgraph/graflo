"""ArangoDB document keys, computed on the client from identity values.

A vertex's ``_key`` is the value of a single-field identity when ArangoDB
accepts it as a key (a string of key characters, or an integer, at most 254
characters), and otherwise the SHA-256 hex digest of the typed identity tuple
(:func:`~graflo.util.typed_values.typed_key`), so ``1`` and ``"1"`` in a composite
identity are two keys. An edge's ``_key`` is the digest of its endpoints'
``_id``, its relation and the values of its identity fields. Because every key
is a function of identity values, edges reference their endpoints without
looking them up, and a resent document lands on the same key.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from collections.abc import Iterable, Sequence
from typing import Any

from graflo.util.typed_values import (
    IdentityKeyError,
    identity_member,
    key_member,
    typed_key,
)

logger = logging.getLogger(__name__)

KEY = "_key"

#: The characters ArangoDB allows in a user-supplied ``_key``, and its length.
_KEY_TEXT = re.compile(r"[A-Za-z0-9_\-:.@()+,=;$!*'%]{1,254}")


def digest(text: str) -> str:
    """The SHA-256 hex digest of *text*: 64 characters, all valid in a key."""
    return hashlib.sha256(text.encode()).hexdigest()


def vertex_key(values: Sequence[Any]) -> str | None:
    """The ``_key`` of a vertex whose identity holds *values*, in identity order.

    ``None`` when no value is set, or one cannot key a document (NaN, not a
    scalar); a composite identity lacking some values keys as
    :func:`~graflo.util.typed_values.typed_key` keys it.
    """
    try:
        if len(values) == 1:
            kind, value = key_member(values[0])
            if kind == "s" or (kind == "n" and isinstance(value, int)):
                text = str(value)
                if _KEY_TEXT.fullmatch(text):
                    return text
        return digest(typed_key(values))
    except IdentityKeyError:
        return None


def endpoint_key(doc: dict[str, Any], match_keys: Sequence[str]) -> str | None:
    """The ``_key`` of the vertex *doc* names by its *match_keys* values."""
    if KEY in match_keys:
        value = doc.get(KEY)
        return str(value) if value is not None and value != "" else None
    return vertex_key([doc.get(k) for k in match_keys])


def edge_key(
    from_id: str, to_id: str, relation: str | None, identity: Iterable[Any]
) -> str:
    """The ``_key`` of an edge: a digest of its endpoints, relation and identity values.

    An identity value is typed as a vertex identity value is; a null or
    non-scalar one keys by its encoded form, as the bulk fold keys it.
    """
    members = [identity_member(v) for v in identity]
    text = json.dumps(
        [from_id, to_id, relation, members], separators=(",", ":"), ensure_ascii=False
    )
    return digest(text)


def vertex_documents(
    docs: Iterable[dict[str, Any]], match_keys: Sequence[str], *, strict: bool
) -> list[dict[str, Any]]:
    """*docs* with ``_key`` set from their *match_keys* values (new dicts).

    A ``_key`` already in a document is replaced unless it is itself a match
    key, so a vertex's key never depends on where the document came from.

    Raises:
        ValueError: With *strict*, a document whose values cannot key it;
            otherwise such a document keeps no ``_key`` and ArangoDB assigns one.
    """
    if not match_keys or KEY in match_keys:
        return list(docs)
    out: list[dict[str, Any]] = []
    for doc in docs:
        key = vertex_key([doc.get(k) for k in match_keys])
        if key is None:
            if strict:
                raise ValueError(
                    f"cannot key a document on {list(match_keys)}: "
                    f"{[doc.get(k) for k in match_keys]!r}"
                )
            out.append({k: v for k, v in doc.items() if k != KEY})
            continue
        out.append({**doc, KEY: key})
    return out


def edge_documents(
    rows: Iterable[Sequence[Any]],
    source_class: str,
    target_class: str,
    match_keys_source: Sequence[str],
    match_keys_target: Sequence[str],
    relation_name: str | None,
    identity_fields: Sequence[str],
    *,
    strict: bool,
    fuse: bool,
) -> list[dict[str, Any]]:
    """Edge documents with ``_key``, ``_from``, ``_to`` and ``relation`` set.

    A row is ``(source_doc, target_doc[, props])``. With *fuse*, rows of one
    edge become one document, later properties winning: one query does not
    see what it wrote itself, so it would otherwise write the edge twice.

    Raises:
        ValueError: With *strict*, a row whose endpoint values cannot key it;
            otherwise the row is dropped.
    """
    if relation_name is not None:
        # The relation is matched by the one being written, never as a property.
        identity_fields = [f for f in identity_fields if f != "relation"]
    out: dict[str, dict[str, Any]] = {}
    listed: list[dict[str, Any]] = []
    dropped = 0
    for row in rows:
        source, target = row[0], row[1]
        props = row[2] if len(row) > 2 and isinstance(row[2], dict) else {}
        skey = endpoint_key(source, match_keys_source)
        tkey = endpoint_key(target, match_keys_target)
        if skey is None or tkey is None:
            if strict:
                raise ValueError(
                    f"cannot key an edge {source_class} -> {target_class}: "
                    f"source {[source.get(k) for k in match_keys_source]!r}, "
                    f"target {[target.get(k) for k in match_keys_target]!r}"
                )
            dropped += 1
            continue
        from_id = f"{source_class}/{skey}"
        to_id = f"{target_class}/{tkey}"
        key = edge_key(
            from_id, to_id, relation_name, [props.get(f) for f in identity_fields]
        )
        doc = {**props, KEY: key, "_from": from_id, "_to": to_id}
        if relation_name is not None:
            doc["relation"] = relation_name
        if not fuse:
            listed.append(doc)
            continue
        earlier = out.get(key)
        out[key] = {**earlier, **doc} if earlier is not None else doc
    if dropped:
        logger.warning(
            "Skipped %s edge row(s) %s -> %s with no complete endpoint identity",
            dropped,
            source_class,
            target_class,
        )
    return listed if not fuse else list(out.values())
