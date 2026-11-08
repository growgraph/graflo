"""Pieces of the ArangoDB bulk writer that need no server.

Documents go to ``POST /_api/import`` as JSON Lines (``type=documents``) with
``onDuplicate=ignore``. Every document carries the ``_key`` its identity
determines, so a replayed request creates nothing twice.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from typing import Any

from graflo.architecture.schema import Schema
from graflo.db.bulk import StoredEdgeId
from graflo.db.util import json_serializer
from graflo.onto import DBType

#: Bytes per import request: large enough to amortize a round trip, small
#: enough that the server never parses a large body at once.
IMPORT_REQUEST_BYTES = 4 << 20


class ArangoImportError(RuntimeError):
    """An import request that did not create every document it sent."""


def import_params(collection: str) -> dict[str, int | str]:
    """Query parameters of an import into *collection*."""
    return {
        "collection": collection,
        "type": "documents",
        "onDuplicate": "ignore",
        "details": "true",
    }


def import_bodies(
    docs: Iterable[dict[str, Any]], limit: int = IMPORT_REQUEST_BYTES
) -> Iterator[tuple[str, int]]:
    """JSON Lines request bodies of at most *limit* bytes, with their line counts.

    Values are encoded as the upsert path encodes them; text is ASCII-escaped,
    as the HTTP client sends a text body. A line longer than *limit* goes alone.
    """
    lines: list[str] = []
    size = 0
    for doc in docs:
        line = json.dumps(doc, default=json_serializer, separators=(",", ":"))
        if lines and size + len(line) + 1 > limit:
            yield "\n".join(lines) + "\n", len(lines)
            lines, size = [], 0
        lines.append(line)
        size += len(line) + 1
    if lines:
        yield "\n".join(lines) + "\n", len(lines)


def check_import(
    result: dict[str, Any], *, sent: int, replay: bool, collection: str
) -> None:
    """Refuse an import that did not store all *sent* documents.

    A document ignored as a duplicate is stored already: expected on a replay,
    and on a first write a sign that two documents share a ``_key``.

    Raises:
        ArangoImportError: The server reported errors, ignored a document of
            a first write, or accounted for fewer documents than were sent.
    """
    errors = int(result.get("errors") or 0)
    created = int(result.get("created") or 0)
    ignored = int(result.get("ignored") or 0)
    details = list(result.get("details") or [])[:5]
    if result.get("error") or errors:
        raise ArangoImportError(
            f"import into {collection}: {errors} error(s) in {sent} document(s): "
            f"{details}"
        )
    if ignored and not replay:
        raise ArangoImportError(
            f"import into {collection}: {ignored} of {sent} document(s) share a "
            f"_key with another: {details}"
        )
    if created + ignored != sent:
        raise ArangoImportError(
            f"import into {collection}: {sent} document(s) sent, {created} "
            f"created and {ignored} ignored"
        )


def edge_collections(schema: Schema) -> dict[str, list[StoredEdgeId]]:
    """Each edge collection of *schema*, with the stored edges it holds."""
    db = schema.resolve_db_aware(DBType.ARANGO)
    out: dict[str, list[StoredEdgeId]] = {}
    for edge in schema.core_schema.edge_config.values():
        runtime = db.edge_config.runtime(edge)
        name = runtime.storage_name()
        if name is None:
            continue
        stored = (runtime.source_storage, edge.relation, runtime.target_storage)
        out.setdefault(name, []).append(stored)
    return out


def edge_count_query(collection: str) -> str:
    """AQL counting *collection*'s edges per source, relation and target collection."""
    return (
        f"FOR e IN {collection} "
        "COLLECT s = PARSE_IDENTIFIER(e._from).collection, r = e.relation, "
        "t = PARSE_IDENTIFIER(e._to).collection WITH COUNT INTO n "
        "RETURN [s, r, t, n]"
    )
