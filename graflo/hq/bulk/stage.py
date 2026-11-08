"""Stage cast batches: one run per vertex type and per concrete edge, on disk.

Each batch goes through the same connection-free steps as an upsert (stamp,
derived fields, minted identities, documents with no identity dropped) and
lands as one file per type it holds. Every row carries:

- ``seq``: ``[resource, source, batch, part, position]``, the order that makes
  a row "later" than another of the same type (``part`` numbers the containers
  one batch casts to, one per stamp);
- ``key``: the typed identity (:func:`~graflo.hq.bulk.encode.typed_key`); for
  an edge, its endpoints' keys and the values of its identity fields;
- for an edge a ``sync`` block owns, ``okey``: the key of its owner vertex;
- the documents, logical names, values encoded losslessly.

A row whose identity is incomplete cannot be folded or written and goes to the
rejects with its reason.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any
from uuid import uuid4

from pydantic import BaseModel

from graflo.architecture.contract.ingestion.sync import OwnedEdge
from graflo.architecture.graph_types import EdgeId, GraphContainer
from graflo.architecture.graph_types.merge import union_members
from graflo.architecture.schema import Schema
from graflo.architecture.schema.edge import Edge
from graflo.hq.bulk.encode import (
    IdentityKeyError,
    dumps,
    encode,
    identity_member,
    typed_key,
    write_rows,
)
from graflo.hq.db_writer import apply_stamp, check_stamp, refuse_reserved_attributes
from graflo.hq.write_prepare import (
    WriteContext,
    edge_union_fields,
    prepare_vertices,
    union_fields,
)

logger = logging.getLogger(__name__)

STAGE = "stage"
REJECTS = "rejects"
TYPE_FILE = "type.json"

_UNSAFE = re.compile(r"[^A-Za-z0-9_]+")


class RunType(BaseModel):
    """What one run holds: a vertex type or one concrete edge."""

    kind: str
    name: str | None = None
    edge_id: tuple[str, str, str | None] | None = None
    union: list[str] = []
    owner: str | None = None
    """For an owned edge: the vertex type that owns it."""
    owner_side: str | None = None
    """For an owned edge: where its owner key is read (``source``, ``target``, ``field``)."""

    @property
    def label(self) -> str:
        return str(self.name) if self.kind == "vertex" else str(self.edge_id)


def slot_for(run: RunType) -> str:
    """The directory name of *run*'s files: readable, and unique per type."""
    ident = run.name if run.kind == "vertex" else json.dumps(run.edge_id)
    readable = (
        run.name
        if run.kind == "vertex"
        else "-".join(str(part) for part in run.edge_id or ())
    )
    digest = hashlib.sha1(str(ident).encode()).hexdigest()[:10]
    return f"{run.kind[0]}-{_UNSAFE.sub('_', str(readable))[:48]}-{digest}"


def batch_name(seq: Sequence[int]) -> str:
    """The file name of one container's rows: sorts in ``seq`` order."""
    resource, source, batch, part = seq
    return f"r{resource:05d}-s{source:07d}-b{batch:010d}-p{part:05d}.jsonl.zst"


class Stager:
    """Writes cast batches into a staging directory, one file per type per batch.

    Batches may be staged concurrently and in any order; ``seq`` orders them.
    """

    def __init__(
        self,
        schema: Schema,
        ctx: WriteContext,
        staging_dir: Path,
        owners: Mapping[EdgeId, OwnedEdge] | None = None,
    ) -> None:
        """*owners*: how each owned edge is owned, by declared edge id."""
        self.schema = schema
        self.ctx = ctx
        self.root = Path(staging_dir)
        self.owners = dict(owners or {})
        self._known: set[str] = set()

    def stage(
        self,
        gc: GraphContainer,
        seq: Sequence[int],
        *,
        stamp: Mapping[str, Any] | None = None,
    ) -> None:
        """Stage *gc*, cast as ``seq = (resource, source, batch, part)``.

        Every reserved field is set from *stamp*, as an upsert write sets it.

        *gc* is changed in place, as an upsert write changes it.

        Raises:
            ValueError: As the upsert write refuses a container (a reserved
                field the stamp does not set, or a copy of one on an edge).
        """
        if gc.is_empty():
            return
        check_stamp(gc, self.schema, stamp)
        refuse_reserved_attributes(gc, self.schema)
        apply_stamp(gc, self.schema, stamp)
        name = batch_name(seq)
        for vcol, docs in gc.vertices.items():
            if docs:
                self._stage_vertices(vcol, docs, seq, name)
        core_ec = self.schema.core_schema.edge_config
        for edge_id, rows in gc.edges.items():
            if not rows:
                continue
            edge = core_ec.declared(edge_id)
            if edge is None:
                logger.warning(
                    "Edge %s is not staged: the schema does not declare it (%s rows)",
                    edge_id,
                    len(rows),
                )
                continue
            self._stage_edges(edge, edge_id, rows, seq, name)

    # -- vertices ---------------------------------------------------------

    def _stage_vertices(
        self, vcol: str, docs: list[dict], seq: Sequence[int], name: str
    ) -> None:
        union = union_fields(self.ctx, vcol)
        identity = self.ctx.db_aware.vertex_config.identity_fields(vcol)
        run = RunType(kind="vertex", name=vcol, union=list(union))
        rows: list[bytes] = []
        rejects: list[bytes] = []
        for position, doc in enumerate(prepare_vertices(self.ctx, vcol, docs)):
            doc = _normalized(doc, union)
            row_seq = [*seq, position]
            try:
                key = typed_key(doc.get(field) for field in identity)
            except IdentityKeyError as exc:
                rejects.append(_reject(row_seq, f"identity {identity}: {exc}", doc))
                continue
            rows.append(dumps({"seq": row_seq, "key": key, "doc": encode(doc)}))
        self._write(run, name, rows, rejects)

    # -- edges ------------------------------------------------------------

    def _stage_edges(
        self,
        edge: Edge,
        edge_id: tuple,
        triples: list,
        seq: Sequence[int],
        name: str,
    ) -> None:
        vc = self.ctx.db_aware.vertex_config
        ec = self.ctx.db_aware.edge_config
        source_fields = vc.identity_fields(edge.source)
        target_fields = vc.identity_fields(edge.target)
        key_fields = list(ec.relationship_merge_property_names(edge))
        union = edge_union_fields(edge)
        source_type, target_type, relation = edge_id
        owned = self.owners.get(edge.edge_id)
        run = RunType(
            kind="edge",
            edge_id=(source_type, target_type, relation),
            union=union,
            owner=owned.owner if owned else None,
            owner_side=owned.side if owned else None,
        )
        rows: list[bytes] = []
        rejects: list[bytes] = []
        for position, triple in enumerate(triples):
            source, target = triple[0], triple[1]
            props = triple[2] if len(triple) > 2 and triple[2] else {}
            props = _normalized(dict(props), union)
            row_seq = [*seq, position]
            try:
                skey = typed_key(source.get(f) for f in source_fields)
                tkey = typed_key(target.get(f) for f in target_fields)
            except IdentityKeyError as exc:
                rejects.append(
                    _reject(
                        row_seq,
                        f"endpoint identity: {exc}",
                        {"source": source, "target": target, "doc": props},
                    )
                )
                continue
            okey: str | None = None
            if owned is not None:
                try:
                    okey = _owner_key(owned, skey, tkey, props)
                except IdentityKeyError as exc:
                    rejects.append(
                        _reject(
                            row_seq,
                            f"owner {owned.owner_field!r}: {exc}",
                            {"source": source, "target": target, "doc": props},
                        )
                    )
                    continue
            extra = [identity_member(props.get(f)) for f in key_fields]
            key = json.dumps(
                [json.loads(skey), json.loads(tkey), extra],
                separators=(",", ":"),
                ensure_ascii=False,
            )
            rows.append(
                dumps(
                    {
                        "seq": row_seq,
                        "key": key,
                        "skey": skey,
                        "tkey": tkey,
                        "okey": okey,
                        "source": encode(source),
                        "target": encode(target),
                        "doc": encode(props),
                    }
                )
            )
        self._write(run, name, rows, rejects)

    # -- files ------------------------------------------------------------

    def _write(
        self, run: RunType, name: str, rows: list[bytes], rejects: list[bytes]
    ) -> None:
        slot = slot_for(run)
        self._declare(slot, run)
        if rows:
            write_rows(self.root / STAGE / slot / name, rows)
        if rejects:
            write_rows(self.root / REJECTS / STAGE / slot / name, rejects)

    def _declare(self, slot: str, run: RunType) -> None:
        if slot in self._known:
            return
        path = self.root / STAGE / slot / TYPE_FILE
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            tmp = path.with_name(f"{TYPE_FILE}.{uuid4().hex}.tmp")
            tmp.write_text(run.model_dump_json())
            tmp.replace(path)
        self._known.add(slot)


def staged_runs(staging_dir: Path) -> Iterator[tuple[str, RunType]]:
    """``(slot, run type)`` of every run staged in *staging_dir*, by slot."""
    root = Path(staging_dir) / STAGE
    if not root.is_dir():
        return
    for slot_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        yield (
            slot_dir.name,
            RunType.model_validate_json((slot_dir / TYPE_FILE).read_text()),
        )


def _owner_key(owned: OwnedEdge, skey: str, tkey: str, props: Mapping[str, Any]) -> str:
    """The key of the vertex that owns an edge, as its own key is written."""
    if owned.side == "source":
        return skey
    if owned.side == "target":
        return tkey
    assert owned.owner_field is not None
    return typed_key([props.get(owned.owner_field)])


def _normalized(doc: dict[str, Any], union: Sequence[str]) -> dict[str, Any]:
    """*doc* with its union fields as an upsert onto nothing leaves them."""
    for field in union:
        if field in doc:
            doc[field] = union_members(None, doc[field])
    return doc


def _reject(seq: list[int], reason: str, row: dict[str, Any]) -> bytes:
    return dumps({"seq": seq, "reason": reason, "row": encode(row)})
