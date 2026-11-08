"""Group folded rows by owner, and give every owner vertex its sync hash.

Runs after the fold, on the staging directory. For each owned vertex type it
writes ``owners/<slot>.jsonl.zst``: one row per folded owner vertex, in key
order, with the owner's document and every edge it owns. The owner's sync hash
is set on that document and on the folded vertex row, so a bulk load writes it
and a sync run compares it.
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from graflo.hq.bulk import _deps
from graflo.hq.bulk.encode import dumps, encode, read_rows, write_rows
from graflo.hq.bulk.fold import FOLDED, FoldCounts, RunCounts, folded_path
from graflo.hq.sync.hash import sync_hash

OWNERS = "owners"
RUNS = "runs.json"


class OwnerRun(BaseModel):
    """One owned vertex type's grouped rows."""

    owner: str
    slot: str
    """The vertex run's slot; also names the owners file."""
    hash_field: str
    edges: list[RunCounts]
    """The edge runs the owner type owns."""
    owners: int


def owners_path(staging_dir: Path, slot: str) -> Path:
    return Path(staging_dir) / OWNERS / f"{slot}.jsonl.zst"


def group_owners(
    staging_dir: Path, counts: FoldCounts, hash_fields: Mapping[str, str]
) -> list[OwnerRun]:
    """Group every owned type of *hash_fields* and set its owners' hashes.

    Args:
        staging_dir: A folded staging directory whose edge rows carry ``okey``.
        counts: Its fold counts.
        hash_fields: Owned vertex type -> its ``set_by: sync`` field.
    """
    root = Path(staging_dir)
    out: list[OwnerRun] = []
    for owner, hash_field in sorted(hash_fields.items()):
        vertex = counts.vertex(owner)
        if vertex is None:
            continue
        edges = [
            r for r in counts.runs if r.type.kind == "edge" and r.type.owner == owner
        ]
        raw = owners_path(root, vertex.slot).with_suffix(".raw")
        _group(root, vertex, edges, raw)
        path = owners_path(root, vertex.slot)
        n = write_rows(path, _hashed(read_rows(raw), hash_field))
        raw.unlink()
        _rehash_vertices(folded_path(root, vertex.slot), path, hash_field)
        out.append(
            OwnerRun(
                owner=owner,
                slot=vertex.slot,
                hash_field=hash_field,
                edges=edges,
                owners=n,
            )
        )
    (root / OWNERS).mkdir(parents=True, exist_ok=True)
    (root / OWNERS / RUNS).write_text(
        json.dumps([run.model_dump(mode="json") for run in out], indent=2)
    )
    return out


def grouped_owners(staging_dir: Path) -> list[OwnerRun]:
    """The owner runs :func:`group_owners` wrote, for a resumed run."""
    path = Path(staging_dir) / OWNERS / RUNS
    if not path.exists():
        return []
    return [OwnerRun.model_validate(run) for run in json.loads(path.read_text())]


def _group(root: Path, vertex: RunCounts, edges: list[RunCounts], raw: Path) -> None:
    duckdb = _deps.duckdb()
    con = duckdb.connect()
    try:
        con.execute(f"SET temp_directory = {_lit(str(root / 'duckdb.tmp'))}")
        parts = []
        for run in edges:
            files = _edge_files(root, run)
            if not files:
                continue
            edge = _lit(json.dumps(list(run.type.edge_id or ())))
            parts.append(
                f"""SELECT okey, json_object('edge', {edge}::JSON, 'source', source,
                           'target', target, 'doc', doc) AS entry
                    FROM read_json({_files(files)}, format = 'newline_delimited',
                        compression = 'zstd', columns = {{'okey': 'VARCHAR',
                        'source': 'JSON', 'target': 'JSON', 'doc': 'JSON'}})"""
            )
        entries = (
            " UNION ALL ".join(parts)
            if parts
            else "SELECT NULL::VARCHAR AS okey, NULL::JSON AS entry WHERE false"
        )
        con.execute(f"CREATE OR REPLACE TABLE ent AS {entries}")
        vertices = folded_path(root, vertex.slot)
        raw.parent.mkdir(parents=True, exist_ok=True)
        con.execute(
            f"""COPY (
                SELECT v.key, any_value(v.doc) AS doc,
                       coalesce(list(e.entry) FILTER (WHERE e.entry IS NOT NULL),
                                []::JSON[]) AS edges
                FROM read_json({_lit(str(vertices))}, format = 'newline_delimited',
                    compression = 'zstd', columns = {{'key': 'VARCHAR', 'doc': 'JSON'}}) v
                LEFT JOIN ent e ON e.okey = v.key
                GROUP BY v.key ORDER BY v.key
            ) TO {_lit(str(raw))} (FORMAT JSON, COMPRESSION ZSTD)"""
        )
    finally:
        con.close()


def _hashed(rows: Iterator[dict[str, Any]], hash_field: str) -> Iterator[bytes]:
    for row in rows:
        doc = row["doc"]
        doc[hash_field] = sync_hash(doc, row["edges"], skip=(hash_field,))
        yield dumps(encode(row))


def _rehash_vertices(vertices: Path, owners: Path, hash_field: str) -> None:
    """Set each folded vertex row's hash from the owners file; both are in key order."""

    def rows() -> Iterator[bytes]:
        for vertex, owner in zip(read_rows(vertices), read_rows(owners), strict=True):
            if vertex["key"] != owner["key"]:
                raise RuntimeError(
                    f"{vertices.name}: owner rows out of step at {vertex['key']}"
                )
            vertex["doc"][hash_field] = owner["doc"][hash_field]
            yield dumps(encode(vertex))

    tmp = vertices.with_name(vertices.name + ".hashed")
    write_rows(tmp, rows())
    tmp.replace(vertices)


def _edge_files(root: Path, run: RunCounts) -> list[Path]:
    """The folded files of an edge run: one file, or the files of its cells."""
    single = folded_path(root, run.slot)
    if single.exists():
        return [single]
    return sorted((root / FOLDED / run.slot).glob("**/*.json*"))


def _files(paths: list[Path]) -> str:
    return "[" + ", ".join(_lit(str(p)) for p in paths) + "]"


def _lit(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"
