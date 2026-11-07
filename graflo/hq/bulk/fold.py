"""Fold staged runs by identity in DuckDB: one row per vertex and per edge.

The rules are the upsert's, applied once to the whole load (the file backend's
read-side fold applies the same ones):

- per field, the value of the latest row (largest ``seq``) that carries it; a
  field a later row lacks keeps its earlier value;
- ``merge: union`` fields take the members of every row in ``seq`` order, each
  once, nulls dropped;
- an edge's endpoint documents fold the same way as its properties.

Rows whose key occurs once pass through; only repeated keys are exploded into
one row per field and reduced. Edges whose endpoint is not a folded vertex are
rejected with their reason. DuckDB spills to the staging directory under the
memory limit it is given.

Output, in the staging directory: ``folded/<slot>.jsonl.zst`` sorted by key,
``rejects/``, and ``counts.json``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from graflo.hq.bulk import _deps
from graflo.hq.bulk.stage import REJECTS, STAGE, RunType, staged_runs

FOLDED = "folded"
COUNTS = "counts.json"


class RunCounts(BaseModel):
    """One run's rows: staged, after the fold, rejected, and its dense-id range."""

    slot: str
    type: RunType
    staged: int
    folded: int
    rejected: int
    dense: tuple[int, int] | None = None


class FoldCounts(BaseModel):
    """Every run of a fold, vertices first."""

    runs: list[RunCounts]

    def vertex(self, name: str) -> RunCounts | None:
        return next(
            (r for r in self.runs if r.type.kind == "vertex" and r.type.name == name),
            None,
        )

    @classmethod
    def load(cls, staging_dir: Path) -> FoldCounts:
        return cls.model_validate_json((Path(staging_dir) / COUNTS).read_text())


def folded_path(staging_dir: Path, slot: str) -> Path:
    return Path(staging_dir) / FOLDED / f"{slot}.jsonl.zst"


def fold(
    staging_dir: Path,
    *,
    dense_ids: bool = False,
    memory_limit: str | None = None,
    threads: int | None = None,
) -> FoldCounts:
    """Fold every staged run of *staging_dir*; write the folded runs and counts.

    Args:
        staging_dir: A directory a :class:`~graflo.hq.bulk.stage.Stager` wrote.
        dense_ids: Number every folded vertex ``0..n-1`` across types, in type
            then key order, and carry the numbers onto edge endpoints.
        memory_limit: DuckDB's memory limit (``"4GB"``); spills beyond it.
        threads: DuckDB worker threads; its default when omitted.

    Raises:
        RuntimeError: The ``bulk`` extra is not installed.
    """
    duckdb = _deps.duckdb()
    root = Path(staging_dir)
    (root / FOLDED).mkdir(parents=True, exist_ok=True)
    (root / REJECTS).mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.execute(f"SET temp_directory = {_lit(str(root / 'duckdb.tmp'))}")
        con.execute("SET preserve_insertion_order = false")
        if memory_limit:
            con.execute(f"SET memory_limit = {_lit(memory_limit)}")
        if threads:
            con.execute(f"SET threads = {int(threads)}")
        runs = list(staged_runs(root))
        vertex_runs = [(s, t) for s, t in runs if t.kind == "vertex"]
        edge_runs = [(s, t) for s, t in runs if t.kind == "edge"]
        counts: list[RunCounts] = []
        key_tables: dict[str, str] = {}
        offset = 0
        for index, (slot, run) in enumerate(vertex_runs):
            table = f"vk{index}"
            result = _fold_vertices(
                con, root, slot, run, table, offset if dense_ids else None
            )
            if dense_ids:
                offset += result.folded
            key_tables[str(run.name)] = table
            counts.append(result)
        for slot, run in edge_runs:
            counts.append(_fold_edges(con, root, slot, run, key_tables, dense_ids))
    finally:
        con.close()
    out = FoldCounts(runs=counts)
    (root / COUNTS).write_text(out.model_dump_json(indent=2))
    return out


# -- vertices -----------------------------------------------------------------


def _fold_vertices(
    con: Any,
    root: Path,
    slot: str,
    run: RunType,
    table: str,
    dense_offset: int | None,
) -> RunCounts:
    staged = _load(
        con, root, slot, {"seq": "BIGINT[]", "key": "VARCHAR", "doc": "JSON"}
    )
    _fold_table(con, ["doc"], run.union, extra=[])
    dense = (
        f", row_number() OVER (ORDER BY key) - 1 + {dense_offset} AS dense"
        if dense_offset is not None
        else ""
    )
    con.execute(
        f"CREATE OR REPLACE TABLE {table} AS SELECT key, doc{dense} FROM folded"
    )
    folded = _count(con, table)
    _copy(con, f"SELECT * FROM {table} ORDER BY key", folded_path(root, slot))
    if dense_offset is None:
        con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT key FROM {table}")
    else:
        con.execute(
            f"CREATE OR REPLACE TABLE {table} AS SELECT key, dense FROM {table}"
        )
    return RunCounts(
        slot=slot,
        type=run,
        staged=staged,
        folded=folded,
        rejected=_staged_rejects(con, root, slot),
        dense=(dense_offset, dense_offset + folded)
        if dense_offset is not None
        else None,
    )


# -- edges --------------------------------------------------------------------


def _fold_edges(
    con: Any,
    root: Path,
    slot: str,
    run: RunType,
    key_tables: dict[str, str],
    dense_ids: bool,
) -> RunCounts:
    columns = {
        "seq": "BIGINT[]",
        "key": "VARCHAR",
        "skey": "VARCHAR",
        "tkey": "VARCHAR",
        "source": "JSON",
        "target": "JSON",
        "doc": "JSON",
    }
    staged = _load(con, root, slot, columns)
    _fold_table(con, ["source", "target", "doc"], run.union, extra=["skey", "tkey"])
    assert run.edge_id is not None
    source_type, target_type, _ = run.edge_id
    src = key_tables.get(source_type)
    tgt = key_tables.get(target_type)
    src_join = f"LEFT JOIN {src} s ON s.key = f.skey" if src else ""
    tgt_join = f"LEFT JOIN {tgt} t ON t.key = f.tkey" if tgt else ""
    src_found = "s.key IS NOT NULL" if src else "false"
    tgt_found = "t.key IS NOT NULL" if tgt else "false"
    dense = (
        ", s.dense AS sdense, t.dense AS tdense" if dense_ids and src and tgt else ""
    )
    con.execute(
        f"""CREATE OR REPLACE TABLE checked AS
        SELECT f.*, {src_found} AS has_source, {tgt_found} AS has_target{dense}
        FROM folded f {src_join} {tgt_join}"""
    )
    _copy(
        con,
        """SELECT
            CASE WHEN NOT has_source AND NOT has_target THEN 'missing source and target'
                 WHEN NOT has_source THEN 'missing source'
                 ELSE 'missing target' END AS reason,
            key, source, target, doc
        FROM checked WHERE NOT (has_source AND has_target) ORDER BY key""",
        root / REJECTS / f"{slot}.endpoints.jsonl.zst",
    )
    kept = "key, source, target, doc" + (", sdense, tdense" if dense else "")
    con.execute(
        f"CREATE OR REPLACE TABLE kept AS SELECT {kept} FROM checked "
        "WHERE has_source AND has_target"
    )
    folded = _count(con, "kept")
    _copy(con, "SELECT * FROM kept ORDER BY key", folded_path(root, slot))
    missing = _count(con, "checked") - folded
    return RunCounts(
        slot=slot,
        type=run,
        staged=staged,
        folded=folded,
        rejected=_staged_rejects(con, root, slot) + missing,
    )


# -- the fold -----------------------------------------------------------------


def _fold_table(
    con: Any, parts: list[str], union: list[str], *, extra: list[str]
) -> None:
    """Fold table ``r`` into table ``folded`` (``key``, *extra*, *parts*).

    *parts* are JSON objects folded field by field; *union* names fields of the
    ``doc`` part. *extra* columns are functions of the key.
    """
    union_set = "[" + ", ".join(_lit(f) for f in union) + "]::VARCHAR[]"
    is_union = f"(part = 'doc' AND list_contains({union_set}, field))"
    con.execute(
        "CREATE OR REPLACE TABLE g AS SELECT key, count(*) AS n FROM r GROUP BY key"
    )
    con.execute(
        "CREATE OR REPLACE TABLE m AS SELECT r.* FROM r "
        "SEMI JOIN (SELECT key FROM g WHERE n > 1) d ON r.key = d.key"
    )
    exploded = " UNION ALL ".join(
        f"""SELECT key, seq, {_lit(p)} AS part, e.key AS field,
               coalesce(e.value, 'null'::JSON) AS value
            FROM (SELECT key, seq, unnest(map_entries({p}::MAP(VARCHAR, JSON))) AS e
                  FROM m)"""
        for p in parts
    )
    con.execute(f"CREATE OR REPLACE TABLE long AS {exploded}")
    con.execute(
        f"""CREATE OR REPLACE TABLE fields AS
        SELECT key, part, field, arg_max(value, seq) AS value
        FROM long WHERE NOT {is_union} GROUP BY key, part, field"""
    )
    if union:
        con.execute(
            f"""INSERT INTO fields
            WITH present AS (
                SELECT DISTINCT key, field FROM long WHERE {is_union}
            ), exploded AS (
                SELECT key, field, seq, generate_subscripts(arr, 1) AS idx,
                       unnest(arr) AS member
                FROM (SELECT key, field, seq, value::JSON[] AS arr
                      FROM long WHERE {is_union})
            ), firsts AS (
                SELECT key, field, member, min(list_append(seq, idx)) AS pos
                FROM exploded WHERE member IS NOT NULL
                GROUP BY key, field, member
            ), members AS (
                SELECT key, field, to_json(list(member ORDER BY pos)) AS value
                FROM firsts GROUP BY key, field
            )
            SELECT p.key, 'doc', p.field, coalesce(members.value, '[]'::JSON)
            FROM present p LEFT JOIN members USING (key, field)"""
        )
    keys = "SELECT key" + "".join(f", any_value({c}) AS {c}" for c in extra)
    ctes = [f"keys AS ({keys} FROM m GROUP BY key)"] + [
        f"""p_{p} AS (SELECT key, json_group_object(field, value) AS {p}
                FROM fields WHERE part = {_lit(p)} GROUP BY key)"""
        for p in parts
    ]
    columns = ", ".join(
        ["keys.key", *(f"keys.{c}" for c in extra)]
        + [f"coalesce(p_{p}.{p}, '{{}}'::JSON) AS {p}" for p in parts]
    )
    joins = " ".join(f"LEFT JOIN p_{p} ON p_{p}.key = keys.key" for p in parts)
    pick = ", ".join(["key", *extra, *parts])
    con.execute(
        f"""CREATE OR REPLACE TABLE folded AS
        WITH {", ".join(ctes)}
        SELECT {pick} FROM r SEMI JOIN (SELECT key FROM g WHERE n = 1) d ON r.key = d.key
        UNION ALL BY NAME
        SELECT {columns} FROM keys {joins}"""
    )


# -- helpers ------------------------------------------------------------------


def _load(con: Any, root: Path, slot: str, columns: dict[str, str]) -> int:
    pattern = root / STAGE / slot / "*.jsonl.zst"
    spec = "{" + ", ".join(f"{_lit(k)}: {_lit(v)}" for k, v in columns.items()) + "}"
    names = ", ".join(columns)
    if not any((root / STAGE / slot).glob("*.jsonl.zst")):
        cols = ", ".join(f"NULL::{t} AS {k}" for k, t in columns.items())
        con.execute(f"CREATE OR REPLACE TABLE r AS SELECT {cols} WHERE false")
        return 0
    con.execute(
        f"""CREATE OR REPLACE TABLE r AS SELECT {names} FROM read_json(
            {_lit(str(pattern))}, format = 'newline_delimited',
            compression = 'zstd', columns = {spec})"""
    )
    return _count(con, "r")


def _staged_rejects(con: Any, root: Path, slot: str) -> int:
    directory = root / REJECTS / STAGE / slot
    if not any(directory.glob("*.jsonl.zst")):
        return 0
    pattern = _lit(str(directory / "*.jsonl.zst"))
    return int(
        con.execute(
            f"SELECT count(*) FROM read_json({pattern}, format = 'newline_delimited', "
            "compression = 'zstd', columns = {'reason': 'VARCHAR'})"
        ).fetchone()[0]
    )


def _copy(con: Any, query: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    con.execute(f"COPY ({query}) TO {_lit(str(tmp))} (FORMAT JSON, COMPRESSION ZSTD)")
    tmp.replace(path)


def _count(con: Any, table: str) -> int:
    return int(con.execute(f"SELECT count(*) FROM {table}").fetchone()[0])


def _lit(text: str) -> str:
    """A SQL string literal."""
    return "'" + text.replace("'", "''") + "'"


def counts_json(staging_dir: Path) -> dict[str, Any]:
    """``counts.json`` as data."""
    return json.loads((Path(staging_dir) / COUNTS).read_text())
