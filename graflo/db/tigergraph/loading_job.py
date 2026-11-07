"""A typed loading job for a bulk load, and the delimited text it is fed.

Pure: nothing here opens a connection. :func:`loading_job` lays out one file
variable per vertex type and one per concrete edge ``(source, relation,
target)``, columns in the order the schema DDL declares the type's attributes;
:func:`create_job_gsql` renders it. :func:`vertex_line` and :func:`edge_line`
encode one folded row, :func:`split_body` packs lines into request bodies, and
:func:`check_load_response` refuses a load that skipped any line.

Rows are separated by :data:`EOL` and fields by :data:`SEP`, list items by
:data:`LIST_SEP`: control characters, so text holding commas, quotes or line
breaks loads unchanged without quoting, which loading jobs do not escape.
"""

from __future__ import annotations

import datetime as dt
import math
from collections.abc import Iterator, Mapping, Sequence
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from graflo.architecture.schema import Schema
from graflo.db.bulk import StoredEdgeId
from graflo.db.field_type_support import tigergraph_type_for_field
from graflo.db.tigergraph.ddl_utils import (
    edge_identity_discriminator_fields,
    tigergraph_ddl_edge_projection,
)
from graflo.db.tigergraph.document_utils import extract_id
from graflo.db.tigergraph.schema_ddl import SchemaDdlBuilder
from graflo.onto import DBType

#: Field separator (ASCII unit separator).
SEP = "\x1f"
#: Row terminator (ASCII record separator).
EOL = "\x1e"
#: Separator of the items of a ``LIST`` or ``SET`` attribute (ASCII group separator).
LIST_SEP = "\x1d"
#: Request body cap: the default ``Nginx.ClientMaxBodySize`` in front of REST++.
MAX_BODY_BYTES = 200 * 1024 * 1024

ColumnKind = Literal["scalar", "list", "datetime"]


class LoadRejectedError(RuntimeError):
    """The target answered a load by skipping or rejecting lines."""


class LoadColumn(BaseModel):
    """One attribute of a type, as a column of its file."""

    model_config = ConfigDict(frozen=True)

    name: str
    gsql_type: str

    @property
    def kind(self) -> ColumnKind:
        token = self.gsql_type.upper()
        if token.startswith(("LIST<", "SET<")):
            return "list"
        if token == "DATETIME":
            return "datetime"
        return "scalar"


class VertexLoad(BaseModel):
    """The file of one vertex type: its attributes, primary id first."""

    model_config = ConfigDict(frozen=True)

    file_var: str
    vertex_type: str
    columns: tuple[LoadColumn, ...]


class EdgeLoad(BaseModel):
    """The file of one concrete edge: source id, target id, then the type's attributes."""

    model_config = ConfigDict(frozen=True)

    file_var: str
    edge_type: str
    source_type: str
    target_type: str
    stored_id: StoredEdgeId
    columns: tuple[LoadColumn, ...]


class LoadingJob(BaseModel):
    """A loading job with one file variable per vertex type and per concrete edge."""

    model_config = ConfigDict(frozen=True)

    name: str
    graph: str
    vertices: tuple[VertexLoad, ...]
    edges: tuple[EdgeLoad, ...]

    def vertex(self, vertex_type: str) -> VertexLoad:
        """The file of *vertex_type*.

        Raises:
            KeyError: The schema declares no such vertex type.
        """
        for load in self.vertices:
            if load.vertex_type == vertex_type:
                return load
        raise KeyError(f"no vertex type {vertex_type!r} in loading job {self.name}")

    def edge(self, stored_id: StoredEdgeId) -> EdgeLoad:
        """The file of the concrete edge *stored_id*.

        Raises:
            KeyError: The schema declares no such edge.
        """
        for load in self.edges:
            if load.stored_id == stored_id:
                return load
        raise KeyError(f"no edge {stored_id!r} in loading job {self.name}")


def job_name(graph: str) -> str:
    """The bulk loading job's name in *graph*."""
    return f"graflo_bulk_{graph}"


def loading_job(schema: Schema, graph: str) -> LoadingJob:
    """The loading job for *schema*, columns in its DDL's attribute order.

    *schema* is the physical schema the DDL was applied from. Edge types shared
    by several endpoint pairs get one file per pair, each with every attribute
    of the type, so pairs never mix layouts in one file.

    Raises:
        ValueError: Two files would get the same variable name.
    """
    db = schema.resolve_db_aware(DBType.TIGERGRAPH)
    vc = db.vertex_config
    ec = db.edge_config

    vertices: list[VertexLoad] = []
    for vertex in schema.core_schema.vertex_config.vertices:
        types = {f.name: tigergraph_type_for_field(f) for f in vertex.properties}
        identity = list(vc.identity_fields(vertex.name))
        names = [f.name for f in vertex.properties]
        if len(identity) == 1:
            # PRIMARY_ID (or a single PRIMARY KEY attribute) is declared first.
            names = [identity[0], *(n for n in names if n != identity[0])]
        vertex_type = vc.vertex_dbname(vertex.name)
        vertices.append(
            VertexLoad(
                file_var=f"v_{vertex_type}",
                vertex_type=vertex_type,
                columns=tuple(
                    LoadColumn(name=n, gsql_type=types.get(n, "STRING")) for n in names
                ),
            )
        )

    # Grouped by stored type as the DDL groups them, in declaration order.
    by_type: dict[str, list[Any]] = {}
    for edge in schema.core_schema.edge_config.values():
        runtime = ec.runtime(edge)
        type_name = runtime.relation_name or f"{edge.source}_{edge.target}"
        by_type.setdefault(type_name, []).append((edge, runtime))

    edges: list[EdgeLoad] = []
    seen: set[StoredEdgeId] = set()
    for type_name, members in by_type.items():
        ddl_edges = [tigergraph_ddl_edge_projection(e, ec) for e, _ in members]
        attributes = SchemaDdlBuilder._edge_type_attributes(ddl_edges, type_name)
        types = {f.name: tigergraph_type_for_field(f) for f in attributes}
        discriminators = sorted(edge_identity_discriminator_fields(ddl_edges[0]))
        columns = tuple(
            LoadColumn(name=n, gsql_type=types.get(n, "STRING"))
            for n in [
                *discriminators,
                *(f.name for f in attributes if f.name not in discriminators),
            ]
        )
        for _edge, runtime in members:
            stored_id = (
                runtime.source_storage,
                runtime.relation_name,
                runtime.target_storage,
            )
            if stored_id in seen:
                continue
            seen.add(stored_id)
            edges.append(
                EdgeLoad(
                    file_var=(
                        f"e_{runtime.source_storage}__{type_name}"
                        f"__{runtime.target_storage}"
                    ),
                    edge_type=type_name,
                    source_type=runtime.source_storage,
                    target_type=runtime.target_storage,
                    stored_id=stored_id,
                    columns=columns,
                )
            )

    file_vars = [v.file_var for v in vertices] + [e.file_var for e in edges]
    clashes = sorted({v for v in file_vars if file_vars.count(v) > 1})
    if clashes:
        raise ValueError(f"loading job file variables clash: {clashes}")
    return LoadingJob(
        name=job_name(graph), graph=graph, vertices=tuple(vertices), edges=tuple(edges)
    )


def _value_expr(index: int, column: LoadColumn) -> str:
    if column.kind == "list":
        return f'SPLIT(${index}, "{LIST_SEP}")'
    return f"${index}"


def create_job_gsql(job: LoadingJob) -> str:
    """``CREATE LOADING JOB`` for *job*: file variables with no path, fed by POST.

    Vertices load with ``NEW_VERTEX_ONLY`` (the target is empty, so a vertex
    already there was written by this load), edges with ``VERTEX_MUST_EXIST``
    and explicit endpoint types, one ``LOAD`` per file.
    """
    using = f'SEPARATOR="{SEP}", EOL="{EOL}", HEADER="false"'
    lines = [f"CREATE LOADING JOB {job.name} FOR GRAPH {job.graph} {{"]
    for var in [v.file_var for v in job.vertices] + [e.file_var for e in job.edges]:
        lines.append(f"  DEFINE FILENAME {var};")
    for v in job.vertices:
        values = ", ".join(_value_expr(i, c) for i, c in enumerate(v.columns))
        lines.append(
            f"  LOAD {v.file_var} TO VERTEX {v.vertex_type} VALUES ({values}) "
            f'USING {using}, NEW_VERTEX_ONLY="true";'
        )
    for e in job.edges:
        values = ", ".join(
            [
                f"$0 {e.source_type}",
                f"$1 {e.target_type}",
                *(_value_expr(i + 2, c) for i, c in enumerate(e.columns)),
            ]
        )
        lines.append(
            f"  LOAD {e.file_var} TO EDGE {e.edge_type} VALUES ({values}) "
            f'USING {using}, VERTEX_MUST_EXIST="true";'
        )
    lines.append("}")
    return "\n".join(lines)


# -- rows ---------------------------------------------------------------------


def _scalar(value: Any, kind: ColumnKind) -> str:
    """One value as the loader parses it; the text the upsert path sends otherwise."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | str):
        return str(value)
    if isinstance(value, float | Decimal):
        number = float(value)
        if not math.isfinite(number):
            raise ValueError(f"a non-finite number cannot be loaded: {value!r}")
        return repr(number)
    if isinstance(value, dt.datetime):
        if kind != "datetime":
            return value.isoformat()
        if value.tzinfo is not None:
            value = value.astimezone(dt.UTC)
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, dt.date | dt.time):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    raise TypeError(f"a {type(value).__name__} value cannot be loaded: {value!r}")


def encode_value(value: Any, column: LoadColumn) -> str:
    """*value* as one field of *column*; ``None`` is an empty field.

    Raises:
        ValueError: The text holds a separator, or a number is not finite.
        TypeError: The value is not a scalar (or, for a list column, a list).
    """
    if value is None:
        return ""
    if column.kind == "list":
        if not isinstance(value, list | tuple | set | frozenset):
            raise TypeError(
                f"column {column.name!r} is {column.gsql_type}; got {value!r}"
            )
        items = [_scalar(v, "scalar") for v in value if v is not None]
        for item in items:
            if LIST_SEP in item:
                raise ValueError(
                    f"a list item of column {column.name!r} holds the list "
                    f"separator: {item!r}"
                )
        text = LIST_SEP.join(items)
    else:
        text = _scalar(value, column.kind)
    if SEP in text or EOL in text:
        raise ValueError(
            f"a value of column {column.name!r} holds a field or row separator: "
            f"{text!r}"
        )
    return text


def _line(fields: Sequence[str]) -> bytes:
    return (SEP.join(fields) + EOL).encode("utf-8")


def vertex_line(load: VertexLoad, doc: Mapping[str, Any]) -> bytes:
    """One vertex row; an attribute absent or null is an empty field."""
    return _line([encode_value(doc.get(c.name), c) for c in load.columns])


def edge_line(
    load: EdgeLoad,
    row: tuple[Mapping[str, Any], Mapping[str, Any], Mapping[str, Any]],
    match_keys_source: Sequence[str],
    match_keys_target: Sequence[str],
) -> bytes:
    """One edge row: the endpoints' primary ids as the upsert path builds them, then attributes.

    Raises:
        ValueError: An endpoint has no identity value.
    """
    source, target, props = row
    ids = []
    for doc, keys in ((source, match_keys_source), (target, match_keys_target)):
        vid = extract_id(dict(doc), list(keys))
        if vid is None:
            raise ValueError(f"edge endpoint {dict(doc)!r} has no value for {keys}")
        ids.append(encode_value(vid, LoadColumn(name="id", gsql_type="STRING")))
    return _line([*ids, *(encode_value(props.get(c.name), c) for c in load.columns)])


def split_body(
    lines: Sequence[bytes], max_bytes: int = MAX_BODY_BYTES
) -> Iterator[tuple[bytes, int]]:
    """*lines* packed in order into bodies of at most *max_bytes*, with their line counts.

    Raises:
        ValueError: One line alone exceeds *max_bytes*.
    """
    part: list[bytes] = []
    size = 0
    for line in lines:
        if len(line) > max_bytes:
            raise ValueError(
                f"a row of {len(line)} bytes exceeds the {max_bytes}-byte body cap"
            )
        if size + len(line) > max_bytes:
            yield b"".join(part), len(part)
            part, size = [], 0
        part.append(line)
        size += len(line)
    if part:
        yield b"".join(part), len(part)


# -- responses ----------------------------------------------------------------

_BAD_COUNTER_PREFIXES = (
    "reject",
    "invalid",
    "failed",
    "notenough",
    "oversize",
    "noid",
    "incorrect",
    "missing",
)


def _count(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, Mapping) and isinstance(value.get("count"), int):
        return value["count"]
    if isinstance(value, list):
        return len(value)
    return None


def _bad_counters(node: Any, path: str, out: list[str]) -> None:
    if isinstance(node, Mapping):
        for key, value in node.items():
            here = f"{path}.{key}" if path else str(key)
            if str(key).lower().startswith(_BAD_COUNTER_PREFIXES):
                count = _count(value)
                if count:
                    out.append(f"{here}={count}")
                    continue
            _bad_counters(value, here, out)
    elif isinstance(node, list):
        for i, value in enumerate(node):
            _bad_counters(value, f"{path}[{i}]", out)


def _valid_lines(node: Any) -> list[int]:
    """Every ``validLine`` counter in *node*; a response may repeat it per level."""
    if isinstance(node, Mapping):
        found: list[int] = []
        for key, value in node.items():
            count = _count(value) if key == "validLine" else None
            if count is not None:
                found.append(count)
            else:
                found.extend(_valid_lines(value))
        return found
    if isinstance(node, list):
        return [count for value in node for count in _valid_lines(value)]
    return []


def check_load_response(response: Any, *, lines: int) -> None:
    """Refuse a load response that reports an error or any skipped line.

    A loading job skips a line it cannot parse and still answers success, so
    every rejection counter must be zero and every posted line valid.

    Raises:
        LoadRejectedError: The response reports an error, a rejected line or
            fewer valid lines than *lines*.
    """
    if not isinstance(response, Mapping):
        raise LoadRejectedError(f"unexpected loading response: {response!r}")
    if response.get("error"):
        raise LoadRejectedError(
            f"loading failed: {response.get('message') or response!r}"
        )
    bad: list[str] = []
    _bad_counters(response.get("results", response), "", bad)
    if bad:
        raise LoadRejectedError(f"loading rejected lines: {', '.join(bad)}")
    short = [n for n in _valid_lines(response.get("results", response)) if n != lines]
    if short:
        raise LoadRejectedError(f"loading accepted {short[0]} of {lines} lines")
