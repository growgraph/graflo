"""The backend page's cells agree with what the code declares."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from graflo.architecture.graph_types import IndexType
from graflo.architecture.graph_types.index_config import Index
from graflo.architecture.schema.edge_direction import (
    REVERSE_TRAVERSAL_COST,
    UNDIRECTED_NATIVE_DBS,
    ReverseTraversalCost,
)
from graflo.architecture.schema.vertex import Field, FieldType
from graflo.connections.onto import TARGET_DATABASES
from graflo.db.conn import ConnectionCapability
from graflo.db.field_type_support import (
    UnsupportedFieldTypeError,
    UnsupportedIndexKindError,
    assert_field_type_supported,
    assert_index_kind_supported,
    index_limit,
)
from graflo.db.manager import ConnectionManager
from graflo.onto import DBType

ROOT = Path(__file__).resolve().parents[2]
PAGE = ROOT / "docs" / "concepts" / "backends.md"

BACKENDS = {
    "ArangoDB": DBType.ARANGO,
    "Neo4j": DBType.NEO4J,
    "Memgraph": DBType.MEMGRAPH,
    "FalkorDB": DBType.FALKORDB,
    "TigerGraph": DBType.TIGERGRAPH,
    "NebulaGraph": DBType.NEBULA,
    "PostgreSQL": DBType.POSTGRES,
    "GraFlo file backend": DBType.GRAFLO_BACKEND,
}

INDEXES = "Indexes"
TYPES = "Types and constraints"
READING = "Reading and changing a graph"
FREE_EDITIONS = "What the free editions allow"

COMPOSITE = Index(fields=["a", "b"], unique=False)
UNIQUE = Index(fields=["a"], unique=True)
PLAIN = Index(fields=["a"], unique=False)
FULLTEXT = Index(fields=["a"], type=IndexType.FULLTEXT, unique=False)

INDEX_SHAPES: dict[str, tuple[Index, bool]] = {
    "Composite on vertices": (COMPOSITE, False),
    "Composite on edges": (COMPOSITE, True),
    "Unique on vertices": (UNIQUE, False),
    "Unique on edges": (UNIQUE, True),
    "Edge index": (PLAIN, True),
}

REVERSE_READ = {
    ReverseTraversalCost.FREE: "same as forward",
    ReverseTraversalCost.CHEAP: "little cost",
    ReverseTraversalCost.CLAUSE_REQUIRED: "reverse clause",
    ReverseTraversalCost.SCHEMA_TIME_ONLY: "needs a declared inverse",
    ReverseTraversalCost.MATERIALIZATION_REQUIRED: "index built on read",
}

CAPABILITIES = {
    "Read back whole": ConnectionCapability.GRAPH_EXPORT,
    "Migration DDL": ConnectionCapability.SCHEMA_DDL,
    "Delete": ConnectionCapability.INSTANCE_DELETE,
    "Delete a scope": ConnectionCapability.SCOPED_DELETE,
    "Read guard": ConnectionCapability.READ_GUARD,
    "Union merge": ConnectionCapability.UNION_MERGE,
}


def _table(heading: str) -> dict[str, dict[str, str]]:
    """The table under ``## heading``, as ``{first cell: {column: cell}}``."""
    text = PAGE.read_text(encoding="utf-8")
    section = text.split(f"\n## {heading}\n", 1)[1].split("\n## ", 1)[0]
    rows = [
        [cell.strip() for cell in line.strip().strip("|").split("|")]
        for line in section.splitlines()
        if line.startswith("|")
    ]
    header, _, *body = rows
    return {row[0]: dict(zip(header[1:], row[1:], strict=True)) for row in body}


def _cell(heading: str, backend: str, column: str) -> str:
    return _table(heading)[backend][column]


def _yes(supported: bool) -> str:
    return "yes" if supported else "no"


def test_the_backends_named_here_are_the_targets() -> None:
    assert set(BACKENDS.values()) == TARGET_DATABASES


@pytest.mark.parametrize("heading", [INDEXES, TYPES, READING])
def test_every_table_lists_every_target(heading: str) -> None:
    assert set(_table(heading)) == set(BACKENDS)


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("column", INDEX_SHAPES)
def test_index_cells_match_the_index_limits(backend: str, column: str) -> None:
    db_type = BACKENDS[backend]
    index, on_edge = INDEX_SHAPES[column]
    cell = _cell(INDEXES, backend, column)
    if index_limit(db_type, index, on_edge=on_edge) is not None:
        assert cell == "refused"
    elif db_type == DBType.GRAFLO_BACKEND:
        assert cell == "no"
    else:
        assert cell in {"yes", "per field"}


@pytest.mark.parametrize("backend", BACKENDS)
def test_fulltext_cell_matches_the_index_kinds(backend: str) -> None:
    db_type = BACKENDS[backend]
    cell = _cell(INDEXES, backend, "Fulltext")
    if db_type == DBType.GRAFLO_BACKEND:
        assert cell == "no"
        return
    try:
        assert_index_kind_supported(db_type, FULLTEXT, "page")
    except UnsupportedIndexKindError:
        assert cell == "refused"
    else:
        assert cell == "yes"


@pytest.mark.parametrize("backend", BACKENDS)
def test_list_cell_matches_the_field_type_check(backend: str) -> None:
    field = Field(name="tags", type=FieldType.LIST, item_type=FieldType.STRING)
    cell = _cell(TYPES, backend, "List properties")
    try:
        assert_field_type_supported(BACKENDS[backend], field)
    except UnsupportedFieldTypeError:
        assert cell == "refused"
    else:
        assert cell == "yes"


@pytest.mark.parametrize("backend", BACKENDS)
def test_undirected_cell_matches_the_native_set(backend: str) -> None:
    native = BACKENDS[backend] in UNDIRECTED_NATIVE_DBS
    assert _cell(TYPES, backend, "Undirected edge type") == _yes(native)


@pytest.mark.parametrize("backend", BACKENDS)
def test_reverse_read_cell_matches_the_cost_table(backend: str) -> None:
    cost = REVERSE_TRAVERSAL_COST[BACKENDS[backend]]
    assert _cell(TYPES, backend, "Reverse read") == REVERSE_READ[cost]


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("column", CAPABILITIES)
def test_capability_cells_match_the_connection_flags(backend: str, column: str) -> None:
    conn_cls = ConnectionManager.target_conn_mapping[BACKENDS[backend]]
    declared = getattr(conn_cls, CAPABILITIES[column].value)
    assert _cell(READING, backend, column) == _yes(declared)


@pytest.mark.parametrize("backend", BACKENDS)
def test_introspection_cell_matches_the_connection_flags(backend: str) -> None:
    conn_cls = ConnectionManager.target_conn_mapping[BACKENDS[backend]]
    assert conn_cls.supports_schema_introspection
    expected = "sampled" if conn_cls.schema_introspection_is_sampled else "catalog"
    assert _cell(READING, backend, "Schema introspection") == expected


def test_every_free_edition_row_is_a_server_with_a_vendor_link() -> None:
    table = _table(FREE_EDITIONS)
    assert set(table) == set(BACKENDS) - {"GraFlo file backend"}
    for server, row in table.items():
        assert "](https://" in " ".join(row.values()), f"{server} has no vendor link"


def _tree_targets() -> dict[str, set[str]]:
    """The tree's leaves, as ``{node id: backends its label names}``."""
    text = PAGE.read_text(encoding="utf-8")
    tree = text.split("```mermaid\n", 1)[1].split("```", 1)[0]
    leaves = re.findall(r'^\s*(\w+)\["(.+)"\]$', tree, flags=re.MULTILINE)
    return {node: {b for b in BACKENDS if b in label} for node, label in leaves}


def _with(heading: str, column: str, value: str) -> set[str]:
    return {b for b, row in _table(heading).items() if row[column] == value}


def test_every_target_is_reachable_in_the_tree() -> None:
    assert set().union(*_tree_targets().values()) == set(BACKENDS)


def test_tree_fulltext_leaf_is_the_target_with_fulltext() -> None:
    assert _tree_targets()["FULLTEXT_LEAF"] == _with(INDEXES, "Fulltext", "yes")


def test_tree_types_leaf_is_the_targets_with_declared_types() -> None:
    column = "Declared types reach the schema"
    assert _tree_targets()["TYPES_LEAF"] == _with(TYPES, column, "yes")


@pytest.mark.parametrize("leaf", ["DEFAULT_LEAF", "GUARD_LEAF"])
@pytest.mark.parametrize("column", ["Read guard", "Union merge"])
def test_tree_guard_leaves_have_guard_and_union(leaf: str, column: str) -> None:
    assert _tree_targets()[leaf] <= _with(READING, column, "yes")


def test_tree_replica_leaf_has_neither_guard_nor_union() -> None:
    for column in ("Read guard", "Union merge"):
        assert not _tree_targets()["REPLICA_LEAF"] & _with(READING, column, "yes")


def test_tree_leaf_warnings_match_the_tables() -> None:
    assert _cell(TYPES, "NebulaGraph", "Parallel edges") == "no"
    assert _cell(TYPES, "NebulaGraph", "List properties") == "refused"
    assert _cell(FREE_EDITIONS, "Neo4j", "Clustering or replication") == "no"


def test_page_is_in_the_nav() -> None:
    nav = (ROOT / "properdocs.yml").read_text(encoding="utf-8")
    assert "concepts/backends.md" in nav
