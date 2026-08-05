"""Pure introspection parsing and query shapes — no database required.

The e2e suite proves the recovered schema is right against one fixture graph;
these prove the parsing is right against the shapes a fixture graph never
produces: multi-endpoint edges, `UNDIRECTED`, compound types, names containing
the separator. Mirrors the split between `test_traversal_queries.py` and
`test_traversal_e2e.py`.
"""

from __future__ import annotations

import pytest

from graflo.architecture.schema.vertex import FieldType
from graflo.db.cypher.introspection import (
    _edge_pattern_query,
    _edge_property_query,
    _vertex_property_query,
    collect_cypher_introspection,
    escape_label,
)
from graflo.db.nebula.util import field_type_from_nebula
from graflo.db.postgres.target_write import field_type_from_postgres
from graflo.db.tigergraph.gsql_parsers import (
    parse_show_edge_ddl,
    parse_show_vertex_ddl,
    split_ddl_terms,
)
from graflo.db.tigergraph.onto import field_type_from_gsql

# ---------------------------------------------------------------------------
# GSQL DDL recovery
# ---------------------------------------------------------------------------

SHOW_VERTEX_OUTPUT = """Using graph 'gf_sec_e2e'
- VERTEX instrument(PRIMARY_ID sid STRING, isin STRING) WITH STATS="OUTDEGREE_BY_EDGETYPE", PRIMARY_ID_AS_ATTRIBUTE="true"

- VERTEX issuer(PRIMARY_ID iid STRING, lei STRING, founded INT) WITH STATS="OUTDEGREE_BY_EDGETYPE"
"""

SHOW_EDGE_OUTPUT = """Using graph 'gf_sec_e2e'
- DIRECTED EDGE issued_by(FROM instrument, TO issuer, share STRING)
- UNDIRECTED EDGE peer_of(FROM issuer, TO issuer)
"""


def test_parse_vertex_ddl_recovers_names_and_attributes() -> None:
    vertices = parse_show_vertex_ddl(SHOW_VERTEX_OUTPUT)
    assert [v.name for v in vertices] == ["instrument", "issuer"]
    assert [a.name for a in vertices[0].attributes] == ["sid", "isin"]


def test_parse_vertex_ddl_recovers_primary_id_as_identity() -> None:
    """PRIMARY_ID is a declared identity; nothing should have to guess it."""
    vertices = parse_show_vertex_ddl(SHOW_VERTEX_OUTPUT)
    assert [v.primary_id for v in vertices] == ["sid", "iid"]


def test_parse_vertex_ddl_stops_at_the_with_clause() -> None:
    """`WITH STATS="..."` is storage detail, not an attribute."""
    vertices = parse_show_vertex_ddl(SHOW_VERTEX_OUTPUT)
    names = {a.name for v in vertices for a in v.attributes}
    assert not any(n.upper().startswith("STATS") for n in names)


def test_parse_edge_ddl_recovers_the_undirected_keyword() -> None:
    """The one place any backend *states* undirectedness."""
    edges = {e.name: e for e in parse_show_edge_ddl(SHOW_EDGE_OUTPUT)}
    assert edges["issued_by"].directed is True
    assert edges["peer_of"].directed is False


def test_parse_edge_ddl_recovers_endpoints_and_attributes() -> None:
    edges = {e.name: e for e in parse_show_edge_ddl(SHOW_EDGE_OUTPUT)}
    assert edges["issued_by"].endpoints == [("instrument", "issuer")]
    assert [a.name for a in edges["issued_by"].attributes] == ["share"]
    assert edges["peer_of"].attributes == []


def test_parse_edge_ddl_recovers_every_endpoint_pair() -> None:
    """One GSQL edge type may declare several `FROM`/`TO` pairs, split by `|`.

    Taking only the first would silently drop edges from the recovered schema.
    """
    output = (
        "- DIRECTED EDGE owns(FROM person, TO car | FROM company, TO car, since INT)"
    )
    edge = parse_show_edge_ddl(output)[0]
    assert edge.endpoints == [("person", "car"), ("company", "car")]
    assert [a.name for a in edge.attributes] == ["since"]


def test_split_ddl_terms_is_depth_aware() -> None:
    """`MAP<INT, STRING>` is one term; a naive comma split makes it two."""
    assert split_ddl_terms("a INT, b MAP<INT, STRING>, c STRING") == [
        "a INT",
        "b MAP<INT, STRING>",
        "c STRING",
    ]


def test_parse_edge_ddl_recovers_declarations_without_a_with_clause() -> None:
    """Edges carry no `WITH`, so each declaration ends at its own line.

    Regression: the pattern originally anchored on end-of-string, which parsed
    exactly one declaration out of a multi-edge `SHOW EDGE *` response.
    """
    assert len(parse_show_edge_ddl(SHOW_EDGE_OUTPUT)) == 2


def test_parse_vertex_ddl_ignores_unrelated_output() -> None:
    assert parse_show_vertex_ddl("Using graph 'g'\nNothing here.\n") == []


# ---------------------------------------------------------------------------
# Declared-type mapping
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "declared,expected",
    [
        ("STRING", FieldType.STRING),
        ("INT", FieldType.INT),
        ("INTEGER", FieldType.INT),  # alias
        ("BOOLEAN", FieldType.BOOL),
        ("LIST<INT>", FieldType.LIST),
        ("MAP<INT, STRING>", None),  # no graflo equivalent
        ("", None),
    ],
)
def test_field_type_from_gsql(declared: str, expected) -> None:
    assert field_type_from_gsql(declared) is expected


@pytest.mark.parametrize(
    "declared,expected",
    [
        ("string", FieldType.STRING),
        ("int64", FieldType.INT),
        ("fixed_string(32)", FieldType.STRING),
        ("double", FieldType.DOUBLE),
        ("timestamp", FieldType.DATETIME),
        ("geography", None),
        (None, None),
    ],
)
def test_field_type_from_nebula(declared, expected) -> None:
    assert field_type_from_nebula(declared) is expected


@pytest.mark.parametrize(
    "declared,expected",
    [
        ("text", FieldType.STRING),
        ("varchar(64)", FieldType.STRING),
        ("int4", FieldType.INT),
        ("double precision", FieldType.DOUBLE),
        ("timestamptz", FieldType.DATETIME),
        ("TEXT[]", FieldType.LIST),
        ("tsvector", None),
    ],
)
def test_field_type_from_postgres(declared, expected) -> None:
    assert field_type_from_postgres(declared) is expected


# ---------------------------------------------------------------------------
# Cypher-family query shapes
# ---------------------------------------------------------------------------


def test_vertex_property_query_bounds_the_sample() -> None:
    query = _vertex_property_query("person", 25)
    assert "LIMIT 25" in query
    assert "MATCH (n:`person`)" in query


def test_edge_pattern_query_reads_both_endpoint_labels() -> None:
    query = _edge_pattern_query()
    assert "labels(a)[0] AS source" in query
    assert "labels(b)[0] AS target" in query


def test_edge_property_query_omits_the_type_clause_when_unnamed() -> None:
    assert "-[r]->" in _edge_property_query("a", "b", None, 10)
    assert "-[r:`knows`]->" in _edge_property_query("a", "b", "knows", 10)


def test_escape_label_strips_backticks() -> None:
    """A label carrying a backtick must not break out of its quoting."""
    assert "`" not in escape_label("we`ird")


# ---------------------------------------------------------------------------
# Collector behaviour
# ---------------------------------------------------------------------------


class _ScriptedRunner:
    """Answers queries from a canned script, recording what was asked."""

    def __init__(self, responses: dict[str, list[dict]]):
        self._responses = responses
        self.queries: list[str] = []

    def __call__(self, query: str, keys) -> list[dict]:
        self.queries.append(query)
        for marker, rows in self._responses.items():
            if marker in query:
                return rows
        return []


def test_collector_records_the_sample_limit() -> None:
    """A consumer cannot judge a sampled schema without knowing the sample size."""
    runner = _ScriptedRunner({"db.labels": []})
    result = collect_cypher_introspection(name="g", run=runner, sample_limit=7)
    assert result.sample_limit == 7


def test_collector_never_claims_an_edge_is_undirected() -> None:
    """Sampling observes one orientation and cannot distinguish the two."""
    runner = _ScriptedRunner(
        {
            "db.labels": [{"label": "person"}],
            "UNWIND keys(n)": [{"key": "id"}],
            "labels(a)[0]": [
                {"source": "person", "relation": "knows", "target": "person"}
            ],
            "UNWIND keys(r)": [{"key": "since"}],
        }
    )
    result = collect_cypher_introspection(name="g", run=runner, sample_limit=10)
    assert [e.directed for e in result.edges] == [True]


def test_collector_falls_back_when_the_labels_procedure_is_absent() -> None:
    """FalkorDB and a locked-down Memgraph may not expose `db.labels()`."""

    def run(query: str, keys) -> list[dict]:
        if "db.labels" in query:
            raise RuntimeError("unknown procedure")
        if "UNWIND labels(n)" in query:
            return [{"label": "person"}]
        return []

    result = collect_cypher_introspection(name="g", run=run, sample_limit=10)
    assert [v.name for v in result.vertices] == ["person"]


# ---------------------------------------------------------------------------
# Capability honesty
# ---------------------------------------------------------------------------


def test_schema_introspection_capability_is_truthful() -> None:
    """`supports_schema_introspection` must mean the method is really there.

    The flag is what the server's `/capabilities` endpoint reports and what
    gates a 501, so a backend claiming introspection it does not have turns a
    clear "not supported" into a `NotImplementedError` surfacing as a 500.
    """
    from graflo.db.conn import Connection
    from graflo.db.manager import ConnectionManager

    for flavor, cls in ConnectionManager.target_conn_mapping.items():
        implemented = (
            cls.introspect_graph_schema is not Connection.introspect_graph_schema
        )
        assert cls.supports_schema_introspection == implemented, (
            f"{flavor.value}: supports_schema_introspection="
            f"{cls.supports_schema_introspection} but "
            f"introspect_graph_schema {'is' if implemented else 'is not'} overridden"
        )


def test_every_backend_supports_schema_introspection() -> None:
    """All eight, which is the point of the stage.

    Introspection reached three backends because it was gated on
    `supports_graph_export` -- "can dump the whole graph" -- rather than on
    whether the backend could describe itself at all.
    """
    from graflo.db.manager import ConnectionManager

    missing = [
        flavor.value
        for flavor, cls in ConnectionManager.target_conn_mapping.items()
        if not cls.supports_schema_introspection
    ]
    assert missing == []
