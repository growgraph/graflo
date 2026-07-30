"""Identity-funnel ingest against a live Neo4j."""

from __future__ import annotations

from graflo.onto import DBType
from test.db.identity_funnel_e2e import (
    assert_funnel_identities_landed,
    ingest_funnel_example,
)


def test_funnel_vertices_land_as_distinct_documents(clean_db, conn_conf) -> None:
    _ = clean_db
    ingest_funnel_example(conn_conf, DBType.NEO4J)
    assert_funnel_identities_landed(conn_conf)
