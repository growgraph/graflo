"""Identity-funnel ingest against a live ArangoDB.

Arango is the interesting second flavor here: its default key field is ``_key``,
so this also covers the writer mirroring the assemble-time identity onto it.
"""

from __future__ import annotations

from graflo.onto import DBType
from test.db.identity_funnel_e2e import (
    assert_funnel_identities_landed,
    ingest_funnel_example,
)


def test_funnel_vertices_land_as_distinct_documents(
    create_db, conn_conf, test_db_name
) -> None:
    _ = create_db
    conn_conf.database = test_db_name
    ingest_funnel_example(conn_conf, DBType.ARANGO)
    assert_funnel_identities_landed(conn_conf)
