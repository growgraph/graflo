"""Table and column comments reach the sample and its profile.

A comment is often the only place a database says what a column means -- a
polymorphic reference, a cross-system key. Dropping it at sampling leaves an
inferencer guessing at what the source already stated.
"""

from __future__ import annotations

import pytest

from graflo.architecture.onto_sample import profile_sample
from graflo.hq.sampler import ResourceSampler


@pytest.fixture
def commented(postgres_conn):
    statements = [
        "DROP TABLE IF EXISTS gf_commented",
        "CREATE TABLE gf_commented (id INTEGER PRIMARY KEY, serial TEXT, plain TEXT)",
        "INSERT INTO gf_commented VALUES (1, 'S-1', 'x')",
        "COMMENT ON TABLE gf_commented IS 'Machines as the maintenance system records them'",
        "COMMENT ON COLUMN gf_commented.serial IS 'Nameplate serial; shared with the sensor feed'",
    ]
    try:
        with postgres_conn.conn.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)
        postgres_conn.conn.commit()
    except Exception as error:  # pragma: no cover - environment dependent
        pytest.skip(f"postgres unavailable: {error}")
    yield
    with postgres_conn.conn.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS gf_commented")
    postgres_conn.conn.commit()


def test_comments_reach_the_sample_and_profile(commented, conn_conf) -> None:
    source = ResourceSampler(max_docs=5).sample_postgres(
        conn_conf, tables=["gf_commented"]
    )
    sample = source.get("gf_commented")
    assert sample is not None
    assert sample.description == "Machines as the maintenance system records them"
    assert sample.field_descriptions == {
        "serial": "Nameplate serial; shared with the sensor feed"
    }

    profile = profile_sample(sample)
    by_path = {field.path: field for field in profile.fields}
    assert (
        by_path["serial"].description == "Nameplate serial; shared with the sensor feed"
    )
    assert by_path["plain"].description is None
    assert profile.description == "Machines as the maintenance system records them"
