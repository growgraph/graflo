"""Shared live-backend assertions for identity-funnel ingest.

Imported by the per-backend suites (``arangos``, ``neo4js``) so the same
expectations run against every flavor without duplicating the fixture data.

This is the end-to-end half of the cast-path cover: the unit tests in
``test/architecture/test_identity_funnel.py`` prove identities exist by assemble
time, and this proves the resulting documents actually land as distinct rows in
a real database.
"""

from __future__ import annotations

import os
from pathlib import Path

from suthing import FileHandle

from graflo import GraphEngine, GraphManifest
from graflo.db.manager import ConnectionManager
from graflo.hq.caster import IngestionParams
from graflo.onto import DBType

EXAMPLE_DIR = Path(__file__).resolve().parents[2] / "examples" / "17-identity-funnel"

#: Six source rows over two resources. Alan Turing carries an email in both, so
#: both of his rows take the ``email`` branch and upsert onto one vertex.
EXPECTED_VERTICES = 5


def ingest_funnel_example(conn_conf, flavor: DBType) -> None:
    """Define and ingest ``examples/17-identity-funnel`` into a live backend."""
    manifest = GraphManifest.from_config(FileHandle.load(EXAMPLE_DIR / "manifest.yaml"))
    manifest.finish_init()
    engine = GraphEngine(target_db_flavor=flavor)
    previous_cwd = os.getcwd()
    try:
        # File connectors resolve ``sub_path: data`` relative to the example.
        os.chdir(EXAMPLE_DIR)
        engine.define_and_ingest(
            manifest=manifest,
            target_db_config=conn_conf,
            ingestion_params=IngestionParams(clear_data=True),
            recreate_schema=True,
        )
    finally:
        os.chdir(previous_cwd)


def assert_funnel_identities_landed(conn_conf) -> None:
    """Every row is keyed, keys are distinct, and equal evidence converges."""
    with ConnectionManager(connection_config=conn_conf) as db_client:
        docs = db_client.fetch_all_docs("party")

    assert len(docs) == EXPECTED_VERTICES, (
        f"expected {EXPECTED_VERTICES} party vertices, got {len(docs)} — "
        "a collapsed batch or a dropped document"
    )

    ids = [doc.get("id") for doc in docs]
    assert all(ids), "a party vertex landed with no identity"
    assert len(set(ids)) == EXPECTED_VERTICES, "funnel keys collided"

    # Alan is the convergence case: two source rows, two resources, one vertex.
    turing = [doc for doc in docs if str(doc.get("email") or "") == "alan@turing.uk"]
    assert len(turing) == 1, (
        f"the same email in two resources must digest to one vertex, got {len(turing)}"
    )
