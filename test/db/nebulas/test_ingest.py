"""Full ingest against a live NebulaGraph space.

Nebula was the only graph backend with no ``define_schema`` + ``ingest``
coverage in its own directory — its suite exercised CRUD against a hand-built
mini schema, so nothing checked that a real manifest survives the path every
other backend is tested on.

That gap mattered more here than elsewhere. Nebula needs a tag index before a
property lookup will run at all, and its storaged schema cache lags graphd by
several heartbeats, so a manifest that defines cleanly can still fail to read
back. Only an ingest exercises that ordering.

Uses the same ``review`` dataset and helpers as the FalkorDB, Memgraph and
Neo4j ingest tests, so the counts below are directly comparable across
backends rather than being Nebula-specific numbers.
"""

import pytest

from graflo.db.manager import ConnectionManager
from graflo.onto import AggregationType
from test.conftest import fetch_schema_obj, ingest_atomic

pytestmark = pytest.mark.nebula

MODE = "review"

#: First row of ``authors.csv.gz``. Its ``id`` is covered by the manifest's
#: author index, which is what makes a filtered lookup answerable at all.
ANCHOR_ID = "309238221625"
ANCHOR_NAME = "Guillaume Lemaître"


@pytest.fixture(scope="module")
def ingested(request):
    """Ingest the review dataset once into a throwaway space."""
    import uuid
    from os.path import dirname, realpath

    from graflo.connections.onto import NebulaConfig

    space = f"test_ingest_{str(uuid.uuid4()).replace('-', '')[:8]}"
    conf = NebulaConfig.from_docker_env()
    conf.uri = f"nebula://localhost:{conf.port}"

    # test/db/nebulas -> test, where the `data/<mode>` fixtures live.
    test_root = dirname(dirname(dirname(realpath(__file__))))

    ingest_atomic(
        conf,
        test_root,
        space,
        schema_o=fetch_schema_obj(MODE),
        mode=MODE,
    )
    try:
        yield conf
    finally:
        try:
            with ConnectionManager(connection_config=conf) as db:
                db.delete_database(space)
        except Exception:
            pass


def test_all_authors_are_ingested(ingested) -> None:
    with ConnectionManager(connection_config=ingested) as db:
        assert len(db.fetch_docs("Author")) == 374


def test_fetch_honours_a_filter_on_an_indexed_field(ingested) -> None:
    """Filtering works on a field the manifest indexes.

    ``review.yaml`` indexes author on ``(id, full_name)``, and the ingest is
    what makes that index real — so this asserts the DDL, the index build and
    the read path line up, not merely that a filter is rendered.
    """
    with ConnectionManager(connection_config=ingested) as db:
        rows = db.fetch_docs("Author", filters=["==", ANCHOR_ID, "id"])
        assert len(rows) == 1
        assert rows[0]["full_name"] == ANCHOR_NAME


def test_filtering_an_unindexed_field_still_answers(ingested) -> None:
    """``hindex`` carries no index of its own, but the tag does, so it scans.

    Nebula refuses a plan over a tag with *no* index at all, which is what made
    this a useful assertion: the same query raised ``IndexNotFound`` until the
    identity index was actually being created. The count matches the FalkorDB,
    Memgraph and Neo4j ingest suites, which run the same dataset.
    """
    with ConnectionManager(connection_config=ingested) as db:
        assert len(db.fetch_docs("Author", filters=["==", "10", "hindex"])) == 8


def test_fetch_honours_a_limit(ingested) -> None:
    with ConnectionManager(connection_config=ingested) as db:
        assert len(db.fetch_docs("Author", limit=1)) == 1


def test_fetch_honours_a_projection(ingested) -> None:
    with ConnectionManager(connection_config=ingested) as db:
        rows = db.fetch_docs(
            "Author", filters=["==", ANCHOR_ID, "id"], return_keys=["full_name"]
        )
        assert rows and all(set(row) == {"full_name"} for row in rows)


def test_research_fields_are_ingested(ingested) -> None:
    with ConnectionManager(connection_config=ingested) as db:
        assert db.aggregate("ResearchField", AggregationType.COUNT) == 17


def test_edges_are_ingested(ingested) -> None:
    """The edge block is the half a vertex-only ingest would leave unchecked.

    Anchored by an identity *mapping* rather than a raw VID on purpose: author
    identity is composite (``id`` + ``full_name``), and Nebula addresses such a
    vertex by a VID composed of both. Resolving the anchor by its first field
    alone yields a VID that exists nowhere, and the traversal then returns
    empty rather than raising.
    """
    schema = fetch_schema_obj(MODE)
    with ConnectionManager(connection_config=ingested) as db:
        container = db.graph_neighbors(
            "author",
            {"id": ANCHOR_ID, "full_name": ANCHOR_NAME},
            hops=1,
            schema=schema,
        )
        assert container.edges, "author has no belongsTo edge after ingest"
        assert "researchField" in container.vertices
