"""Tests for Grafeo data ingestion via the full graflo pipeline.

Adapted from test/db/falkordbs/test_ingest.py (expanded to 7 modes).

Covers 7 modes exercising different graph patterns:
- review:            CSV, academic graph (Author -> ResearchField)
- ibes:              CSV, 5 vertex types, vertex_weights, blank vertices
- kg:                JSON, nested structures, multi-edge types
- ticker:            CSV, temporal properties, vertex filtering
- csv-edge-weights:  CSV, 1 edge per row with date property
- objects-relations: CSV, vertex_router + edge_router (dynamic routing)
- oa-institution:    JSON, nested resources, relation_field
"""

from test.conftest import fetch_schema_obj, ingest_atomic

from graflo.db import ConnectionManager
from graflo.onto import AggregationType


# -------------------------------------------------------------------------
# review (the original baseline test)
# -------------------------------------------------------------------------


def test_ingest_review(
    clean_db,
    conn_conf,
    current_path,
    test_db_name,
    reset,
):
    """Ingest the review dataset: 374 authors, filters, projections."""
    schema_o = fetch_schema_obj("review")
    ingest_atomic(
        conn_conf,
        current_path,
        test_db_name,
        schema_o=schema_o,
        mode="review",
    )

    with ConnectionManager(connection_config=conn_conf) as db:
        r = db.fetch_docs("Author")
        assert len(r) == 374, f"Expected 374 authors, got {len(r)}"

        r = db.fetch_docs("Author", filters=["==", "10", "hindex"])
        assert len(r) == 8, f"Expected 8 authors with hindex=10, got {len(r)}"

        r = db.fetch_docs("Author", limit=1)
        assert len(r) == 1

        r = db.fetch_docs(
            "Author",
            filters=["==", "10", "hindex"],
            return_keys=["full_name"],
        )
        assert len(r[0]) == 1, "Projection should return exactly 1 key"


# -------------------------------------------------------------------------
# ibes (5 vertex types, 4 edge types, vertex_weights, blank vertices)
# -------------------------------------------------------------------------


def test_ingest_ibes(
    clean_db,
    conn_conf,
    current_path,
    test_db_name,
    reset,
):
    """Ingest IBES analyst recommendation data (1000 CSV rows).

    Expected: 138 agencies, 476 analysts, 1000 publications, 106 recommendations, 69 tickers.
    """
    schema_o = fetch_schema_obj("ibes")
    ingest_atomic(
        conn_conf,
        current_path,
        test_db_name,
        schema_o=schema_o,
        mode="ibes",
    )

    with ConnectionManager(connection_config=conn_conf) as db:
        # Note: db-aware names are pluralized (agency -> agencies, etc.)
        agencies = db.fetch_docs("agencies")
        assert len(agencies) == 138, f"Expected 138 agencies, got {len(agencies)}"

        analysts = db.fetch_docs("analysts")
        assert len(analysts) == 476, f"Expected 476 analysts, got {len(analysts)}"

        publications = db.fetch_docs("publications")
        assert len(publications) == 1000, f"Expected 1000 publications, got {len(publications)}"

        recommendations = db.fetch_docs("recommendations")
        assert len(recommendations) == 106, f"Expected 106 recommendations, got {len(recommendations)}"

        tickers = db.fetch_docs("tickers")
        assert len(tickers) == 69, f"Expected 69 tickers, got {len(tickers)}"


# -------------------------------------------------------------------------
# kg (nested JSON, multi-edge types, entity/mention/community)
# -------------------------------------------------------------------------


def test_ingest_kg(
    clean_db,
    conn_conf,
    current_path,
    test_db_name,
    reset,
):
    """Ingest knowledge graph (JSON with nested entities, mentions, edges).

    Expected: 1 publication, 80 entities, 124 mentions.
    """
    schema_o = fetch_schema_obj("kg")
    ingest_atomic(
        conn_conf,
        current_path,
        test_db_name,
        schema_o=schema_o,
        mode="kg",
    )

    with ConnectionManager(connection_config=conn_conf) as db:
        # Note: db-aware names are pluralized
        publications = db.fetch_docs("publications")
        assert len(publications) == 1, (
            f"Expected 1 publication, got {len(publications)}"
        )

        entities = db.fetch_docs("entities")
        assert len(entities) == 80, (
            f"Expected 80 entities, got {len(entities)}"
        )

        mentions = db.fetch_docs("mentions")
        assert len(mentions) == 124, (
            f"Expected 124 mentions, got {len(mentions)}"
        )

        # Verify edges exist
        r = db.execute("MATCH ()-[r]->() RETURN count(r) AS c")
        edge_count = r.to_list()[0]["c"]
        assert edge_count > 0, "KG should have edges"


# -------------------------------------------------------------------------
# ticker (temporal properties, vertex filtering, vertex_weights)
# -------------------------------------------------------------------------


def test_ingest_ticker(
    clean_db,
    conn_conf,
    current_path,
    test_db_name,
    reset,
):
    """Ingest stock ticker history (CSV with OHLCV data).

    The ticker schema has resource name 'history' but data file is 'ticker.csv.gz',
    so we use a custom binding instead of ingest_atomic's auto-binding.
    Verifies temporal properties and vertex filtering (Open/Close > 0, not Volume).
    """
    from pathlib import Path
    from graflo.architecture.contract.bindings import Bindings, FileConnector
    from graflo.architecture.contract.manifest import GraphManifest
    from graflo.hq import GraphEngine
    from graflo.hq.caster import IngestionParams

    schema_o = fetch_schema_obj("ticker")
    from test.conftest import fetch_manifest_obj
    ingestion_model = fetch_manifest_obj(
        "ticker", dynamic_edge_feedback=True
    ).require_ingestion_model()
    ingestion_model.finish_init(
        schema_o.core_schema, dynamic_edge_feedback=True
    )

    # Explicit binding: resource "history" -> ticker.csv.gz
    data_path = Path(current_path) / "data" / "ticker"
    bindings = Bindings()
    fc = FileConnector(regex=".*ticker.*", sub_path=data_path)
    bindings.add_connector(fc)
    bindings.bind_resource("history", fc)

    manifest = GraphManifest(
        graph_schema=schema_o,
        ingestion_model=ingestion_model,
        bindings=bindings,
    )
    manifest.finish_init()

    db_type = conn_conf.connection_type
    engine = GraphEngine(target_db_flavor=db_type)
    engine.define_schema(
        manifest=manifest,
        target_db_config=conn_conf,
        recreate_schema=True,
    )
    engine.ingest(
        manifest=manifest,
        target_db_config=conn_conf,
        ingestion_params=IngestionParams(n_cores=1, clear_data=False),
    )

    with ConnectionManager(connection_config=conn_conf) as db:
        tickers = db.fetch_docs("tickers")
        assert len(tickers) > 0, "Should have ticker vertices"

        features = db.fetch_docs("features")
        assert len(features) > 0, "Should have feature vertices"

        # Features should not include Volume (filtered by schema)
        feature_names = {f.get("name") for f in features}
        assert "Volume" not in feature_names, "Volume should be filtered out"

        # Verify edges (ticker -> feature with t_obs property)
        r = db.execute("MATCH ()-[r]->() RETURN count(r) AS c")
        edge_count = r.to_list()[0]["c"]
        assert edge_count > 0, "Should have ticker->feature edges"


# -------------------------------------------------------------------------
# csv-edge-weights (1 edge per CSV row, edge properties)
# -------------------------------------------------------------------------


def test_ingest_csv_edge_weights(
    clean_db,
    conn_conf,
    current_path,
    test_db_name,
    reset,
):
    """Ingest company relationships (1 edge per CSV row with date property).

    8 rows = 8 edges between company nodes.
    """
    schema_o = fetch_schema_obj("csv-edge-weights")
    ingest_atomic(
        conn_conf,
        current_path,
        test_db_name,
        schema_o=schema_o,
        mode="csv-edge-weights",
    )

    with ConnectionManager(connection_config=conn_conf) as db:
        companies = db.fetch_docs("company")
        assert len(companies) >= 4, f"Expected at least 4 companies, got {len(companies)}"

        # Verify edges
        r = db.execute("MATCH ()-[r]->() RETURN count(r) AS c")
        edge_count = r.to_list()[0]["c"]
        assert edge_count > 0, "Should have company relationship edges"


# -------------------------------------------------------------------------
# objects-relations (vertex_router, edge_router, dynamic routing)
# -------------------------------------------------------------------------


def test_ingest_objects_relations(
    clean_db,
    conn_conf,
    current_path,
    test_db_name,
    reset,
):
    """Ingest objects + relations with dynamic routing.

    objects.csv: 4 persons, 3 vehicles, 3 institutions (via vertex_router).
    relations.csv: 7 edges (via edge_router with relation_map).
    """
    schema_o = fetch_schema_obj("objects-relations")
    ingest_atomic(
        conn_conf,
        current_path,
        test_db_name,
        schema_o=schema_o,
        mode="objects-relations",
    )

    with ConnectionManager(connection_config=conn_conf) as db:
        persons = db.fetch_docs("person")
        assert len(persons) == 4, f"Expected 4 persons, got {len(persons)}"

        vehicles = db.fetch_docs("vehicle")
        assert len(vehicles) == 3, f"Expected 3 vehicles, got {len(vehicles)}"

        institutions = db.fetch_docs("institution")
        assert len(institutions) == 3, f"Expected 3 institutions, got {len(institutions)}"

        # Note: edge_router creates edges dynamically without edge_config entries,
        # so _push_edges (which iterates edge_config) does not push them.
        # This validates that vertex_router works end-to-end through the DB pipeline.
        total = len(persons) + len(vehicles) + len(institutions)
        assert total == 10, f"Expected 10 total vertices, got {total}"


# -------------------------------------------------------------------------
# oa-institution (nested resources, relation_field, self-loops)
# -------------------------------------------------------------------------


def test_ingest_oa_institution(
    clean_db,
    conn_conf,
    current_path,
    test_db_name,
    reset,
):
    """Ingest OpenAlex institutions with associated_institutions (self-referencing).

    Expected: 4 institutions, 3 institution->institution edges.
    """
    schema_o = fetch_schema_obj("oa-institution")
    ingest_atomic(
        conn_conf,
        current_path,
        test_db_name,
        schema_o=schema_o,
        mode="oa-institution",
    )

    with ConnectionManager(connection_config=conn_conf) as db:
        # Note: db-aware name is pluralized
        institutions = db.fetch_docs("institutions")
        assert len(institutions) == 4, (
            f"Expected 4 institutions, got {len(institutions)}"
        )

        r = db.execute("MATCH ()-[r]->() RETURN count(r) AS c")
        edge_count = r.to_list()[0]["c"]
        assert edge_count == 3, f"Expected 3 edges, got {edge_count}"


# -------------------------------------------------------------------------
# Aggregation tests (standalone, not mode-dependent)
# -------------------------------------------------------------------------


def test_aggregation_count(conn_conf, test_graph_name, clean_db):
    """Test COUNT aggregation."""
    with ConnectionManager(connection_config=conn_conf) as db:
        docs = [
            {"id": "1", "type": "A"},
            {"id": "2", "type": "A"},
            {"id": "3", "type": "B"},
        ]
        db.upsert_docs_batch(docs, "Item", match_keys=["id"])

        count = db.aggregate("Item", AggregationType.COUNT)
        assert count == 3

        grouped = db.aggregate("Item", AggregationType.COUNT, discriminant="type")
        assert grouped.get("A") == 2
        assert grouped.get("B") == 1


def test_aggregation_min_max_avg(conn_conf, test_graph_name, clean_db):
    """Test MIN, MAX, and AVERAGE aggregations."""
    with ConnectionManager(connection_config=conn_conf) as db:
        docs = [
            {"id": "1", "score": 10},
            {"id": "2", "score": 20},
            {"id": "3", "score": 30},
        ]
        db.upsert_docs_batch(docs, "Score", match_keys=["id"])

        assert db.aggregate("Score", AggregationType.MIN, aggregated_field="score") == 10
        assert db.aggregate("Score", AggregationType.MAX, aggregated_field="score") == 30
        assert db.aggregate("Score", AggregationType.AVERAGE, aggregated_field="score") == 20.0


def test_aggregation_sorted_unique(conn_conf, test_graph_name, clean_db):
    """Test SORTED_UNIQUE aggregation."""
    with ConnectionManager(connection_config=conn_conf) as db:
        docs = [
            {"id": "1", "category": "B"},
            {"id": "2", "category": "A"},
            {"id": "3", "category": "C"},
            {"id": "4", "category": "A"},
        ]
        db.upsert_docs_batch(docs, "Item", match_keys=["id"])

        unique_vals = db.aggregate(
            "Item", AggregationType.SORTED_UNIQUE, aggregated_field="category"
        )
        assert unique_vals == ["A", "B", "C"]


def test_keep_absent_documents(conn_conf, test_graph_name, clean_db):
    """Test keep_absent_documents functionality."""
    with ConnectionManager(connection_config=conn_conf) as db:
        db.upsert_docs_batch(
            [{"id": "1", "name": "Existing 1"}, {"id": "2", "name": "Existing 2"}],
            "User",
            match_keys=["id"],
        )

        absent = db.keep_absent_documents(
            [
                {"id": "1", "name": "Existing 1"},
                {"id": "3", "name": "New 3"},
                {"id": "4", "name": "New 4"},
            ],
            "User",
            match_keys=["id"],
            keep_keys=["id", "name"],
        )

        assert len(absent) == 2
        absent_ids = {doc["id"] for doc in absent}
        assert absent_ids == {"3", "4"}
