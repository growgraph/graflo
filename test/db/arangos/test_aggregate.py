from graflo.db import ConnectionManager
from graflo.onto import AggregationType


def test_average(create_db, conn_conf, test_db_name):
    _ = create_db

    conn_conf.database = test_db_name
    docs = [
        {"class": "a", "value": 1},
        {"class": "a", "value": 2},
        {"class": "a", "value": 3},
        {"class": "b", "value": 4},
        {"class": "b", "value": 5},
    ]
    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.delete_graph_structure(["samples"])
        db_client.create_collection("samples")
        r = db_client.upsert_docs_batch(docs, "samples")
        r = db_client.aggregate(
            "samples",
            AggregationType.AVERAGE,
            discriminant="class",
            aggregated_field="value",
        )
        assert r == [
            {"class": "a", "_value": 2},
            {"class": "b", "_value": 4.5},
        ]

        r = db_client.aggregate("samples", AggregationType.COUNT)
        assert r == [{"_value": 5}]
