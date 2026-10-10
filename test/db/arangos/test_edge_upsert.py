"""Edge upsert keeps distinct relations between the same two vertices."""

from graflo.db.manager import ConnectionManager


def test_two_relations_on_one_pair_stay_two_edges(conn_conf, create_db, test_db_name):
    _ = create_db
    conn_conf.database = test_db_name
    with ConnectionManager(connection_config=conn_conf) as db:
        db.create_collection("person")
        db.create_collection("org")
        db.conn.create_collection("person_org_edges", edge=True)
        db.upsert_docs_batch([{"_key": "p1"}], "person", match_keys=["_key"])
        db.upsert_docs_batch([{"_key": "o1"}], "org", match_keys=["_key"])

        for relation in ("works_at", "advises", "works_at"):
            db.insert_edges_batch(
                docs_edges=[[{"_key": "p1"}, {"_key": "o1"}, {}]],
                source_class="person",
                target_class="org",
                relation_name=relation,
                on_duplicate="upsert",
                filter_uniques=False,
            )

        relations = sorted(db.execute("FOR e IN person_org_edges RETURN e.relation"))

    assert relations == ["advises", "works_at"]
