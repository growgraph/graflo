"""Edge upsert: distinct relations stay distinct; edges reach partly keyed vertices."""

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
                filter_uniques=False,
            )

        relations = sorted(db.execute("FOR e IN person_org_edges RETURN e.relation"))

    assert relations == ["advises", "works_at"]


def test_an_edge_reaches_a_vertex_lacking_one_identity_member(
    conn_conf, create_db, test_db_name
):
    _ = create_db
    conn_conf.database = test_db_name
    with ConnectionManager(connection_config=conn_conf) as db:
        db.create_collection("publications")
        db.create_collection("mentions")
        db.conn.create_collection("publications_mentions_edges", edge=True)
        db.upsert_docs_batch(
            [{"arxiv": "2101.1", "created": "2021"}],
            "publications",
            match_keys=["arxiv", "doi"],
        )
        db.upsert_docs_batch([{"_key": "m1"}], "mentions", match_keys=["_key"])
        db.insert_edges_batch(
            docs_edges=[[{"arxiv": "2101.1"}, {"_key": "m1"}, {}]],
            source_class="publications",
            target_class="mentions",
            relation_name=None,
            match_keys_source=("arxiv", "doi"),
            match_keys_target=("_key",),
            filter_uniques=False,
        )
        joined = list(
            db.execute(
                "FOR e IN publications_mentions_edges "
                "RETURN [DOCUMENT(e._from).arxiv, DOCUMENT(e._to)._key]"
            )
        )

    assert joined == [["2101.1", "m1"]]
