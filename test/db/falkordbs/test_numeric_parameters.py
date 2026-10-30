"""Numeric writes and rejected parameters on the C and Rust engines."""

import sys

import pytest

from graflo.connections.onto import FalkordbConfig
from graflo.db.manager import ConnectionManager


@pytest.mark.parametrize(
    "value",
    [-(2**63), 2**63 - 1, 2**53 + 1, 0, True, False, sys.float_info.min, 1e308],
)
def test_numeric_properties_preserve_their_value_and_type(
    conn_conf: FalkordbConfig, test_graph_name: str, clean_db: None, value: float
) -> None:
    """Integers stay exact; floating results retain the server's wire precision."""
    with ConnectionManager(connection_config=conn_conf) as db:
        db.upsert_docs_batch([{"id": "1", "value": value}], "number", ["id"])

        (doc,) = db.fetch_docs("number")
        assert type(doc["value"]) is type(value)
        if isinstance(value, float):
            assert doc["value"] == pytest.approx(value, rel=1e-14, abs=0)
        else:
            assert doc["value"] == value


@pytest.mark.parametrize("value", [2**63, -(2**63) - 1, 2**64 - 1])
def test_integer_overflow_does_not_write_a_partial_batch(
    conn_conf: FalkordbConfig, test_graph_name: str, clean_db: None, value: int
) -> None:
    """Overflow is explicit on both engines, before any document is written."""
    with ConnectionManager(connection_config=conn_conf) as db:
        with pytest.raises(OverflowError, match="signed 64-bit"):
            db.upsert_docs_batch(
                [{"id": "valid", "value": 1}, {"id": "invalid", "value": value}],
                "number",
                ["id"],
            )

        assert db.fetch_docs("number") == []


@pytest.mark.parametrize("value", [2**63, -(2**63) - 1, 2**64 - 1])
@pytest.mark.parametrize("null_code", [False, True])
def test_integer_overflow_does_not_write_an_earlier_edge_group(
    conn_conf: FalkordbConfig,
    test_graph_name: str,
    clean_db: None,
    value: int,
    null_code: bool,
) -> None:
    """A valid property-MERGE group cannot precede rejection of another group."""
    with ConnectionManager(connection_config=conn_conf) as db:
        db.upsert_docs_batch(
            [{"id": key} for key in ("s1", "t1", "s2", "t2")], "number", ["id"]
        )
        props: dict[str, int | None] = {"weight": value}
        if null_code:
            props["code"] = None

        with pytest.raises(OverflowError, match="signed 64-bit"):
            db.insert_edges_batch(
                [
                    [{"id": "s1"}, {"id": "t1"}, {"code": "valid", "weight": 1}],
                    [{"id": "s2"}, {"id": "t2"}, props],
                ],
                "number",
                "number",
                "LINK",
                ("id",),
                ("id",),
                relationship_merge_properties=("code",),
            )

        assert db.execute("MATCH ()-[r]->() RETURN count(r)").result_set == [[0]]
        assert {doc["id"] for doc in db.fetch_docs("number")} == {
            "s1",
            "t1",
            "s2",
            "t2",
        }


@pytest.mark.parametrize("value", [1e-308, -1e-308, 5e-324, -5e-324])
def test_subnormal_floats_round_trip_or_are_explicitly_refused(
    conn_conf: FalkordbConfig, test_graph_name: str, clean_db: None, value: float
) -> None:
    """4.x accepts subnormals; 6.0.3 refuses their parameter representation."""
    with ConnectionManager(connection_config=conn_conf) as db:
        try:
            db.upsert_docs_batch([{"id": "1", "value": value}], "number", ["id"])
        except ValueError as error:
            assert "query parameter" in str(error)
            assert db.fetch_docs("number") == []
        else:
            assert db.fetch_docs("number") == [{"id": "1", "value": value}]
