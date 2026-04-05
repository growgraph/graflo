"""Tests for DatabaseProfile.default_property_values."""

from __future__ import annotations

from graflo.architecture.database_features import (
    DatabaseProfile,
    DefaultPropertyValues,
    EdgePropertyDefaults,
)
from graflo.onto import DBType


def test_database_profile_parses_default_property_values_yaml_shape() -> None:
    dp = DatabaseProfile(
        db_flavor=DBType.TIGERGRAPH,
        default_property_values=DefaultPropertyValues(
            vertices={"Sensor": {"reading": -1.0}},
            edges=[
                EdgePropertyDefaults(
                    source="Person",
                    target="Company",
                    relation="works_at",
                    values={"since_year": 0},
                )
            ],
        ),
    )
    assert dp.vertex_property_default("Sensor", "reading") == -1.0
    assert dp.has_vertex_property_default("Sensor", "reading")
    assert not dp.has_vertex_property_default("Sensor", "missing")

    eid = ("Person", "Company", "works_at")
    assert dp.edge_property_default(eid, "since_year") == 0
    assert dp.has_edge_property_default(eid, "since_year")
    assert not dp.has_edge_property_default(eid, "nope")


def test_default_property_values_accepts_properties_alias_in_raw_dict() -> None:
    raw = {
        "db_flavor": "tigergraph",
        "default_property_values": {
            "vertices": {"Sensor": {"reading": -1.0}},
            "edges": [
                {
                    "source": "A",
                    "target": "B",
                    "relation": None,
                    "properties": {"x": 1},
                }
            ],
        },
    }
    dp = DatabaseProfile.model_validate(raw)
    assert dp.edge_property_default(("A", "B", None), "x") == 1


def test_edge_property_defaults_last_spec_wins() -> None:
    dp = DatabaseProfile(
        default_property_values=DefaultPropertyValues(
            edges=[
                EdgePropertyDefaults(
                    source="A", target="B", relation=None, values={"w": 1}
                ),
                EdgePropertyDefaults(
                    source="A", target="B", relation=None, values={"w": 2}
                ),
            ]
        )
    )
    assert dp.edge_property_default(("A", "B", None), "w") == 2
