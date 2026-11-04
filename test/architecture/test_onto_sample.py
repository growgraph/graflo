from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from graflo.architecture.onto_sample import SourceSample


def test_declared_field_descriptions_reach_the_profile() -> None:
    from graflo.architecture.onto_sample import ResourceSample, profile_sample

    sample = ResourceSample(
        resource_name="machine",
        docs=[{"serial": "S-1", "plain": "x"}],
        description="Machines as recorded",
        field_descriptions={"serial": "Nameplate serial"},
    )
    profile = profile_sample(sample)
    by_path = {field.path: field for field in profile.fields}
    assert by_path["serial"].description == "Nameplate serial"
    assert by_path["plain"].description is None
    assert profile.description == "Machines as recorded"


def _source(name: str, *tables: str) -> SourceSample:
    from graflo.architecture.onto_sample import (
        ForeignKeyHint,
        ResourceSample,
        SourceSample,
    )

    return SourceSample(
        source_name=name,
        description=f"{name} database",
        samples=[
            ResourceSample(
                resource_name=table,
                connector=table,
                docs=[{"id": 1}],
                foreign_keys=[
                    ForeignKeyHint(
                        field="owner_id",
                        references_resource=tables[0],
                        references_field="id",
                    )
                ],
            )
            for table in tables
        ],
    )


def test_combine_one_source_is_unchanged() -> None:
    from graflo.architecture.onto_sample import SourceSample

    crm = _source("crm", "contacts", "companies")
    assert SourceSample.combine({"crm": crm}) is crm


def test_combine_prefixes_names_connectors_and_references() -> None:
    from graflo.architecture.onto_sample import SourceSample

    combined = SourceSample.combine(
        {"crm": _source("crm", "users", "orders"), "shop": _source("shop", "users")}
    )
    assert combined.source_name == "crm, shop"
    assert combined.description == "crm: crm database; shop: shop database"
    assert [s.resource_name for s in combined.samples] == [
        "crm_users",
        "crm_orders",
        "shop_users",
    ]
    assert [s.connector for s in combined.samples] == [
        "crm_users",
        "crm_orders",
        "shop_users",
    ]
    orders = combined.get("crm_orders")
    assert orders is not None
    assert orders.foreign_keys[0].references_resource == "crm_users"
    assert orders.foreign_keys[0].references_field == "id"


def test_combine_keeps_a_missing_connector_missing() -> None:
    from graflo.architecture.onto_sample import ResourceSample, SourceSample

    files = SourceSample(
        source_name="files", samples=[ResourceSample(resource_name="rows")]
    )
    combined = SourceSample.combine({"a": files, "b": files})
    assert [s.connector for s in combined.samples] == [None, None]
    assert combined.description is None


def test_combine_rejects_colliding_prefixed_names() -> None:
    import pytest

    from graflo.architecture.onto_sample import ResourceSample, SourceSample

    left = SourceSample(source_name="a", samples=[ResourceSample(resource_name="b_c")])
    right = SourceSample(source_name="a_b", samples=[ResourceSample(resource_name="c")])
    with pytest.raises(ValueError, match="duplicate resource names"):
        SourceSample.combine({"a": left, "a_b": right})


def test_combine_rejects_nothing() -> None:
    import pytest

    from graflo.architecture.onto_sample import SourceSample

    with pytest.raises(ValueError, match="at least one"):
        SourceSample.combine({})
