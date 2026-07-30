"""Resource sampling and profiling.

Covers the substrate shared by algorithmic and agentic schema inference: samples
stay pure JSON, connector provenance survives, and nesting is described by path
rather than flattened away at the boundary.
"""

from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

from graflo.architecture.onto_sample import (
    ForeignKeyHint,
    ResourceSample,
    SourceSample,
    infer_field_type,
    iter_paths,
    profile_sample,
    profile_source,
)
from graflo.architecture.schema.vertex import FieldType
from graflo.hq.sampler import ResourceSampler, _jsonable

SOURCE_DIR = Path(__file__).parent.parent / "data" / "sample-source"


@pytest.fixture
def sampler() -> ResourceSampler:
    return ResourceSampler(max_docs=10)


@pytest.fixture
def file_sample(sampler: ResourceSampler) -> SourceSample:
    return sampler.sample_files(SOURCE_DIR)


# ----------------------------------------------------------------------
# Sampling
# ----------------------------------------------------------------------


def test_sample_files_records_connector_provenance(file_sample):
    """Every sample must know which connector produced it -- that relation is
    what later becomes a resource_connector binding."""
    assert file_sample.source_name == "sample-source"
    for sample in file_sample.samples:
        assert sample.connector, f"{sample.resource_name} lost its connector"
        assert sample.connector == sample.resource_name


def test_sample_files_skips_non_data_files(file_sample):
    """A source directory routinely holds notes beside its data."""
    names = {sample.resource_name for sample in file_sample.samples}
    assert names == {"customers", "orders", "api_orders"}


def test_sample_files_keeps_documents_verbatim(file_sample):
    """Tabular rows arrive as flat dicts, nested documents keep their structure."""
    customers = file_sample.get("customers")
    assert customers is not None
    assert customers.docs[0]["email"] == "alice@example.com"

    api = file_sample.get("api_orders")
    assert api is not None
    assert api.docs[0]["customer"] == {"id": "c1", "city": "Berlin"}
    assert api.docs[0]["items"][0]["sku"] == "A-1"
    assert api.docs[0]["tags"] == ["priority", "gift"]


def test_samples_by_resource_matches_inferencer_input_shape(file_sample):
    """dict[str, list[dict]] is what cross-resource identity inference consumes."""
    by_resource = file_sample.samples_by_resource
    assert set(by_resource) == {"customers", "orders", "api_orders"}
    assert all(
        isinstance(docs, list) and all(isinstance(doc, dict) for doc in docs)
        for docs in by_resource.values()
    )


def test_max_docs_caps_and_marks_truncated():
    sampler = ResourceSampler(max_docs=2)
    sample = sampler.sample_file(SOURCE_DIR / "orders.csv")
    assert len(sample.docs) == 2
    assert sample.truncated is True


def test_max_docs_below_one_is_rejected():
    with pytest.raises(ValueError, match="at least 1"):
        ResourceSampler(max_docs=0)


def test_sample_single_file_directly(sampler):
    sample = sampler.sample_file(SOURCE_DIR / "customers.csv")
    assert sample.resource_name == "customers"
    assert len(sample.docs) == 3
    assert sample.truncated is False


def test_sample_files_rejects_directory_without_data(tmp_path, sampler):
    (tmp_path / "readme.txt").write_text("nothing here")
    with pytest.raises(ValueError, match="No sampleable files"):
        sampler.sample_files(tmp_path)


def test_source_sample_requires_at_least_one_resource():
    with pytest.raises(ValueError):
        SourceSample(source_name="empty", samples=[])


# ----------------------------------------------------------------------
# JSON coercion
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    "value,expected",
    [
        (datetime(2026, 7, 30, 12, 0), "2026-07-30T12:00:00"),
        (Decimal("9.50"), 9.5),
        (None, None),
        (True, True),
        (7, 7),
        ("plain", "plain"),
    ],
)
def test_jsonable_coerces_database_types(value, expected):
    """DB and columnar readers hand back types json.dumps cannot serialise."""
    coerced, _ = _jsonable(value, max_cell_chars=64)
    assert coerced == expected


def test_jsonable_clips_long_values_and_reports_it():
    coerced, clipped = _jsonable("x" * 100, max_cell_chars=10)
    assert coerced == "x" * 10
    assert clipped is True


def test_jsonable_recurses_into_containers():
    coerced, _ = _jsonable(
        {"when": datetime(2026, 1, 1), "amounts": [Decimal("1.5")]}, max_cell_chars=64
    )
    assert coerced == {"when": "2026-01-01T00:00:00", "amounts": [1.5]}


def test_jsonable_summarises_binary():
    coerced, clipped = _jsonable(b"\x00\x01\x02", max_cell_chars=64)
    assert coerced == "<3 bytes>"
    assert clipped is True


# ----------------------------------------------------------------------
# Path walking and type inference
# ----------------------------------------------------------------------


def test_iter_paths_describes_nesting_by_path():
    doc = {
        "id": 1,
        "customer": {"id": "c1", "address": {"city": "Berlin"}},
        "items": [{"sku": "A"}, {"sku": "B"}],
        "tags": ["x"],
    }
    paths = {path for path, _depth, _value in iter_paths(doc)}
    assert paths == {
        "id",
        "customer.id",
        "customer.address.city",
        "items[].sku",
        "tags",
    }


def test_iter_paths_reports_depth():
    doc = {"a": 1, "b": {"c": {"d": 2}}}
    depths = {path: depth for path, depth, _value in iter_paths(doc)}
    assert depths == {"a": 0, "b.c.d": 2}


@pytest.mark.parametrize(
    "values,expected",
    [
        ([True, False], FieldType.BOOL),
        ([1, 2, 3], FieldType.INT),
        ([1.5, 2], FieldType.FLOAT),
        (["a", "b"], FieldType.STRING),
        ([datetime(2026, 1, 1)], FieldType.DATETIME),
        (["2026-07-30", "2026-07-31"], FieldType.DATETIME),
        (["4f8c1d2e-1111-4222-8333-abcdefabcdef"], FieldType.UUID),
        ([None, None], FieldType.STRING),
    ],
)
def test_infer_field_type(values, expected):
    field_type, _item_type = infer_field_type(values)
    assert field_type == expected


def test_infer_field_type_checks_bool_before_int():
    """bool is an int subclass, so the naive order mistypes booleans as INT."""
    assert infer_field_type([True, False])[0] == FieldType.BOOL
    assert infer_field_type([0, 1])[0] == FieldType.INT


def test_infer_field_type_lists_carry_item_type():
    field_type, item_type = infer_field_type([["a", "b"], ["c"]])
    assert field_type == FieldType.LIST
    assert item_type == FieldType.STRING


# ----------------------------------------------------------------------
# Profiling
# ----------------------------------------------------------------------


def test_profile_sample_types_tabular_columns(sampler):
    profile = profile_sample(sampler.sample_file(SOURCE_DIR / "orders.csv"))
    assert profile.max_depth == 0
    assert profile.nested is False
    types = {field.path: field.type for field in profile.fields}
    assert types["id"] == FieldType.STRING
    assert types["customer_id"] == FieldType.STRING


def test_profile_sample_describes_nesting(sampler):
    """Nesting depth is the signal an ingestion model needs descend steps."""
    profile = profile_sample(sampler.sample_file(SOURCE_DIR / "api_orders.json"))
    assert profile.nested is True
    assert profile.max_depth == 1
    assert "items[].sku" in profile.field_paths
    assert "customer.city" in profile.field_paths


def test_profile_sample_tracks_nulls_and_cardinality(sampler):
    profile = profile_sample(sampler.sample_file(SOURCE_DIR / "api_orders.json"))
    fields = {field.path: field for field in profile.fields}
    assert fields["customer.city"].null_count == 1
    assert fields["order_id"].unique is True
    assert fields["items[].sku"].present == 3


def test_profile_sample_types_scalar_lists(sampler):
    profile = profile_sample(sampler.sample_file(SOURCE_DIR / "api_orders.json"))
    tags = next(field for field in profile.fields if field.path == "tags")
    assert tags.type == FieldType.LIST
    assert tags.item_type == FieldType.STRING


def test_profile_carries_declared_keys_not_guesses():
    """Declared PK/FK are ground truth for edge inference."""
    sample = ResourceSample(
        resource_name="orders",
        connector="orders",
        docs=[{"id": "o1", "customer_id": "c1"}],
        primary_key=["id"],
        foreign_keys=[
            ForeignKeyHint(
                field="customer_id",
                references_resource="customers",
                references_field="id",
            )
        ],
    )
    profile = profile_sample(sample)
    assert profile.primary_key == ["id"]
    assert profile.foreign_keys[0].references_resource == "customers"


def test_profile_max_paths_caps_and_marks_truncated():
    sample = ResourceSample(
        resource_name="wide",
        docs=[{f"col_{i}": i for i in range(50)}],
    )
    profile = profile_sample(sample, max_paths=10)
    assert len(profile.fields) == 10
    assert profile.truncated is True


def test_flat_docs_makes_nested_sources_eligible_for_identity_inference(sampler):
    """Identity inference operates on flat records; this is the bridge."""
    sample = sampler.sample_file(SOURCE_DIR / "api_orders.json")
    profile = profile_sample(sample)
    flat = profile.flat_docs(sample.docs)
    assert flat[0]["customer.id"] == "c1"
    assert flat[0]["items[].sku"] == "A-1"
    assert all(not isinstance(value, dict) for value in flat[0].values())


def test_profile_source_covers_every_resource(file_sample):
    profiles = profile_source(file_sample)
    assert {profile.resource_name for profile in profiles} == {
        "customers",
        "orders",
        "api_orders",
    }
    assert all(profile.connector for profile in profiles)


# ----------------------------------------------------------------------
# Serialization
# ----------------------------------------------------------------------


def test_source_sample_round_trips_through_json(file_sample):
    """The sample crosses an HTTP boundary, so it must survive model_dump/validate
    with nested documents intact."""
    payload = file_sample.model_dump(mode="json")
    restored = SourceSample.model_validate(payload)
    assert restored.samples_by_resource == file_sample.samples_by_resource
    api = restored.get("api_orders")
    assert api is not None
    assert api.docs[0]["items"][0]["sku"] == "A-1"
