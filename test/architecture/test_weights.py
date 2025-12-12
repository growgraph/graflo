import logging
from pathlib import Path

from graflo.architecture.actor import ActorWrapper
from graflo.architecture.edge import WeightConfig
from graflo.architecture.onto import ActionContext
from graflo.architecture.vertex import Field, FieldType
from graflo.plot.plotter import assemble_tree

logger = logging.getLogger(__name__)


def test_act_openalex(resource_openalex_authors, vc_openalex, sample_openalex_authors):
    ctx = ActionContext()
    anw = ActorWrapper(*resource_openalex_authors)
    anw.finish_init(vertex_config=vc_openalex, transforms={})
    ctx = anw(ctx, doc=sample_openalex_authors)
    assemble_tree(anw, Path("test/figs/openalex_authors.pdf"))
    edge = ctx.acc_global[("author", "institution", None)][0]
    assert edge[-1] == {
        "updated_date": "2023-06-08",
        "created_date": "2023-06-08",
    }


def test_kg_mention(resource_kg_menton_triple, vertex_config_kg_mention, mention_data):
    ctx = ActionContext()
    anw = ActorWrapper(*resource_kg_menton_triple)
    anw.finish_init(vertex_config=vertex_config_kg_mention, transforms={})
    ctx = anw(ctx, doc=[mention_data])
    roles = set(
        item[-1]["_role"] for item in ctx.acc_global[("mention", "mention", None)]
    )
    assert roles == {"relation", "target", "source"}


def test_weight_config_direct_strings():
    """Test WeightConfig.direct with string inputs (backward compatibility)."""
    wc = WeightConfig(direct=["date", "weight", "confidence"])  # type: ignore[arg-type]
    assert len(wc.direct) == 3
    assert all(isinstance(f, Field) for f in wc.direct)
    assert wc.direct[0].name == "date"
    assert wc.direct[0].type is None
    assert wc.direct[1].name == "weight"
    assert wc.direct[2].name == "confidence"
    # Field objects behave like strings
    assert "date" in wc.direct
    assert wc.direct[0] == "date"
    assert wc.direct[0] != "weight"


def test_weight_config_direct_field_objects():
    """Test WeightConfig.direct with Field objects."""
    wc = WeightConfig(
        direct=[
            Field(name="date", type=FieldType.DATETIME),
            Field(name="weight", type=FieldType.FLOAT),
            Field(name="confidence", type=None),
        ]
    )
    assert len(wc.direct) == 3
    assert wc.direct[0].name == "date"
    assert wc.direct[0].type == FieldType.DATETIME
    assert wc.direct[1].name == "weight"
    assert wc.direct[1].type == FieldType.FLOAT
    assert wc.direct[2].name == "confidence"
    assert wc.direct[2].type is None
    # Field objects behave like strings
    assert "date" in wc.direct
    assert wc.direct[0] == "date"


def test_weight_config_direct_dicts():
    """Test WeightConfig.direct with dict inputs (from YAML/JSON)."""
    wc = WeightConfig(
        direct=[
            {"name": "date", "type": "DATETIME"},
            {"name": "weight", "type": "FLOAT"},
            {"name": "confidence"},  # defaults to None type
        ]  # type: ignore[arg-type]
    )
    assert len(wc.direct) == 3
    assert wc.direct[0].name == "date"
    assert wc.direct[0].type == FieldType.DATETIME
    assert wc.direct[1].name == "weight"
    assert wc.direct[1].type == FieldType.FLOAT
    assert wc.direct[2].name == "confidence"
    assert wc.direct[2].type is None


def test_weight_config_direct_mixed():
    """Test WeightConfig.direct with mixed input types."""
    wc = WeightConfig(
        direct=[
            "date",  # string
            Field(name="weight", type=FieldType.FLOAT),  # Field object
            {"name": "confidence", "type": "FLOAT"},  # dict
        ]  # type: ignore[arg-type]
    )
    assert len(wc.direct) == 3
    assert all(isinstance(f, Field) for f in wc.direct)
    assert wc.direct[0].name == "date"
    assert wc.direct[0].type is None
    assert wc.direct[1].name == "weight"
    assert wc.direct[1].type == FieldType.FLOAT
    assert wc.direct[2].name == "confidence"
    assert wc.direct[2].type == FieldType.FLOAT


def test_weight_config_direct_names_property():
    """Test WeightConfig.direct_names property."""
    wc = WeightConfig(direct=["date", "weight", "confidence"])  # type: ignore[arg-type]
    names = wc.direct_names
    assert names == ["date", "weight", "confidence"]
    assert isinstance(names, list)
    assert all(isinstance(n, str) for n in names)


def test_weight_config_direct_backward_compatibility():
    """Test that Field objects in direct behave like strings for backward compatibility."""
    wc = WeightConfig(direct=["date", "weight"])  # type: ignore[arg-type]

    # Test iteration (used in actor_util.py)
    field_names = [field for field in wc.direct]
    assert len(field_names) == 2
    assert all(isinstance(f, Field) for f in field_names)

    # Test membership (used in actor_util.py: `if field in u_.ctx`)
    ctx = {"date": "2023-01-01", "weight": 1.5}
    for field in wc.direct:
        assert field in ctx, f"Field {field} should be in ctx"
        assert ctx[field] is not None

    # Test dict key usage (used in transform.py)
    result_dict = {}
    for field in wc.direct:
        result_dict[field] = f"value_{field.name}"
    assert "date" in result_dict
    assert result_dict["date"] == "value_date"
    assert result_dict[wc.direct[0]] == "value_date"


def test_weight_config_direct_empty():
    """Test WeightConfig.direct with empty list."""
    wc = WeightConfig(direct=[])
    assert len(wc.direct) == 0
    assert wc.direct_names == []


def test_weight_config_direct_invalid_dict():
    """Test WeightConfig.direct with invalid dict (missing 'name' key)."""
    import pytest

    with pytest.raises(ValueError, match="Field dict must have 'name' key"):
        WeightConfig(direct=[{"type": "STRING"}])  # type: ignore[arg-type]


def test_weight_config_direct_invalid_type():
    """Test WeightConfig.direct with invalid type."""
    import pytest

    with pytest.raises(TypeError, match="Field must be str, Field, or dict"):
        WeightConfig(direct=[123])  # type: ignore


def test_weight_config_from_dict():
    """Test WeightConfig.from_dict with dict inputs (YAML/JSON deserialization)."""
    # Test with dicts containing name and type
    data = {
        "direct": [
            {"name": "date", "type": "DATETIME"},
            {"name": "weight", "type": "FLOAT"},
            {"name": "confidence"},  # no type specified
        ],
        "vertices": [],
    }
    wc = WeightConfig.from_dict(data)
    assert len(wc.direct) == 3
    assert wc.direct[0].name == "date"
    assert wc.direct[0].type == FieldType.DATETIME
    assert wc.direct[1].name == "weight"
    assert wc.direct[1].type == FieldType.FLOAT
    assert wc.direct[2].name == "confidence"
    assert wc.direct[2].type is None

    # Test with strings (backward compatibility)
    data2 = {"direct": ["date", "weight"], "vertices": []}
    wc2 = WeightConfig.from_dict(data2)
    assert len(wc2.direct) == 2
    assert wc2.direct[0].name == "date"
    assert wc2.direct[0].type is None
    assert wc2.direct[1].name == "weight"
    assert wc2.direct[1].type is None
