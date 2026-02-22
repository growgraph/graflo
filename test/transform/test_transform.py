import logging

import pytest

from graflo.architecture.transform import Transform
from graflo.util.transform import parse_multi_item

logger = logging.getLogger(__name__)


@pytest.fixture
def quoted_multi_row():
    return """1486058874058,"['id:206158957580, name:Marcello Martini'
    'id:360777873683, name:F. Giudicepietro'
    "id:489626818966, name:Luca D'Auria"]",[127313418 165205528],2015,10.1038/SREP13100"""


@pytest.fixture
def quoted_multi_item():
    return """['id:206158957580, name:Marcello Martini'
 'id:360777873683, name:F. Giudicepietro'
 "id:489626818966, name:Luca D'Auria"]"""


def test_to_int():
    kwargs = {
        "module": "builtins",
        "foo": "int",
        "input": "x",
        "output": "y",
    }
    t = Transform(**kwargs)  # type: ignore
    assert t("12345") == {"y": 12345}


def test_round():
    kwargs = {
        "module": "builtins",
        "foo": "round",
        "input": "x",
        "output": "y",
        "params": {"ndigits": 3},
    }
    t = Transform(**kwargs)  # type: ignore
    r = t(0.1234)
    assert r == {"y": 0.123}


def test_map():
    kwargs = {"map": {"x": "y"}}
    t = Transform(**kwargs)  # type: ignore
    r = t(0.1234)
    assert r["y"] == 0.1234


def test_map_doc():
    kwargs = {"map": {"x": "y"}}
    t = Transform(**kwargs)  # type: ignore
    r = t({"x": 0.1234})
    assert r["y"] == 0.1234


def test_input_output():
    kwargs = {"input": ["x"], "output": ["y"]}
    t = Transform(**kwargs)  # type: ignore
    assert t(0.1)["y"] == 0.1


def test_parse_multi_item(quoted_multi_item):
    r = parse_multi_item(quoted_multi_item, mapper={"name": "full_name"}, direct=["id"])
    assert r["full_name"][0] == "Luca D'Auria"
    assert r["id"][-1] == "360777873683"


def test_dress():
    kwargs = {
        "module": "builtins",
        "foo": "round",
        "input": ["Open"],
        "dress": {"key": "name", "value": "value"},
        "params": {"ndigits": 3},
    }
    t = Transform(**kwargs)  # type: ignore
    r = t({"Open": 0.1234})
    assert r == {"name": "Open", "value": 0.123}


def test_dress_complete():
    doc = {
        "Date": "2014-04-15",
        "Open": "17.899999618530273",
        "High": "17.920000076293945",
        "Low": "15.149999618530273",
        "Close": "15.350000381469727",
        "Volume": "3531700",
        "Dividends": "0",
        "Stock Splits": "0",
        "__ticker": "AAPL",
    }

    kwargs = {
        "module": "graflo.util.transform",
        "foo": "round_str",
        "input": ["Open"],
        "dress": {"key": "name", "value": "value"},
        "params": {"ndigits": 3},
    }
    t = Transform(**kwargs)  # type: ignore
    r = t(doc)
    assert r == {"name": "Open", "value": 17.9}


def test_dress_derives_output():
    """dress auto-derives output=(key, value) field names."""
    kwargs = {
        "module": "builtins",
        "foo": "int",
        "input": ["Volume"],
        "dress": {"key": "name", "value": "value"},
    }
    t = Transform(**kwargs)  # type: ignore
    assert t.output == ("name", "value")
    assert t.dress is not None
    assert t.dress.key == "name"
    assert t.dress.value == "value"


def test_switch_legacy_compat():
    """switch is still accepted and converted to input + dress internally."""
    kwargs = {
        "module": "builtins",
        "foo": "round",
        "switch": {"Open": ["name", "value"]},
        "params": {"ndigits": 3},
    }
    t = Transform(**kwargs)  # type: ignore
    r = t({"Open": 0.1234})
    assert r == {"name": "Open", "value": 0.123}
    assert t.dress is not None
    assert t.dress.key == "name"
    assert t.dress.value == "value"
    assert t.input == ("Open",)


def test_dress_list_legacy_compat():
    """List-style dress is still accepted and converted to DressConfig."""
    kwargs = {
        "module": "builtins",
        "foo": "round",
        "input": ["Open"],
        "dress": ["name", "value"],
        "params": {"ndigits": 3},
    }
    t = Transform(**kwargs)  # type: ignore
    r = t({"Open": 0.1234})
    assert r == {"name": "Open", "value": 0.123}
    assert t.dress is not None
    assert t.dress.key == "name"


def test_split_keep_part():
    doc = {"id": "https://openalex.org/A123"}

    kwargs = {
        "module": "graflo.util.transform",
        "foo": "split_keep_part",
        "fields": "id",
        "params": {"sep": "/", "keep": -1},
    }
    t = Transform(**kwargs)  # type: ignore
    r = t(doc)
    assert r == {"id": "A123"}


def test_split_keep_part_longer():
    doc = {"doi": "https://doi.org/10.1007/978-3-123"}

    kwargs = {
        "module": "graflo.util.transform",
        "foo": "split_keep_part",
        "fields": "doi",
        "params": {"sep": "/", "keep": [-2, -1]},
    }
    t = Transform(**kwargs)  # type: ignore
    r = t(doc)
    assert r["doi"] == "10.1007/978-3-123"
