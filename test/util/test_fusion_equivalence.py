"""Fusion must preserve values, aliases and failures across container types."""

from __future__ import annotations

import struct
from copy import deepcopy
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from graflo.architecture.graph_types import VertexRep
from graflo.architecture.graph_types.merge import fuse_doc_basis
from test.hypothesis_budget import examples


def _typed(value: Any) -> Any:
    """Keep ordering and scalar types, including bool/int and float bit patterns."""
    if isinstance(value, dict):
        return (type(value), tuple((_typed(k), _typed(v)) for k, v in value.items()))
    if isinstance(value, (list, tuple)):
        return (type(value), tuple(_typed(v) for v in value))
    if isinstance(value, VertexRep):
        return (type(value), _typed(value.model_dump()))
    if isinstance(value, float):
        return (type(value), struct.pack("!d", value))
    return (type(value), value)


@pytest.mark.parametrize("identity", [None, "", 0, False, -0.0, 2**63, "é · 東京"])
def test_empty_fragments_preserve_typed_values_and_shallow_copy(identity: Any) -> None:
    payload = [{"value": [False, 0, -0.0, None]}]
    doc = {"tenant": "t", "id": identity, "payload": payload}
    docs = [doc, {}, {}, {}]
    original = deepcopy(docs)

    (result,) = fuse_doc_basis(docs, ("tenant", "id"))

    assert _typed(result) == _typed(doc)
    assert _typed(docs) == _typed(original)
    assert result is not doc
    assert result["payload"] is payload
    assert list(result) == ["tenant", "id", "payload"]


def test_keyless_fragments_still_apply_ordered_union() -> None:
    docs = [
        {"zones": [2, 1, 2], "name": "first"},
        {},
        {},
        {"zones": [1, 3], "name": "last"},
    ]
    assert fuse_doc_basis(docs, ("id",), ("zones",)) == [
        {"zones": [2, 1, 3], "name": "last"}
    ]
    assert docs[0]["zones"] == [2, 1, 2]


def test_keyed_single_observation_does_not_normalize_its_list() -> None:
    zones = [2, 1, 2]
    (result,) = fuse_doc_basis([{"id": 0, "zones": zones}, {}, {}], ("id",), ("zones",))
    assert result["zones"] is zones
    assert result["zones"] == [2, 1, 2]


def test_leading_keyless_fields_keep_the_existing_mutation_and_order() -> None:
    leading = {"name": "leading", "extra": 1}
    identified = {"id": "a", "name": "identified"}
    docs = [leading, {}, identified, {}, {"id": "b"}, {"tail": 2}]

    result = fuse_doc_basis(docs, ("id",))

    assert result == [
        {"id": "a", "name": "leading", "extra": 1},
        {"id": "b", "tail": 2},
    ]
    assert identified == {"id": "a", "name": "leading", "extra": 1}
    assert result[0] is not identified
    assert list(result[0]) == ["id", "name", "extra"]


def test_empty_only_observations_still_produce_one_fresh_document() -> None:
    docs = [{}, {}, {}, {}]
    (result,) = fuse_doc_basis(docs, ("id",))
    assert result == {}
    assert all(result is not doc for doc in docs)
    empty: list[dict] = []
    assert fuse_doc_basis(empty, ("id",)) is empty


@pytest.mark.parametrize("identity", [[], {}, [1], ([],)])
def test_empty_fragments_do_not_hide_unhashable_identity_errors(identity: Any) -> None:
    with pytest.raises(TypeError, match="unhashable type"):
        fuse_doc_basis([{"id": identity}, {}, {}], ("id",))


def test_empty_fragments_do_not_hide_unhashable_union_errors() -> None:
    with pytest.raises(TypeError, match="unhashable type: 'dict'"):
        fuse_doc_basis([{"zones": [{}]}, {}, {}], ("id",), ("zones",))


def test_custom_identity_hash_is_still_called_in_the_same_order() -> None:
    class Identity:
        calls = 0

        def __hash__(self) -> int:
            self.calls += 1
            if self.calls == 2:
                raise ValueError("second hash refused")
            return 7

    identity = Identity()
    with pytest.raises(ValueError, match="second hash refused"):
        fuse_doc_basis([{"id": identity}, {}, {}], ("id",))
    assert identity.calls == 2


def test_identity_callback_cannot_hide_an_invalid_appended_observation() -> None:
    docs: list[Any] = []

    class Identity:
        calls = 0

        def __hash__(self) -> int:
            self.calls += 1
            if self.calls == 1:
                docs.append(None)
            return 7

    docs.append({"id": Identity()})
    with pytest.raises(TypeError, match="expected dict, got NoneType"):
        fuse_doc_basis(docs, ("id",))


def test_appended_mapping_subclass_keeps_its_items_callback() -> None:
    docs: list[Any] = []

    class EmptyMapping(dict):
        def items(self) -> Any:
            return [("extra", 2)]

    class Identity:
        calls = 0

        def __hash__(self) -> int:
            self.calls += 1
            if self.calls == 1:
                docs.append(EmptyMapping())
            return 7

    identity = Identity()
    docs.append({"id": identity})
    (result,) = fuse_doc_basis(docs, ("id",))
    assert result == {"id": identity, "extra": 2}


def test_invalid_copy_from_an_appended_mapping_keeps_its_error() -> None:
    docs: list[Any] = []

    class InvalidCopy(dict):
        def copy(self) -> Any:
            return None

    class Identity:
        calls = 0

        def __hash__(self) -> int:
            self.calls += 1
            if self.calls == 1:
                docs.extend([InvalidCopy(id="other"), {}])
            return 7

    docs.append({"id": Identity()})
    with pytest.raises(TypeError, match="expected dict for both target and source"):
        fuse_doc_basis(docs, ("id",))


def test_mapping_subclass_callbacks_and_tags_are_preserved() -> None:
    class Observed(dict):
        def copy(self):
            return Observed({**self, "copied": True})

    original = Observed(id=1)
    (result,) = fuse_doc_basis([original, {}, {}], ("id",))
    assert type(result) is Observed
    assert result == {"id": 1, "copied": True}
    assert original == {"id": 1}
    reps = [
        VertexRep(vertex={"id": 1}, lookup_only=True, find="by_id"),
        VertexRep(vertex={}, lookup_only=True, find="by_id"),
        VertexRep(vertex={"id": 1}, lookup_only=False, find="by_id"),
    ]
    assert [
        (r.vertex, r.lookup_only, r.find) for r in fuse_doc_basis(reps, ("id",))
    ] == [
        ({"id": 1}, True, "by_id"),
        ({"id": 1}, False, "by_id"),
    ]


@pytest.mark.parametrize("bad", [None, 1, VertexRep(vertex={"id": 1})])
def test_invalid_later_observations_are_not_silently_removed(bad: Any) -> None:
    with pytest.raises(TypeError, match="expected dict"):
        fuse_doc_basis([{"id": 1}, {}, {}, bad], ("id",))


def test_custom_key_names_are_checked_even_for_empty_observations() -> None:
    class Key:
        def __hash__(self) -> int:
            raise RuntimeError("key name refused")

    keys: Any = (Key(),)
    with pytest.raises(RuntimeError, match="key name refused"):
        fuse_doc_basis([{}, {}, {}], keys)


def test_key_callback_can_fill_an_earlier_empty_observation() -> None:
    leading: dict[str, int] = {}

    class Key:
        def __hash__(self) -> int:
            return hash("id")

        def __eq__(self, other) -> bool:
            leading["payload"] = 42
            return other == "id"

    key = Key()
    (result,) = fuse_doc_basis([leading, {key: 1}], ("id",))
    assert result["payload"] == 42
    assert result[key] == 1
    assert leading == {"payload": 42}


def test_list_subclass_is_not_read_more_often() -> None:
    class ReadOnce(list):
        reads = 0

        def __len__(self) -> int:
            self.reads += 1
            if self.reads > 1:
                raise RuntimeError("length is read-once")
            return super().__len__()

    docs = ReadOnce([{"id": 1}, {}, {}])
    assert fuse_doc_basis(docs, ("id",)) == [{"id": 1}]


def test_cyclic_payload_keeps_the_existing_shallow_reference() -> None:
    doc: dict[str, Any] = {"id": 1}
    doc["self"] = doc
    (result,) = fuse_doc_basis([doc, {}, {}], ("id",))
    assert result is not doc
    assert result["self"] is doc
    assert doc["self"] is doc


_scalars = st.one_of(
    st.none(), st.booleans(), st.integers(), st.floats(), st.text(max_size=12)
)
_values = st.recursive(
    _scalars,
    lambda leaves: st.one_of(
        st.lists(leaves, max_size=4),
        st.dictionaries(st.sampled_from(["x", "y"]), leaves, max_size=2),
    ),
    max_leaves=12,
)
_docs = st.dictionaries(
    st.sampled_from(["id", "tenant", "zones", "name", "payload"]), _values, max_size=5
)


def _outcome(docs, keys, union):
    try:
        return ("value", _typed(fuse_doc_basis(docs, keys, union)))
    except Exception as error:
        return ("error", type(error), _typed(error.args))


@settings(max_examples=examples(500))
@given(
    first=_docs.filter(bool),
    rest=st.lists(_docs, max_size=8),
    keys=st.sampled_from([(), ("id",), ("tenant", "id"), ("id", "id")]),
    union=st.sampled_from([(), ("zones",), ("id", "zones")]),
)
def test_empty_observations_are_neutral_for_values_errors_and_mutation(
    first, rest, keys, union
) -> None:
    expanded = deepcopy([first, {}, {}, *rest, {}])
    compact = deepcopy([doc for doc in [first, *rest] if doc])

    assert _outcome(expanded, keys, union) == _outcome(compact, keys, union)
    assert _typed([doc for doc in expanded if doc]) == _typed(compact)


class _ObservationList(list):
    """A supported container whose ordinary list behaviour is unchanged."""


@settings(max_examples=examples(500))
@given(
    docs=st.lists(_docs, max_size=12),
    keys=st.sampled_from([(), ("id",), ("tenant", "id"), ("id", "id")]),
    union=st.sampled_from([(), ("zones",), ("id", "zones")]),
)
def test_plain_and_subclassed_lists_agree_on_full_fusion(docs, keys, union) -> None:
    plain = deepcopy(docs)
    general = _ObservationList(deepcopy(docs))
    if not docs:
        # Empty input is returned as-is, including its container type.
        assert fuse_doc_basis(plain, keys, union) is plain
        assert fuse_doc_basis(general, keys, union) is general
        return
    assert _outcome(plain, keys, union) == _outcome(general, keys, union)
    assert _typed(plain) == _typed(list(general))
