import pytest

from graflo.util.misc import sorted_dicts


def test_a_list_of_flat_dicts_is_sorted_by_items():
    assert sorted_dicts([{"y": 2}, {"x": 1}]) == [{"x": 1}, {"y": 2}]


def test_lists_of_flat_dicts_nested_in_a_dict_are_sorted():
    data = {"b": 2, "c": [{"y": 2}, {"x": 1}], "d": {"e": [{"k": 2}, {"k": 1}]}}
    assert sorted_dicts(data) == {
        "b": 2,
        "c": [{"x": 1}, {"y": 2}],
        "d": {"e": [{"k": 1}, {"k": 2}]},
    }


@pytest.mark.parametrize(
    "value",
    [
        [3, 1, 2],
        ["b", "a"],
        [[2], [1]],
        [{"k": 2}, 1],
        [{"fields": ["b", "a"]}, {"fields": ["a"]}],
        [],
    ],
)
def test_any_other_list_is_returned_unchanged(value):
    assert sorted_dicts(value) == value


def test_a_scalar_list_in_a_dict_keeps_its_order():
    assert sorted_dicts({"c": [3, 1, 2]}) == {"c": [3, 1, 2]}
