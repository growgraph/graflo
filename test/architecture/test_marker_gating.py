"""The opt-in marker gate in ``test/conftest.py`` skips marked tests only.

``item.keywords`` also holds parametrize ids and parent node names, so gating on
it skips an unmarked test whose parameter happens to be called ``nebula``.
"""

from __future__ import annotations

from typing import Any

import pytest

from test.conftest import pytest_collection_modifyitems

GATED_MARKERS = ["bulk_e2e", "performance", "nebula", "kafka", "tigergraph"]
# Prefixed: an id equal to a marker name would itself be caught by the defect under test.
GATE_IDS = [f"gate-{name}" for name in GATED_MARKERS]


class _Config:
    """A run with no ``--run-*`` option given."""

    def getoption(self, option: str) -> bool:
        return False


class _Item:
    def __init__(self, *, keywords: set[str], marks: set[str]) -> None:
        self.keywords = dict.fromkeys(keywords, True)
        self._marks = marks
        self.added: list[Any] = []

    def get_closest_marker(self, name: str) -> Any:
        return pytest.mark.skip if name in self._marks else None

    def add_marker(self, marker: Any) -> None:
        self.added.append(marker)


@pytest.mark.parametrize("name", GATED_MARKERS, ids=GATE_IDS)
def test_a_parameter_named_like_a_marker_is_not_skipped(name: str) -> None:
    item = _Item(keywords={name, "test_something"}, marks=set())

    pytest_collection_modifyitems(_Config(), [item])

    assert item.added == []


@pytest.mark.parametrize("name", GATED_MARKERS, ids=GATE_IDS)
def test_a_marked_test_is_skipped_without_its_option(name: str) -> None:
    item = _Item(keywords={name, "test_something"}, marks={name})

    pytest_collection_modifyitems(_Config(), [item])

    assert len(item.added) == 1
