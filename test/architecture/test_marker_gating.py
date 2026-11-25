"""The opt-in marker gate in ``test/conftest.py`` skips marked tests only.

``item.keywords`` also holds parametrize ids and parent node names, so gating on
it skips an unmarked test whose parameter happens to be called ``nebula``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from test.conftest import pytest_collection_modifyitems

GATED_MARKERS = ["performance", "nebula", "kafka", "tigergraph"]
# Prefixed: an id equal to a marker name would itself be caught by the defect under test.
GATE_IDS = [f"gate-{name}" for name in GATED_MARKERS]


class _Config:
    """A run with no ``--run-*`` option given."""

    def getoption(self, option: str) -> bool:
        return False


TEST_ROOT = Path(__file__).parents[1]


class _Item:
    def __init__(
        self, *, keywords: set[str], marks: set[str], path: Path = Path(__file__)
    ) -> None:
        self.keywords = dict.fromkeys(keywords, True)
        self._marks = marks
        self.path = path
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


def test_a_live_service_file_runs_on_one_worker() -> None:
    item = _Item(
        keywords={"test_something"},
        marks=set(),
        path=TEST_ROOT / "data_source" / "test_kafka_data_source.py",
    )

    pytest_collection_modifyitems(_Config(), [item])

    [group] = item.added
    assert group.name == "xdist_group"
    assert group.args == ("data_source/test_kafka_data_source.py",)


def test_a_pure_test_is_not_grouped() -> None:
    item = _Item(keywords={"test_something"}, marks=set())

    pytest_collection_modifyitems(_Config(), [item])

    assert item.added == []
