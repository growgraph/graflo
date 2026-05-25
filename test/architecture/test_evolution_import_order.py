"""Regression tests for evolution / rewrite import order (avoid circular imports)."""

from __future__ import annotations

import subprocess
import sys

import pytest

from graflo.architecture.evolution.rewrite import pipeline_mentions_any_vertex


@pytest.mark.parametrize(
    "snippet",
    [
        "from graflo.architecture.evolution.rewrite import pipeline_mentions_any_vertex",
        "from graflo.hq.sanitizer import Sanitizer",
        "from graflo import EdgeConfig; "
        "from graflo.architecture.evolution.rewrite import pipeline_mentions_any_vertex",
    ],
)
def test_critical_imports_in_clean_subprocess(snippet: str) -> None:
    """Fresh interpreter avoids false negatives from cached partially-initialized modules."""
    proc = subprocess.run(
        [sys.executable, "-c", snippet],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, (proc.stdout, proc.stderr)


def test_pipeline_mentions_empty_names_false() -> None:
    assert pipeline_mentions_any_vertex([{"vertex": "users"}], set()) is False


def test_pipeline_mentions_vertex_step() -> None:
    assert pipeline_mentions_any_vertex([{"vertex": "users"}], {"users"}) is True
    assert pipeline_mentions_any_vertex([{"vertex": "orders"}], {"users"}) is False


def test_pipeline_mentions_edge_step() -> None:
    step = {"type": "edge", "source": "users", "target": "orders"}
    assert pipeline_mentions_any_vertex([step], {"users"}) is True


def test_pipeline_mentions_descend_nested() -> None:
    step = {"type": "descend", "pipeline": [{"vertex": "users"}]}
    assert pipeline_mentions_any_vertex([step], {"users"}) is True
