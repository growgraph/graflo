"""Regression tests for evolution / rewrite import order (avoid circular imports)."""

from __future__ import annotations

import importlib
import subprocess
import sys

import pytest

from graflo.architecture.evolution.rewrite import pipeline_mentions_any_vertex


@pytest.mark.parametrize(
    "snippet",
    [
        "from graflo.architecture.evolution.rewrite import pipeline_mentions_any_vertex",
        "from graflo.hq.sanitizer import Sanitizer",
        (
            "from graflo import EdgeConfig; "
            "from graflo.architecture.evolution.rewrite import pipeline_mentions_any_vertex"
        ),
        (
            "from graflo.architecture.evolution import "
            "apply_add_resource_transforms, alignment_to_ops"
        ),
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


# ── every lazy façade must mean what its __all__ says ───────────────────────

#: The packages that resolve names through a PEP 562 ``__getattr__`` rather
#: than importing them eagerly. Deleting a module is the operation that breaks
#: them: the dispatch branch goes, the ``__all__`` entry stays, and nothing
#: notices until someone stars the import.
LAZY_FACADES = [
    "graflo",
    "graflo.architecture",
    "graflo.architecture.evolution",
    "graflo.db",
    "graflo.data_source",
    "graflo.hq",
    "graflo.connections",
]


@pytest.mark.parametrize("module_name", LAZY_FACADES)
def test_every_exported_name_resolves(module_name: str) -> None:
    """A name in ``__all__`` that does not resolve is a lie `import *` trips on.

    This is not hypothetical: retiring ``evolution/revision.py`` left five
    names advertised with no dispatch behind them, so
    ``from graflo.architecture.evolution import *`` raised ``AttributeError``
    while every ordinary import kept working. Ordinary imports cannot catch it,
    because nobody imports the name that is gone.
    """
    module = importlib.import_module(module_name)
    exported = getattr(module, "__all__", [])
    assert exported, f"{module_name} declares no __all__"

    broken = []
    for name in exported:
        try:
            getattr(module, name)
        except AttributeError:
            broken.append(name)
    assert not broken, (
        f"{module_name}.__all__ advertises names that do not resolve: {broken}. "
        "Either restore the dispatch branch or drop them from __all__."
    )


@pytest.mark.parametrize("module_name", LAZY_FACADES)
def test_star_import_succeeds(module_name: str) -> None:
    """The symptom, asserted directly rather than only through its cause."""
    namespace: dict[str, object] = {}
    exec(f"from {module_name} import *", namespace)  # noqa: S102 - the thing under test
