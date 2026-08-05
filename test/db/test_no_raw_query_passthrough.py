"""Traversal must never route through `Connection.execute`.

`execute` takes a raw backend query. The moment a traversal path calls it, the
agent-facing surface has a passthrough — not because any current caller passes
a query string through, but because the next one can, and nothing would catch it.

This is a guard rail, not a proof: it checks the shared traversal module, which
is the only code path every backend's `graph_neighbors` default goes through.
Per-backend native overrides legitimately call `execute` — that is what a native
query *is* — and are excluded deliberately rather than by oversight. What keeps
them honest is that they build their query from a typed builder, never from
caller input, which the query-shape suites assert.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

#: Modules that compose backend primitives rather than issuing queries.
GENERIC_TRAVERSAL_MODULES = [
    "graflo/db/traversal.py",
]


def _calls(source: str) -> list[str]:
    """Every attribute call in *source*, as dotted names."""
    names: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            names.append(node.func.attr)
    return names


@pytest.mark.parametrize("relative", GENERIC_TRAVERSAL_MODULES)
def test_generic_traversal_never_calls_execute(relative: str) -> None:
    path = Path(__file__).resolve().parents[2] / relative
    assert path.exists(), f"{relative} moved; update this guard rather than deleting it"
    assert "execute" not in _calls(path.read_text()), (
        f"{relative} calls .execute(): traversal must compose fetch_edges and "
        "fetch_docs, so that no raw backend query is reachable from an "
        "agent-facing path"
    )


def test_the_query_contract_does_not_import_db() -> None:
    """Caps must be enforceable without a driver.

    If `architecture/query` reached into `db/`, enforcement would only hold
    wherever a driver happened to be importable, and the layering test's L2
    entry would be a comment rather than a constraint.
    """
    root = Path(__file__).resolve().parents[2] / "graflo/architecture/query"
    for module in sorted(root.glob("*.py")):
        tree = ast.parse(module.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                assert not node.module.startswith("graflo.db"), (
                    f"{module.name} imports {node.module}"
                )
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    assert not alias.name.startswith("graflo.db"), (
                        f"{module.name} imports {alias.name}"
                    )
