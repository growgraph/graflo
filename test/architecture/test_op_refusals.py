"""Every op precondition refuses with an :class:`OpRefusal` of a registered kind.

A refusal is compared by its ``check``, never by its prose, so a handler that
raises a bare ``ValueError`` leaves a refusal nobody can classify. This scans
the modules that apply evolution ops and fails on any ``raise`` that is not an
``OpRefusal`` with a literal, registered ``check``, a re-raise, or a listed
internal invariant.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

import graflo
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution.apply import apply_evolution
from graflo.architecture.evolution.codec import op_from_dict
from graflo.architecture.refusal import REFUSAL_KINDS, OpRefusal

EVOLUTION = Path(graflo.__file__).parent / "architecture" / "evolution"

#: The modules whose functions run when an op is applied.
HANDLER_MODULES = [
    "apply.py",
    "db_profile.py",
    "identity.py",
    "ingestion.py",
    "inverse_edges.py",
    "physical.py",
    "project.py",
    "rewrite.py",
    "semantics.py",
    "structure.py",
]

#: ``(module, function, exception)`` raises that are not refusals of user input.
ALLOWED = {
    # An op type the dispatcher does not know: a programming error.
    ("apply.py", "_dispatch_op", "TypeError"),
    # Internal invariants: states no op input can reach.
    ("rewrite.py", "rewrite_vertex_names_in_step", "ValueError"),
    ("rewrite.py", "cast_step", "TypeError"),
    # Reached only from merge, which has its own refusal classes.
    ("db_profile.py", "_merge_vertex_default_maps", "ValueError"),
    ("db_profile.py", "_merge_declared_scalar", "ValueError"),
}

#: ``(module, function)`` whose ``check=`` is a ``RefusalKind`` computed elsewhere.
COMPUTED_CHECK = {
    ("ingestion.py", "apply_set_inverse_emission"),
}


def _violations(path: Path) -> list[str]:
    found: list[str] = []
    tree = ast.parse(path.read_text(), filename=str(path))

    def visit(node: ast.AST, function: str) -> None:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            function = node.name
        if isinstance(node, ast.Raise) and node.exc is not None:
            problem = _problem(path.name, function, node.exc)
            if problem:
                found.append(f"{path.name}:{node.lineno} in {function}: {problem}")
        for child in ast.iter_child_nodes(node):
            visit(child, function)

    visit(tree, "<module>")
    return found


def _problem(module: str, function: str, exc: ast.expr) -> str | None:
    if isinstance(exc, ast.Name):
        return None  # re-raise of a caught exception
    if not isinstance(exc, ast.Call) or not isinstance(exc.func, ast.Name):
        return f"unrecognised raise {ast.unparse(exc)[:60]}"
    name = exc.func.id
    if name != "OpRefusal":
        if (module, function, name) in ALLOWED:
            return None
        return f"raises {name}, not OpRefusal"
    check = next((kw.value for kw in exc.keywords if kw.arg == "check"), None)
    if check is None:
        return "OpRefusal without check="
    if isinstance(check, ast.Constant) and isinstance(check.value, str):
        if check.value in REFUSAL_KINDS:
            return None
        return f"check={check.value!r} is not a registered kind"
    if (module, function) in COMPUTED_CHECK:
        return None
    return "check= is not a string literal"


@pytest.mark.parametrize("module", HANDLER_MODULES)
def test_every_raise_in_a_handler_module_is_a_registered_op_refusal(
    module: str,
) -> None:
    path = EVOLUTION / module
    assert path.exists(), f"{module} moved; update HANDLER_MODULES"
    assert _violations(path) == []


def test_every_allowlisted_raise_still_exists() -> None:
    """An allowlist entry for code that is gone hides the next real violation."""
    present: set[tuple[str, str, str]] = set()
    for module in {module for module, _, _ in ALLOWED}:
        tree = ast.parse((EVOLUTION / module).read_text())
        for function in ast.walk(tree):
            if not isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for node in ast.walk(function):
                if (
                    isinstance(node, ast.Raise)
                    and isinstance(node.exc, ast.Call)
                    and isinstance(node.exc.func, ast.Name)
                ):
                    present.add((module, function.name, node.exc.func.id))
    assert ALLOWED <= present


# ── behaviour: kind and subjects on representative refusals ─────────────────


def _manifest(
    edges: list[dict] | None = None, with_schema: bool = True
) -> GraphManifest:
    payload: dict = {} if with_schema else {"ingestion_model": {"resources": []}}
    if with_schema:
        payload["schema"] = {
            "metadata": {"name": "t"},
            "graph": {
                "vertex_config": {
                    "vertices": [
                        {
                            "name": name,
                            "properties": [{"name": "id"}, {"name": "name"}],
                            "identity": ["id"],
                        }
                        for name in ("person", "company")
                    ]
                },
                "edge_config": {"edges": edges or []},
            },
        }
    return GraphManifest.from_dict(payload)


KNOWS = {"source": "person", "target": "company", "relation": "knows"}


@pytest.mark.parametrize(
    ("op", "edges", "check", "subjects"),
    [
        (
            {"op": "remove_vertices", "names": ["ghost"]},
            None,
            "unknown-vertex",
            ("ghost",),
        ),
        (
            {"op": "add_edge_properties", "additions": {"ghost": ["w"]}},
            [KNOWS],
            "unknown-relation",
            ("ghost",),
        ),
        (
            {
                "op": "remove_edges",
                "edges": [
                    {"source": "person", "target": "person", "relation": "knows"}
                ],
            },
            [KNOWS],
            "unknown-edge",
            ('["person","person","knows"]',),
        ),
        (
            {"op": "remove_vertex_properties", "removals": {"person": ["ghost"]}},
            None,
            "unknown-property",
            ("person.ghost",),
        ),
        (
            {"op": "remove_vertex_properties", "removals": {"person": ["id"]}},
            None,
            "identity-field",
            ("person.id",),
        ),
        (
            {"op": "rename_vertices", "renames": {"person": "company"}},
            None,
            "name-collision",
            ("company",),
        ),
        (
            {"op": "add_edges", "edges": [KNOWS]},
            [KNOWS],
            "already-exists",
            ('["person","company","knows"]',),
        ),
    ],
)
def test_a_refusal_names_its_kind_and_subjects(
    op: dict, edges: list[dict] | None, check: str, subjects: tuple[str, ...]
) -> None:
    with pytest.raises(OpRefusal) as info:
        apply_evolution(_manifest(edges), [op_from_dict(op)])
    assert info.value.check == check
    assert info.value.subjects == subjects


def test_an_op_on_a_missing_block_is_refused_as_such() -> None:
    op = op_from_dict({"op": "add_vertex_properties", "additions": {"person": ["x"]}})
    with pytest.raises(OpRefusal) as info:
        apply_evolution(_manifest(with_schema=False), [op])
    assert info.value.check == "missing-block"
    assert info.value.subjects == ("schema",)


def test_a_result_that_fails_validation_is_refused_as_invalid_result() -> None:
    """A vertex made its own superclass is no precondition of the op; the result is invalid."""
    op = op_from_dict(
        {
            "op": "set_vertex_subclass_of",
            "subclass_of": {"person": [{"vertex": "person"}]},
        }
    )
    with pytest.raises(OpRefusal) as info:
        apply_evolution(_manifest(), [op])
    assert info.value.check == "invalid-result"
