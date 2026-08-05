"""Layering enforcement: import-time dependencies must respect the layer order.

The package is layered (low to high):

    L0  onto, util, architecture.base
    L1  filter, architecture.graph_types
    L2  architecture.schema
    L3  connections (configs), architecture.contract
    L4  architecture.pipeline, architecture.backend, architecture.evolution,
        data_source, connections.provider
    L5  db, object_storage
    L6  hq, migrate, rdf, plot, cli

Rules enforced here (import-time = top-level, non-``TYPE_CHECKING`` imports):

1. A module may only import from layers at or below its own.
2. ``graflo.connections.provider`` is the one deliberate exception inside
   ``connections``: it resolves contract connectors, so it sits at L4 while its
   sibling config modules sit at L3. The package façade is lazy for this reason.
3. Runtime factories may cross layers **only** via deferred (function-level)
   imports — those are not import-time edges and are not flagged.

If this test fails, either the new import goes the wrong way (fix the code) or
a module genuinely changed layer (update the map here *deliberately*).
"""

from __future__ import annotations

import ast
from pathlib import Path

import graflo

PKG_ROOT = Path(graflo.__file__).parent

# Layer of each package/module prefix. Longest-prefix match wins.
LAYERS: dict[str, int] = {
    "graflo": 0,  # graflo/__init__ (lazy façade) and graflo.onto
    "graflo.onto": 0,
    "graflo.util": 0,
    "graflo.architecture": 4,  # default for the architecture umbrella
    "graflo.architecture.base": 0,
    "graflo.filter": 1,
    "graflo.architecture.graph_types": 1,
    "graflo.architecture.schema": 2,
    # Redundant under longest-prefix matching, but stated so the intent survives:
    # schema context must never reach db/data_source, since its whole premise is
    # answering "what can I ask?" without a live database.
    "graflo.architecture.schema.context": 2,
    # The read contract. Must be stated: prefix matching would otherwise fall
    # through to the L4 `graflo.architecture` default and silently grant it
    # permission to import `db`, which is exactly what it must not do — caps
    # have to be enforceable without a driver present.
    "graflo.architecture.query": 2,
    "graflo.connections": 3,
    "graflo.connections.provider": 4,
    "graflo.architecture.contract": 3,
    "graflo.architecture.pipeline": 4,
    "graflo.architecture.backend": 4,
    "graflo.architecture.evolution": 4,
    "graflo.architecture.util": 1,  # helpers over graph_types only
    "graflo.architecture.onto_sql": 1,  # leaf pydantic models (SQL introspection)
    "graflo.architecture.onto_sample": 2,  # leaf sample models; needs FieldType (L2)
    "graflo.data_source": 4,
    "graflo.db": 5,
    "graflo.object_storage": 5,
    "graflo.hq": 6,
    "graflo.migrate": 6,
    "graflo.rdf": 6,
    "graflo.plot": 6,
    "graflo.cli": 6,
}


def layer_of(module: str) -> int:
    parts = module.split(".")
    for i in range(len(parts), 0, -1):
        prefix = ".".join(parts[:i])
        if prefix in LAYERS:
            return LAYERS[prefix]
    raise AssertionError(f"module {module} not covered by the layer map")


def _resolve_relative(module: str, is_package: bool, node: ast.ImportFrom) -> str:
    base = module.split(".")
    pkg = base if is_package else base[:-1]
    target = pkg[: len(pkg) - (node.level - 1)]
    if node.module:
        target += node.module.split(".")
    return ".".join(target)


class _ImportCollector(ast.NodeVisitor):
    """Collect import-time graflo imports (skips function bodies and TYPE_CHECKING)."""

    def __init__(self, module: str, is_package: bool) -> None:
        self.module = module
        self.is_package = is_package
        self.imports: list[tuple[str, int]] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        return  # deferred imports are allowed

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        return

    def visit_If(self, node: ast.If) -> None:
        if "TYPE_CHECKING" in ast.unparse(node.test):
            for child in node.orelse:
                self.visit(child)
            return
        self.generic_visit(node)

    def _add(self, target: str, lineno: int) -> None:
        if target == "graflo" or target.startswith("graflo."):
            self.imports.append((target, lineno))

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            self._add(alias.name, node.lineno)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.level > 0:
            self._add(
                _resolve_relative(self.module, self.is_package, node), node.lineno
            )
        elif node.module:
            self._add(node.module, node.lineno)


def iter_modules():
    for py in sorted(PKG_ROOT.rglob("*.py")):
        if "__pycache__" in py.parts:
            continue
        rel = py.relative_to(PKG_ROOT.parent)
        module = ".".join(rel.with_suffix("").parts)
        is_package = module.endswith(".__init__")
        if is_package:
            module = module.removesuffix(".__init__")
        yield py, module, is_package


def test_layer_map_covers_every_module():
    for _, module, _ in iter_modules():
        layer_of(module)  # raises on gaps


def test_no_upward_import_time_dependencies():
    violations: list[str] = []
    for py, module, is_package in iter_modules():
        collector = _ImportCollector(module, is_package)
        collector.visit(ast.parse(py.read_text()))
        src_layer = layer_of(module)
        for target, lineno in collector.imports:
            if layer_of(target) > src_layer:
                violations.append(
                    f"{module}:{lineno} (L{src_layer}) imports {target} "
                    f"(L{layer_of(target)}) at import time"
                )
    assert not violations, "upward import-time dependencies:\n" + "\n".join(violations)


def test_contract_never_imports_runtime_at_import_time():
    """The declarative contract must be importable without the pipeline runtime."""
    forbidden_prefixes = (
        "graflo.architecture.pipeline",
        "graflo.db",
        "graflo.data_source",
        "graflo.hq",
    )
    violations: list[str] = []
    for py, module, is_package in iter_modules():
        if not module.startswith("graflo.architecture.contract"):
            continue
        collector = _ImportCollector(module, is_package)
        collector.visit(ast.parse(py.read_text()))
        for target, lineno in collector.imports:
            if target.startswith(forbidden_prefixes):
                violations.append(f"{module}:{lineno} imports {target}")
    assert not violations, "contract imports runtime at import time:\n" + "\n".join(
        violations
    )


def test_facades_expose_public_api_lazily():
    """Public names stay importable through the lazy façades."""
    from graflo import GraphManifest, Schema  # noqa: F401
    from graflo.architecture import DatabaseProfile  # noqa: F401
    from graflo.connections import PostgresConfig  # noqa: F401
    from graflo.data_source import DataSourceFactory  # noqa: F401


def test_contract_import_loads_no_db_driver():
    """Importing the declarative contract must not load any DB driver.

    Runs in a subprocess: in-process ``sys.modules`` is polluted by whichever
    tests ran earlier.
    """
    import subprocess
    import sys

    code = (
        "import sys\n"
        "import graflo.architecture.contract\n"
        "from graflo.connections.onto import PostgresConfig\n"
        "loaded = [m for m in ('arango', 'neo4j', 'confluent_kafka', 'pyTigerGraph',"
        " 'falkordb', 'nebula3', 'mgclient', 'psycopg2') if m in sys.modules]\n"
        "assert not loaded, f'contract import loaded DB drivers: {loaded}'\n"
    )
    subprocess.run([sys.executable, "-c", code], check=True)
