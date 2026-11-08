"""The ``bulk`` extra is optional: nothing loads it until a bulk run needs it."""

from __future__ import annotations

import builtins
import subprocess
import sys

import pytest

from graflo.hq.bulk import _deps


def test_importing_graflo_loads_no_duckdb() -> None:
    code = (
        "import sys, graflo, graflo.hq, graflo.hq.caster, graflo.hq.graph_engine; "
        "import graflo.hq.bulk.stage, graflo.hq.bulk.fold; "
        "print(sorted(m for m in ('duckdb', 'zstandard') if m in sys.modules))"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    assert out.stdout.strip() == "[]"


def test_a_missing_extra_is_named(monkeypatch) -> None:
    real_import = builtins.__import__

    def _no_duckdb(name, *args, **kwargs):
        if name == "duckdb":
            raise ModuleNotFoundError("No module named 'duckdb'", name="duckdb")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _no_duckdb)
    with pytest.raises(RuntimeError, match=r"`bulk` extra.*'duckdb'"):
        _deps.require()
