"""The ``bulk`` extra is optional: nothing loads it until a bulk run needs it."""

from __future__ import annotations

import subprocess
import sys

import pytest

from graflo.hq.bulk import _deps
from graflo.util.extras import MissingExtraError


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
    monkeypatch.setitem(sys.modules, "duckdb", None)  # blocks the import
    with pytest.raises(MissingExtraError, match=r"`bulk` extra.*'duckdb'"):
        _deps.require()
