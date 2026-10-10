"""A Nebula connection describes a tag when it first writes it, not at connect."""

from __future__ import annotations

from typing import Any

import pytest

from graflo.connections.onto import NebulaConfig
from graflo.db.nebula import conn as nebula_conn


class _Result:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def rows_as_dicts(self) -> list[dict[str, Any]]:
        return self._rows


class _Adapter:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def use_space(self, space: str) -> None:
        self.statements.append(f"USE {space}")

    def execute(self, statement: str) -> _Result:
        self.statements.append(statement)
        if statement == "SHOW TAGS":
            return _Result([{"Name": f"t{i}"} for i in range(50)])
        if statement.startswith("DESCRIBE TAG `missing`"):
            raise RuntimeError("TagNotFound")
        if statement.startswith("DESCRIBE TAG"):
            return _Result([{"Field": "id"}, {"Field": "name"}])
        return _Result([])


@pytest.fixture
def adapter(monkeypatch) -> _Adapter:
    fake = _Adapter()
    monkeypatch.setattr(nebula_conn, "create_adapter", lambda config: fake)
    return fake


def _connect() -> nebula_conn.NebulaConnection:
    return nebula_conn.NebulaConnection(
        NebulaConfig(uri="http://localhost:9669", schema_name="g")
    )


def test_connecting_reads_no_tag_catalogue(adapter: _Adapter) -> None:
    _connect()

    assert adapter.statements == ["USE g"]


def test_a_tag_is_described_once_when_first_needed(adapter: _Adapter) -> None:
    db = _connect()

    assert db._tag_field_names("t1") == ["id", "name"]
    assert db._tag_field_names("t1") == ["id", "name"]
    assert adapter.statements == ["USE g", "DESCRIBE TAG `t1`"]


def test_a_tag_that_does_not_exist_has_no_fields(adapter: _Adapter) -> None:
    db = _connect()

    assert db._tag_field_names("missing") == []
