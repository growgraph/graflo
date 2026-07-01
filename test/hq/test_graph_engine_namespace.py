"""Unit tests for GraphEngine namespace/schema separation."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from graflo.hq.graph_engine import GraphEngine
from graflo.onto import DBType


@patch("graflo.hq.graph_engine.ConnectionManager")
def test_define_schema_default_calls_ensure_and_apply(mock_cm: MagicMock) -> None:
    conn = MagicMock()
    mock_cm.return_value.__enter__.return_value = conn
    engine = GraphEngine(target_db_flavor=DBType.TIGERGRAPH)
    manifest = MagicMock()
    schema = MagicMock()
    manifest.require_schema.return_value = schema
    target_config = MagicMock()
    target_config.connection_type = DBType.TIGERGRAPH
    target_config.can_be_target.return_value = True

    engine.define_schema(manifest, target_config)

    conn.ensure_target_namespace.assert_called_once()
    _, kwargs = conn.ensure_target_namespace.call_args
    assert kwargs["create"] is True
    conn.apply_target_schema.assert_called_once()
    apply_kwargs = conn.apply_target_schema.call_args.kwargs
    assert apply_kwargs["recreate"] is False
    assert apply_kwargs["create_namespace"] is True


@patch("graflo.hq.graph_engine.ConnectionManager")
def test_define_schema_no_create_namespace(mock_cm: MagicMock) -> None:
    conn = MagicMock()
    mock_cm.return_value.__enter__.return_value = conn
    engine = GraphEngine(target_db_flavor=DBType.TIGERGRAPH)
    manifest = MagicMock()
    schema = MagicMock()
    manifest.require_schema.return_value = schema
    target_config = MagicMock()
    target_config.connection_type = DBType.TIGERGRAPH
    target_config.can_be_target.return_value = True

    engine.define_schema(manifest, target_config, create_namespace=False)

    _, kwargs = conn.ensure_target_namespace.call_args
    assert kwargs["create"] is False
    assert conn.apply_target_schema.call_args.kwargs["create_namespace"] is False


@patch("graflo.hq.graph_engine.ConnectionManager")
def test_create_target_namespace_only_ensure(mock_cm: MagicMock) -> None:
    conn = MagicMock()
    mock_cm.return_value.__enter__.return_value = conn
    engine = GraphEngine(target_db_flavor=DBType.TIGERGRAPH)
    manifest = MagicMock()
    schema = MagicMock()
    manifest.require_schema.return_value = schema
    target_config = MagicMock()
    target_config.connection_type = DBType.TIGERGRAPH
    target_config.can_be_target.return_value = True

    engine.create_target_namespace(manifest, target_config)

    conn.ensure_target_namespace.assert_called_once()
    assert conn.ensure_target_namespace.call_args.kwargs["create"] is True
    conn.apply_target_schema.assert_not_called()
