"""Tests for TigerGraph namespace vs schema separation."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from graflo.db.conn import NamespaceNotFoundError, SchemaExistsError
from graflo.db.tigergraph.graph_admin import GraphAdmin


def _make_admin(
    *, graph_exists: bool = False, has_artifacts: bool = False
) -> GraphAdmin:
    conn = MagicMock()
    conn._configured_graph_name.return_value = "test_graph"
    conn.config.database = "test_graph"
    conn.config.schema_name = "test_graph"
    conn.graph_exists.return_value = graph_exists
    conn._get_graph_type_names.return_value = (
        {"Person"} if has_artifacts else set(),
        {"knows"} if has_artifacts else set(),
    )
    admin = GraphAdmin(conn)
    admin.schema_has_artifacts = MagicMock(return_value=has_artifacts)  # type: ignore[method-assign]
    return admin


def test_ensure_create_false_missing_graph_raises():
    admin = _make_admin(graph_exists=False)
    schema = MagicMock()
    schema.metadata.name = "test_graph"

    with pytest.raises(NamespaceNotFoundError, match="does not exist"):
        admin.ensure_target_namespace(schema, create=False)

    admin._conn.create_database.assert_not_called()


def test_ensure_create_true_creates_missing_graph():
    admin = _make_admin(graph_exists=False)
    schema = MagicMock()
    schema.metadata.name = "test_graph"

    admin.ensure_target_namespace(schema, create=True)

    admin._conn.create_database.assert_called_once_with("test_graph")


def test_ensure_create_true_existing_empty_graph_noop():
    admin = _make_admin(graph_exists=True)
    schema = MagicMock()
    schema.metadata.name = "test_graph"

    admin.ensure_target_namespace(schema, create=True)

    admin._conn.create_database.assert_not_called()


def test_apply_precreated_empty_graph_succeeds():
    admin = _make_admin(graph_exists=True, has_artifacts=False)
    admin.define_indexes = MagicMock()
    schema = MagicMock()
    schema.metadata.name = "test_graph"

    admin.apply_target_schema(schema, recreate=False, create_namespace=False)

    admin._conn._define_schema_local.assert_called_once_with(schema)
    admin.define_indexes.assert_called_once_with(schema)


def test_apply_schema_exists_raises():
    admin = _make_admin(graph_exists=True, has_artifacts=True)
    schema = MagicMock()
    schema.metadata.name = "test_graph"

    with pytest.raises(SchemaExistsError, match="Schema already exists"):
        admin.apply_target_schema(schema, recreate=False, create_namespace=False)


def test_apply_recreate_without_dropping_graph_shell():
    admin = _make_admin(graph_exists=True, has_artifacts=True)
    schema = MagicMock()
    schema.metadata.name = "test_graph"
    admin._conn._get_all_graph_names.return_value = ["other_graph"]
    admin.define_indexes = MagicMock()

    admin.apply_target_schema(schema, recreate=True, create_namespace=False)

    admin._conn.delete_database.assert_not_called()
    admin._conn._drop_global_schema_types.assert_called_once()
    admin._conn._define_schema_local.assert_called_once_with(schema)


def test_apply_recreate_with_create_namespace_drops_graph():
    admin = _make_admin(graph_exists=True, has_artifacts=True)
    schema = MagicMock()
    schema.metadata.name = "test_graph"
    admin._conn.graph_exists.side_effect = [True, False]
    admin._conn._get_all_graph_names.return_value = []
    admin._conn._snapshot_all_queries.return_value = {}
    admin.define_indexes = MagicMock()

    admin.apply_target_schema(schema, recreate=True, create_namespace=True)

    admin._conn.delete_database.assert_called_once_with("test_graph")
    admin._conn.create_database.assert_called_once_with("test_graph")


def test_init_db_create_false_never_calls_create_database_on_missing():
    admin = _make_admin(graph_exists=False)
    schema = MagicMock()
    schema.metadata.name = "test_graph"

    with pytest.raises(NamespaceNotFoundError):
        admin.init_db(schema, create_namespace=False)

    admin._conn.create_database.assert_not_called()
