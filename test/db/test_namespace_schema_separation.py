"""Cross-backend namespace vs schema separation tests (local/file backends)."""

from __future__ import annotations

import pytest

from graflo.architecture.schema import CoreSchema, GraphMetadata, Schema
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig
from graflo.db.conn import NamespaceNotFoundError, SchemaExistsError
from graflo.db.graflo_backend.config import GraFloBackendConfig
from graflo.db.graflo_backend.connection import GraFloBackendConnection


def _sample_schema() -> Schema:
    return Schema(
        metadata=GraphMetadata(name="demo"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(
                vertices=[
                    Vertex(
                        name="person",
                        properties=[Field(name="id"), Field(name="name")],
                        identity=["id"],
                    )
                ]
            ),
            edge_config=EdgeConfig(
                edges=[Edge(source="person", target="person", relation="knows")]
            ),
        ),
    )


@pytest.fixture
def graflo_schema() -> Schema:
    schema = _sample_schema()
    schema.finish_init()
    return schema


def test_graflo_backend_default_bootstrap(tmp_path, graflo_schema: Schema) -> None:
    out = tmp_path / "backend"
    conn = GraFloBackendConnection(GraFloBackendConfig(output_dir=out))
    conn.init_db(graflo_schema, recreate_schema=False, create_namespace=True)
    assert (out / "schema.yaml").exists()


def test_graflo_backend_missing_namespace_raises(
    tmp_path, graflo_schema: Schema
) -> None:
    out = tmp_path / "missing"
    conn = GraFloBackendConnection(GraFloBackendConfig(output_dir=out))
    with pytest.raises(NamespaceNotFoundError):
        conn.ensure_target_namespace(graflo_schema, create=False)


def test_graflo_backend_precreated_namespace_define_schema(
    tmp_path, graflo_schema: Schema
) -> None:
    out = tmp_path / "precreated"
    out.mkdir()
    conn = GraFloBackendConnection(GraFloBackendConfig(output_dir=out))
    conn.ensure_target_namespace(graflo_schema, create=False)
    conn.apply_target_schema(graflo_schema, recreate=False, create_namespace=False)
    assert (out / "schema.yaml").exists()


def test_graflo_backend_schema_exists_raises(tmp_path, graflo_schema: Schema) -> None:
    out = tmp_path / "backend"
    conn = GraFloBackendConnection(GraFloBackendConfig(output_dir=out))
    conn.init_db(graflo_schema, create_namespace=True)
    with pytest.raises(SchemaExistsError):
        conn.apply_target_schema(graflo_schema, recreate=False)
