from pydantic import BaseModel


class ColumnInfo(BaseModel):
    """Column information from PostgreSQL table."""

    name: str
    type: str
    description: str = ""
    is_nullable: str = "YES"
    column_default: str | None = None
    is_pk: bool = False


class ForeignKeyInfo(BaseModel):
    """Foreign key relationship information."""

    column: str
    references_table: str
    references_column: str | None = None
    constraint_name: str | None = None


class VertexTableInfo(BaseModel):
    """Vertex table information from schema introspection."""

    name: str
    schema_name: str
    columns: list[ColumnInfo]
    primary_key: list[str]
    foreign_keys: list[ForeignKeyInfo]


class EdgeTableInfo(BaseModel):
    """Edge table information from schema introspection."""

    name: str
    schema_name: str
    columns: list[ColumnInfo]
    primary_key: list[str]
    foreign_keys: list[ForeignKeyInfo]
    source_table: str
    target_table: str
    source_column: str
    target_column: str
    relation: str | None = None


class SchemaIntrospectionResult(BaseModel):
    """Result of PostgreSQL schema introspection."""

    vertex_tables: list[VertexTableInfo]
    edge_tables: list[EdgeTableInfo]
    schema_name: str
