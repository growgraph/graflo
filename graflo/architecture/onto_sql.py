from pydantic import BaseModel


class ColumnInfo(BaseModel):
    """Column information from PostgreSQL table."""

    name: str
    type: str
    description: str = ""
    is_nullable: str = "YES"
    column_default: str | None = None
    is_pk: bool = False
    is_unique: bool = False
    ordinal_position: int | None = None
    sample_values: list[str] = []
    """Up to 5 sample values from the column (first rows); empty for vertex/edge ColumnInfo."""


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


class RawTableInfo(BaseModel):
    """Raw table metadata: all tables with columns, types, and constraint metadata."""

    name: str
    schema_name: str
    columns: list[ColumnInfo]
    primary_key: list[str]
    foreign_keys: list[ForeignKeyInfo]
    row_count_estimate: int | None = None
    """Approximate row count from pg_class.reltuples (updated by ANALYZE)."""


class SchemaIntrospectionResult(BaseModel):
    """Result of PostgreSQL schema introspection."""

    vertex_tables: list[VertexTableInfo]
    edge_tables: list[EdgeTableInfo]
    raw_tables: list[RawTableInfo]
    schema_name: str
