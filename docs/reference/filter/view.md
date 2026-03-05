# `graflo.filter.view`

Declarative SQL-like view specifications for advanced filtering and projection of data before feeding into Resources.

## SelectSpec

`SelectSpec` describes queries in a structured way, similar to `FilterExpression`. It is an alternative to `TablePattern`'s `table_name` + `joins` + `filters` — use `view: SelectSpec` when you need full control over the SQL query.

**Two modes:**

- **`kind="select"`** — Full SQL-like spec with `from`, `joins`, `select`, and `where` (using `FilterExpression`). Use for custom multi-table queries with explicit column selection and filters.
- **`kind="type_lookup"`** — Shorthand for edge tables where source/target types come from a lookup table via FK joins. Specify `table`, `identity`, `type_column`, `source`, `target`, and optional `relation` to auto-build JOINs that resolve entity types from a discriminator table.

**Usage with TablePattern:**

```yaml
# In a TablePattern (e.g. when building patterns for SQL ingestion)
view:
  kind: type_lookup
  table: entity_types
  identity: entity_id
  type_column: type_name
  source: parent
  target: child
  relation: link_type
```

`ResourceMapper.create_patterns_from_postgres()` automatically creates `SelectSpec` views for edge tables when type lookup metadata is available.

::: graflo.filter.view
