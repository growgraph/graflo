# Table connector views and `SelectSpec`

`TableConnector` normally describes a **base table** plus optional declarative
`JoinClause` rows, `FilterExpression` filters, and `select_columns`. For richer SQL
(polymorphic relation rows joined to lookup tables, asymmetric joins, or a
fully custom `SELECT` list), set **`view`** to a **`SelectSpec`**.

`SelectSpec` is a structured, YAML-friendly alternative to embedding a raw SQL
string in the manifest. `TableConnector.build_query()` either:

- uses **`view`** when present (delegates to `SelectSpec.build_sql()`), or
- builds SQL from `table_name` + `joins` + filters (the default path).

Implementation references:

- `graflo.architecture.contract.bindings.TableConnector` (`view`, `build_query`)
- `graflo.filter.select.SelectSpec` (`kind="type_lookup"` | `kind="select"`)

## When to use `view` vs plain `joins`

| Approach | Best for |
| -------- | -------- |
| **`joins` only** | One or more `JoinClause` rows; base table column `ON` join keys; optional `alias` when the same physical table appears twice. |
| **`view` with `kind="type_lookup"`** | A **fact** table (e.g. relations) plus **lookup** table(s) for endpoint types: emits `source_id`, `source_type`, `target_id`, `target_type`, and optional `relation` with a fixed pattern. Supports **per-side** lookup tables/columns via `source_table`, `target_table`, `source_identity`, `target_identity`, `source_type_column`, `target_type_column`. |
| **`view` with `kind="select"`** | Full control: `from`, `joins`, explicit `select` (including `expr`/`alias`), `where` as `FilterExpression`. Use when `type_lookup` is not expressive enough. |
| **Database VIEW** | Same logical outcome as `select`, but SQL owned by the DBA; `TableConnector` points at the view name as `table_name` and omits `view` / `joins`. |

## Interaction with edge auto-join

`enrich_edge_connector_with_joins` (HQ `RegistryBuilder`) adds `JoinClause` rows
for resources whose pipeline uses **`EdgeActor`** steps
with `match_source` / `match_target`. It runs only when the connector has
**no** `view` and **no** pre-existing `joins`.

Pipelines that use **`EdgeRouterActor`** do not participate in that heuristic.
For polymorphic edges, prefer **`type_lookup`** (or `select`) on `TableConnector.view`
so each row already carries `source_type` / `target_type` (and `relation`) for
`edge_router` configuration—see [Example 7 – Polymorphic objects and relations](../examples/example-7.md).

## `kind="type_lookup"` (shorthand)

Declare the lookup table, identity column, type discriminator, FK columns on
the base (relation) table, and optional relation column:

```yaml
connectors:
  - name: relations_enriched
    table_name: relations
    schema_name: public
    view:
      kind: type_lookup
      table: objects
      identity: id
      type_column: type
      source: source_id
      target: target_id
      relation: relation
```

Expanded SQL selects (conceptually):

- `r.source_id AS source_id`, `s.<type_column> AS source_type`
- `r.target_id AS target_id`, `t.<type_column> AS target_type`
- `r.relation AS relation` when `relation` is set

Pair this with an `edge_router` step whose field names match those aliases, for example:

```yaml
ingestion_model:
  resources:
    - name: relations
      pipeline:
        - edge_router:
            source_type_field: source_type
            target_type_field: target_type
            source_fields:
              id: source_id
            target_fields:
              id: target_id
            relation_field: relation
            type_map:
              Car: car
              Person: person
```

## `kind="select"` (full declarative query)

Use the same building blocks as `TableConnector.joins` (`JoinClause`), plus an
explicit `select` list and optional `where`:

```yaml
view:
  kind: select
  from: relations
  select:
    - r.source_id
    - r.target_id
    - expr: "s.kind"
      alias: source_type
    - expr: "t.kind"
      alias: target_type
  joins:
    - table: object_dim
      alias: s
      on_self: source_id
      on_other: id
      join_type: LEFT
    - table: object_dim
      alias: t
      on_self: target_id
      on_other: id
      join_type: LEFT
  where:
    kind: leaf
    field: r.tenant_id
    cmp_operator: "=="
    value: ["acme"]
```

The exact YAML shape for `where` follows `FilterExpression` conventions used
elsewhere (see [Transforms](transforms.md) and filter docs).

## Summary

- **`TableConnector.view`** + **`SelectSpec`** keeps multi-table SQL **declarative**
  and aligned with `build_query()` / SQL data sources.
- **`type_lookup`** is the user-friendly path for **polymorphic relations +
  type lookup** feeding **`EdgeRouterActor`**.
- **`kind="select"`** covers **asymmetric** or **non-standard** join/select logic
  without giving up structured config.
- Edge **`EdgeActor`** auto-join in HQ is orthogonal; set `view` or explicit
  `joins` if you need full control over the SQL for that resource.
