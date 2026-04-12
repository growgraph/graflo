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
| **`view` with `kind="select"`** | Full control: optional `from` (defaults to `table_name`), `joins`, explicit `select` (``all_base``, structured `base` / `from_join`, simple column names, or legacy `expr`/`alias`), optional `base_alias` (default `base`), `where` as `FilterExpression`. Use when `type_lookup` is not expressive enough. |
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

Expanded SQL selects (conceptually; base row alias defaults to `base`):

- `base.source_id AS source_id`, `s.<type_column> AS source_type`
- `base.target_id AS target_id`, `t.<type_column> AS target_type`
- `base.relation AS relation` when `relation` is set

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
explicit `select` list and optional `where`.

### Base table name

You do **not** need to repeat the base table: omit `from` when it is the same as
the connector’s `table_name`. `SelectSpec.build_sql()` uses `table_name` as the
`FROM` target in that case. Set `from` only when the queried object differs
(e.g. a synonym or a view name that is not `table_name`).

### Base row alias (`base_alias`)

`SelectSpec.base_alias` and `TableConnector.base_alias` default to **`base`**: that
is the SQL identifier used for the base table row whenever joins are generated
(`FROM "schema"."table" base ...`). Override only if you need a different name or
to avoid a clash with a join alias. `where` clauses that use qualified fields
should use this name (e.g. `base.tenant_id`).

### Ergonomic `select` items

Each `select` entry can be:

- **`all_base`** — all columns from the base row: expands to `base.*` when joins
  are present (using your `base_alias`), or plain `*` when there are no joins.
  This is the default single entry when `select` is omitted; prefer it over raw
  `*` when joining so you do not accidentally select every column from every
  joined table.
- A **simple identifier string** (letters, digits, underscore): a column on the
  **base** row. When `joins` are present, it is emitted as `base."column"` (no
  need to type the alias in YAML).
- `*` or any string that is **not** a simple identifier (expressions, quoted SQL,
  `base.*`, etc.) is passed through unchanged.
- A dict **`{ base: <col>, as: <output> }`** — base-table column with an optional
  output alias (`alias` is accepted as well as `as`).
- A dict **`{ from_join: <join_alias>, column: <col>, as: <output> }`** — column
  from a joined table; `from_join` must match the `alias` (or table name) of a
  `joins` entry.
- Legacy dict **`{ expr: "...", alias: ... }`** for arbitrary SQL expressions.

Example (two joins to the same lookup table, no duplicated `from`, no manual
base alias in `select`):

```yaml
connectors:
  - name: relations_enriched
    table_name: relations
    schema_name: public
    view:
      kind: select
      select:
        - { base: source_id, as: source_id }
        - { from_join: s, column: kind, as: source_type }
        - { base: target_id, as: target_id }
        - { from_join: t, column: kind, as: target_type }
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
        field: base.tenant_id
        cmp_operator: "=="
        value: ["acme"]
```

`where` still uses SQL flavor and may reference `base` / join aliases where needed.

### Legacy `expr` style

You can still spell projections as free-form SQL:

```yaml
select:
  - expr: "s.kind"
    alias: source_type
```

### Composing two `SelectSpec`-shaped fragments

`TableConnector` supports **one** `view` per connector (one SQL query). To reuse
logic in two places you can:

1. **`SelectSpec.concat_select_parts(head, *tail)`** (Python): merges
   `kind="select"` specs by concatenating `joins` and `select`. The **head** may
   set `from` and `where`; each **tail** must omit `from` and `where` so the base
   table and filters stay on the head. At build time, `from` still defaults to
   `TableConnector.table_name` when omitted.

2. **YAML anchors** (no code): define a reusable join block and reference it
   twice, or split `joins` / `select` across anchors the same way you would
   duplicate list entries.

Sketch (Python):

```python
from graflo.filter.select import SelectSpec

ci_lookup = SelectSpec(
    kind="select",
    joins=[
        {
            "table": "all_classes",
            "alias": "ci_types",
            "on_self": "ci_id",
            "on_other": "sys_id",
        }
    ],
    select=[
        {"from_join": "ci_types", "column": "type_name", "as": "ci_type"},
    ],
)
app_lookup = SelectSpec(
    kind="select",
    joins=[
        {
            "table": "all_classes",
            "alias": "app_types",
            "on_self": "app_id",
            "on_other": "sys_id",
        }
    ],
    select=[
        {"base": "app_id"},
        {"from_join": "app_types", "column": "type_name", "as": "app_type"},
    ],
)
view = SelectSpec.concat_select_parts(
    SelectSpec(
        kind="select",
        select=[{"base": "ci_id"}],
    ),
    ci_lookup,
    app_lookup,
)
# TableConnector(..., table_name="incidents", view=view)
```

The exact YAML shape for `where` follows `FilterExpression` conventions used
elsewhere (see [Transforms](transforms.md) and filter docs).

## Summary

- **`TableConnector.view`** + **`SelectSpec`** keeps multi-table SQL **declarative**
  and aligned with `build_query()` / SQL data sources.
- **`type_lookup`** is the user-friendly path for **polymorphic relations +
  type lookup** feeding **`EdgeRouterActor`**.
- **`kind="select"`** covers **asymmetric** or **non-standard** join/select logic
  without giving up structured config; **`from`** defaults to **`table_name`**;
  **`all_base`**, **`base` / `from_join`**, and default **`base_alias`** reduce noise
  versus raw `expr` and ad-hoc SQL aliases.
- **`SelectSpec.concat_select_parts`** merges join/select fragments when you want
  multiple **`SelectSpec`**-shaped pieces composed in code.
- Edge **`EdgeActor`** auto-join in HQ is orthogonal; set `view` or explicit
  `joins` if you need full control over the SQL for that resource.
