# Table filters and views

A [connector](../glossary.md#connector) that reads a PostgreSQL table hands every row of the table to its [resource](../glossary.md#resource). Often you want fewer rows, or rows that carry columns from other tables: only the open work orders, or links between pieces of equipment that say what kind of equipment sits at each end. This page shows how to declare that on the table connector, so that the database filters and joins the rows before GraFlo reads them.

## A first example

A plant's maintenance database has a `work_orders` table. This connector reads only the open work orders:

```yaml
bindings:
  connectors:
    - name: open_work_orders
      table_name: work_orders
      filters:
        - {field: status, cmp_operator: "==", value: open}
  resource_connector:
    - {resource: work_orders, connector: open_work_orders}
```

GraFlo turns the connector into one SQL query and reads the result in batches:

```sql
SELECT * FROM "public"."work_orders" WHERE "status" = 'open'
```

The schema in the query is the connector's `schema_name`, else the schema of the connection settings, else `public`. To check the query a connector runs, build the connector in Python and call `build_query()`:

```python
from graflo.architecture.contract.bindings import TableConnector

connector = TableConnector(
    name="open_work_orders",
    table_name="work_orders",
    filters=[{"field": "status", "cmp_operator": "==", "value": "open"}],
)
print(connector.build_query())
```

## Filter rows with `filters` { #bindings-filter-cookbook-tableconnectorfilters }

Each entry of `filters` is one condition. A condition on one column has three keys:

- `field`: the column.
- `cmp_operator`: one of `==`, `!=`, `>`, `>=`, `<`, `<=`, `IS_NULL`, `IS_NOT_NULL`.
- `value`: what the column is compared with. Strings are quoted in the SQL, numbers are not. `IS_NULL` and `IS_NOT_NULL` take no value.

The entries are joined with `AND`. To combine conditions in another way, write the logical operator as a key with a list of conditions under it:

| Key | Conditions under it | SQL |
| --- | --- | --- |
| `AND` | two or more | `a AND b` |
| `OR` | two or more | `a OR b` |
| `NOT` | one | `NOT a` |
| `IF_THEN` | two: a premise and what must hold when it is true | `(NOT (a) OR (b))` |

These filters keep the work orders that are open or in progress at the north plant, and among repair orders only those that record labor:

```yaml
filters:
  - AND:
      - OR:
          - {field: status, cmp_operator: "==", value: open}
          - {field: status, cmp_operator: "==", value: in_progress}
      - {field: plant, cmp_operator: "==", value: north}
  - IF_THEN:
      - {field: order_type, cmp_operator: "==", value: repair}
      - {field: labor_hours, cmp_operator: ">", value: 0}
```

```sql
SELECT * FROM "public"."work_orders"
WHERE ("status" = 'open' OR "status" = 'in_progress') AND "plant" = 'north'
  AND (NOT ("order_type" = 'repair') OR ("labor_hours" > 0))
```

A condition nested under another is wrapped in parentheses, so the SQL keeps the structure of the YAML.

!!! warning "Keep `OR` inside `AND`"
    GraFlo joins the top-level conditions of a query with `AND` and does not put parentheses around them. The top-level conditions are the `filters` entries, the window of a `time_filter`, and the conditions a view adds. An `OR` entry next to any of them changes meaning: `a OR b AND c` reads as `a OR (b AND c)`. Nest the `OR` in one `AND:` entry with the other conditions, as above, or make it the connector's only condition.

The syntax is the same as that of a vertex type's `filters` in the schema. The long form, `operator: AND` with the conditions under `deps:`, is accepted too. Each entry is parsed when the manifest loads, so an unknown `cmp_operator` is reported then, not when the query runs.

For a date or time window, use `time_filter`, described in [Runtime connector updates](runtime_updates.md#time-windows-with-time_filter).

## Join other tables with `joins`

A work order names the asset it concerns by `asset_id`. The asset's kind and name are in the `equipment` table. A `joins` entry brings them into each row:

```yaml
connectors:
  - name: machine_work_orders
    table_name: work_orders
    joins:
      - table: equipment
        alias: asset
        on_self: asset_id
        on_other: id
        select_fields: [kind, name]
    filters:
      - {field: status, cmp_operator: "==", value: open}
      - {field: asset.kind, cmp_operator: "==", value: Machine}
```

```sql
SELECT base.*, asset."kind" AS "asset__kind", asset."name" AS "asset__name"
FROM "public"."work_orders" base
LEFT JOIN "public"."equipment" asset ON base."asset_id" = asset."id"
WHERE base."status" = 'open' AND asset."kind" = 'Machine'
```

The keys of a join:

- `table`: the table to join.
- `on_self`: the column of the connector's own table.
- `on_other`: the column of the joined table that `on_self` must equal.
- `alias`: the name of the joined table in the query. You need it when you join the same table twice.
- `select_fields`: the columns to take from the joined table. Each arrives as `<alias>__<column>`. Without it, every column of the joined table is selected.
- `join_type`: `LEFT` by default.
- `schema_name`: by default the connector's schema.

With joins, the connector's own table is called `base` in the query; `base_alias` changes that name. A field without a dot in `filters` refers to `base`; write `asset.kind` for a column of a joined table. `select_columns` replaces the whole column list with the SQL expressions you give.

## Look up the kind of each end with `view: type_lookup`

A `view` replaces the connector's query with one you describe. Its `type_lookup` kind is made for a table of relations whose rows do not say what kind of thing each end is.

The `equipment` table holds machines, production lines and sensors side by side, with a `kind` column that says which is which:

| id | kind | name |
| --- | --- | --- |
| 1 | Machine | Press 1 |
| 2 | Machine | Lathe 2 |
| 10 | ProductionLine | Line A |
| 20 | Sensor | Vibration sensor 20 |

The `equipment_links` table records how they are related, by id only:

| source_id | target_id | relation |
| --- | --- | --- |
| 1 | 10 | part_of |
| 2 | 10 | part_of |
| 20 | 1 | monitors |
| 99 | 1 | monitors |

To make an edge out of a link, the resource needs the kind of both ends, and that is in the other table. `kind: type_lookup` describes this query:

```yaml
connectors:
  - name: equipment_links
    table_name: equipment_links
    view:
      kind: type_lookup
      table: equipment
      identity: id
      type_column: kind
      source: source_id
      target: target_id
      relation: relation
```

`table` is the lookup table, `identity` its key column and `type_column` the column that holds the kind. `source` and `target` are the columns of the links table that refer to the lookup table, and `relation` is an optional column to pass through. The query joins the lookup table once for each end:

```sql
SELECT base."source_id" AS source_id, s."kind" AS source_type,
       base."target_id" AS target_id, t."kind" AS target_type,
       base."relation" AS relation
FROM "public"."equipment_links" base
LEFT JOIN "public"."equipment" s ON base."source_id" = s."id"
LEFT JOIN "public"."equipment" t ON base."target_id" = t."id"
WHERE s."id" IS NOT NULL AND t."id" IS NOT NULL
```

It returns these rows:

| source_id | source_type | target_id | target_type | relation |
| --- | --- | --- | --- | --- |
| 1 | Machine | 10 | ProductionLine | part_of |
| 2 | Machine | 10 | ProductionLine | part_of |
| 20 | Sensor | 1 | Machine | monitors |

The link from 99 is left out: a row whose end is missing from the lookup table does not reach the resource.

The output columns always have these names: `source_id`, `source_type`, `target_id`, `target_type`, and `relation` when you set it. The resource reads them with two [vertex routers](../glossary.md#vertex-router) and an [edge step](../glossary.md#edge-step):

```yaml
ingestion_model:
  resources:
    - name: equipment_links
      pipeline:
        - vertex_router:
            type_field: source_type
            role: source
            from: {id: source_id}
            type_map: {Machine: machine, ProductionLine: production_line, Sensor: sensor}
        - vertex_router:
            type_field: target_type
            role: target
            from: {id: target_id}
            type_map: {Machine: machine, ProductionLine: production_line, Sensor: sensor}
        - edge:
            source_role: source
            target_role: target
            relation_field: relation
```

Each router reads the vertex type from a `*_type` column, maps the table's words to vertex types with `type_map`, takes the vertex's `id` from the matching `*_id` column, and names its node with a [role](../glossary.md#role). The edge step links the two roles and takes the relation of each edge from the `relation` column. On the rows above it writes two `part_of` edges from a machine to a production line and one `monitors` edge from a sensor to a machine. The example of [one table that holds many kinds of things (07)](../../examples/vertex-router-type-map/index.md) builds the same kind of pipeline over CSV files.

When the two ends are described in different tables, set the keys for each side: `source_table`, `source_identity` and `source_type_column` for the source, `target_table`, `target_identity` and `target_type_column` for the target. A key you leave out falls back to `table`, `identity` or `type_column`. The lookup tables are called `s` and `t` in the query, so `base_alias` cannot take either name.

When GraFlo writes the bindings for a whole PostgreSQL database with `GraphEngine.create_bindings`, the `type_lookup_overrides` argument maps the name of an edge table to these same keys.

## Write the whole query with `view: select`

`type_lookup` has a fixed shape. When it does not fit, for example when one end always has the same type or you need other columns, `kind: select` describes the query piece by piece. This view reads the open work orders with the kind of their asset and the name of their production line, both looked up in `equipment`:

```yaml
connectors:
  - name: work_order_assets
    table_name: work_orders
    view:
      kind: select
      select:
        - {base: id, as: work_order_id}
        - asset_id
        - {from_join: asset, column: kind, as: asset_type}
        - {from_join: line, column: name, as: line_name}
      joins:
        - {table: equipment, alias: asset, on_self: asset_id, on_other: id}
        - {table: equipment, alias: line, on_self: line_id, on_other: id}
      where: {field: base.status, cmp_operator: "==", value: open}
```

```sql
SELECT base."id" AS work_order_id, base."asset_id",
       asset."kind" AS asset_type, line."name" AS line_name
FROM "public"."work_orders" base
LEFT JOIN "public"."equipment" asset ON base."asset_id" = asset."id"
LEFT JOIN "public"."equipment" line ON base."line_id" = line."id"
WHERE base."status" = 'open'
```

The keys of a `select` view:

- `select`: the output columns, in order. Each entry is one of:
    - a column name such as `asset_id`: that column of the connector's own table;
    - `{base: <column>, as: <name>}`: the same, renamed;
    - `{from_join: <alias>, column: <column>, as: <name>}`: a column of a joined table, named by its `alias`;
    - `all_base`: every column of the connector's own table;
    - `{expr: <SQL>, as: <name>}`, or any other string: an SQL expression, copied into the query as written.

    Without `select`, the view selects `all_base`. The `as` key is optional; `alias` is accepted in its place.

- `joins`: joins with the keys described in [Join other tables with `joins`](#join-other-tables-with-joins).
- `where`: one condition, in the syntax of `filters`. It is parsed when the query is built. When the view has joins, qualify each column with `base.` or a join alias.
- `from`: the table to read. By default it is the connector's `table_name`.
- `base_alias`: the name of the connector's own table in the query, `base` by default.

The connector's `filters` and `time_filter` also apply to a view: GraFlo adds them to the view's `WHERE` as conditions on the base table. That works on a `type_lookup` view and on a `select` view with joins. On a `select` view without joins, write the conditions in `where`.

If you can create a view in the database, you can point `table_name` at it instead. GraFlo reads a database view like a table.

## Assemble a view in Python with `SelectSpec.concat_select_parts`

When several connectors need the same lookup, for example every table that refers to an asset needs the asset's kind, you can build each lookup once in Python and combine the parts. `SelectSpec.concat_select_parts` appends the `joins` and `select` lists of its arguments to each other, in order. This code builds the same query as the YAML above:

```python
from graflo.architecture.contract.bindings import TableConnector
from graflo.filter import JoinClause, SelectSpec

asset_lookup = SelectSpec(
    joins=[
        JoinClause(table="equipment", alias="asset", on_self="asset_id", on_other="id")
    ],
    select=["asset_id", {"from_join": "asset", "column": "kind", "as": "asset_type"}],
)
line_lookup = SelectSpec(
    joins=[
        JoinClause(table="equipment", alias="line", on_self="line_id", on_other="id")
    ],
    select=[{"from_join": "line", "column": "name", "as": "line_name"}],
)
open_work_orders = SelectSpec(
    select=[{"base": "id", "as": "work_order_id"}],
    where={"field": "base.status", "cmp_operator": "==", "value": "open"},
)

view = SelectSpec.concat_select_parts(open_work_orders, asset_lookup, line_lookup)
connector = TableConnector(
    name="work_order_assets", table_name="work_orders", view=view
)
print(connector.build_query())
```

Only the first part may set `where` and `from`; the others raise a `ValueError` if they do, so that the base table and the row conditions are declared once. A part without `select` contributes `all_base`, so give every part its own `select`.

## Automatic joins for edge tables

A table connector with neither `view` nor `joins` can get joins that GraFlo adds when ingestion starts. This happens for an edge step of the connector's resource that names both vertex types with `from` and `to` and sets both `match_source` and `match_target`. GraFlo reads those two keys as columns of the connector's table. For each end it finds the table connector of the resource named after the vertex type and joins that table on the vertex type's first identity property, as `s` for the source and `t` for the target. Rows whose ends are not found are left out.

Declaring `view` or `joins` turns this off, and the query is then exactly the one you declared. An edge step that takes its ends from roles gets no automatic join; use `type_lookup` for it.

## What to read next

- [Runtime connector updates](runtime_updates.md): change a connector's filters or time window for one run, without editing the manifest.
- [One table that holds many kinds of things (07)](../../examples/vertex-router-type-map/index.md): the router pipeline, step by step.
- [A graph from a PostgreSQL database (09)](../../examples/infer-from-postgres/index.md): let GraFlo write the table connectors for a whole database.
