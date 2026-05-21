# `graflo.filter.onto`

Filter expressions for graph queries, Python document checks, and **SQL pushdown on Bindings**.

## Logical operators in Bindings

`TableConnector.filters` and `SelectSpec.where` accept YAML shorthand aligned with
`VertexConfig.filters`:

```yaml
filters:
  - OR:
      - {field: status, cmp_operator: "==", value: [active]}
      - {field: status, cmp_operator: "==", value: [pending]}
```

Supported keys: **`AND`**, **`OR`**, **`NOT`**, **`IF_THEN`** (implication). In SQL,
`IF_THEN` renders as `(NOT antecedent OR consequent)`, not the literal token `IF_THEN`.

Load-time parsing goes through **`parse_filter_expression`**, which also accepts
discriminated `kind` / `operator` + `deps` forms.

Authoring guide: [Bindings filter cookbook](../../concepts/table_connector_views.md#bindings-filter-cookbook-tableconnectorfilters).

## API reference

::: graflo.filter.onto
