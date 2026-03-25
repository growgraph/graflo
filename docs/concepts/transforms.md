# Transforms

`Transform` is the core normalization mechanism in GraFlo ingestion pipelines. It handles value conversion, field renaming, reshaping, and key normalization before `vertex` and `edge` actors consume data.

This page documents the transform DSL as implemented in:

- `graflo.architecture.contract.declarations.transform.Transform`
- `graflo.architecture.pipeline.runtime.actor.transform.TransformActor`
- `graflo.architecture.pipeline.runtime.actor.config.models.TransformCallConfig`

## Mental model

Two layers work together:

- `ProtoTransform`: function wrapper (`module`, `foo`, `params`) with invocation logic.
- `Transform`: adds input selection, output mapping, dressing, key targeting, and execution strategy.

In a resource pipeline, each transform step applies to the current document and emits an update payload for downstream actor steps.

## Where transforms can be defined

### 1) Inline (local) transform in a resource

Use this when logic is specific to one place in one pipeline.

```yaml
resources:
  - name: papers
    apply:
      - transform:
          call:
            module: builtins
            foo: int
            input: citations
            output: citations_count
      - vertex: paper
```

### 2) Reusable transform in `ingestion_model.transforms`

Use this when the same transform should be referenced from multiple resources or steps.

```yaml
ingestion_model:
  transforms:
    - name: keep_suffix_id
      module: graflo.util.transform
      foo: split_keep_part
      input: id
      output: _key
      params: {sep: "/", keep: -1}
```

Then reference it from a transform step:

```yaml
resources:
  - name: works
    apply:
      - transform:
          call:
            use: keep_suffix_id
      - vertex: work
```

### Local override of reusable transform

A `call.use` step can override `input`, `output`, `params`, and/or `dress` while reusing `module` + `foo` from the vocabulary entry. Put shared `dress` and `params` on the named transform when several steps only differ by `input`:

```yaml
# ingestion_model.transforms
- name: round_metric
  module: graflo.util.transform
  foo: round_str
  params: {ndigits: 3}
  dress: {key: name, value: value}

# resource apply
- transform:
    call: {use: round_metric, input: [Open]}
- transform:
    call: {use: round_metric, input: [Close]}
```

Rename-style override (same idea, different fields):

```yaml
- transform:
    call:
      use: keep_suffix_id
      input: doi
      output: work_id
      params: {sep: "/", keep: [-2, -1]}
```

## Transform forms

### A) Rename-only transform (`transform.rename`)

Pure field mapping with no function call.

```yaml
- transform:
    rename:
      Date: observed_at
      Open: open_price
```

Equivalent behavior to a map-based transform (`map`).

### B) Function-call transform (`transform.call`)

Function-backed transform using `module` + `foo`, or a reusable `use` reference.

```yaml
- transform:
    call:
      module: builtins
      foo: round
      input: confidence
      output: confidence_rounded
      params:
        ndigits: 3
```

## Output behavior

### Direct output mapping

- `input` selects fields from the current document.
- function result is assigned to `output`.
- if `output` is omitted and `input` exists, output defaults to input field names.

### Dress output (`dress`)

Use `dress` when a single-input transform should emit a `{key, value}` style payload.

```yaml
- transform:
    call:
      module: graflo.util.transform
      foo: round_str
      input: Open
      params: {ndigits: 3}
      dress:
        key: name
        value: value
```

For input field `Open`, this emits:

```yaml
name: Open
value: 17.9
```

`dress` rules:

- requires a function transform (`module` + `foo` or `use` that resolves to one)
- requires exactly one input field
- sets output field names to `(dress.key, dress.value)`

## Multi-field transforms

### Grouped calls (`input_groups` / `output_groups`)

Use **groups** when the same function should run multiple times on different argument tuples (not the same as `strategy: each`, which runs once per *single* input field from a flat `input` list).

- Each inner group is a list of field names whose values are read from the document and passed as `*args` to the function for that call.
- The function is invoked **once per group**, in order.
- **`output`**: list of field names, one per group, when each call returns a single value.
- **`output_groups`**: list of field-name lists, parallel to `input_groups`, when each call returns multiple values (e.g. a tuple mapped to several outputs).
- **Omitting outputs**: only valid when every group has exactly **one** input field; results are written back to those same keys (passthrough). If any group has more than one field, you must set `output` or `output_groups`.

YAML accepts a **shorthand** for unary groups: a group can be a single string, and `input_groups` can be a list of strings (one field per group):

```yaml
- transform:
    call:
      module: builtins
      foo: int
      input_groups:
        - age_parent
        - age_child
```

Grouped mode is incompatible with `dress` and with `strategy: each` or `strategy: all`. Omit `strategy` or use `single` (default).

### Strategy: `single` (default)

Call function once with all selected input values (flat `input`, no groups).

```yaml
- transform:
    call:
      module: graflo.util.transform
      foo: parse_date_ibes
      input: [ANNDATS, ANNTIMS]
      output: datetime_announce
```

### Explicit grouped calls (nested field lists)

When the same function should run repeatedly on explicit argument tuples, use
nested lists in `input_groups` (see [Grouped calls](#grouped-calls-input_groups--output_groups) above).

```yaml
- transform:
    call:
      module: my_pkg.transforms
      foo: join_name
      input_groups:
        - [fname_parent, lname_parent]
        - [fname_child, lname_child]
      output: [parent_name, child_name]
```

`input_groups` can also use grouped outputs:

```yaml
- transform:
    call:
      module: my_pkg.transforms
      foo: split_name
      input_groups:
        - [parent_name]
        - [child_name]
      output_groups:
        - [parent_fname, parent_lname]
        - [child_fname, child_lname]
```

Grouped passthrough is supported when outputs are omitted and each group maps
back to its own keys (for example unary casts):

```yaml
- transform:
    call:
      module: builtins
      foo: int
      input_groups:
        - [age_parent]
        - [age_child]
```

### Strategy: `each`

Call function independently for each selected input field.

```yaml
- transform:
    call:
      module: builtins
      foo: int
      input: [x, y]
      output: [x, y]
      strategy: each
```

### Strategy: `all`

Pass the whole document as one argument to the transform function.

```yaml
- transform:
    call:
      module: builtins
      foo: dict
      strategy: all
```

`strategy: all` rules:

- do not provide `input`
- incompatible with `dress`

## Key transforms (`target: keys`)

Transforms can operate on document keys instead of values:

```yaml
- transform:
    call:
      module: graflo.util.transform
      foo: camel_to_snake
      target: keys
      keys:
        mode: all
```

### Key selection

`call.keys.mode` supports:

- `all`: apply to every key
- `include`: apply only to `keys.names`
- `exclude`: apply to all keys except `keys.names`

Example with include:

```yaml
- transform:
    call:
      module: graflo.util.transform
      foo: remove_prefix
      params: {prefix: "raw_"}
      target: keys
      keys:
        mode: include
        names: [raw_id, raw_label]
```

`target: keys` rules:

- requires a function transform
- does not allow `input`, `output`, or `dress`
- does not allow `input_groups` or `output_groups`
- does not allow explicit `strategy` (key mode is implicit per-key execution)
- transformed keys must remain unique (collisions raise an error)

## Config reference (transform DSL)

### `transform.rename`

- Type: `dict[str, str]`
- Meaning: `{source_field: target_field}`

### `transform.call`

- `use: str | null` - named transform from `ingestion_model.transforms`
- `module: str | null` - python module path for inline function
- `foo: str | null` - function name in module
- `params: dict` - keyword args passed to function
- `input: str | list[str] | null` - input fields (not used for key mode)
- `output: str | list[str] | null` - output fields (not used for key mode)
- `input_groups: list[list[str]] | null` — grouped calls (values mode only); each entry is a group. YAML may use a **list of strings** as shorthand for unary groups (one field name per group).
- `output_groups: list[list[str]] | null` - grouped outputs aligned to `input_groups`
- `strategy: single | each | all | null` - function execution mode (with `input_groups`, omit or use `single` only; `each` / `all` are rejected)
- `target: values | keys | null` - operate on values or keys. With `use`, omit to inherit defaults from the matching `ingestion_model.transforms` entry; inline calls (no `use`) default to `values` when omitted.
- `keys`:
  - `mode: all | include | exclude`
  - `names: list[str]`
- `dress`:
  - `key: str`
  - `value: str`

### `Transform` (Python API only)

Named transforms in `ingestion_model.transforms` are `ProtoTransform` entries (`module`, `foo`, `params`, flat/grouped `input` / `output`, `dress`, and optional `target` / `keys` for key-mode defaults). A `transform.call` with `use` inherits those defaults; set `call.target` or `call.keys` to override. Inline `transform.call` steps supply execution options (`target`, `keys`, `strategy`) and may override IO; `TransformActor` assembles a runtime `Transform`, which adds:

- `passthrough_group_output: bool` (default `true`) — when `input_groups` is used and neither `output` nor `output_groups` is set, allow writing unary group results back onto the input keys. Not exposed on manifest `transform.call` today; omit outputs in YAML only for unary groups.

When the effective target is `keys` (from the call or the named proto), `call.input` / `call.output` / `call.input_groups` / `call.output_groups` / `call.dress` are rejected at merge time so invalid combinations are not silently ignored.

## Validation and compatibility rules

- A transform step must define exactly one of:
  - `transform.rename`
  - `transform.call`
- `call.use` cannot be combined with `call.module` or `call.foo`.
- If `call.use` is absent, both `call.module` and `call.foo` are required.
- `map`/rename and function mode are mutually exclusive.
- Use either `call.input` or `call.input_groups`, not both.
- With `call.input_groups`, do not set `call.strategy` to `each` or `all`.
- For grouped calls, use either `call.output` (one output per input group) or
  `call.output_groups` (full per-group output tuples), not both.
- `call.output_groups` must have the same number of groups as `call.input_groups`.
- Passthrough (no `output` / `output_groups`) requires every group to contain exactly one input field.
- Legacy `switch` is not supported.
- List-style `dress` is not supported (`dress` must be a dict with `key` and `value`).

## Practical patterns

- Keep keys stable early:
  - run one `target: keys` transform near pipeline start.
- Use reusable named transforms for:
  - ID normalization
  - date/time parsing
  - repeated casting logic
  - shared `target: keys` + `keys` selection so resources only reference `use:` without repeating key-mode config
- Use local overrides when:
  - same function, different input/output fields per resource
- Use `strategy: each` with a flat `input` list for repeated unary casting (for example, multiple numeric columns). For the same callable over **different argument tuples**, use `input_groups` instead.
- Use `dress` to pivot wide metrics into tidy key/value records before routing into vertices/edges.

## Related docs

- [Concepts Overview](index.md)
- [Creating a Manifest](../getting_started/creating_manifest.md)
- [Architecture Transform API](../reference/architecture/contract/declarations/transform.md)
- [Transform Actor API](../reference/architecture/pipeline/runtime/actor/transform.md)
