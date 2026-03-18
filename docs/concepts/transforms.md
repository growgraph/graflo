# Transforms

`Transform` is the core normalization mechanism in GraFlo ingestion pipelines. It handles value conversion, field renaming, reshaping, and key normalization before `vertex` and `edge` actors consume data.

This page documents the transform DSL as implemented in:

- `graflo.architecture.transform.Transform`
- `graflo.architecture.actor.transform.TransformActor`
- `graflo.architecture.actor.config.models.TransformCallConfig`

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

A `call.use` step can override `input`, `output`, and/or `params` for local context while reusing the base function (`module` + `foo`):

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

### Strategy: `single` (default)

Call function once with all selected input values.

```yaml
- transform:
    call:
      module: graflo.util.transform
      foo: parse_date_ibes
      input: [ANNDATS, ANNTIMS]
      output: datetime_announce
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
- `strategy: single | each | all | null` - function execution mode
- `target: values | keys` - operate on values (default) or keys
- `keys`:
  - `mode: all | include | exclude`
  - `names: list[str]`
- `dress`:
  - `key: str`
  - `value: str`

## Validation and compatibility rules

- A transform step must define exactly one of:
  - `transform.rename`
  - `transform.call`
- `call.use` cannot be combined with `call.module` or `call.foo`.
- If `call.use` is absent, both `call.module` and `call.foo` are required.
- `map`/rename and function mode are mutually exclusive.
- Legacy `switch` is not supported.
- List-style `dress` is not supported (`dress` must be a dict with `key` and `value`).

## Practical patterns

- Keep keys stable early:
  - run one `target: keys` transform near pipeline start.
- Use reusable named transforms for:
  - ID normalization
  - date/time parsing
  - repeated casting logic
- Use local overrides when:
  - same function, different input/output fields per resource
- Use `strategy: each` for repeated unary casting (for example, multiple numeric columns).
- Use `dress` to pivot wide metrics into tidy key/value records before routing into vertices/edges.

## Related docs

- [Concepts Overview](index.md)
- [Creating a Manifest](../getting_started/creating_manifest.md)
- [Architecture Transform API](../reference/architecture/transform.md)
- [Transform Actor API](../reference/architecture/actor/transform.md)
