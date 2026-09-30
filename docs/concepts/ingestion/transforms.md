# Transforms

Records rarely arrive in the shape of the vertices you want: a column has
another name, a number is a string, a date needs a time zone, one wide row
holds several measurements. A transform step fixes that before the
[vertex and edge steps](../glossary.md#step) read the record. This page shows
every form of the step, how to share one transform between resources, and the
functions GraFlo ships.

## The smallest transform

A sensor feed calls the serial number `sn`, and the `machine` vertex calls it
`serial`. A `rename` transform fixes that:

```yaml
resources:
  - name: readings
    pipeline:
      - transform:
          rename: { sn: serial }
      - vertex: machine
```

Every transform step holds exactly one of two keys: `rename`, a map of old
field names to new ones, or `call`, a Python function to run on chosen fields.

```yaml
- transform:
    call:
      module: builtins
      foo: int
      input: [reading]
      output: [reading_int]
```

- `module`: the Python module that holds the function. It must be importable
  where the ingest runs.
- `foo`: the name of the function in `module`, such as `int` or
  `split_keep_part`; the key is literally `foo`. It is a plain name, not a
  dotted path.
- `input`: the fields whose values are passed to the function, in order, as
  positional arguments. One name may be written without the list.
- `output`: the fields that receive the result. A single value goes into the
  field; a tuple or list is spread over the fields, one item each, so a list
  is never stored whole in one field. When `output` is omitted, the result
  goes back into the `input` fields.
- `params`: keyword arguments passed on every call.

## What a transform sees and writes

A transform does not change the record. It adds its output next to the
record, and later steps read the record and those outputs as one view: an
output wins over a record field of the same name. Three rules follow.

- **Transforms run before vertex and edge steps.** Within one level of a
  pipeline, GraFlo runs `descend` steps first, then the transforms in the
  order written, then vertex routers, vertex steps and edge steps. A vertex or
  edge step therefore sees every transform of its level, wherever you write
  it. Write the transforms first anyway, so the file reads in the order it
  runs.
- **Transforms chain.** Each transform sees the outputs of the transforms
  before it, so a transform that renames keys can be followed by one that
  converts the value under a new key.
- **Transforms stay at their level.** A transform at the top of the pipeline
  is not visible inside a `descend` step, and a transform inside a `descend`
  is not visible outside it. When a `descend` reaches a bare value instead of
  a mapping, such as a string in a list, a `call` receives that value as its
  only argument, and `output` names the field it becomes (see
  [records that refer to their own kind](../../examples/json-self-edges/index.md)
  (2)).

An edge step reads `relation_field` from the same view, so a transform can
compute the relation name before the edge is built.

## Reusable transforms

A transform that several resources use, or one resource uses several times,
is declared once under `ingestion_model.transforms` with a `name`, and a step
runs it with `use`. This one keeps the last part of a URL and writes it back
to `id`:

```yaml
ingestion_model:
  transforms:
    - name: short_id
      module: graflo.util.transform
      foo: split_keep_part
      params: { sep: "/", keep: -1 }
      input: [id]
      output: [id]
  resources:
    - name: works
      pipeline:
        - transform:
            call: { use: short_id }
        - vertex: work
```

A named transform takes the keys of `call` except `use` and `strategy`, plus
its `name`. A step that uses it may set any key of `call` except `module` and
`foo`; each key it sets replaces the named transform's value as a whole, so
`params` on the step replace the named `params` rather than add to them. Put
the shared parts on the named transform and vary the rest per step:

```yaml
ingestion_model:
  transforms:
    - name: round_metric
      module: graflo.util.transform
      foo: round_str
      params: { ndigits: 3 }
      dress: { key: name, value: value }
  resources:
    - name: prices
      pipeline:
        - transform:
            call: { use: round_metric, input: [Open] }
        - transform:
            call: { use: round_metric, input: [Close] }
```

## Reference

The forms below are ordered from most used to least used.

### `rename`

```yaml
- transform:
    rename:
      Date: observed_at
      Open: open_price
```

A field mapping with no function. Later steps see each field under its new
name only. A source field missing from the record is skipped, and the others
are renamed. With `fail_fast: true` on the resource, every source field must
be present, or the record fails.

### `call` with `input` and `output`

```yaml
- transform:
    call:
      module: graflo.util.transform
      foo: parse_date_ibes
      input: [ANNDATS, ANNTIMS]
      output: [datetime_announce]
```

The function is called once with all input values (`strategy: single`, the
default). When an input field is missing from the record, the step writes
nothing; with `fail_fast: true` on the resource the record fails instead.

### `dress`

A `dress` turns one field into a key and a value: the field's name goes into
`dress.key`, the function's result into `dress.value`. A wide row of
measurements becomes one small record per measurement, which a vertex step can
turn into one vertex each.

```yaml
- transform:
    call:
      module: graflo.util.transform
      foo: round_str
      input: [Open]
      params: { ndigits: 3 }
      dress: { key: name, value: value }
```

Given `{Open: "6.430062"}`, this writes `{name: "Open", value: 6.43}`. Without
a function, `dress` copies the value as it is:

```yaml
- transform:
    call:
      input: [vol]
      dress: { key: type, value: value }
```

Given `{vol: 0.123}`, this writes `{type: "vol", value: 0.123}`. A `dress`
takes exactly one input field, and its output fields are `dress.key` and
`dress.value`. The [filters and weights example](../../examples/vertex-filters-and-weights/index.md)
(5) builds one vertex per measurement this way.

### `when`

A guard runs the step only when one field of the record holds one of the
listed values. The listed values are strings (quote numbers in YAML), and a
record value matches only when it is equal to one of them: the number `1`
does not match `"1"`.

```yaml
- transform:
    when: { field: kind, in: [sensor] }
    call:
      module: graflo.util.transform
      foo: remove_prefix
      input: [device_tag]
      output: [serial]
      params: { prefix: "SN-" }
```

A row whose `kind` is `sensor` gets `serial` without the `SN-` prefix. Any
other row gets no `serial` field at all, not a `null`. That difference matters
when several steps may write the same field: two guarded steps never collide,
because on any one record at most one of them runs, whereas a function that
returns `null` would overwrite a value an earlier step wrote. Use a guard when
the kind of record decides how a field is derived; when the value itself
decides, a function that returns `null` is enough. A record without the
`field` fails the guard. `when` applies to `rename` and to `call`.

### `strategy: each`

```yaml
- transform:
    call:
      module: builtins
      foo: int
      input: [x, y]
      strategy: each
```

The function is called once per input field, with that field's value alone:
the same conversion over several columns. `output`, when given, has one name
per input field.

### `input_groups` and `output_groups`

A grouped call runs the same function once per group of fields:

```yaml
- transform:
    call:
      module: operator
      foo: add
      input_groups:
        - [first_shift, second_shift]
        - [first_idle, second_idle]
      output: [run_hours, idle_hours]
```

- Each group is a list of fields passed as positional arguments to one call.
  The calls run in order.
- `output` names one field per group, when each call returns one value.
- `output_groups` is a list of field lists, one per group, when each call
  returns several values.
- With neither, each result goes back into its group's own fields. A group of
  one field may be written as a bare string, so `input_groups: [a, b]` runs
  the function on `a` and on `b` and writes each result back.

Use groups for one function over different sets of arguments, and
`strategy: each` for one function over single fields.

### `strategy: all`

```yaml
- transform:
    call:
      module: graflo.util.transform
      foo: coalesce_fields
      strategy: all
      params: { fields: [serial, serial_no] }
      output: [serial_number]
```

The whole record is passed to the function as one argument. Here the first
non-empty of `serial` and `serial_no` becomes `serial_number`. `input`,
`input_groups` and `dress` are not allowed.

### `target: keys`

A key transform renames the fields themselves by running a function on each
field name:

```yaml
- transform:
    call:
      module: graflo.util.transform
      foo: camel_to_snake
      target: keys
      keys: { mode: all }
```

`keys.mode` selects the fields: `all`, `include` (only `keys.names`) or
`exclude` (all but `keys.names`):

```yaml
- transform:
    call:
      module: graflo.util.transform
      foo: remove_prefix
      params: { prefix: "raw_" }
      target: keys
      keys: { mode: include, names: [raw_serial, raw_model] }
```

The function is called once per selected field name and must return a
string. Two fields that end up with the same name fail the step. `input`,
`output`, `input_groups`, `output_groups`, `dress` and `strategy` are not
allowed. A named transform may carry `target: keys` and `keys`, so that
resources only write `use`: a step with `use` takes the target of the named
transform unless it sets its own, while a step without `use` works on values
unless it says `target: keys`.

## Functions shipped with GraFlo

`graflo.util.transform` holds functions for common cases. Any other function
works the same way: name its module and its name.

| Function | Does |
|---|---|
| `split_keep_part(s, sep="/", keep=-1)` | split on `sep` and keep one part, or several parts joined by `sep` (`keep: [-2, -1]`) |
| `remove_prefix(s, prefix)`, `remove_suffix(s, suffix)` | strip a prefix or suffix when present |
| `camel_to_snake(s)`, `snake_to_camel(s, upper_first=False)` | convert naming styles; usual with `target: keys` |
| `round_str(x, ndigits=...)` | read a number from a string and round it |
| `try_int(x)` | convert to `int`, and keep the value when that fails |
| `parse_date_yahoo(date0)`, `parse_date_ibes(date0, time0)` | turn two vendor date formats into ISO 8601 strings |
| `normalized_key(value, strip_prefix=None, casefold=True, strip_chars=None)` | trim, strip a prefix and casefold a key, so two sources spell it alike |
| `affix_gated_key(value, prefix="", suffix="", casefold=True, strip_chars=None)` | strip a marker prefix and suffix, or return `null` when the value lacks either |
| `tagged_key(value, tag, sep=":")` | prefix a key with a tag, so keys from different sources cannot collide |
| `coalesce_fields(doc, fields)` | the first non-empty value among `fields`; use with `strategy: all` |

## Rules

GraFlo checks these when the manifest is loaded or initialized
(`GraphManifest.finish_init()`, which every ingest runs), before any record is
read.

- A step holds exactly one of `rename` and `call`.
- A `call` names `use`, or both `module` and `foo`, or `dress` with `input`.
- `module` must import and `foo` must exist in it.
- `input` and `input_groups` are exclusive. `output_groups` requires
  `input_groups` and the same number of groups. With `input_groups`, `output`
  has one name per group, and `output` and `output_groups` are exclusive.
- `dress` requires exactly one input field and works on values only; it is
  refused with `input_groups` and with `strategy: all`.
- `strategy` applies only to a function, and `input_groups` allows only
  `strategy: single`.
- `when.in` lists at least one value.
- Every named transform has a `name`, and names are unique. A `use` that names
  no transform is refused when an ingest starts.

A transform that raises while a record is cast does not stop the record by
default: its output fields are set to `null` and the failure is recorded. See
[document cast errors](doc_errors.md).

## What to read next

- [Core components](../architecture/core_components.md#resource-and-its-steps):
  the other steps of a resource.
- [Document cast errors](doc_errors.md): what happens when a transform raises
  while a record is cast.
- [Filter rows and attach measurements](../../examples/vertex-filters-and-weights/index.md)
  (5): `dress` and reusable transforms in a complete manifest.
