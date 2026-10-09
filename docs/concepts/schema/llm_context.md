# Schema context for a language model

How do I hand a schema to a language model so that what it reads out of a text
fits the schema? This page covers the model-free pieces: a slice of the schema
around what the text is about, a compact rendering of that slice, the JSON
Schema of the answer, and the coercion of the values that come back. After
reading it you can build the prompt half and the validation half of a
schema-guided extractor. The model call itself is not part of graflo.

## The pieces

```python
from graflo.architecture.schema.coerce import coerce_value
from graflo.architecture.schema.context import (
    Budget,
    instance_json_schema,
    iter_elements,
    render_type_sheet,
    subschema,
)

sliced, report = subschema(
    schema,
    ["Machine", ("Person", "Company", "WORKS_AT")],
    budget=Budget(max_tokens=2000),
)
sheet = render_type_sheet(sliced)  # goes into the prompt
shape = instance_json_schema(sliced)  # goes to the model as the output format
```

### Slice: `subschema`

`subschema(schema, seeds, budget=…)` returns a valid standalone `Schema` around
its seeds, and an `ElisionReport` of what it left out. A seed is a vertex type
name or an edge id `(source, target, relation)`. An edge seed admits the edge
and both of its endpoints. Seeds are always admitted, even over budget. Identity,
secondary-identity and digest-source properties are never dropped.

### Type sheet: `render_type_sheet`

One line per vertex type and per edge, each followed by its typed properties:

```
Machine  id: hash(first of: serial | plate_no)
    serial · plate_no
Person  "A human actor."  ~ employee; staff  id: email  alt id: phone
    email STRING · phone STRING · name STRING "Full name."
Substation  id: hash(code)
    code STRING · voltage FLOAT [kV] ~ rating
(Person)-[WORKS_AT]->(Company)  "Employment."
    since DATETIME
```

- A short legend explaining the notation comes first. The sheet ends with the
  closed-world sentence: a type, relation or property absent from it does not
  exist.
- `~` lists synonyms. `id:` states how the vertex is keyed. `[kV]` is the
  declared unit.
- Minted keys — the `id` of an `assigned`, `blank` or hash vertex — are not
  listed, because no text supplies them.
- The output is byte-deterministic. Vertices are sorted by name and edges by
  `(source, target, relation)`, and properties keep their declared order. The
  same schema always renders the same bytes, so prompt caches keep hitting.

### Answer shape: `instance_json_schema`

The JSON Schema of one extraction. It is flat:

```json
{
  "vertices": [
    {"ref": "v1", "type": "Person", "quote": "Ann Lee (ann@x.com)", "mention": false,
     "props": [{"name": "email", "value": "ann@x.com", "quote": "ann@x.com"}]}
  ],
  "edges": [
    {"relation": "WORKS_AT", "from": "v1", "to": "v2", "quote": "Ann joined Acme",
     "props": [{"name": "since", "value": "2024-03", "quote": "2024-03"}]}
  ],
  "unmapped": [{"quote": "the north conveyor", "note": "no conveyor type"}]
}
```

- Types, relations and property names are enums taken from the schema. Vertex
  properties and edge properties have separate enums.
- A property is a name/value pair. A value is a scalar or a list of scalars.
- Edge endpoints are `ref`s to vertices of the same answer.
- Every assertion carries the `quote` it was read from. The caller computes
  character offsets by finding the quote in the text.
- `mention: true` marks a reference to an existing vertex rather than a new one.
- `unmapped` holds what the model saw but could not type.
- Every object is closed and lists all its keys as required. The same schema
  therefore works as an OpenAI strict `json_schema` response format, as a
  `json_object` contract, and as an Ollama `format`.
- It bounds shape only. Whether a property belongs to the vertex type it
  appears on, and whether a value fits the property's type, are checked after
  decoding.

### Values: `coerce_value`

`coerce_value(value, field)` returns the value a `Field` declares, or raises
`CoercionError`:

| Type | Result |
|---|---|
| `INT`, `UINT` | `int`. Digit strings and integral floats are accepted. `bool` and grouped digits (`1,000`) are not. `UINT` refuses negatives |
| `FLOAT`, `DOUBLE` | Finite `float` |
| `BOOL` | `bool`. Also accepts `true/false/yes/no/1/0` in any case |
| `STRING` | Stripped `str`. Numbers are stringified |
| `DATETIME` | ISO 8601 **string**. `YYYY` and `YYYY-MM` are kept as written, never padded |
| `UUID` | Lower-cased UUID string |
| `LIST` | List of `item_type` values. A bare scalar becomes a one-item list |
| untyped | The value unchanged |

On a numeric field with a declared unit, `"200 kV"` becomes `200.0` when the
unit is `kV`. Any other unit raises a `unit_mismatch`, and no conversion is
attempted.

The error's `kind` is one of:

- `type`
- `unit_mismatch`
- `list_item` (with `index`)
- `empty`

Group failures by `kind` to ask for one targeted correction.

### Retrieval text: `iter_elements`

`iter_elements(schema)` yields one `ElementText` per vertex, per edge and per
property:

- `terms`: the name and its synonyms, for lexical matching.
- `anchors`: the IRIs.
- `unit`: the declared unit.
- `text`: one rendering of the element, ready to embed.

Any index that stores this text should record `ELEMENT_TEXT_VERSION` beside it.
The version changes whenever the rendering does.

## API

| Symbol | Module |
|---|---|
| `subschema` | `graflo.architecture.schema.context.subschema` |
| `render_type_sheet` | `graflo.architecture.schema.context.type_sheet` |
| `instance_json_schema` | `graflo.architecture.schema.context.instance_shape` |
| `ElementText`, `iter_elements`, `vertex_text`, `edge_text`, `field_text`, `extractable_properties`, `minted_key_fields`, `ELEMENT_TEXT_VERSION` | `graflo.architecture.schema.context.element_text` |
| All of the above, re-exported | `graflo.architecture.schema.context` |
| `coerce_value`, `CoercionError` | `graflo.architecture.schema.coerce` |

## What to read next

- [Cards](cards.md): a bounded summary of a schema, for orientation before slicing.
- [Vertex identity](vertex_identity.md): the identity modes behind `id:`.
