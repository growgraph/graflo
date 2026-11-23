# How do I write a schema without learning the full format?

Write it in the compact notation: types, keys, typed fields and relations in a
few lines of YAML. GraFlo turns it into the schema it stands for, and turns a
schema back when it fits. If you already have a neo4j-graphrag `GraphSchema`
file, GraFlo reads that too, and writes one from any schema. At the end you have
a manifest every `graflo` command reads, and a list of anything left out.

## What you need

- GraFlo installed; see [Installation](../getting_started/installation.md).
- A schema to write, or a `GraphSchema` YAML or JSON file.

## Steps

### 1. Write the schema in the compact notation

```yaml
name: plant
version: 1.0.0
types:
  Machine:
    about: A machine on the shop floor.
    aka: [press, station]
    key: serial
    alt_keys: [asset_tag]
    fields:
      serial: str
      asset_tag: str
      power: {type: float, about: Rated power., unit: kW}
  ProductionLine:
    key: code
    fields: {code: str, name: str}
  Inspection:
    about: One inspection of a machine.
    fields: {at: datetime, passed: bool}
relations:
  - Machine PART_OF ProductionLine
  - {from: Inspection, name: CHECKED, to: Machine, fields: {by: str}}
```

| Key | Means | In the schema |
|---|---|---|
| `name`, `version`, `about` | the schema | `metadata` |
| `types.<T>.about`, `aka` | what an instance is, other names | `description`, `semantics.synonyms` |
| `key` | a field, or a list of fields, that identifies an entity | `identity` |
| `hashed: true` | the key is a value, not a name: the identity is its digest | `hash_identity_properties` |
| no `key` | each mention is a new entity | `blank: true` |
| `alt_keys` | other keys: a list (each named `by_<fields>`) or a mapping name → fields | `secondary_identities` |
| `fields.<f>` | `str`, `int`, `float`, `bool`, `datetime`, `uuid` or `any`; or `{type, about, aka, unit}` | `properties` |
| `relations` | `"Source RELATION Target"`, or `{from, name, to, about, aka, fields}` | `edge_config.edges` |

Quote a description that holds `: ` or `, ` inside braces, and a relation named
like a YAML boolean (`ON`, `YES`) in the long form.

### 2. Turn it into a manifest

```bash
graflo schema-from-compact plant.yaml -o manifest.yaml
```

The notation is a subset of the schema: a schema that needs more (inverses,
edge identities, filters, IRIs, other field types) is written in the full
format. To go back:

```bash
graflo schema-to-compact manifest.yaml
```

This is refused, naming each part that would be lost, when the schema uses more
than the notation says. In Python: `graflo.interop.compact.from_compact` and
`to_compact`.

### 3. Read or write a neo4j-graphrag `GraphSchema`

```bash
graflo schema-from-neo4j-graphrag schema.yaml -o manifest.yaml --losses losses.md
graflo schema-to-neo4j-graphrag manifest.yaml -o schema.yaml
```

`GraphSchema` is the YAML or JSON document neo4j-graphrag's knowledge-graph
pipeline reads. Both directions print what they leave out on stderr, and
`--losses` also writes it to a file.

| GraphSchema | Schema |
|---|---|
| node type | vertex |
| a node type's first `KEY` | `identity` |
| a further `KEY`, `UNIQUENESS` | `secondary_identities` |
| a node type without a `KEY` | `blank: true`: each mention is a new entity |
| each pattern `[A, R, B]` | an edge carrying `R`'s properties |
| a relationship `KEY` | edge `identities` |
| `BOOLEAN`, `INTEGER`, `FLOAT`, `STRING` | `BOOL`, `INT`, `FLOAT`, `STRING` |
| `DATE`, `LOCAL_DATETIME`, `ZONED_DATETIME` | `DATETIME` |

Left out on the way in:

- `EXISTENCE` constraints;
- `LIST`, `DURATION`, `POINT` and the time types, which become untyped fields;
- the `additional_*` switches, because a schema accepts only what it declares;
- relationship types that no pattern names.

Left out on the way out:

- other names, units and IRIs;
- edge `identities`: a relationship `KEY` is unique across all relationships of
  its type, and an edge identity only between one pair of endpoints;
- filters, inverses and the database profile.

A hashed identity is written as a `KEY` on the fields it digests.

## What you should see

`manifest.yaml` loads with every `graflo` command. `schema-to-compact` on it
prints the file you started from. `GraphSchema.from_file` loads what
`schema-to-neo4j-graphrag` wrote.
