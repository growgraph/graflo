# 22 — state-core: lifting a manifest into a twin-ready schema

Most manifests are not twin-ready. They declare types with no grounding, edges
whose direction nobody stated, floats with no unit, and facts that change over
time stored flat on the thing they are about — so "what was this asset's status
last March, and how do we know" has no answer in the schema at all. Every
manifest an inference pass produces looks like this.

`state-core` converts one. It is a **meta-layer, not a model**: given any
manifest plus a statement of what its types *mean*, it emits the evolution
operations that add temporal validity and provenance.

```bash
cd examples/22-state-core

# 1. the starting point fails every assertion
uv run graflo check --profile world-model manifest_in.yaml

# 2. lift it, keeping the op stream to read
uv run graflo lift manifest_in.yaml --spec lift.yaml \
    --emit-ops artifacts/ops.yaml -o artifacts/manifest_lifted.yaml

# 3. the result passes, on its own
uv run graflo check --profile world-model artifacts/manifest_lifted.yaml
```

## Why operations rather than a transform

A transform is a black box that either did the right thing or did not.
`graflo lift` emits a list of the same `ManifestOp` values every other manifest
change uses, so the conversion is:

- **reviewable** — `--emit-ops` writes the plan before anything is applied;
- **replayable** — the op file applied to the input reproduces the output exactly;
- **invertible** — `invert_ops` turns the lift back into the manifest you started
  with, because every op it emits is reversible.

## What you must declare, and what the lift works out

The split is the whole design. A lift can see **structure**: which edges leave
`directed` unstated, which types carry no grounding, where a float has no unit.
It cannot see **meaning** — nothing in a schema says `ConfigurationItem` denotes
a `sosa:FeatureOfInterest`, that `operating_temp` is degrees Celsius rather than
a count, or that `status` changes over time while `ci_id` does not.

`lift.yaml` is exactly those four things:

| Key | What it says |
|-----|--------------|
| `grounding` / `edge_grounding` | what a type or relation denotes |
| `stateful` | which properties are facts that change |
| `observed` | which types get measured at a time |
| `measured` | the unit of an existing measurement |

Everything else is mechanical and never appears in the spec: restating
directionality, minting the scaffolding types and grounding them, wiring the
provenance edges.

## What the lift builds

For `stateful: {ConfigurationItem: [status]}` it mints
`ConfigurationItemState`, carrying the subject's key, the moved properties, and
a `valid_from` / `valid_to` interval — keyed on **subject plus `valid_from`**,
because the same property of the same entity holds many values over time and
those are different facts rather than revisions of one. Closing `valid_to`
instead of overwriting is what makes history queryable.

`observed:` mints `<Type>Observation`, whose `result_unit` travels **per row**:
an abstract observation type serves temperature and pressure alike, so it cannot
name one unit in its contract without lying.

`provenance:` (on by default) mints `Evidence` and `Agent` with
`wasDerivedFrom` / `wasAttributedTo`, which is what makes "where did this fact
come from" a question the schema can answer.

## The destructive half is last, and optional

A property named in `stateful` is **moved**: it lands on the state type and is
removed from the entity, because a fact that changes over time does not belong
on the thing it is about. That is the honest lift, and it is the final op in the
stream — everything before it is additive. Set `retire: keep` and the op list
simply ends one step earlier, leaving a denormalized current value beside the
history.

## Conventions this commits to

- **Units are UCUM tokens** (`Cel`, `m/s`). UCUM has no currency, so currency
  falls back to ISO-4217 alpha codes (`USD`).
- **Time is grounded in PROV-O and SOSA, not OWL-Time.** `time:hasBeginning`
  ranges over a `time:Instant`, not a literal, so grounding a `DATETIME` column
  in it is a claim that becomes false the moment the schema is projected to OWL.
  `prov:generatedAtTime`, `prov:invalidatedAtTime` and `sosa:resultTime` are
  literal-ranged and say the same thing truthfully. OWL-Time stays in
  `exact_match` at the concept level, where it is about the type.
- **Evidence is a vertex, not a property.** It needs its own identity, and a
  provenance *edge* needs two endpoint types.

## What a lift does not do

It changes the **contract, not the data flow**. The lifted manifest declares
`ConfigurationItemState` and `Evidence`, but nothing populates them: a manifest
carrying an `ingestion_model` still needs its pipelines wired to the new types,
and the profile will keep reporting that provenance is not materialised at
ingest — correctly. Scaffolding the pipelines needs resource-level operations
and a statement of which resource feeds which type, and is deliberately out of
scope here.

## Known limit

`GraphMetadata.base_iri` does not exist, so a lifted manifest carries
`exact_match` into external vocabularies and mints no IRIs of its own. It
*aligns* to standards; it cannot yet be published as an ontology.

## See also

- `graflo check --profile world-model` — the six assertions, and the acceptance
  test for any lift.
- Example 19 — composing two manifests, the binary counterpart to this unary
  transformation.
