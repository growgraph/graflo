# Conformance profiles

A **profile** is a named, versioned list of mechanically checkable assertions
about a manifest. `finish_init()` already refuses a manifest that is broken; a
profile asks the other question — the manifest is valid, but is the model
usable by someone who did not write it?

```bash
graflo check --profile world-model manifest.yaml
graflo check --profile world-model manifest.yaml --json
graflo check --list-profiles
```

Exit codes are `0` conformant, `1` non-conformant, `2` the check could not run.
The last is separate so a CI job can tell a bad model from a bad file.

A profile introduces no semantics and touches no backend: every assertion reads
fields the contract already has. That is what lets it run against manifests
GraFlo did not author — an inferred model, or someone else's.

## The `world-model` profile

Six assertions, version 0.1.

### 1. `grounded-types`

Every vertex and every edge carries `semantics.iri` or `semantics.exact_match`,
and every IRI parses as an absolute IRI.

A type called `Node` means nothing to a consumer; `sosa:Observation` does. An
IRI outside the vocabularies the checker recognises **warns** rather than
failing — "we have not heard of this namespace" is a weaker claim than "this
namespace is dead", and reporting it as a failure would be dishonest.

### 2. `declared-identity`

Every vertex declares how it is identified: `identity`, `blank`, `assigned`,
`hash_identity_properties` or `identity_funnel`.

`blank` fails outright — instances carry no natural key, so re-ingesting the
same data creates it twice. The subtler failure is a vertex that declares
nothing under `identity_from_all_properties`, which silently keys it on every
property it happens to carry. That one is only visible in the **authored
document**: `VertexConfig` fills the field in during validation, so the parsed
model cannot tell an undeclared identity from a declared one.

### 3. `declared-directionality`

Every edge writes `directed:` explicitly.

Blast radius is computed by following an edge one way, so whether source-to-
target order is meaningful has to be stated rather than inherited from a
default. `Edge.directed` defaults to `True`, so — like assertion 2 — this is
decidable only against the authored document.

### 4. `declared-units`

Every **measured** property says what it is measured in. Measured means a
floating-point property that is not part of the key.

Two forms pass:

- **Schema-level** — the property carries `semantics.unit`. Use this when the
  type measures one thing.
- **Row-level** — the type declares a companion property grounded in
  `qudt:hasUnit`, `qudt:ucumCode` or `qudt:hasQuantityKind`, so each instance
  carries its own unit. Use this when the type is abstract: one `Observation`
  type serving temperature and pressure cannot name a unit in its contract
  without lying.

The report says which form it saw, in `detail.unit_source`. A `unit` written as
an IRI warns about mixed conventions — units cannot be compared across a
manifest that uses both spellings.

### 5. `temporal`

At least one `DATETIME` property is grounded in a validity or observation-time
vocabulary — `prov:generatedAtTime`, `prov:invalidatedAtTime`,
`prov:startedAtTime`, `prov:endedAtTime`, `sosa:resultTime` or
`sosa:phenomenonTime`.

That is time expressed as **modelled state**: validity is a property of a state
or observation entity, not a hidden axis on every type. No new primitive is
needed, and upserting means closing the current row rather than overwriting it.

All six IRIs are literal-ranged on purpose. OWL-Time's `time:hasBeginning`
ranges over a `time:Instant` rather than a literal, so grounding a `DATETIME`
field in it would be a claim that becomes false once the schema is projected to
OWL. OWL-Time belongs in `exact_match` at the concept level.

### 6. `provenance`

Two halves.

The **contract** half asks whether the schema can express provenance at all: a
vertex grounded as an agent, and an edge grounded in a derivation or
attribution property (`prov:wasDerivedFrom`, `prov:wasAttributedTo`,
`prov:wasGeneratedBy`, `prov:used`, `sosa:madeBySensor`). Without both, the
model holds assertions with no accountable source.

The **ingestion** half is reported `not_applicable` when the manifest declares
no ingestion model, rather than passing on a question it never asked.

This is unrelated to `ManifestMetadata.provenance`, which is the *artifact's*
content address and lineage. Assertion 6 is about the data.

## Waivers

An assertion an operator has decided does not apply is excused by a **sidecar
document**, not a manifest field — a waiver is a statement about a deployment,
and putting it on the contract would make two manifests describing the same
world compare unequal.

```yaml
# waivers.yaml
profile: world-model
subject: reference-data          # advisory; nothing enforces the match
waivers:
  - assertion: temporal
    reason: static reference data; the values have no valid-from
```

```bash
graflo check --profile world-model manifest.yaml --waivers waivers.yaml
```

A waived assertion reports `waived`, **never** `pass`, and its reason is
printed. `reason` is required: a waiver without one is a silent pass with extra
steps. A waiver covers only the assertion it names.

The cost is real and worth stating: a waiver travels out of band, so a registry
holding the manifest cannot see it, and it is not covered by the manifest's
content address.

## Three outcomes that are not passes

| Status | Meaning |
|---|---|
| `not_applicable` | The assertion had nothing to look at. A question that was not asked has not been answered. |
| `waived` | An operator excused it, with a reason. Stays visible in the report. |
| `warn` | Advisory. Does not gate unless `--warnings-as-errors` is given. |

## Using it from Python

```python
from graflo.architecture.profile import check_manifest_config

report = check_manifest_config(config, profile="world-model")
report.ok                # no error-severity finding survived waivers
report.errors()          # the findings that make it non-conformant
"\n".join(report.to_lines())
```

`check_manifest_config` takes the manifest **as authored** and is the entry
point to prefer. `check_manifest` accepts an already-parsed `GraphManifest`, but
without the authored document assertions 2 and 3 can only warn — the model has
already normalized away the difference between a declared value and a default.

## See also

- [Vertex identity](vertex_identity.md) — the identity modes assertion 2 accepts.
- [GraFlo ontology](ontology.md) — how `semantics` is serialized to RDF.
- [Example 22](../../examples/example-22.md) — `state-core`, the lift that
  satisfies every assertion.

## Further reading

A profile is a conformance level over a manifest, not a schema language for the graph. The
languages that *do* constrain property-graph instances are the comparators:

- Angles, Bonifati, Dumbrava, Fletcher et al. — *PG-Schema: Schemas for Property Graphs*, SIGMOD
  2023, and *PG-Keys: Keys for Property Graphs*, SIGMOD 2021. Types, inheritance and key
  constraints for property graphs. GraFlo's identity modes are the operational counterpart of
  PG-Keys; the profile's `declared-identity` assertion checks only that one was chosen.
- ISO/IEC 39075:2024 *Database languages — GQL*. Graph types as the standard's schema notion.
- W3C SHACL. Shape-based validation of RDF instances; the profile's grounding and unit assertions
  check the manifest, not the data, and a SHACL projection of the schema is the natural next step.
- LinkML — *How to model property graphs*. A polyglot modelling language whose property-graph
  guidance is the nearest neighbour to the `semantics` block.
