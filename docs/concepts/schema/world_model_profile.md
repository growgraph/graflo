# Conformance profiles

A manifest can load without errors and still leave a reader guessing: what a
type means, what unit a value is in, when a fact was true, where it came from.
A conformance profile is a named, versioned list of checks, called assertions,
that answers such questions for any manifest. Use one in CI to hold manifests
to a standard, including manifests GraFlo did not write, such as an inferred
one or another team's.

```bash
graflo check --profile world-model manifest.yaml
graflo check --profile world-model manifest.yaml --json
graflo check --list-profiles
```

The command exits `0` when the manifest conforms, `1` when it does not, and `2`
when the check could not run, so a CI job can tell a failing manifest from a
missing or broken file. `--warnings-as-errors` makes advisory findings fail;
`--exit-zero` reports without failing.

A profile reads only fields the manifest already has, adds no meaning of its
own and touches no database. `finish_init()` refuses a manifest that is broken;
a profile asks whether a valid manifest can be used by someone who did not
write it.

Two profiles ship with GraFlo:

| Profile | Checks |
|---|---|
| `world-model` (0.1) | that every type is grounded and keyed, every edge's direction and every measurement's unit is stated, and that the manifest can say when a fact was true and where it came from |
| `inverses` (1) | that declared inverse relations are stored consistently; see [Inverse relations in detail](manifest_evolution.md#inverse-relations-in-detail) |

## The `world-model` profile

Six assertions, version 0.1. The
[state-core lift example (23)](../../examples/state-core-lift/index.md) turns a
manifest that fails them into one that passes.

### 1. `grounded-types`

Every vertex and every edge carries `semantics.iri` or `semantics.exact_match`,
and every IRI parses as an absolute IRI.

A type called `Node` means nothing to a reader; `sosa:Observation` does. An IRI
outside the vocabularies the check recognizes (PROV, SOSA, SSN, OWL-Time, QUDT,
SKOS, Dublin Core terms, schema.org, FOAF, OWL, RDFS) is reported as a warning,
not a failure, because an unknown namespace is not the same as a wrong one.

### 2. `declared-identity`

Every vertex declares how it is identified: `identity`, `blank`, `assigned`,
`hash_identity_properties` or `identity_funnel`.

`blank` fails: its nodes carry no natural key, so loading the same data twice
creates every node twice. The less visible failure is a vertex that declares
nothing and so, under `identity_from_all_properties` (on by default), is keyed
on every property it happens to carry. Only the authored document shows that:
when the manifest is parsed, the missing identity is filled in, and the parsed
manifest looks the same as one that declared it.

### 3. `declared-directionality`

Every edge writes `directed:` explicitly.

The reach of a vertex, the set of vertices you get to by following its edges,
depends on which way each edge may be followed. So whether source-to-target
order means something must be stated, not left to the default.
`Edge.directed` defaults to `True`, so, as with assertion 2, only the authored
document can tell a stated direction from a default.

### 4. `declared-units`

Every measured property says what it is measured in. Measured means a `FLOAT`
or `DOUBLE` property that is not part of the key.

Two forms pass:

- **In the schema**: the property carries `semantics.unit`. Use this when the
  type measures one thing.
- **In each row**: the type declares a companion property grounded in
  `qudt:hasUnit`, `qudt:unit`, `qudt:ucumCode` or `qudt:hasQuantityKind`, so
  each record carries its own unit. Use this when the type is general: one
  `Observation` type holding temperatures and pressures has no single unit to
  declare.

The report says which form it found, in `detail.unit_source`. A `unit` written
as an IRI produces a warning about mixed conventions, since units cannot be
compared across a manifest that spells them both ways.

### 5. `temporal`

At least one `DATETIME` property is grounded in a validity or observation-time
term: `prov:generatedAtTime`, `prov:invalidatedAtTime`, `prov:startedAtTime`,
`prov:endedAtTime`, `sosa:resultTime` or `sosa:phenomenonTime`.

Time is modeled as state: a validity interval is a property of a state or
observation record, not a hidden dimension of every type. When the state
changes, the current record is closed and a new one opened, rather than the
old values overwritten.

All six terms take a literal value on purpose. OWL-Time's `time:hasBeginning`
points to a `time:Instant` rather than to a literal, so grounding a `DATETIME`
property in it would become a false statement once the schema is exported to
OWL. OWL-Time belongs in `exact_match`, at the type level.

### 6. `provenance`

Two halves.

The schema half asks whether the manifest can express provenance at all: a
vertex grounded as an agent (`prov:Agent`, `prov:SoftwareAgent`, `sosa:Sensor`
or `foaf:Agent`), and an edge grounded in a derivation or attribution term
(`prov:wasDerivedFrom`, `prov:wasAttributedTo`, `prov:wasGeneratedBy`,
`prov:used`, `sosa:madeBySensor`). Without both, the graph holds statements
with no accountable source.

The ingestion half is reported `not_applicable` when the manifest has no
ingestion model, rather than passing a question it never asked.

This is unrelated to `ManifestMetadata.provenance`, which records the manifest
file's own content address and lineage. Assertion 6 is about the data.

## Waivers

An assertion that does not apply to a deployment can be waived. Waivers live in
a separate document, not in the manifest: a waiver is a decision about one
deployment, and putting it in the manifest would make two manifests that
describe the same graph hash differently.

```yaml
# waivers.yaml
profile: world-model
subject: maintenance           # for the reader; nothing checks it
waivers:
  - assertion: temporal
    reason: the register records no validity time
```

```bash
graflo check --profile world-model manifest.yaml --waivers waivers.yaml
```

```text
  [WAIVED] temporal: Temporal validity is declared or waived  (1 checked)
           waived: the register records no validity time
```

A waived assertion is reported as `waived`, never as `pass`, with its reason
and its findings still printed. `reason` is required, so every waiver says why.
A waiver covers only the assertion it names, and changes nothing for an
assertion that passes.

A waiver travels outside the manifest, so anyone who has only the manifest does
not see it, and the manifest's content hash does not cover it.

## Outcomes that are not passes

| Status | Meaning |
|---|---|
| `not_applicable` | The assertion had nothing to examine. It has not been satisfied, only not asked. |
| `waived` | An operator excused it, with a reason. It stays visible in the report. |
| `warn` | Advisory. It fails the check only with `--warnings-as-errors`. |

## Using it from Python

```python
from suthing import FileHandle

from graflo.architecture.profile import check_manifest_config

config = FileHandle.load("manifest.yaml")
report = check_manifest_config(config, profile="world-model")
report.ok  # no error finding survived the waivers
report.errors()  # the findings that make it non-conformant
print("\n".join(report.to_lines()))
```

`check_manifest_config` takes the manifest as written, and is the one to use.
`check_manifest` accepts an already parsed `GraphManifest`, but without the
authored document assertions 2 and 3 can only warn, because parsing has already
erased the difference between a declared value and a default. Both take
`waivers=`, a `ProfileWaivers` document.

## Further reading

A profile is a conformance level over a manifest, not a schema language for the
graph. The languages that do constrain property-graph data are the comparison
points:

- Angles, Bonifati, Dumbrava, Fletcher et al.: *PG-Schema: Schemas for Property
  Graphs*, SIGMOD 2023, and *PG-Keys: Keys for Property Graphs*, SIGMOD 2021.
  Types, inheritance and key constraints for property graphs. GraFlo's identity
  modes are the operational counterpart of PG-Keys; `declared-identity` checks
  only that one was chosen.
- ISO/IEC 39075:2024 *Database languages: GQL*. Graph types as the standard's
  schema notion.
- W3C SHACL. Shape-based validation of RDF data. The profile's grounding and
  unit assertions check the manifest, not the data.
- LinkML: *How to model property graphs*. A modeling language whose
  property-graph guidance is closest to the `semantics` block.

## What to read next

- [State-core lift example (23)](../../examples/state-core-lift/index.md): a lift
  that makes a manifest pass every assertion.
- [Vertex identity](vertex_identity.md): the identity modes assertion 2 accepts.
- [GraFlo ontology](ontology.md): how `semantics` is written to RDF.
