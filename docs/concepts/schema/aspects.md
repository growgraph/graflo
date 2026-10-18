# Aspects

Every graph sooner or later has to answer questions that are not about its
domain: *where did this fact come from? who reported it? what was this value
last March?* No domain schema owns the types that answer them, and every team
that adds them by hand names and keys them differently.

An **aspect** is such a vocabulary, packaged once: a small set of vertex and
edge types, grounded in a W3C standard, that GraFlo adds to any manifest. You
do not copy it into your schema; you **lift** your manifest with it, and the
lift writes the change as [ops](manifest_evolution.md) you review before
anything is applied.

Two aspects ship with GraFlo:

| Aspect | Adds | Answers | Standard | Needs a spec |
|---|---|---|---|---|
| `provenance` | `SourceRecord`, `Agent`, `wasDerivedFrom`, `wasAttributedTo` | where a fact was read from, and who produced that record | PROV-O | no |
| `state` | `<Type>State`, `<Type>Observation`, `specializationOf`, `hasFeatureOfInterest` | how a fact changed over time, and what was measured in which unit | PROV-O, SOSA, QUDT | yes |

They are independent: lift either one alone, or both (the default).

## Lifting a manifest

```bash
graflo lift manifest.yaml --aspect provenance -o lifted.yaml
graflo lift manifest.yaml --spec lift.yaml --emit-ops ops.yaml -o lifted.yaml
```

The first lifts `provenance` alone, which needs no spec. The second lifts both
aspects; `lift.yaml` is a [lift spec](#the-lift-spec). `--emit-ops` writes the
plan, `--dry-run` writes nothing, and the lifted manifest is checked against the
[`world-model` profile](world_model_profile.md) unless you pass `--no-check`.

From Python:

```python
from graflo.architecture.evolution import apply_evolution, invert_ops
from graflo.architecture.evolution.aspects import LiftSpec, plan_lift

ops = plan_lift(manifest, LiftSpec(provenance_for=["Ticket"]), aspects=["provenance"])
lifted = apply_evolution(manifest, ops)
undo, blockers = invert_ops(ops, manifest=manifest)
```

The plan lists everything it adds before anything it removes: groundings,
directions and units first, then the state types, then the provenance types,
and last the removal of properties that moved onto a state type. `ASPECTS`
lists the aspects by name.

A lift changes the **schema only**. Nothing fills the new types until a
resource does; see [Filling the provenance types](#filling-the-provenance-types).

## The provenance aspect

Two types and two edges:

| Type | Key | Properties | Grounded as |
|---|---|---|---|
| `SourceRecord` | `source_record_id` | `source_uri`, `media_type`, `content_hash`, `retrieved_at` | `prov:Entity` |
| `Agent` | `agent_id` | `label`, `agent_kind` | `prov:Agent`, `sosa:Sensor` |

| Edge | Properties | Grounded as |
|---|---|---|
| `<Type> -wasDerivedFrom-> SourceRecord` | — | `prov:wasDerivedFrom` |
| `SourceRecord -wasAttributedTo-> Agent` | `role` | `prov:wasAttributedTo` |

A `SourceRecord` is the record a fact was read from: a document, a message, an
API response, a table row. It holds the record's **address and content hash,
never its content**; the content stays where it was stored, and the graph
points at it. An `Agent` is whoever or whatever produced the record: a person,
a team, a sensor, a system. `role` says how, such as `author` or `reporter`.

Which types gain `wasDerivedFrom`:

- the types you name in the spec's `provenance_for`;
- every `<Type>State` and `<Type>Observation`, when the `state` aspect is
  lifted too.

`SourceRecord -wasAttributedTo-> Agent` is added even when nothing else is, so
the schema can always say who produced a record.

## Filling the provenance types

A lifted `Ticket` is filled like any other type: a resource writes the ticket,
the record it came from and its author, and the two edges between them.

```yaml
ingestion_model:
    resources:
    -   name: tickets
        pipeline:
        -   vertex: Ticket
        -   vertex: SourceRecord
        -   vertex: Agent
            from: {agent_id: reporter_email, label: reporter_name}
        -   edge: {from: Ticket, to: SourceRecord, relation: wasDerivedFrom}
        -   edge: {from: SourceRecord, to: Agent, relation: wasAttributedTo}
```

Each row carries the ticket, its record's address and the reporter:

```json
{"ticket_id": "SUP-42", "title": "Pump P-200 loses pressure",
 "source_record_id": "tracker:SUP-42@3", "source_uri": "https://tracker.example/SUP-42",
 "content_hash": "sha256:9f2c", "retrieved_at": "2026-10-18T09:00:00Z",
 "reporter_email": "ann@example.com", "reporter_name": "Ann Lee", "role": "reporter"}
```

`Ticket` and `SourceRecord` take the fields named like their properties;
`Agent` maps its fields with `from`. `role` lands on `wasAttributedTo`, because that edge
declares it. Give `source_record_id` a value that changes when the record
does (here the ticket and its revision), so an edited record is a new
`SourceRecord` rather than an overwrite.

## The state aspect

For each type named in the spec's `stateful`, a `<Type>State` takes the
properties that change over time:

| Type | Key | Properties |
|---|---|---|
| `<Type>State` | the type's key + `valid_from` | the type's key, the moved properties, `valid_from`, `valid_to` |
| `<Type>Observation` | the type's key + `observed_property` + `result_time` | the type's key, `observed_property`, `result_value`, `result_unit`, `result_time` |

| Edge | Grounded as |
|---|---|
| `<Type>State -specializationOf-> <Type>` | `prov:specializationOf` |
| `<Type>Observation -hasFeatureOfInterest-> <Type>` | `sosa:hasFeatureOfInterest` |

A state is keyed on its subject *and* `valid_from`, so each value is a separate
fact: a new value closes the old interval instead of overwriting it. An
observation is one measurement at one time; its unit travels in each row
(`result_unit`, a UCUM code), because one observation type holds temperatures
and pressures alike. Types named in `observed` gain an observation type.

The state aspect also makes the manifest say what it left implicit: it grounds
the types you list, states `directed` on every edge, sets the units you declare
on existing properties, and pins an identity for types that only had one by
fallback.

By default the moved properties are removed from their type (`retire: move`);
`retire: keep` leaves them in place as the current value beside the history.

## The lift spec

A lift can see structure, not meaning: nothing in a schema says that `temp_c`
is in degrees Celsius or that `status` changes while `serial_number` does not.
The spec declares that. Only the `state` aspect requires one; `provenance`
reads `provenance_for` and `provenance` from it when one is given.

| Key | Aspect | Declares |
|---|---|---|
| `grounding` | state | per type, the concept it denotes: `{Machine: {iri: …, exact_match: […], synonyms: […]}}` |
| `edge_grounding` | state | the same per edge, addressed by `source`, `target` and `relation` |
| `identity` | state | a key for a type that declares none |
| `stateful` | state | per type, the properties that change over time: `{Machine: [status]}` |
| `observed` | state | the types that are measured at a time |
| `measured` | state | units of existing properties: `{Machine.operating_temp: Cel}` (UCUM; currency as ISO 4217, `USD`) |
| `retire` | state | `move` (default) or `keep` the properties named in `stateful` |
| `provenance_for` | provenance | the types that gain `wasDerivedFrom` |
| `provenance` | provenance | `false` leaves the aspect out, like not naming it |

## When a lift refuses

A lift never guesses. It stops with a `LiftError` that names what is missing
when:

- an aspect name is unknown, or the manifest has no schema block;
- a type the lift would add already exists (`SourceRecord`, `Agent`,
  `MachineState`, …): rename it first, or leave that aspect out if your type
  already plays its role;
- the spec names a type or property the manifest does not have;
- a `stateful` or `observed` type has no key to point back to, or a `stateful`
  property is part of the key;
- a type relies on the identity fallback and the spec gives it no `identity`.

## See also

- [Track state and measurements (example 23)](../../examples/lift-aspects/index.md):
  a manifest lifted with both aspects, before and after.
- [Conformance profiles](world_model_profile.md): the checks a lifted manifest
  passes.
- [Manifest evolution](manifest_evolution.md): applying, recording and undoing
  the ops a lift plans.
