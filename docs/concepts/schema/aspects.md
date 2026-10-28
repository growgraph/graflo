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

Three aspects ship with GraFlo:

| Aspect | Adds | Answers | Standard | Needs a spec |
|---|---|---|---|---|
| `provenance` | `SourceRecord`, `Agent`, `wasDerivedFrom`, `wasAttributedTo` | where a fact was read from, and who produced that record | PROV-O | no |
| `state` | `<Type>State`, `<Type>Observation`, `specializationOf`, `hasFeatureOfInterest` | how a fact changed over time, and what was measured in which unit | PROV-O, SOSA, QUDT | yes |
| `access` | a reserved `access_zones` label on the types you name | which readers may see each element | — | yes |

They are independent. `provenance` and `state` are lifted by default; `access`
only when you name it.

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

A manifest that already has a `SourceRecord` or an `Agent` keeps it when it
conforms: the same key and class IRI, and every property in the table with the
same type. Further properties, secondary identities and its own description
stay. An edge the manifest already has is not added again. So manifests that
carry the vocabulary, such as source kits, can be composed first and lifted
after. A type with the name that does not conform is refused, and the error
says what differs.

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

## The access aspect

The lift adds one field, `access_zones: LIST<INT>`, to each vertex type in the
spec's `access_for` and each edge relation in `access_edges`. Each vertex type
names how an upsert combines its zones: `replace` (the element takes the zones of
its latest write) or `union` (it keeps every zone it was written with). Edges
always replace.

The field is **reserved**: no document supplies it, and a type sheet, element
text or instance schema never shows it. The writer sets it from a stamp, and
refuses to write a lifted type without one:

```python
await writer.write(container, conn_conf, "messages", stamp={"access_zones": [zone_id]})
```

A traversal refuses a filter or anchor key that names `access_zones`. For
every other query, a service calls `query.refuse_reserved(schema.reserved_fields())`
before reading, and leaves the field out of what it returns. To read only what a
reader may see, pass a guard that matches the zones that reader opens; every
vertex and edge of a traversal must pass it, so a path through a hidden element
reaches nothing:

```python
from graflo.filter.onto import ComparisonOperator, FilterExpression

guard = FilterExpression(
    kind="leaf",
    field="access_zones",
    cmp_operator=ComparisonOperator.ANY_IN,
    value=[sorted(reader_zones)],
)
found = conn.graph_neighbors("Ticket", "SUP-42", hops=2, schema=schema, guard=guard)
```

A write's own lookups (an edge endpoint found by a secondary identity, an
attached record, an extra weight) take the same guard, so a write never links
to, attaches onto or copies from a vertex the guard hides:

```python
await writer.write(container, conn_conf, "messages", stamp=stamp, guard=guard)
```

An ingest stamps and guards per record: the caller passes
`IngestionParams(stamp_for=..., guard_for=...)`, each called with the
resource name and a record, and what a record casts to is written with that
record's stamp and guard only (see
[Parallelism](../ingestion/parallelism.md#a-stamp-and-a-guard-per-record)):

```python
params = IngestionParams(
    stamp_for=lambda resource, record: {"access_zones": [zone_for(record)]},
    guard_for=lambda resource, record: guard_for_zone(zone_for(record)),
)
engine.ingest(manifest=manifest, target_db_config=conn_conf, ingestion_params=params)
```

What a zone is and which zones a reader opens are not part of the graph; your
service decides both. Combine several conditions with `conjoin()`. Guards and
`union` labels are supported on Neo4j and the file backend; other backends
refuse them. The guard
names the field as stored, so do not rename `access_zones` in `db_profile`. A
reserved field cannot be part of an identity.

## The lift spec

A lift can see structure, not meaning: nothing in a schema says that `temp_c`
is in degrees Celsius or that `status` changes while `serial_number` does not.
The spec declares that. The `state` and `access` aspects require one;
`provenance` reads `provenance_for` and `provenance` from it when one is given.

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
| `access_for` | access | per vertex type, the merge mode of its zones: `{Ticket: replace, Product: union}` |
| `access_edges` | access | the edge relations that gain `access_zones` |

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
- a type relies on the identity fallback and the spec gives it no `identity`;
- `access` is named with nothing to label, a type already declares
  `access_zones`, or an edge is given `shared`.

## See also

- [Track state and measurements (example 23)](../../examples/lift-aspects/index.md):
  a manifest lifted with both aspects, before and after.
- [Conformance profiles](world_model_profile.md): the checks a lifted manifest
  passes.
- [Manifest evolution](manifest_evolution.md): applying, recording and undoing
  the ops a lift plans.
