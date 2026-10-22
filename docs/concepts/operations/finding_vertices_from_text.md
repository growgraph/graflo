# Finding the vertices a text names

A short text — a message, a ticket comment, a question — often names things your graph already holds: "the Berlin pump is down again", "see SUP-42". `lookup_candidates` reads the graph for those vertices and returns their keys, so an extractor or an agent can link to them instead of creating new ones.

It uses no model and no vectors. A candidate is found in one of four ways:

| Lane | Found when |
|---|---|
| `exact` | A run of the text's words equals a label field |
| `key` | A declared pattern matches the text and its value equals a key field |
| `normalized` | A run of words, normalised, equals a stored normalised label field |
| `alias` | A label lane finds an alias vertex, and its alias edge leads to the target |

Each candidate carries the vertex type, its identity fields, the label it was found by, the lane and the words of the text it came from — never the vertex's other fields.

## A first example

```python
from graflo.hq.lookup import AliasHop, KeyPattern, LookupSpec, lookup_candidates

spec = LookupSpec(
    normalized_fields={"Product": ["name_norm"], "Alias": ["label_norm"]},
    key_patterns=[
        KeyPattern(vertex_type="Ticket", field="number", pattern=r"\bSUP-\d+\b")
    ],
    aliases=[AliasHop(alias_type="Alias", relation="ALIAS_OF", target_type="Product")],
)
candidates = lookup_candidates(
    conn, schema, "the Berlin pump is down, see SUP-42", spec
)
# [Candidate(vertex_type="Ticket", key={"number": "SUP-42"}, lane="key", ...),
#  Candidate(vertex_type="Product", key={"sku": "P-200"}, label="Berlin pump", lane="alias", ...)]
```

## Label fields

A field is a label when its [semantics](../glossary.md#grounding) IRI is `skos:prefLabel`, `skos:altLabel`, `rdfs:label` or `schema:name`. `LookupSpec.label_fields` replaces that choice for a type.

## Normalised labels

`graflo.util.transform.normalize_label` folds case, Unicode width, punctuation and spacing, so `"PUMP-x1!"` and `"Pump X1"` both become `"pump x1"`. To match labels written differently, store that form in a field with a transform at ingestion, and name the field in `LookupSpec.normalized_fields`:

```yaml
transforms:
  - foo: normalize_label
    module: graflo.util.transform
    input: [name]
    output: [name_norm]
```

## Results

- One candidate per vertex, from its best lane, in the order of the table above; within a lane, the longer mention first.
- At most `max_seeds` candidates (default 20). Runs of up to `max_words` words (default 4) are tried as labels.
- `vertex_types` restricts the types returned. Alias types are not returned unless listed there.

## Candidates for staged records

`graflo.link` links partial records (`PartialRecord`: a type and the properties a source states) to entities. It is provisional scaffolding over the key contract, and its API may change in any release. `resolve_records` identifies each record of a batch with [`identify`](../schema/vertex_identity.md#checking-what-some-values-identify), also matching records against the batch's other records, and stages the rest with a reason. Each record may carry an opaque `evidence` dict, which graflo stores and never reads.

After `resolve_records` (and its write), `gather_candidates` lists the entities each staged record may be. It runs the lookup above over the label fields the record states and the text `lookup_text` returns for it, and adds two lanes:

| Lane | Found when |
|---|---|
| `alternate` | A stated secondary identity matched several entities (an `ambiguous` record) |
| `relation` | A stated relation links the record to a resolved one: "the customer" that `RAISED` a resolved HD-4411 gives the customers linked to HD-4411 by `RAISED` |

```python
from graflo.link.candidates import gather_candidates


def quote(record):
    return (record.evidence or {}).get("quote")


entries = gather_candidates(schema, resolution, conn, spec=spec, lookup_text=quote)
for entry in entries:
    print(entry.record.ref, entry.reason, [c.key for c in entry.candidates])
```

- A candidate never closes a record; only keys do. A pick, a rule or a person chooses.
- A candidate whose stored key or secondary-identity values contradict what the record states is left out.
- A relation between two records states two entities: a record is never offered the entity a related record of the same source resolved to, and a relation whose two records resolve to one entity is not written. `promote` skips it and `resolution.dropped` lists it (`DroppedRelation`, reason `same_entity`); after fixing records by hand, `dropped_relations(resolution)` recomputes the list.
- Best lane first, at most `max_candidates` per record (default 10); `truncated` says when more were found.
- `guard` limits every read, as in `resolve_records`.
