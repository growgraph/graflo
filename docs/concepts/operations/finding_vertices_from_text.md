# Finding the vertices a text names

A short text — a message, a ticket comment, a question — often names things your graph already holds: "the Berlin pump is down again", "see SUP-42". `lookup_candidates` reads the graph for those vertices and returns their keys, so an extractor or an agent can link to them instead of creating new ones.

It uses no model and no vectors. A candidate is found in one of five ways:

| Lane | Found when |
|---|---|
| `exact` | A run of the text's words equals a label field |
| `key` | A declared pattern matches the text and its value equals a key field |
| `normalized` | A run of words, in its source's normal form, equals a [derived](../schema/comparing_values.md) `form: normal` field |
| `alias` | A label lane finds an alias vertex, and its alias edge leads to the target |
| `compatible` | A run of words, in its source's normal form, is one of a derived `form: keys` field's values: "Berlin" finds every unit whose site starts with Berlin |

Each candidate carries the vertex type, its identity fields, its label and `hits`: for every lane that found it, the words of the text it came from. It never carries the vertex's other fields.

## A first example

```python
from graflo.hq.lookup import AliasHop, KeyPattern, LookupSpec, lookup_candidates

spec = LookupSpec(
    key_patterns=[
        KeyPattern(vertex_type="Ticket", field="number", pattern=r"\bSUP-\d+\b")
    ],
    aliases=[AliasHop(alias_type="Alias", relation="ALIAS_OF", target_type="Product")],
)
candidates = lookup_candidates(
    conn, schema, "the Berlin pump is down, see SUP-42", spec
)
# [Candidate(vertex_type="Ticket", key={"number": "SUP-42"},
#            hits=[LaneHit(lane="key", matched="SUP-42")]),
#  Candidate(vertex_type="Product", key={"sku": "P-200"}, label="Berlin pump",
#            hits=[LaneHit(lane="alias", matched="Berlin pump", targets=1)])]
```

An `alias` hit carries `targets`, the number of vertices the alias reached: an alias that names one vertex in the reader's view identifies it, one that names several does not.

## Label fields

A field is a label when its [semantics](../glossary.md#grounding) IRI is `skos:prefLabel`, `skos:altLabel`, `rdfs:label` or `schema:name`. `LookupSpec.label_fields` replaces that choice for a type.

## How do I match labels written differently?

Declare the normal form as a derived field; GraFlo fills it on every write, and the `normalized` lane reads it:

```yaml
-   name: name
    match: {normalizer: label_v1}
-   name: name_norm
    derived: {source: name, form: normal}
```

A `form: keys` field feeds the `compatible` lane the same way. It is read by `ANY_IN`, which Neo4j, FalkorDB and the file backend support (`supports_membership_read`); on another backend, a lookup of a type with a keys field is refused. See [Comparing and deriving field values](../schema/comparing_values.md).

## Results

- One candidate per vertex. Its `hits` hold the best hit of each lane that found it, in the order of the table above; within a lane, the longer match wins. Vertices are ordered by their best hit.
- At most `max_seeds` vertices (default 20). Runs of up to `max_words` words (default 4) are tried as labels.
- `vertex_types` restricts the types returned. Alias types are not returned unless listed there.

## Candidates for staged records

`graflo.link` links partial records (`PartialRecord`: a type and the properties a source states) to entities. `resolve_records` identifies each record of a batch with [`identify`](../schema/vertex_identity.md#checking-what-some-values-identify), also matching records against the batch's other records, and stages the rest with a reason. Each record may carry an opaque `evidence` dict, which graflo stores and never reads.

After `resolve_records` (and its write), `gather_candidates` lists the entities each staged record may be. It runs the lookup above over the label fields the record states, the fields it states that others are derived from, and the text `lookup_text` returns for it, and adds three lanes:

| Lane | Found when |
|---|---|
| `alternate` | A stated secondary identity matched several entities (an `ambiguous` record) |
| `relation` | A stated relation links the record to a resolved one: "the customer" that `RAISED` a resolved HD-4411 gives the customers linked to HD-4411 by `RAISED` |
| `context` | The entity is a neighbour of the record's type, one hop in either direction from an entity the same source resolved by key, or from a subject the caller says the source concerns (`subjects`): "its controller" in a section about one pump |

```python
from graflo.link.candidates import gather_candidates


def quote(record):
    return (record.evidence or {}).get("quote")


entries = gather_candidates(schema, resolution, conn, spec=spec, lookup_text=quote)
for entry in entries:
    print(entry.record.ref, entry.reason, [c.key for c in entry.candidates])
```

- A candidate never closes a record; only keys do. A pick, a rule or a person chooses.
- One candidate per entity. `lanes` lists every way it was found, best lane first. A `relation` hit names the relation and the anchor; a `metadata` hit (added by the caller) a role; a `context` hit its anchor (a record's ref, or the role `subject`) and the level of the record's stated label against the entity's, by which context hits are ranked; every other hit what matched.
- `attributes` grades each other property the record states against the entity's stored value, under the field's `match` (`exact`, `compatible`, `differs`, `conflict`, `unknown`), with whether the field is `exclusive`. Stored values are never returned. Derived fields are never compared; their sources are.
- A candidate whose stored key or secondary-identity values are in `conflict` with what the record states is left out. Any other level is reported, not a bar.
- `support` holds what identifies the candidate: a hit in `key`, `alternate`, `exact` or `normalized`; an exclusive field stated and stored alike; an alias naming only it; and whether any field is in `conflict`. `graflo.link.candidates.support(candidate)` returns `accept` when one identifies it and nothing conflicts, else `propose`. A `compatible` or `context` hit never identifies on its own.
- `comparable(vertex_type, field)` limits which stated properties are probed, looked up by or checked, keys included. Reserved fields, such as access zones, are never compared.
- A relation between two records states two entities: a record is never offered the entity a related record of the same source resolved to, and a relation whose two records resolve to one entity is not written. `promote` skips it and `resolution.dropped` lists it (`DroppedRelation`, reason `same_entity`); after fixing records by hand, `dropped_relations(resolution)` recomputes the list.
- Best lane first, at most `max_candidates` entities per record (default 10), counted after lanes are merged and keys checked; `truncated` says when more were found.
- `guard` limits every read, as in `resolve_records`, the context lane's hops included.
