# Cards

A caller with a limited context budget, such as a language-model agent, needs
to orient itself in a manifest before it can afford to load any part of it in
full: what kind of graph this is, how big it is, where a query can start. A
card is a bounded summary of one part of a manifest that answers those
questions at a small, predictable cost. This page is for anyone building such a
caller on the library; after reading it you can build a card for any part of a
manifest, read its token estimate, and know what a card leaves out.

## The idea

A card is a small pydantic model. Every list on it is cut to a fixed length,
and the full count is reported next to it, so the size of a card is set by its
limits, not by the size of what it summarizes.

```python
from suthing import FileHandle

from graflo import GraphManifest
from graflo.architecture.schema.context import build_card

manifest = GraphManifest.from_config(FileHandle.load("manifest.yaml"))
manifest.finish_init()
schema = manifest.require_schema()

card = build_card(schema, top_n=10, max_names=25)
print(card.vertex_count)  # every declared type is counted
print([hub.name for hub in card.hub_types])  # only the top_n most central are listed
print(card.estimated_tokens)  # what including the card would cost
```

Cards are built from the manifest alone. No database connection is needed, and
no data is read.

## Card types

| Card | Summarizes | Carries |
|---|---|---|
| `SchemaCard` | `Schema` | Name, version and description; target database flavor; vertex, edge and property counts; the `top_n` most central vertex types (`hub_types`); entry points; a count of vertex types per identity mode; isolated types and relation names, each cut to `max_names` with the full count alongside |
| `VertexCard` | `Vertex` | Identity mode and identity fields, property count, number of secondary identities, description |
| `EdgeCard` | `Edge` | Source and target types, relation, whether it is directed or symmetric, the declared inverse and its state, identity and property counts, description |
| `ResourceCard` | `ResourceConfig` | Step count, the vertex types its steps name and the edges it enriches with `extra_weights` (each cut to `top_n`), encoding, whether edges are inferred |
| `TransformCard` | `Transform` | Name, whether it wraps a Python function, module and function name, input and output counts, call strategy |
| `ConnectorCard` | any connector | Connector class, name, the resource it feeds, and a one-line summary: the file pattern, table name, RDF class, API path or Kafka topics |
| `DatabaseProfileCard` | `DatabaseProfile` | Database flavor, number of secondary vertex indexes, number of edge specs, target namespace |
| `ManifestCard` | `GraphManifest` | Name and version, which blocks are present, and vertex, edge and resource counts |

An entry point is a vertex type a query can start from: one of the most central
types that has an identity or a secondary index to look it up by. For each, the
card lists its identity, identity mode, secondary identity names and indexed
field sets. A blank type without a secondary index is never an entry point,
because nothing about it can be looked up.

Every card has two common fields:

- `id`, an optional identifier the caller attaches, for instance the key under
  which it stores the manifest. GraFlo never sets or reads it.
- `estimated_tokens`, the approximate cost of the serialized card, filled in by
  every builder.

## Building a card

Each card type has a builder. `build_card` builds the `SchemaCard`; `top_n`
bounds the hub types and entry points, and `max_names` the isolated types and
relation names.

```python
from graflo.architecture.schema.context import (
    build_card,
    build_connector_card,
    build_database_profile_card,
    build_edge_card,
    build_manifest_card,
    build_resource_card,
    build_vertex_card,
)

vertex = schema.core_schema.vertex_config["machine"]
edge = next(iter(schema.core_schema.edge_config.values()))
resource = manifest.require_ingestion_model().resources[0]
connector = manifest.require_bindings().connectors[0]

schema_card = build_card(schema, top_n=10, max_names=25)
vertex_card = build_vertex_card(vertex, id="my-key")
edge_card = build_edge_card(edge, schema=schema)
resource_card = build_resource_card(resource, top_n=5)
connector_card = build_connector_card(connector)
profile_card = build_database_profile_card(schema.db_profile)
manifest_card = build_manifest_card(manifest)
```

`build_transform_card(transform)` summarizes a `Transform` the same way. Every
builder accepts `id`. `build_edge_card` also takes the schema, because the
declared inverse of an edge and the state of that pair are recorded on the
schema, not on the edge.

## Budgeting

`estimated_tokens` is the length of the card's compact JSON form
(`to_minimal_canonical_dict()`, serialized without spaces) divided by four
characters per token and rounded up. That is the form a caller would send. No
tokenizer is involved; a caller that has one can count the serialized form
itself. The estimate lets a caller decide before it includes a card:

```python
budget = 2000
parts = []
for vertex in schema.core_schema.vertex_config.vertices:
    card = build_vertex_card(vertex)
    if card.estimated_tokens > budget:
        break
    parts.append(card.to_minimal_canonical_dict())
    budget -= card.estimated_tokens
```

`estimate_tokens` applies the same estimate to any JSON-serializable value.

## Limits

- A card is an orientation, not the object. It names the `top_n` most central
  types and the first `max_names` relations; the counts say how many were left
  out.
- A card holds no data from the database and no sample records.
- Text fields such as descriptions are carried whole, so a manifest with long
  descriptions gives larger cards.
- The token estimate is approximate. Treat it as a planning number, not a hard
  limit.

## API

| Symbol | Module |
|---|---|
| `BaseCard`, `SchemaCard`, `VertexCard`, `EdgeCard`, `ResourceCard`, `TransformCard`, `ConnectorCard`, `DatabaseProfileCard`, `ManifestCard`, `EntryPoint` | `graflo.architecture.schema.context.card` |
| `build_card`, `build_vertex_card`, `build_edge_card`, `build_resource_card`, `build_transform_card`, `build_connector_card`, `build_database_profile_card`, `build_manifest_card` | `graflo.architecture.schema.context.card` |
| `estimate_tokens` | `graflo.architecture.schema.context.budget` |
| All of the above, re-exported | `graflo.architecture.schema.context` |

## What to read next

- [Vertex identity](vertex_identity.md): the identity modes a `VertexCard`
  reports.
- [Backend indexes](backend_indexes.md): the indexes that make a vertex type an
  entry point.
