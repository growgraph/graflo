# Cards

A **card** is a compact, bounded summary of a GraFlo primitive — the cheapest useful thing
to hand an agent (or a human) when they encounter that primitive for the first time. Cards
answer orientation questions ("what kind of thing is this?", "how big is it?", "where do I
start?") without requiring a database connection or the full object tree.

## Design

All cards inherit from `BaseCard`, which provides two common fields:

- **`id`** (`str | None`) — optional stable identifier (e.g. a UUID from the artifact
  registry). Core `graflo` never requires or generates this; it exists so consumers like
  `graflo-server` can attach registry identity to cards without the core carrying any
  server dependency.
- **`estimated_tokens`** (`int`) — approximate LLM token cost of the serialized card.
  Every builder calculates this automatically via `estimate_tokens`.

Cards are **declarative pydantic models** — lightweight, serializable, and self-documenting
via field descriptions. Every list on a card is bounded (with a count alongside), so cost
does not scale with the size of the object being summarized.

## Card types

| Card | Primitive | Key information |
|------|-----------|-----------------|
| `SchemaCard` | `Schema` | Vertex/edge/property counts, hub types, entry points, identity mode histogram, isolated types, relation vocabulary |
| `VertexCard` | `Vertex` | Identity mode, identity fields, property count, secondary identities |
| `EdgeCard` | `Edge` | Source/target types, relation, directedness, identity and property counts |
| `ResourceCard` | `ResourceConfig` | Pipeline actor count, top vertex/edge targets, encoding, edge inference flag |
| `TransformCard` | `Transform` | Functional vs declarative, module/function, I/O field counts, call strategy |
| `ConnectorCard` | `AnyConnector` | Connector type (File, Table, SPARQL, API, Kafka), resource binding, type-specific summary |
| `DatabaseProfileCard` | `DatabaseProfile` | DB flavor, vertex index count, edge spec count, namespace |
| `ManifestCard` | `GraphManifest` | Block presence (schema/ingestion/bindings), aggregate counts |

## Building cards

Each card type has a corresponding `build_*_card` function:

```python
from graflo.architecture.schema.context import (
    build_card,
    build_vertex_card,
    build_edge_card,
    build_resource_card,
    build_transform_card,
    build_connector_card,
    build_database_profile_card,
    build_manifest_card,
)

# Schema-level summary (the original card)
schema_card = build_card(schema, top_n=10, max_names=25)

# Individual primitive cards
vertex_card = build_vertex_card(vertex, id="optional-uuid")
edge_card = build_edge_card(edge)
resource_card = build_resource_card(resource, top_n=5)
transform_card = build_transform_card(transform)
connector_card = build_connector_card(connector)
db_card = build_database_profile_card(db_profile)
manifest_card = build_manifest_card(manifest)
```

All builders accept an optional `id` keyword for registry integration. The `id` is
pass-through: core `graflo` never interprets it.

## Token estimation

Every card carries an `estimated_tokens` field. The estimate uses `estimate_tokens` from
the budget module, applied to the card's minimal canonical dict representation. This lets
agents budget prompt space before including a card:

```python
card = build_vertex_card(vertex)
if card.estimated_tokens < remaining_budget:
    prompt_parts.append(card.to_minimal_canonical_dict())
```

## API

| Symbol | Module |
|--------|--------|
| `BaseCard`, `SchemaCard`, `VertexCard`, `EdgeCard`, `ResourceCard`, `TransformCard`, `ConnectorCard`, `DatabaseProfileCard`, `ManifestCard` | `graflo.architecture.schema.context.card` |
| `build_card`, `build_vertex_card`, `build_edge_card`, `build_resource_card`, `build_transform_card`, `build_connector_card`, `build_database_profile_card`, `build_manifest_card` | `graflo.architecture.schema.context.card` |
| `EntryPoint` | `graflo.architecture.schema.context.card` |
| All of the above (re-exported) | `graflo.architecture.schema.context` |
