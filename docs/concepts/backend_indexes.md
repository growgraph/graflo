# Backend Index Behavior

This document describes how vertex and edge indexes are handled across different graph database backends. Understanding this helps ensure your schema has the right indexes for efficient lookups and MERGE operations.

## Identity vs Secondary Indexes

- **Identity index**: Required for vertex matching/upserts. Uses `Vertex.identity` (or `_key`/`id` for blank vertices). Each backend handles this differently.
- **Secondary indexes**: Optional indexes for query performance. Configured in `database_features.vertex_indexes` and `database_features.edge_specs[*].indexes`.

The `vertex_indexes` in `database_features` is for **secondary** indexes only. Identity is handled by the backend during `define_vertex_indexes` or at collection/vertex-type creation.

## Backend Summary

| Backend | Identity index | How |
|---------|----------------|-----|
| **Neo4j** | Explicit | `define_vertex_indexes` prepends identity index when schema is provided. No implicit primary index. |
| **Memgraph** | Explicit | Same as Neo4j. `upsert_docs_batch` also auto-creates on `match_keys` at runtime. |
| **FalkorDB** | Explicit | Same as Neo4j. |
| **Nebula** | Explicit | `define_vertex_indexes` always creates identity index first (required for LOOKUP/MATCH). |
| **ArangoDB** | At collection creation | `create_collection` receives `vertex_config.index(u)` and adds it. `_key` is auto-indexed and skipped. |
| **TigerGraph** | Implicit | Primary keys are auto-indexed at vertex type creation. |

## Implications

- **Neo4j, Memgraph, FalkorDB**: If you omit `database_features.vertex_indexes` for a vertex, the identity index is still created automatically when `define_vertex_indexes` runs with a schema. You only need `vertex_indexes` for **additional** (secondary) indexes.
- **ArangoDB, TigerGraph**: Identity is covered at collection/vertex-type creation. `define_vertex_indexes` adds only secondary indexes from `vertex_indexes`.
- **Nebula**: Identity index is always created in `define_vertex_indexes`; `vertex_indexes` adds secondary indexes.

## Schema Required

When `schema` is `None` in `define_vertex_indexes`, identity indexes cannot be ensured for Neo4j, Memgraph, FalkorDB, and Nebula. A warning is logged. Always pass the schema when calling `define_vertex_indexes` or `define_indexes` during `init_db`.

## Edge upserts and `MERGE` (Neo4j, Memgraph, FalkorDB)

Vertex upserts use node keys from `Vertex` identity. For edges, endpoints are matched on those vertex keys; the relationship itself is merged using a **relationship property map** so parallel edges remain distinct.

GraFlo chooses property names for that map from the edge’s logical identity policy: the **first** entry in `Edge.identities` (excluding `source` / `target` tokens; including a `relation` token as the relationship’s `relation` property when applicable). If `identities` is empty or does not name any relationship fields, **all** declared edge **`properties`** names are used instead. Compile-time edge **indexes** from `identities` (via `database_features`) remain separate from this writer-time `MERGE` key selection; both should agree with your intended uniqueness for a given edge definition.
