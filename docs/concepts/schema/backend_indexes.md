# Backend Index Behavior

This document describes how vertex and edge indexes are handled across different graph database backends. Understanding this helps ensure your schema has the right indexes for efficient lookups and MERGE operations.

In manifests, physical index and naming configuration lives under **`schema.db_profile`** (the `DatabaseProfile` model; Python module `graflo.architecture.schema.database_features`). Below, **`db_profile`** refers to that object—whether loaded from YAML or constructed in code.

## Identity vs Secondary Indexes

- **Identity index**: Required for vertex matching/upserts. Uses `Vertex.identity` (one or more fields for **natural** mode, or synthetic `id` for **hash** and **blank** modes). Each backend handles this differently. See [Vertex identity modes](vertex_identity.md).
- **Secondary indexes**: Optional indexes for query performance. Configured in `db_profile.vertex_indexes` and `db_profile.edge_specs[*].indexes`.
`edge_specs` entries may also set TigerGraph-only **`reverse_edge`** (paired reverse edge type; see [Directed, undirected, and bidirectional edges](../architecture/core_components.md#directed-undirected-and-bidirectional-edges)) and **`relation_name`** overrides in addition to **`indexes`**.


The `vertex_indexes` on **`db_profile`** are for **secondary** indexes only. Identity is handled by the backend during `define_vertex_indexes` or at collection/vertex-type creation.

### Indexes from `secondary_identities`

Declaring [`secondary_identities`](vertex_identity.md#secondary-identities-edge-endpoint-lookup) on a vertex automatically registers one **non-unique** index per field-set into `db_profile.vertex_indexes`. This happens in `Schema.finish_init`, so it applies whether or not a DB-aware view is resolved, and it is idempotent.

The index is not merely an optimization: endpoint resolution filters on those fields, and on **NebulaGraph** a tag index is required for the property lookup to run at all.

Indexes are non-unique by design. Secondary identities are *softly* unique, and a unique constraint would reject exactly the duplicate data the [ambiguity policy](vertex_identity.md#soft-uniqueness-and-ambiguity) exists to handle.

**TigerGraph** supports single-field attribute indexes only; a composite secondary identity logs a warning and is skipped. Resolution there uses an interpreted GSQL query and does not depend on the index.

## Backend Summary

| Backend | Identity index | How |
|---------|----------------|-----|
| **Neo4j** | Explicit | `define_vertex_indexes` prepends identity index when schema is provided. No implicit primary index. |
| **Memgraph** | Explicit | Same as Neo4j. `upsert_docs_batch` also auto-creates on `match_keys` at runtime. |
| **FalkorDB** | Explicit | Same as Neo4j. |
| **Nebula** | Explicit | `define_vertex_indexes` always creates identity index first (required for LOOKUP/MATCH). |
| **ArangoDB** | At collection creation | `create_collection` receives `vertex_config.index(u)` and adds it. `_key` is auto-indexed and skipped. |
| **TigerGraph** | Implicit | Primary keys are auto-indexed at vertex type creation. Secondary indexes are single-field only. |
| **PostgreSQL** | At table creation | Vertex table `PRIMARY KEY`. `define_vertex_indexes` issues `CREATE INDEX` for `vertex_indexes`. |

## Implications

- **Neo4j, Memgraph, FalkorDB**: If you omit `db_profile.vertex_indexes` for a vertex, the identity index is still created automatically when `define_vertex_indexes` runs with a schema. You only need `vertex_indexes` for **additional** (secondary) indexes.
- **ArangoDB, TigerGraph**: Identity is covered at collection/vertex-type creation. `define_vertex_indexes` adds only secondary indexes from `vertex_indexes`.
- **Nebula**: Identity index is always created in `define_vertex_indexes`; `vertex_indexes` adds secondary indexes.

## Schema Required

When `schema` is `None` in `define_vertex_indexes`, identity indexes cannot be ensured for Neo4j, Memgraph, FalkorDB, and Nebula. A warning is logged. Always pass the schema when calling `define_vertex_indexes` or `define_indexes` during `init_db`.

## Edge upserts and `MERGE` (Neo4j, Memgraph, FalkorDB)

Vertex upserts use node keys from `Vertex` identity. For edges, endpoints are matched on those vertex keys; the relationship itself is merged using a **relationship property map** so parallel edges remain distinct.

GraFlo chooses property names for that map from the edge’s logical identity policy: the **first** entry in `Edge.identities` (excluding `source` / `target` tokens; including a `relation` token as the relationship’s `relation` property when applicable). If `identities` is empty or does not name any relationship fields, **all** declared edge **`properties`** names are used instead. Compile-time edge **indexes** from `identities` (via **`db_profile` / `EdgeConfigDBAware`**) remain separate from this writer-time `MERGE` key selection; both should agree with your intended uniqueness for a given edge definition.
