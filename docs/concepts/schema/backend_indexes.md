# Backend indexes

Every upsert looks up the stored vertex with the same identity, and without an
index on the identity fields each lookup scans the whole vertex type. This page
is for anyone who writes to a graph database with GraFlo: it says which indexes
GraFlo creates on each backend when it defines the schema, which ones you
declare yourself, and how Neo4j, Memgraph and FalkorDB match edges. After
reading it you know what to put in the `db_profile` block and what to leave to
GraFlo.

## Identity indexes and secondary indexes

GraFlo distinguishes two kinds of vertex index:

- The **identity index** covers the fields a vertex type upserts on: its
  natural key, or `id` for hash, funnel, blank and assigned vertices (see
  [Vertex identity](vertex_identity.md)). GraFlo creates it on every database
  backend. You do not declare it.
- **Secondary indexes** speed up other lookups. You declare them in the
  database profile of the manifest, `schema.db_profile`: per vertex type under
  `vertex_indexes`, per edge under `edge_specs[*].indexes`.

```yaml
schema:
    db_profile:
        vertex_indexes:
            machine:
            -   fields: [plant, model]
                unique: false
        edge_specs:
        -   source: work_order
            target: machine
            relation: targets
            indexes:
            -   fields: [opened_at]
                unique: false
```

An index entry takes `fields` and, optionally, `unique` (default `true`),
`type` (`persistent`, the default, `hash`, `skiplist` or `fulltext`),
`sparse`, `deduplicate` and `name`. Every backend builds `persistent`, `hash`
and `skiplist` as a plain index. Only ArangoDB builds `fulltext`; on any other
target a `fulltext` index is refused when the schema is applied, with the
vertex or edge it is declared on. Because `unique` defaults to
`true`, ArangoDB and PostgreSQL build a declared index as a uniqueness
constraint and reject duplicate values. Write `unique: false` unless that is
what you want.

An `edge_specs` entry can also set `relation_name`, the name the database
stores the relation under. TigerGraph native inverses are set per relation in
`db_profile.native_inverses`, not on a spec; see [Directed, undirected, and
bidirectional edges](../architecture/core_components.md#directed-undirected-and-bidirectional-edges).

## Indexes from secondary identities

Every [secondary identity](../glossary.md#secondary-identity) on a vertex adds
one non-unique index over its fields to `db_profile.vertex_indexes` when the
schema is loaded. You do not declare it. If `vertex_indexes` already has an
index on the same fields, GraFlo keeps yours and adds nothing.

The index is more than an optimization. Endpoint lookups filter on those
fields, and on NebulaGraph a filtered lookup cannot run without a tag index.

The index is non-unique because a secondary identity is only softly unique: a
unique index would reject exactly the duplicate values that the
[ambiguity policy](vertex_identity.md#when-a-lookup-matches-several-vertices)
exists to handle. An index you declare yourself on the same fields keeps the
`unique` setting you gave it.

TigerGraph accepts indexes on a single field only. A composite secondary
identity logs a warning and gets no index there; the lookup does not depend on
it, because GraFlo finds TigerGraph endpoints with an interpreted GSQL query.

NebulaGraph refuses a tag index on a string column unless the index gives a
length. GraFlo gives every string column in a tag index a length of 256. That
includes untyped properties and `DATETIME` and `UUID` properties, which
NebulaGraph stores as strings. When NebulaGraph rejects an index anyway, GraFlo
logs a warning, because every filtered read on that tag then fails with
`IndexNotFound`.

## Per backend

| Backend | Identity index | Declared vertex indexes | Declared edge indexes |
|---|---|---|---|
| Neo4j | One index over the identity fields, created with the other indexes. An index, not a uniqueness constraint | `CREATE INDEX` over the fields; `unique` is not applied | A relationship index, or a uniqueness constraint when `unique: true` |
| Memgraph | One single-property index per identity field, created with the other indexes and again on the match fields when vertices are written | One single-property index per field | One `CREATE EDGE INDEX ON :<relation>(<field>)` per field |
| FalkorDB | One index per identity field, created with the other indexes | One index per field | One relationship index per field |
| NebulaGraph | A tag index over the identity fields, always created, because `LOOKUP` and filtered `MATCH` need it | A tag index | An edge index |
| ArangoDB | A unique persistent index over the identity fields, created with the collection; none when the identity is `_key`, which ArangoDB indexes itself | As declared, with `unique` and `type` applied | As declared, on each edge collection |
| TigerGraph | The primary key (`PRIMARY_ID`, or `PRIMARY KEY` for a composite identity), which TigerGraph indexes itself | Single-field indexes only; a multi-field index is skipped with a warning | Not supported; skipped with a log message |
| PostgreSQL | The vertex table's `PRIMARY KEY` | `CREATE INDEX`, or `CREATE UNIQUE INDEX` when `unique: true` | Not created. Every edge table gets an index on `target_id`, and a unique index over `source_id`, `target_id` and the edge's properties when it has properties |
| GraFlo file backend | None | None | None |

On ArangoDB, TigerGraph and PostgreSQL the identity is covered when the
collection, vertex type or table is created. On Neo4j, Memgraph, FalkorDB and
NebulaGraph, GraFlo creates the identity index together with the declared
indexes when it defines the schema.

## Defining indexes without a schema

The low-level `define_vertex_indexes` call takes the schema as an optional
argument, and only the schema says which fields form each identity. Called
without it, Neo4j, Memgraph and FalkorDB get no identity index and GraFlo logs
a warning; NebulaGraph gets one anyway, and PostgreSQL gets no secondary
indexes. `GraphEngine.define_schema` and `define_and_ingest` always pass the
schema. Pass it yourself when you call `define_vertex_indexes` or
`define_indexes` directly.

## Edge upserts and `MERGE` (Neo4j, Memgraph, FalkorDB)

On the Cypher backends an edge is written with `MERGE`. Its endpoints are
matched on their vertex identities. The relationship itself is merged on a map
of relationship properties, so that two edges between the same endpoints with
different property values stay two edges.

GraFlo takes the property names for that map from the first entry of the
edge's `identities`, leaving out the `source` and `target` tokens and turning a
`relation` token into the relationship's `relation` property. If `identities`
is empty or names no relationship property, it uses all the edge's declared
`properties`. If the edge declares neither, it merges on the endpoints and the
relation alone: one edge per pair of endpoints, whose properties the last
record overwrites.

An edge's `identities` also feed edge indexes, which are defined with the
schema. The `MERGE` key is chosen separately, when the graph is written. Keep
both in line with the uniqueness you intend for the edge.

## What to read next

- [Vertex identity](vertex_identity.md): what the identity fields are for each
  identity mode.
- [Directed, undirected, and bidirectional
  edges](../architecture/core_components.md#directed-undirected-and-bidirectional-edges):
  how edge direction and inverses are stored on each backend.
