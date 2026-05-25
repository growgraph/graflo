# Graph database targets

GraFlo projects one logical LPG manifest into several **output** backends (`DBType` in `graflo.onto`). The same ingestion pipeline applies; differences show up in `DatabaseProfile` (indexes, naming, TigerGraph defaults) and in each backend's `Connection` implementation.

Use this page to pick a target. For connection setup, see [Installation](../getting_started/installation.md) and [Quick Start](../getting_started/quickstart.md). To try **embedded [Grafeo](https://github.com/GrafeoDB/grafeo)** without Docker, follow [Example 7](../examples/example-7.md#grafeo-embedded-alternative).

## Comparison matrix

| Target | Deployment | Storage (typical) | Query languages (database) | GraFlo filter dialect | Docker stack in repo | GraFlo highlights | Trade-offs |
|--------|------------|-------------------|----------------------------|----------------------|----------------------|-------------------|------------|
| **[ArangoDB](https://www.arangodb.com/)** | Server | Disk (+ memory cache) | AQL, multi-model | AQL | `docker/arango` | Mature document/graph hybrid; flexible collections | Separate server to operate; AQL differs from openCypher |
| **[Neo4j](https://neo4j.com/)** | Server | Disk | Cypher (openCypher) | Cypher | `docker/neo4j` | Widely used; strong MERGE / relationship tooling in Graflo | Licensing/ops for enterprise deployments; server required |
| **[TigerGraph](https://www.tigergraph.com/)** | Server | Disk (cluster) | GSQL, openCypher (REST++) | GSQL | `docker/tigergraph` | **`default_property_values`** in `db_profile`; native **bulk load** (CSV + `LOADING JOB`, S3 staging — [Example 10](../examples/example-10.md)) | Heavier install; GSQL-specific schema rules (sanitizer) |
| **[FalkorDB](https://www.falkordb.com/)** | Server (Redis module) | Memory-oriented | Cypher | Cypher | `docker/falkordb` | Low-latency Cypher on Redis; good for smaller graphs | Redis memory limits; module deployment model |
| **[Memgraph](https://memgraph.com/)** | Server | In-memory or disk | Cypher | Cypher | `docker/memgraph` | Fast openCypher engine; similar MERGE semantics to Neo4j in Graflo | Primarily in-memory positioning; server required |
| **[NebulaGraph](https://www.nebula-graph.io/)** | Server (distributed) | Disk | nGQL (v3) / GQL (v5) | nGQL (v3) or GQL (v5) | `docker/nebula` | Large-scale distributed LPG; version-aware adapter in Graflo | Ops complexity; set `NEBULA_VERSION` (3 vs 5) to match dialect |
| **[Grafeo](https://github.com/GrafeoDB/grafeo)** | **Embedded** (in-process) | In-memory or file (`.grafeo`) | GQL, Cypher, SPARQL, Gremlin, … | Cypher-style filters → **GQL** execution | *None* (no server) | **No Docker**; ships as a core dependency; fast local dev and tests; optional persistence via `GrafeoConfig.path` | Single-process scope; Graflo uses the embedded API (not Grafeo Server); RDF/LPG dual model in Grafeo itself — Graflo RDF ingest still targets LPG projection |

!!! note "Grafeo vs server backends"
    Grafeo runs inside your Python process. Other targets expect a running service (local Docker compose under [`docker/`](https://github.com/growgraph/graflo/tree/main/docker) or your own deployment).

## Feature notes (GraFlo-specific)

| Concern | ArangoDB | Neo4j / FalkorDB / Memgraph | TigerGraph | NebulaGraph | Grafeo |
|---------|----------|----------------------------|------------|-------------|--------|
| **Identity / upsert** | Collection `_key` + identity fields | Node MERGE on identity; edge MERGE on relationship keys | Primary keys on vertex types | TAG/edge indexes + LOOKUP | GQL `MERGE` on labels/properties |
| **Secondary indexes** | `db_profile.vertex_indexes` | Explicit identity + optional secondary | Mostly implicit PK; extras via profile | Identity required in `define_vertex_indexes` | Profile-driven where supported |
| **Bulk ingest path** | Batched upserts | Batched upserts | Optional **bulk CSV** + staging ([guide](../guides/tigergraph_bulk_load.md)) | Batched upserts | Batched upserts (embedded) |
| **Server-side filters** | AQL pushdown where implemented | Cypher | GSQL / REST++ | nGQL/GQL | In-process GQL |
| **Best for** | Document + graph, AQL shops | Ecosystem, graph analytics on Cypher | High-scale load, GSQL | Distributed LPG | **Local iteration**, CI, notebooks, no infra |

## Choosing a target

- **Learning Graflo / CSV examples** — ArangoDB or Neo4j via Docker (Examples 1–4).
- **Polymorphic rows + dynamic edges** — Any Cypher or AQL target; **Grafeo** if you want zero ops ([Example 7](../examples/example-7.md)).
- **PostgreSQL inference → graph** — [Example 5](../examples/example-5.md); pick any output row from the matrix above.
- **RDF / OWL → LPG** — [Example 6](../examples/example-6.md); Grafeo's native RDF store is separate from Graflo's LPG projection path.
- **TigerGraph production load** — [Example 10](../examples/example-10.md) + [bulk load guide](../guides/tigergraph_bulk_load.md).
- **No database server** — **Grafeo** only among current `DBType` outputs.

## Configuration entry points

| Target | Config class | Typical setup |
|--------|--------------|---------------|
| ArangoDB | `ArangoConfig` | `from_docker_env()` / `ARANGO_*` env vars |
| Neo4j | `Neo4jConfig` | `from_docker_env()` / `NEO4J_*` |
| TigerGraph | `TigergraphConfig` | `from_docker_env()` / `TIGERGRAPH_*` |
| FalkorDB | `FalkordbConfig` | `from_docker_env()` / `FALKORDB_*` |
| Memgraph | `MemgraphConfig` | `from_docker_env()` / `MEMGRAPH_*` |
| NebulaGraph | `NebulaConfig` | `from_docker_env()` / `NEBULA_*` (+ version) |
| Grafeo | `GrafeoConfig` | `GrafeoConfig.in_memory()` or `GrafeoConfig(path=...)` — [Quick Start → Grafeo](../getting_started/quickstart.md#grafeo-embedded-target) |

API details: [Database reference](../reference/index.md) (per-backend `Connection` modules).
