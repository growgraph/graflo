# Examples

Runnable walkthroughs with sample data under `examples/` in the repository.

## By topic

| Topic | Examples | Related docs |
|-------|----------|--------------|
| CSV / files | [1](example-1.md), [3](example-3.md), [8](example-8.md), [11](example-11.md), [12](example-12.md), [15](example-15.md) | [Transforms](../concepts/ingestion/transforms.md), [Identity inference guide](../guides/identity_inference.md) |
| JSON / nested | [2](example-2.md), [4](example-4.md), [7](example-7.md) | [Core components](../concepts/architecture/core_components.md) |
| PostgreSQL | [5](example-5.md) | [Capabilities](../concepts/architecture/capabilities.md) |
| RDF / SPARQL | [6](example-6.md) | [GraFlo ontology](../concepts/schema/ontology.md) |
| API | [14](example-14.md) | [API env wiring guide](../guides/api_env_wiring.md), [API connector](../concepts/connectors/api_connector.md) |
| TigerGraph / S3 | [10](example-10.md) | [TigerGraph bulk load guide](../guides/tigergraph_bulk_load.md) |
| Graph export / file backend | [13](example-13.md) | [Graph export guide](../guides/graph_export_and_replay.md) |
| Graph DB migration (Neo4j/Arango → any target) | — | [Graph DB migration guide](../guides/graph_db_migration.md), [Quick start](../getting_started/quickstart.md#graph-export-and-migration) |
| Connectors / proxy wiring | [9](example-9.md) | [Runtime connector updates](../concepts/connectors/runtime_updates.md) |

## Full list

1. [CSV with Multiple Tabular Sources](example-1.md)
2. [JSON with Self-Reference Vertices](example-2.md)
3. [CSV with Edge Weights and Multiple Relations](example-3.md)
4. [Neo4j Ingestion with Dynamic Relations from Keys](example-4.md)
5. **[PostgreSQL Schema Inference and Ingestion](example-5.md)** — automatically infer graph schemas from normalized PostgreSQL databases (3NF) with PK/FK heuristics.
6. **[RDF / Turtle Ingestion with Explicit Resource Mapping](example-6.md)** — infer schemas from OWL ontologies and ingest RDF via `SparqlConnector`.
7. **[Polymorphic Objects and Relations](example-7.md)** — `vertex_router` + dynamic `edge` for type discriminators and relation maps.
8. **[Multi-edge properties with filters and `dress` transforms](example-8.md)** — ticker-style CSV with vertex filters and metric transforms.
9. **[Explicit `connector_connection` Proxy Wiring](example-9.md)** — resolve `conn_proxy` labels at runtime via `ConnectionProvider`.
10. **[TigerGraph bulk load and S3 staging](example-10.md)** — CSV staging, native `LOADING JOB`, `staging_proxy`.
11. **[Flat-row dynamic edges with `vertex_router`](example-11.md)** — one row encodes `(source, target, relation)`.
12. **[Vertex roles and multi-intent edges](example-12.md)** — `role` slots and `links` on edge steps.
13. **[GraFlo file backend](example-13.md)** — export, ingest to disk, replay to ArangoDB or PostgreSQL.
14. **[API env wiring](example-14.md)** — `register_all_api_configs_from_env` for multi-proxy manifests.
15. **[Identity inference from CSV](example-15.md)** — infer vertex identities from flat CSV, ingest to file backend.
