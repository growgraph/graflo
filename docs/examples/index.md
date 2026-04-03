# Examples 

1. [CSV with Multiple Tabular Sources](example-1.md)
2. [JSON with Self-Reference Vertices](example-2.md)
3. [CSV with Edge Weights and Multiple Relations](example-3.md)
4. [Neo4j Ingestion with Dynamic Relations from Keys](example-4.md)
5. **[🚀 PostgreSQL Schema Inference and Ingestion](example-5.md)** - **Automatically infer graph schemas from normalized PostgreSQL databases (3NF)** with proper primary keys (PK) and foreign keys (FK). Uses intelligent heuristics to detect vertices and edges - no manual schema definition needed! Perfect for migrating relational data to graph databases.
6. **[🔗 RDF / Turtle Ingestion with Explicit Resource Mapping](example-6.md)** - **Infer graph schemas from OWL ontologies and ingest RDF data** using explicit `SparqlConnector` resource mapping. Supports local Turtle files and remote SPARQL endpoints. Perfect for knowledge graph pipelines built on semantic web standards.
7. **[Polymorphic Objects and Relations](example-7.md)** — **Route polymorphic entities and dynamic relations** using `vertex_router` and `edge_router`. One objects table (Person, Vehicle, Institution) and one relations table (EMPLOYED_BY, OWNS, FUNDS, etc.) map to a rich graph with type discriminators and `relation_map`.
8. **[Multi-edge properties with filters and `dress` transforms](example-8.md)** — **Ticker-style CSV to Neo4j** with vertex filters, rich relationship payload, and `dress`-scoped transforms on metric name/value pairs.
9. **[Explicit `connector_connection` Proxy Wiring](example-9.md)** — Show how manifest proxy labels (`conn_proxy`) are resolved at runtime into real DB configs via `ConnectionProvider`.