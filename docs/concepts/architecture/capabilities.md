# Capabilities

Product features at a glance — schema abstraction, inference, connectors, and ingestion patterns.

## Schema and abstraction

- **Declarative LPG schema** — `Schema` defines vertices, edges, identity rules, and edge **`properties`** in YAML or Python; the single source of truth for graph structure. Transforms/resources are defined in `IngestionModel`.
- **Database abstraction** — one logical schema, multiple backends; each target uses its own `Connection` type behind `ConnectionManager` / `DBWriter`, with DB-specific behavior applied in DB-aware projection (`Schema.resolve_db_aware(...)`, `VertexConfigDBAware`, `EdgeConfigDBAware`).
- **Resource abstraction** — each `Resource` is a reusable actor pipeline that maps raw records to graph elements, decoupled from data retrieval.
- **DataSourceRegistry** — pluggable `AbstractDataSource` adapters (`FILE`, `SQL`, `API`, `SPARQL`, `IN_MEMORY`) bound to Resources by name.

## Schema features

- **Flexible identity and indexing** — logical identity plus DB-specific secondary indexes (`schema.db_profile.vertex_indexes`, `edge_specs`, …). See [Vertex identity modes](../schema/vertex_identity.md).
- **Typed properties** — optional type information on vertex and edge **`properties`** (INT, FLOAT, STRING, DATETIME, BOOL).
- **Hierarchical edge definition** — define edges at any level of nested documents (via resource **edge** steps and actors).
- **Relationship payload** — logical edges declare **`properties`**; additional payload from vertices or row shape is wired in **edge actors** (`vertex_weights`, maps, etc.) with optional types.
- **Blank vertices** — create intermediate vertices for complex relationships.
- **Actor pipeline** — process documents through a sequence of specialised actors (descend, transform, vertex, edge).
- **Reusable transforms** — define and reference transformations by name across Resources. See [Transforms](../ingestion/transforms.md).
- **Vertex filtering** — filter vertices based on custom conditions.
- **PostgreSQL schema inference** — infer schemas from normalised PostgreSQL databases (3NF) with PK/FK constraints.
- **Graph export and migration** — introspect Neo4j or ArangoDB (or a file backend) and **`migrate_graph()`** to any supported target (graph→graph, graph→PostgreSQL); optional file backend for large exports. See [Graph export and migration](../operations/graph_export_migration.md) and [Graph DB migration guide](../../guides/graph_db_migration.md).
- **RDF / OWL schema inference** — infer schemas from OWL/RDFS ontologies: `owl:Class` → vertices, `owl:ObjectProperty` → edges, `owl:DatatypeProperty` → vertex **properties**.
- **SelectSpec** — declarative SQL view on top of `TableConnector` (`view` field): `kind="type_lookup"` for polymorphic relation rows joined to type lookup table(s), or `kind="select"` for full `from` / `joins` / `select` / `where`. See [Table connector views and SelectSpec](../connectors/table_views.md).
- **Bindings SQL filters** — `TableConnector.filters` and `view.where` use the same YAML logical-operator shorthand as vertex `filters` (`OR:`, `AND:`, `NOT:`, `IF_THEN:`), validated when Bindings load and rendered to SQL `WHERE` (including `IF_THEN` as `(NOT … OR …)`). See [Bindings filter cookbook](../connectors/table_views.md#bindings-filter-cookbook-tableconnectorfilters).
