# Architecture diagrams

When you drive GraFlo from Python, you meet a handful of objects: the engine
you call, the models behind the three blocks of a manifest, and the classes
that run an ingest. These diagrams show how they relate and which one owns
what. You do not need them to write a manifest; for that, see
[core components](core_components.md).

## GraphEngine and what it delegates to

`GraphEngine` is the object you call. Look at its methods: some write a
manifest or a schema for you (`infer_manifest` from PostgreSQL,
`infer_schema_from_rdf` from an ontology, `infer_schema_from_graph` from a
graph database), `define_schema` creates the schema in the target, `ingest`
and `define_and_ingest` load data, `delete_vertices` and `delete_edges`
remove data by identity, `export_graph` reads a whole graph back out, and
`migrate_graph` moves one to another database. For each call it creates the
objects its arrows point to.

```mermaid
classDiagram
    direction LR

    class GraphEngine {
        +target_db_flavor: DBType
        +sample_resources(source) SourceSample
        +introspect(postgres_config) SchemaIntrospectionResult
        +infer_manifest(postgres_config) GraphManifest
        +create_bindings(postgres_config) Bindings
        +infer_schema_from_rdf(source) tuple~Schema, IngestionModel~
        +infer_schema_from_graph(source_config) Schema
        +define_schema(manifest, target_db_config)
        +ingest(manifest, target_db_config, ingestion_params)
        +define_and_ingest(manifest, target_db_config, ingestion_params)
        +diff_live_schema(conn_conf, schema) LiveSchemaDrift
        +delete_vertices(target_db_config, schema, vertex, key_docs)
        +delete_edges(target_db_config, schema, edge_id, endpoints)
        +export_graph(source_config) GraFloOutput
        +migrate_graph(source_config, target_config)
    }

    class SQLInferenceManager {
        +introspect(schema_name) SchemaIntrospectionResult
        +infer_complete_schema(schema_name) tuple~Schema, IngestionModel~
    }

    class Sanitizer {
        +db_flavor: DBType
        +sanitize_manifest(manifest) GraphManifest
    }

    class Caster {
        +schema: Schema
        +ingestion_model: IngestionModel
        +ingestion_params: IngestionParams
        +ingest(target_db_config, bindings)
    }

    class ConnectionManager {
        +config: DBConfig
    }

    class Connection {
        <<abstract>>
        +ensure_target_namespace(schema, create)
        +apply_target_schema(schema, recreate)
        +fetch_docs(class_name)
        +graph_neighbors(vertex_type, key)
    }

    GraphEngine --> SQLInferenceManager : introspect, infer_manifest
    GraphEngine --> Sanitizer : infer_manifest
    GraphEngine --> Caster : ingest
    GraphEngine --> ConnectionManager : define_schema, migrate_graph
    ConnectionManager --> Connection : opens
```

`SQLInferenceManager` infers a schema and resources from a SQL database but
does not adjust names for the target. `GraphEngine.infer_manifest` does, through
`Sanitizer`; when you build a manifest from `SQLInferenceManager` output
yourself, call `Sanitizer(db_flavor=...).sanitize_manifest(manifest)` once at
the end. `ConnectionManager` is a context manager: `with
ConnectionManager(connection_config=config) as conn:` opens the `Connection`
of the backend that `config` describes, one subclass per database.

## The manifest models

One `GraphManifest` holds up to three blocks. Look at the split: `Schema`
holds the graph (vertex and edge types) and the database profile;
`IngestionModel` holds resources and named transforms; `Bindings` holds
connectors and their wiring to resources. A resource's `pipeline` is kept as
the list of step mappings you wrote; the steps are checked against the schema
when the manifest is initialized with `finish_init()`.

```mermaid
classDiagram
    direction TB

    class GraphManifest {
        +schema: Schema?
        +ingestion_model: IngestionModel?
        +bindings: Bindings?
        +finish_init()
    }

    class Schema {
        +metadata: GraphMetadata
        +core_schema: CoreSchema
        +db_profile: DatabaseProfile
    }

    class GraphMetadata {
        +name: str
        +version: str?
        +description: str?
    }

    class CoreSchema {
        +vertex_config: VertexConfig
        +edge_config: EdgeConfig
    }

    class VertexConfig {
        +vertices: list~Vertex~
        +identity_from_all_properties: bool
    }

    class Vertex {
        +name: str
        +properties: list~Field~
        +identity: list~str~
        +filters: list
        +blank: bool
        +assigned: bool
        +hash_identity_properties: list~str~
        +identity_funnel: IdentityFunnel?
        +secondary_identities: list~SecondaryIdentity~
    }

    class Field {
        +name: str
        +type: FieldType?
        +item_type: FieldType?
    }

    class EdgeConfig {
        +edges: list~Edge~
        +inverses: list~EdgeInverse~
        +symmetric: list~str~
    }

    class Edge {
        +source: str
        +target: str
        +relation: str?
        +directed: bool
        +identities: list~list~str~~
        +properties: list~Field~
    }

    class DatabaseProfile {
        +db_flavor: DBType
        +target_namespace: str?
        +vertex_storage_names: dict
        +vertex_property_names: dict
        +vertex_indexes: dict
        +edge_specs: list~EdgePhysicalSpec~
        +native_inverses: list~str~
    }

    class IngestionModel {
        +resources: list~ResourceConfig~
        +transforms: list~ProtoTransform~
        +endpoints_on_ambiguous: str
    }

    class ResourceConfig {
        +name: str
        +pipeline: list~dict~
        +infer_edges: bool
        +tolerate_transform_errors: bool
    }

    class ProtoTransform {
        +name: str?
        +module: str?
        +foo: str?
        +params: dict
    }

    class Bindings {
        +connectors: list
        +resource_connector: list
        +connector_connection: list
        +get_connectors_for_resource(name) list
    }

    GraphManifest *-- Schema : schema
    GraphManifest *-- IngestionModel : ingestion_model
    GraphManifest *-- Bindings : bindings
    Schema *-- GraphMetadata : metadata
    Schema *-- CoreSchema : core_schema
    Schema *-- DatabaseProfile : db_profile
    CoreSchema *-- VertexConfig : vertex_config
    CoreSchema *-- EdgeConfig : edge_config
    VertexConfig *-- "0..*" Vertex : vertices
    Vertex *-- "0..*" Field : properties
    EdgeConfig *-- "0..*" Edge : edges
    Edge *-- "0..*" Field : properties
    IngestionModel *-- "0..*" ResourceConfig : resources
    IngestionModel *-- "0..*" ProtoTransform : transforms
```

Two names differ between YAML and Python. `Schema.core_schema` is written
`graph` in YAML, and the manifest's `schema` key is the attribute
`GraphManifest.graph_schema` in Python.

## The ingestion runtime

`Caster` runs an ingest. Look at the flow from left to right: it builds a
`DataSourceRegistry` from the bindings, reads each data source in batches,
casts every batch into a `GraphContainer`, and hands the container to a
`DBWriter`, which writes vertices and then edges through a connection to the
target. `IngestionParams` holds the settings of one run: batch size,
parallelism and error handling. Its fields are explained on
[parallelism](../ingestion/parallelism.md) and
[document cast errors](../ingestion/doc_errors.md).

```mermaid
classDiagram
    direction LR

    class Caster {
        +ingestion_params: IngestionParams
        +ingest(target_db_config, bindings)
    }

    class IngestionParams {
        +batch_size: int
        +n_cores: int
        +max_in_flight_batches: int
        +max_concurrent_sources: int?
        +max_concurrent_db_ops: int
        +on_doc_error: str
        +doc_error_sink_path: Path?
    }

    class RegistryBuilder {
        +build(bindings, ingestion_params) DataSourceRegistry
    }

    class DataSourceRegistry {
        +register(data_source, resource_name)
        +get_data_sources(resource_name) list
    }

    class AbstractDataSource {
        <<abstract>>
        +resource_name: str?
        +iter_batches(batch_size, limit)
        +acknowledge(batch_index)
        +close()
    }

    class GraphContainer {
        +vertices: dict
        +edges: dict
    }

    class DBWriter {
        +write(gc, conn_conf, resource_name)
    }

    Caster --> IngestionParams : ingestion_params
    Caster --> RegistryBuilder : builds the registry
    RegistryBuilder --> DataSourceRegistry : builds
    DataSourceRegistry o-- "0..*" AbstractDataSource : per resource
    Caster ..> GraphContainer : casts each batch into
    Caster --> DBWriter : one per run
    DBWriter ..> GraphContainer : writes
```

`AbstractDataSource` has one subclass per kind of source: `FileDataSource`,
`SQLDataSource`, `RdfFileDataSource`, `SparqlEndpointDataSource`,
`APIDataSource`, `KafkaDataSource`, and `InMemoryDataSource` for Python
objects. Each is registered under the name of the resource it feeds, so a
resource does not know what kind of source it reads. The ingest calls
`acknowledge` for each batch once it is written and `close` when the source is
done; `KafkaDataSource` commits its offsets there, and the other sources do
nothing.

## What to read next

- [Core components](core_components.md): the keys behind the manifest models.
- [Concepts overview](../index.md): the path a record takes, in words.
- [Importing and layering](../../guides/importing.md): which modules to import
  from.
