# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.8.12]

### Added

- **`proxy_name` and `alias` on `DBConfig`** (`graflo/db/connection/onto.py`) — survives validation round-trips; bridges manifest `conn_proxy` to registry credentials.

### Fixed

- **TigerGraph schema batching** — `SCHEMA_CHANGE` jobs now batch `ADD VERTEX` and `ADD EDGE` statements in separate phases with order-preserving splits, so edges are never applied before their endpoint vertex types exist.

## [1.8.11]

### Added

- **Namespace vs schema separation** — `Connection.ensure_target_namespace()` and `Connection.apply_target_schema()` as canonical op 1 / op 2; `GraphEngine.create_target_namespace()`; `create_namespace` flag on `define_schema`, `define_and_ingest`, and `migrate_graph` (default `True`).
- **`NamespaceNotFoundError`** — raised when `create_namespace=False` and the target graph/database/space is missing.
- **TigerGraph** — pre-created empty graphs work with `create_namespace=False`; `SchemaExistsError` is raised only when vertex/edge types exist, not when the graph shell is empty.
- **Server / CLI** — `create_namespace` on ingest/define API bodies; `--no-create-namespace` on the ingest CLI.
- Guide: [Graph namespace and schema](docs/guides/graph_namespace_and_schema.md).

- **`graflo.db.identity_inference`** — `IdentityInferencer`, `IdentityInferenceConfig`, `infer_identities_from_snapshot`, and `apply_identity_inference_to_vertices` for algorithmic vertex identity discovery from record samples (bootstrap validation, natural composite keys, hash fallback, `no_viable_identity`).
- **`Vertex.hash_identity_properties`** — deterministic SHA256 identity from explicit source fields (distinct from `blank` random UUIDs).
- **`Vertex.identity_mode`** — derived runtime mode: `natural`, `hash`, or `blank`. Unary and composite natural keys share the `natural` mode (same upsert path).
- **`VertexConfig.hash_identity_vertices`**, **`VertexConfig.vertices_by_identity_mode()`** — derived vertex lists for introspection and `db_writer` branching.
- **`db_writer._assign_hash_identity_ids`** — hash-mode pre-write hook before vertex upserts.
- **[Example 15](docs/examples/example-15.md)** — CSV identity inference → manifest → GraFlo file backend ingest (`examples/15-identity-inference/`).

### Documentation

- [Vertex identity modes](docs/concepts/schema/vertex_identity.md); updates to [core components](docs/concepts/architecture/core_components.md), [backend indexes](docs/concepts/schema/backend_indexes.md), [creating a manifest](docs/getting_started/creating_manifest.md), and [examples index](docs/examples/index.md).

## [1.8.9]

### Added

- **`ApiResponseStructure`** — declarative mapping of JSON response envelopes on **`PaginationConfig.response`**: `records_path`, `total_count_path`, `offset_path`, `next_offset_path`, `has_more_path`, `cursor_path`, `batch_metadata_paths`, and optional **`auto_detect`** (first-response heuristics for unset paths).
- **`PaginationRequestConfig`** — request-side pagination on **`PaginationConfig.request`** (strategy, query param names, page size, initial offset/page/cursor).
- **`graflo.data_source.api_response`** — path resolution, stop/advance logic, batch metadata extraction, and auto-detection helpers used by **`APIDataSource`**.

### Changed

- **Breaking:** **`PaginationConfig`** is split into **`request`** + **`response`** sub-blocks. Top-level **`data_path`**, **`has_more_path`**, and **`cursor_path`** are removed — migrate manifests to **`response.records_path`**, **`response.has_more_path`**, and **`response.cursor_path`** (under **`pagination.response`** in YAML).
- **`APIDataSource`** — parses paginated JSON via **`pagination.response`**; advances offset from **`next_offset_path`** when configured (URL always built from connector **`base_url` + `path`**); merges batch metadata into each row.
- **Documentation** — [API connector and pagination](docs/concepts/connectors/api_connector.md), quickstart, and data-source reference updated for the request/response model, stop/advance rules, and **`auto_detect`**.

## [1.8.7]

### Added

- 
- **`IngestionParams.connectors`** — optional subset filter for ingestion bindings (connector name or hash, same refs as `resource_connector.connector`). Intersects with `resources` when both are set; `RegistryBuilder` registers only matching connectors as data sources.
- **`Bindings.resolve_connector_refs_to_hashes()`** — resolves connector refs for validation and registry filtering.
- **On-disk layout** — `schema.yaml`, `INDEX.json`, and gzip JSONL chunks under `vertices/` and `edges/` (`graflo.architecture.backend`: **`GraFloIndex`**, **`GraFloLayout`**, **`GraFloBackendWriter`**, **`GraFloBackendReader`**).
- **`target_flavor_hint`** on `GraFloBackendConfig` — optional pre-sanitization of exported `schema.yaml` for a known downstream `DBType`.
- **API env wiring** — **`InMemoryConnectionProvider.register_api_config_from_env`** and **`register_all_api_configs_from_env`** load **`RestApiConnConfig`** from environment variables using proxy-scoped prefixes (`user_service` → `USER_SERVICE_BASE_URL`, …). **`RestApiConnConfig.from_env`** supports all **`ApiAuth`** types via **`AUTH_TYPE`**.
- **[Example 14](docs/examples/example-14.md)** — multi-proxy API env wiring walkthrough (`examples/14-api-env-wiring/`).
- **`GraphEngine._resolve_target_schema()`** — skips sanitization when migrating to a file backend unless `target_flavor_hint` is set.

### Changed

- **[Example 13](docs/examples/example-13.md)** — reworked around file backend: `export-backend`, `ingest-backend`, and replay via `--from-backend`; bundled CSV manifest for ingest-to-disk demo.
- **Documentation** — [Graph export and migration](docs/concepts/operations/graph_export_migration.md), README, quickstart, and docs index updated for file-backend workflows (1.8.7).
- **API env wiring docs** — [API connector and pagination](docs/concepts/connectors/api_connector.md), quickstart, data-source reference, and [Example 14](docs/examples/example-14.md) document **`register_all_api_configs_from_env`** and proxy-scoped env prefixes.
- **Ingestion scope docs** — `IngestionParams.connectors` documented in README, quickstart, [features and practices](docs/concepts/operations/migration_and_practices.md), architecture diagrams, and data-source reference.

## [1.8.6]

### Added

- **`GraFloOutput`** — self-describing export artifact combining a full **`Schema`** (metadata, `core_schema`, `db_profile`) with a **`GraphContainer`** of vertices and edges. Python attribute `graph_schema` serializes as `"schema"` in JSON/YAML.
- **Graph DB as source** — **`Connection.introspect_graph_schema()`**, **`fetch_all_docs()`**, and **`fetch_all_edges()`** on Neo4j and ArangoDB (`supports_graph_export = True`). Shared inference helpers in **`graflo.db.graph_introspection`** (`GraphSchemaInferencer`, `GraphIntrospectionResult`).
- **`ConnectionManager.open_graph_connection()`** — opens graph connections for introspection/export without target-only validation; **`graph_export_flavors()`** lists backends with export support.
- **`GraphEngine` graph workflows** — **`infer_schema_from_graph()`**, **`export_graph()`** (returns `GraFloOutput`), and **`migrate_graph()`** (graph→graph or graph→PostgreSQL in one pass with a single source connection and one target sanitization).
- **PostgreSQL as graph target** — **`DBType.POSTGRES`** in target databases; **`PostgresTargetWriteMixin`** creates vertex tables and junction edge tables, with batch upserts and edge inserts via **`PostgresConnection`**.

### Changed

- **`GraphContainer` JSON edge keys** — edge dictionary keys serialize as JSON arrays `[source, target, relation]` (via **`serialize_edge_key`**) so vertex or relation names may contain `|` or other special characters without ambiguity.

### Documentation

- **[Graph export and migration](docs/concepts/operations/graph_export_migration.md)** — quick-start sketch, `GraFloOutput`, graph-source introspection, `export_graph` / `migrate_graph`, and PostgreSQL as a relational graph target.
- **[Example 13](docs/examples/example-13.md)** — step-by-step walkthrough and `examples/13-graph-export-migration/export_migrate.py` CLI.
- **README**, **quickstart**, and **docs index** — PostgreSQL target, bi-directional graph workflows, and links to the new pages.

## [1.8.5]

### Changed

- **TigerGraph connection refactor** — split the ~5,200-line `conn.py` into focused modules; `TigerGraphConnection` remains the sole public `Connection` implementation and orchestrates delegates for auth, REST++, GSQL, schema DDL, graph admin, and data operations.
- **New internal modules** — `auth`, `rest_client`, `gsql_client`, `schema_ddl`, `graph_admin`, `data_ops`, `token_cache`, `document_utils`, `gsql_parsers`, `compat`, `bulk_session`, and `name_validation`; shared DDL helpers consolidated in `ddl_utils`.
- **Token cache** — moved from `conn.py` to `token_cache.py` (re-exported from `conn` for backward compatibility).

### Fixed

- **`keep_absent_documents`** — restored ID extraction after the refactor (would have raised `AttributeError` at runtime).
- **Graph deletion on multi-graph servers** — `delete_database` / `_drop_global_schema_types` no longer drop vertex or edge types still referenced by other graphs on the same instance.
- **Internal delegate routing** — GSQL query discovery, token initialization, and REST++ calls in helper modules now route through `TigerGraphConnection` so test patches and token-cache invalidation behave consistently.

### Added

- **`ProjectManifestOp`** — manifest evolution op (`project_manifest`) that projects a `GraphManifest` to a requested vertex/edge subgraph. Keeps vertices and/or edge triples `(source, target, relation)` with `connectivity: induced_prune` (drops isolated vertex types from `keep_vertices`). Cascades consistently to schema, `db_profile`, ingestion (pipeline steps, `infer_edge_only` / `infer_edge_except`, `extra_weights`), and bindings. Optional `keep_resources` filters ingestion resources. Fails if projection would leave zero ingestion resources (same policy as `RemoveVerticesOp`). **`EdgeSelector`** models edge triple selectors; **`apply_remove_edge_ids`** / **`rewrite_remove_edge_ids_in_pipeline`** provide edge-id-aware removal (finer-grained than relation-only **`RemoveEdgesOp`**).
- **Tests** — `test/architecture/test_manifest_projection.py` for **`ProjectManifestOp`**; `test_document_utils.py` for document helpers and `keep_absent_documents`; surviving-graph regression coverage in `test_db_creation.py`.
- **Docs** — API reference pages for each TigerGraph submodule under `docs/reference/db/tigergraph/`.
- **[Manifest evolution](docs/concepts/schema/manifest_evolution.md)** — **`ProjectManifestOp`** subgraph projection recipe and operations table entry.

## [1.8.2]

### Added

- **`APIConnector`** — REST API bindings contract with `path`, HTTP options, and `PaginationConfig` (offset/limit, cursor, page-based).
- **Docs:** [API connector and pagination](docs/concepts/connectors/api_connector.md) — pagination strategies, field reference, and examples.
- **`ApiGeneralizedConnConfig` / `RestApiConnConfig` / `ApiAuth`** — runtime base URL and bearer/basic/digest/api_key credentials via `conn_proxy`.
- **`RegistryBuilder`** — builds `APIDataSource` instances from `APIConnector` + connection provider.

### Changed

- **Breaking:** API ingestion must use manifest `bindings` (`APIConnector` + `connector_connection`); removed `DataSourceFactory.create_api_data_source`, inline `APIConfig` auth/URL construction, and CLI `--data-source-config-path` support for `source_type: api`.

## [1.8.1]

### Added

- **`AddInverseEdgesOp`** — manifest evolution op (`add_inverse_edges`) that backfills inverse schema edges and ingestion mirrors for a `{forward_relation: inverse_relation}` map, with deduplication by `(source, target, relation)`.

## [1.8.0]

### Added
- **GraFlo meta-ontology** — OWL vocabulary at `https://ontology.growgraph.dev/graflo` (`owl:versionIRI` `…/1.0.0`, `owl:versionInfo` `1.0.0`) describing `GraphManifest`, `Schema`, `IngestionModel`, `ProtoTransform`, pipeline actor steps, bindings, and related enumerations. Shipped as `graflo/rdf/ontology/graflo.ttl` plus JSON-LD context `graflo-context.jsonld`.
- **`graflo.rdf`** — `ManifestRdfSerializer` / `ManifestRdfDeserializer` for bidirectional conversion between `GraphManifest` (YAML/Pydantic) and RDF (Turtle, JSON-LD, N-Triples, RDF/XML).
- **CLI** — `manifest-to-rdf` and `rdf-to-manifest` console scripts (`graflo.rdf.cli`).

### Documentation

- **[Manifest evolution](docs/concepts/schema/manifest_evolution.md)** — documents **`AddInverseEdgesOp`** (inverse schema edges and ingestion mirrors).
- **[GraFlo ontology](docs/concepts/schema/ontology.md)** — meta-model vs user-domain RDF (`RdfInferenceManager`), versioning, URI layout, CLI, and round-trip semantics.
- **Interactive ontology visualization** — custom hierarchical class graph (rectangular nodes, subClassOf and optional property edges, pan/zoom) embedded on the GraFlo ontology page; built via `docs/_build/scripts/build_ontology_viz.py` with committed assets under `docs/assets/graflo-ontology-viz/`.
- **README** and **docs index** — feature overview and quick links for manifest ↔ RDF workflows.

## [1.7.33]

### Added

- **`parse_filter_expression`** — unified YAML/JSON filter loader for Bindings, `SelectSpec.where`, `VertexConfig.filters`, and Arango helpers.
- **Bindings filter tests** — `test/architecture/test_bindings_filters.py` for logical-operator SQL pushdown.

### Fixed

- **`TableConnector.filters`** — YAML logical shorthand (`OR`, `AND`, `NOT`, `IF_THEN` keys) now parses at Bindings load (same as vertex filters); previously failed at `build_query` via `model_validate` only.
- **SQL `IF_THEN`** — renders as `(NOT antecedent OR consequent)` instead of invalid `... IF_THEN ...` text.
- **SQL nested composites** — parenthesize composite operands under `AND`/`OR` for correct precedence.

### Changed

- **`TableConnector.filters`** — coerced through `parse_filter_expression` at connector validation (fail fast on malformed shorthand).

## [1.7.32]

### Added

- **`Resource.fail_fast`** (default **`false`**) — when **`true`**, transform steps raise if required input keys are missing; when **`false`**, rename applies only to keys present in the document and functional transforms skip the step when inputs are missing.

### Changed

- **Rename transforms** — partial per-document rename (absent source keys are ignored); **`TransformPayload.removed_keys`** lists only source keys actually renamed in that row.
- **`drop_trivial_input_fields`** — no longer controls transform missing-key policy (use **`fail_fast`** instead).

## [1.7.31]

### Added

- **`install_tigergraph_queries` CLI** — upload `.gsql` query definitions from a directory to a target graph and run **`INSTALL QUERY`** for each (connection via **`TigergraphConfig`** / **`TIGERGRAPH_*`** env vars; optional **`--graph`**, **`--prefix`**).
- **TigerGraph API token cache** — secret-based REST tokens are cached per process for **`(gsql_url, graph, secret)`**, so ingestion no longer calls the token API on every **`ConnectionManager`** open during batch upserts. Entries respect server expiration (with a refresh buffer); cache is invalidated on REST++ **401** responses.

### Breaking

- **`Resource.skip_actors_on_missing_input_keys`** removed — use **`fail_fast`** (inverted semantics: **`fail_fast: false`** ≈ old **`skip_actors_on_missing_input_keys: true`**).

### Fixed

- **SQL auto-join with declarative resources** — **`apply_auto_joins`** no longer assumes **`ResourceConfig.root`**; it builds the actor tree from **`pipeline`** when only the contract config is available (**`ResourceRuntime.root`** unchanged at runtime).

### Documentation

- **[Features and practices](docs/concepts/operations/migration_and_practices.md)** — TigerGraph token caching under Performance Optimization.

## [1.7.30]

### Added

- **`tolerate_transform_errors`** on **`ResourceConfig`** (default **`true`**) — a failing transform step sets its declared output fields to **`None`**, records a **`failure_kind=transform`** row in the doc error sink, and the rest of the resource pipeline (vertices, edges, later transforms) continues for that document. Set **`tolerate_transform_errors: false`** to fail fast on transform exceptions.

### Changed

- **`VertexActor` + `from_doc`** — transform-buffer projection is selective: only **`TransformPayload`** entries whose **`named`** keys cover the **`from_doc`** source fields are consumed, so dressed or pivot outputs for other vertex types are not stolen. Dressed dict payloads (`__transformed_value#*`) are handled consistently with passthrough from the merged observation doc.
- **Blank vertices in `VertexConfig`** — mark placeholder types with **`blank: true`** on each **`Vertex`** (identity defaults to **`id`**). **`VertexConfig.blank_vertices`** is now a derived name list, not a separate manifest field. Runtime **`ResourceRuntime`** scopes **`VertexConfig`** to vertices referenced by the resource pipeline only; unreferenced blank types are no longer injected automatically.
- **Ingestion contract layout** — declarative **`ResourceConfig`** lives under **`graflo.architecture.contract.ingestion`**; schema-bound execution is **`ResourceRuntime`** / **`build_resource_runtime`** under **`graflo.architecture.contract.runtime`**. **`Resource`** remains an internal alias for **`ResourceConfig`**.

### Breaking

- **Top-level `blank_vertices` on `vertex_config`** — no longer read from manifests; set **`blank: true`** on the corresponding **`vertices`** entries instead (silent ignore under `extra="ignore"` if the old key is left in place).
- **Runtime blank vertex scope** — blank vertex types must appear in the resource pipeline (or edge inference selectors) to be present in the per-resource runtime **`VertexConfig`**; relying on schema-wide blank placeholders without a matching actor step will not add them at cast time.
- **Imports** — prefer **`ResourceConfig`** from **`graflo.architecture.contract`** (or **`graflo.architecture.contract.ingestion`**); **`graflo.architecture.contract.declarations.resource`** is not the canonical module path.

### Documentation

- **[Document cast errors](docs/concepts/ingestion/doc_errors.md)** — **`tolerate_transform_errors`** and transform failure records.
- **[Core components](docs/concepts/architecture/core_components.md)** — **`ResourceConfig`** / **`ResourceRuntime`**, per-vertex **`blank`**, **`from_doc`** with dressed transforms, identity defaults.
- **[Architecture diagrams](docs/concepts/architecture/diagrams.md)** — contract and blank-vertex model aligned with 1.7.30.
- **[Creating a manifest](docs/getting_started/creating_manifest.md)** — **`tolerate_transform_errors`** and blank vertex YAML.

## [1.7.29]

### Added

- **Empty-identity filter on cast batches** — after resource casting, **`Caster`** can drop vertex docs and edge tuples whose schema identity fields are all missing, `null`, or `""` before **`DBWriter`** (identity rules from **`VertexConfig`**, not **`GraphContainer`**). Controlled by **`IngestionParams.drop_empty_identity_docs`** (default **`true`**). Blank vertex collections are exempt.

## [1.7.27]

### Added

- **`ColumnTimeFilter`** — shared pandas-like time window on a single column (`column`, optional `start` / `end`, optional `interval` as a **`pandas.Timedelta`** string such as `"7D"` or `"2h"` for day/hour windows, optional `not_equals`, optional `start_inclusive` / `end_inclusive`). Rendered to SQL via **`FilterExpression`** (same path as other pushdown filters). Calendar-style offsets (for example month arithmetic) are not supported when `pandas.Timedelta` rejects the string; use explicit `start` / `end` ISO bounds instead.
- **`FileConnector.time_filter`** and **`TableConnector.time_filter`** — canonical field replacing duplicated `date_field` / `date_filter` / `date_range_*` fields on the wire.
- **Bindings — runtime connector patches**: **`ConnectorUpdate`**, **`Bindings.apply_connector_update`**, and **`Bindings.replace_connector`** so defining-field changes re-hash and reindex correctly while preserving **`conn_proxy`** wiring. Patches are applied **after** manifest load (not stored on `GraphManifest`).

### Breaking

- **Connector time fields** — top-level `date_field`, `date_filter`, `date_range_start`, and `date_range_days` are no longer accepted on manifests or merged patches; use nested **`time_filter`** (**`ColumnTimeFilter`**) only.

### Documentation

- **[Runtime connector updates](docs/concepts/connectors/runtime_updates.md)** — `time_filter` / **`ColumnTimeFilter`** (YAML + Python), patch examples, and registry timing.
- **[Concepts overview](docs/concepts/index.md)** — bindings bullet and focused-topic link for runtime patches and SQL time filters.
- **[Table connector views](docs/concepts/connectors/table_views.md)** — cross-link to time filters vs `view` / `joins`.
- **[Example 5 – PostgreSQL](docs/examples/example-5.md)** — `datetime_columns` now documented as setting **`time_filter.column`** on connectors; ingestion date-range comment aligned.
- **`creating_manifest.md`** — `connectors` may include optional **`time_filter`** on file/table connectors.


## [1.7.26]

### Breaking

- **`graflo.architecture.evolution.RenameEntitiesOp` removed**: evolution rename operations are now split into
  **`RenameVerticesOp`**, **`RenameRelationsOp`**, and **`RenameResourcesOp`** (no backward-compat alias).
  Migration example:

  ```python
  # old
  # RenameEntitiesOp(vertices=..., edges=..., resources=...)

  # new
  [
      RenameVerticesOp(vertices=...),
      RenameRelationsOp(relations=...),
      RenameResourcesOp(resources=...),
  ]
  ```

### Added

- **Manifest evolution ops expanded**:
  **`RemoveEdgesOp`**, **`MergeEdgesOp`**, **`RenameEdgePropertiesOp`**, **`RemoveEdgePropertiesOp`**,
  **`AddVertexPropertiesOp`**, and **`AddEdgePropertiesOp`**.
- **Propagation coverage for new ops** across manifest surfaces:
  schema (`core_schema`), ingestion resources/selectors, and `DatabaseProfile` (`edge_specs`, edge defaults/indexes).

## [1.7.25]

### Fixed

- **Import cycles** between `graflo.architecture.evolution`, `graflo.hq`, and `graflo.db`
  (including “partially initialized module” errors when importing `rewrite` or PostgreSQL
  paths during package startup).

### Changed

- **`graflo` package**: `GraphEngine`, `Caster`, `IngestionParams`, and other `graflo.hq`
  exports resolve lazily via `__getattr__`; `ConnectionManager` and `ConnectionType` are
  also lazy so `import graflo` does not eagerly load orchestration or the full DB stack.
- **`graflo.architecture.evolution`**: op models (`SanitizeOp`, …) load at import time;
  `apply_evolution` and other `apply_*` functions load lazily on first attribute access.
- **`Sanitizer`**: applies ops via public `apply_manifest_ops_inplace` instead of private
  `_dispatch_op`.
- **`rewrite_vertex_weights_vertex_field_names`**: defers `Weight` import to avoid pulling
  heavy modules during `rewrite` initialization.
- **Evolution / DB utilities**: `load_reserved_words` and `sanitize_attribute_name` are
  imported lazily inside the functions that need them (avoids `graflo.db` ↔ `graflo.hq`
  recursion during evolution apply).
- **DB ↔ HQ boundaries**: TigerGraph `bulk_load_finalize` lazily imports S3 connection
  types; PostgreSQL inference/mapping lazily imports `FuzzyMatcher`; `BulkSessionCoordinator`
  lazily imports `ConnectionManager`; several `hq` modules import config and Postgres types
  from `graflo.db.connection` / `graflo.db.manager` / `graflo.db.postgres.conn` instead of
  `from graflo.db import …`.

### Documentation

- [Manifest evolution](docs/concepts/schema/manifest_evolution.md) now includes a tutorial section with
  relation/property evolution recipes and guidance on `RenameRelationsOp` vs `MergeEdgesOp`.

## [1.7.24] - 2026-05-07

### Added

- **`graflo.architecture.evolution` — sanitization and field renames as ops**:
  **`SanitizeOp`** / **`apply_sanitize`** (reserved-word-safe storage names, per-vertex field renames,
  TigerGraph per-relation identity harmonization) and **`RenameVertexFieldsOp`** /
  **`apply_rename_vertex_fields`** (schema + ingestion rewrite for explicit vertex-field renames),
  with helpers in **`sanitize`**, **`rewrite`**, and **`db_profile`** modules.
- **`SparqlEndpointConfig`**: when **`dataset`** is unset or empty, endpoint URLs use the
  **`test`** path segment so Fuseki never receives invalid paths such as **`//sparql`** (aligned
  with integration-test defaults).
- **TigerGraph connection**: list graph and per-graph vertex/edge type helpers; query snapshots
  around destructive work; **`delete_all=True`** on full teardown now requires
  **`confirm_global_teardown=True`**; global **`DROP VERTEX` / `DROP EDGE`** skips types still
  referenced by **other** graphs so unrelated installed queries are not silently invalidated.

### Changed

- **`Sanitizer`**: manifest sanitization is implemented by dispatching
  **`SanitizeOp`** through **`graflo.architecture.evolution`** (same **`sanitize_manifest`**
  entrypoint for callers).
- **`SQLInferenceManager`**: PostgreSQL inference no longer mutates the contract for the target
  DB flavor; **`infer_artifacts`** / **`infer_complete_schema`** return **unsanitized** schema +
  ingestion. Apply **`Sanitizer(...).sanitize_manifest(...)`** (or **`apply_sanitize`**) when you
  need reserved-word / TigerGraph normalization.
- **`PostgresResourceMapper`**: inferred **`Resource`** pipelines keep **source (PostgreSQL) column
  names**; field renames for the target flavor are applied when the manifest is sanitized, not
  during mapper construction.
- **`GraphEngine.infer_manifest`**: still returns a full **`GraphManifest`** (schema +
  ingestion_model + bindings) and now runs **`Sanitizer`** on that manifest **before** returning,
  so the high-level PostgreSQL inference path stays target-flavor-safe.
- **`docker/fuseki`**: Fuseki 6–style image, **`fix-perms`** init for the data volume, and a
  **`shiro.ini`** template wired through compose for basic auth (credentials from **`TS_*`**
  env vars).

### Documentation

- [Manifest evolution](docs/concepts/schema/manifest_evolution.md), [Core components](docs/concepts/architecture/core_components.md),
  [Architecture diagrams](docs/concepts/architecture/diagrams.md), and the [documentation home](docs/index.md)
  updated for evolution-backed **`SanitizeOp`**, the **`SQLInferenceManager`** vs **`GraphEngine.infer_manifest`**
  sanitization split, and **`SparqlEndpointConfig`** dataset URL behavior.

## [1.7.23] - 2026-04-23

### Added

- **`GraphManifest.rename_entities(...)`**: manifest-level rename helper for coordinated vertex/edge/resource renames across `schema`, `ingestion_model`, and `bindings` references.

### Changed

- **`GraphEngine.infer_manifest` now returns full contracts by default** for PostgreSQL inference:
  inferred manifests now include `schema`, `ingestion_model`, and `bindings` (`connectors`,
  `resource_connector`, `connector_connection`) instead of requiring a separate bindings pass.
- **Inference pipeline reuse**: PostgreSQL schema/resources/bindings now share a single
  introspection snapshot, avoiding duplicate introspection and keeping inferred contract blocks
  in sync.

### Documentation

- Quick start and PostgreSQL example docs now describe `infer_manifest(...)` as producing
  full manifests with bindings by default, while still documenting manual/override bindings
  workflows for advanced cases.

## [1.7.22] - 2026-04-22

### Added

- **`graflo.architecture.evolution`**: Manifest evolution MVP — `apply_evolution`, `RemoveVerticesOp`,
  `MergeVerticesOp`; cascade remove (schema edges, ingestion resources, `resource_connector` rows,
  `db_profile`); merge vertices (union logical schema, redirect/dedupe edges, rewrite pipelines and
  infer/extra_weights, merge DB profile keys). Compare contract identity with `graflo.migrate.io.manifest_hash`.
  Default MINOR bump on `schema.metadata.version` via `bump_semver_minor` (opt out with `bump_version=False`).

### Documentation

- [Manifest evolution](docs/concepts/schema/manifest_evolution.md) concept page; [Creating a Manifest](docs/getting_started/creating_manifest.md) “Evolving a manifest” section.
- **Concepts split**: [Concepts overview](docs/concepts/index.md) is a short landing page; long-form content moved to
  [Architecture diagrams](docs/concepts/architecture/diagrams.md), [Core components](docs/concepts/architecture/core_components.md),
  and [Features, migration, and practices](docs/concepts/operations/migration_and_practices.md). Site nav and cross-links updated
  (e.g. schema migration anchors on the home page and Quick Start).

## [1.7.21] - 2026-04-21

### Added

- **`graflo.object_storage`**: S3-compatible helpers — `MinioConfig` / `S3EndpointConfig`, boto3 client factories,
  `ensure_bucket_exists` / `ensure_staging_bucket_for_config`, and `upload_staged_csvs`
  (TigerGraph bulk staging imports `upload_staged_csvs` from here).
- **Documentation**: [Object storage (S3 staging)](docs/concepts/operations/object_storage.md) concept page;
  Concepts overview links staging to that page.

### Breaking

- **`InferenceManager` removed**: PostgreSQL inference now lives in
  `graflo.hq.sql_inferencer.SQLInferenceManager`. Replace
  `from graflo.hq.inferencer import InferenceManager` with
  `from graflo.hq.sql_inferencer import SQLInferenceManager` (also exported from
  `graflo.hq`).

### Changed

- **`RdfInferenceManager.infer_schema`**: RDF-inferred vertices use `identity: ["_uri"]`.
  Cross-class `owl:ObjectProperty` declarations produce per-resource pipeline steps that
  materialize the target vertex from the predicate URI field, then emit the edge. Same-class
  object properties are not expanded automatically (custom pipelines still apply).

- **`VertexRouterActor` role normalization**: `role` is now normalized from
  `type_field` when omitted, and router storage/addressing uses `role` as the single
  internal slot key. `type_field` remains the type discriminator source.
- **Explicit vertex extraction policy**: `VertexActorConfig` and
  `VertexRouterActorConfig` now support `extraction_scope: full | mapped_only`
  (default `full`). `mapped_only` limits extraction to explicitly mapped fields
  from `from`/`vertex_from_map`; `full` preserves passthrough behavior.
- **Shared vertex extraction config surface**: common options (`from`, `keep_fields`,
  `extraction_scope`, `role`) are consolidated in a shared
  `VertexExtractionOptionsConfig`, and actor-specific models inherit from it.

## [1.7.20]- 2026-04-19

### Changed

- **`vertex_router`**: Removed `field_map` and `prefix`. Use `from` (same contract as `vertex`) for
  `{vertex_field: doc_field}` projection, optional `keep_fields` to restrict passthrough, and
  `vertex_from_map` for per-type overrides. The merged observation is passed through to the routed
  `VertexActor` with no separate rename/slice step.

## [1.7.19] - 2026-04-14

### Added

- **`role` on `vertex` step** (`VertexActorConfig`): optional named accumulator slot. When set,
  the vertex is stored at `lindex.extend((role, 0))` instead of bare `lindex`, allowing multiple
  vertices of the same type to occupy distinct slots in the same flat row (e.g. `role: self`,
  `role: parent`, `role: child` — all `person`). A downstream `edge` step references the slot
  via the new `source_role` / `target_role` aliases.

- **`source_role` / `target_role` on `edge` step** (`EdgeActorConfig`): ergonomic aliases for
  `source_type_field` / `target_type_field`. Both names look up the same accumulator slot;
  `source_role` is preferred when the slot was populated by a `vertex+role` step, while
  `source_type_field` remains idiomatic when a `vertex_router` step was used. Mutually exclusive
  with their counterparts.

- **`links` list on `edge` step** (`EdgeActorConfig`): multi-intent edge declaration. When set,
  each item in `links` is an `EdgeLinkConfig` that emits one edge intent per row. This replaces
  the need for two (or more) nearly identical `edge` steps when a single flat row encodes multiple
  relationship types. Mutually exclusive with all top-level source/target fields on the same step.

- **`EdgeLinkConfig`** model: per-link binding inside a `links` edge step. Supports `source` /
  `target` (static types), `source_type_field` / `target_type_field` (dynamic slots from
  `vertex_router`), `source_role` / `target_role` (dynamic slots from `vertex+role`), `relation`,
  `relation_field`, `match_source`, `match_target`.

- **`keep_fields` now enforced in passthrough** (`VertexActor`): the field was already present in
  `VertexActorConfig` but had no effect. The passthrough step in `VertexActor.__call__` now
  restricts the set of automatically absorbed columns to `keep_fields` when it is set. Use on
  role-vertex steps to prevent shared row columns (e.g. `name`) from leaking into placeholder
  vertices that only carry an ID.

### Changed

- **Passthrough non-mutation when `role` is set** (`VertexActor.__call__`): without `role`,
  the existing `doc.pop` behaviour is preserved (backward-compatible). When `role` is set,
  passthrough uses `doc.get` so that sibling role-vertex steps operating on the same shared doc
  each see all row columns. This is the correct behaviour for multi-role flat-row pipelines and
  has no effect on single-vertex pipelines.

### Documentation

- **`docs/examples/example-12.md`**: new example — *Vertex Roles and Multi-intent Edges*.
  CSV `person,parent,child,name,age`; one `person` vertex type; two `person→person` edge types
  (`is_child_of`, `is_parent_of`); three `vertex+role` steps + one `edge: links` step.
  Covers `role`, `keep_fields`, `from` direction, passthrough behaviour, and `links`.
- **`docs/examples/index.md`**: entry 12 added.
- **`mkdocs.yml`**: examples 11 and 12 added to nav (example 11 was missing).
- **`docs/concepts/index.md`**: Actor section updated — `role` and `keep_fields` on `VertexActor`;
  `source_role` / `target_role` / `links` on `EdgeActor`; scenario matrix extended.

### Examples

- **`examples/12-vertex-roles-multi-edge/`**: `family_edges.csv`, `manifest.yaml`, `ingest.py`
  demonstrating the vertex-role + multi-link pattern end-to-end.

---

## [1.7.18] - 2026-04-14

### Added

- **Dynamic `EdgeActor` mixed mode**: `EdgeActorConfig` now accepts one static side
  (`from` / `to`) combined with one dynamic slot side (`source_type_field` /
  `target_type_field`). Previously both slot fields had to be set together; now any
  combination is valid — both dynamic (fully dynamic), one static + one dynamic
  (mixed), or both static (static mode).  The `_call_dynamic` path handles all three
  uniformly.

### Removed

- **`EdgeRouterActor`** and **`EdgeRouterActorConfig`** are removed from the codebase.
  The replacement is a `vertex_router` step per dynamic endpoint (each with its own
  `type_field`) followed by a dynamic `edge` step with `source_type_field` /
  `target_type_field` set to the corresponding `type_field` values.  Mixed-mode edges
  (one static endpoint, one dynamic) are now supported natively by `EdgeActor`.
  `CHANGELOG.md` retains historical references to `EdgeRouterActor` for audit purposes.

### Changed

- **`VertexRouterActor`** vertices are always stored at `lindex.extend((type_field, 0))`
  (`type_field` doubles as the accumulator slot name); this was already the behaviour
  after the previous refactor and is now the only supported mode.
- **`EdgeActorConfig.validate_type_sources`**: the constraint
  `"source_type_field and target_type_field must both be set or both be absent"` is
  lifted; each side is validated independently (must have exactly one of static or
  dynamic, but the two sides may differ).
- **`objects-relations` test schema and example 7 manifest** migrated from `edge_router`
  to two `vertex_router` steps + dynamic `edge`.

### Documentation

- **`docs/examples/example-7.md`**: rewritten to describe the `vertex_router` +
  dynamic `edge` pattern; flat-row variant section retained as a cross-reference to
  Example 11.
- **`docs/concepts/index.md`**: actor class diagram and scenario matrix updated;
  deprecated `EdgeRouterActor` entry removed.
- **`docs/concepts/connectors/table_views.md`**: YAML pipeline sketch updated from
  `edge_router` to `vertex_router` + `edge`.

## [1.7.17] - 2026-04-13

### Added

- **`SelectSpec`** (`kind="select"`): ergonomic **`select`** items — simple identifier
  strings and dicts **`{base, as}`** / **`{from_join, column, as}`** (plus legacy
  **`{expr, alias}`**). **`SelectSpec.concat_select_parts`** merges join/select
  fragments from multiple specs when composing in Python.
- **`ALL_BASE_COLUMNS`** (`"all_base"`): vocal default for “all base columns”
  (expands to **`{base_alias}.*`** when joins exist). **`SelectSpec.base_alias`**
  and **`TableConnector.base_alias`** (default **`base`**) replace the former
  hard-coded `r` alias in generated SQL.

### Changed

- **Default `SelectSpec.select`**: **`["all_base"]`** instead of **`["*"]`** so
  multi-table views default to base-row columns only.

### Documentation

- **`docs/concepts/connectors/table_views.md`**: base table defaults, structured
  select, **`all_base`** / **`base_alias`**, **`concat_select_parts`** sketch, YAML
  anchor note.

## [1.7.16] - 2026-04-10

### Added

- **`merge_observation_with_transform_buffer`** (and alias **`merge_row_doc_with_transform_buffer`**) in **`graflo.architecture.graph_types`**: merges a nested JSON observation slice with **`ExtractionContext.buffer_transforms`** entries at the same **`LocationIndex`** in pipeline order (later transform output overrides earlier keys and conflicts with the raw observation).
- **`IngestionParams.batch_prefetch`**: bounded queue depth for prefetching the next source batch(es) while the current batch is cast and written—keeps **`iter_batches`** lazy with smoother overlap between fetch and processing.
- **`BulkSessionCoordinator`** (**`graflo.hq.bulk_session`**): backend-agnostic begin/finalize lifecycle for optional native bulk ingest sessions (feature detection and **`UnsupportedBulkLoad`** handling stay on connections).

### Changed

- **Native bulk writes**: **`DBWriter.write`** treats a non-empty **`bulk_session_id`** as “append via the connection’s bulk interface” (no TigerGraph-only branching in the writer). **`bindings`** / **`connection_provider`** are no longer passed into **`write`**; finalize still receives them from the coordinator after the ingest run.
- **`VertexRouterActor` / `EdgeRouterActor`**: routing reads type fields, relation fields, and projected identity columns from the **merged** observation (raw slice + transform buffer), including **`{prefix}{type_field}`** fallback for vertex routers when **`prefix`** is set.
- **`Caster`**: TigerGraph-specific bulk session helpers replaced with **`_ensure_bulk_session`** / **`_finalize_bulk_session`** backed by **`BulkSessionCoordinator`**; **`process_data_source`** pipelines batch iteration through the prefetch queue.

### Documentation

- Concepts: router actors + transform buffer merge; **`docs/concepts/connectors/table_views.md`** (table connector views).

## [1.7.15] - 2026-04-08

### Added

- **TigerGraph native bulk ingest**: Optional **`TigergraphConfig.bulk_load`** stages per-type CSV under a local **`staging_dir`**, then runs **`CREATE LOADING JOB` / `RUN LOADING JOB`** via GSQL (REST++ upsert path unchanged). **`Bindings.staging_proxy`** names map to runtime S3 credentials through **`ConnectionProvider.get_generalized_config_by_proxy`**; use **`S3GeneralizedConnConfig`** and **`boto3`** upload when **`s3_conn_proxy` / `s3_staging_name`** is set. Not supported: **`blank_vertices`** and resources with **`extra_weights`**. Dependency: **`boto3`** (required for S3 staging).

### Changed

## [1.7.14] - 2026-04-08

- **`Resource.drop_trivial_input_fields` + actor missing-key handling**: Added **`Resource.skip_actors_on_missing_input_keys`** (optional). When enabled, transform actors skip execution if required input keys are missing (instead of raising key-index errors). If unset (`null`/`None`), it automatically defaults to the value of **`drop_trivial_input_fields`**.

## [1.7.13] - 2026-04-07


### Changed

- **`SQLDataSource`**: Executes the configured query once per `iter_batches` call and streams rows with SQLAlchemy **`stream_results`** and **`fetchmany`**, instead of mutating SQL with **`LIMIT`/`OFFSET`** and re-running per page. This avoids large-offset discarded scans on backends that support server-side cursors. Optional **`limit`** still caps the number of rows read in application code (no SQL **`OFFSET`** pagination).
- **`SQLConfig`**: Fields **`pagination`** and **`page_size`** are deprecated and ignored (left optional so existing configs keep validating under **`extra="forbid"`**). Control batch size via **`iter_batches(batch_size=...)`** and total row cap via **`limit`**.
- **`RegistryBuilder`**: No longer passes SQL pagination options into **`SQLConfig`** when registering PostgreSQL table sources.
- **Sanitizer API is manifest-first**: `graflo.hq.sanitizer.Sanitizer` now exposes `sanitize_manifest(GraphManifest)` as the contract-level entrypoint, applying naming/index normalization on `schema` and synchronizing ingestion mappings in `ingestion_model` when needed.

### Added

- **Tests**: **`test_sql_data_source_postgres_streaming_limit_25`** in **`test/data_source/test_api_data_source.py`** hits a real PostgreSQL instance from **`PostgresConfig.from_docker_env()`** (skips when unavailable) and asserts **`iter_batches`** batching and **`limit`**.
- **Document cast failure sink (gzip JSONL)**: Optional **`IngestionParams.doc_error_sink_path`**, CLI **`ingest --doc-error-sink`**, and **`DocErrorSink`** / **`JsonlGzDocErrorSink`** append gzip-compressed JSON lines (one **`DocCastFailure`** JSON object per line, field **`doc_index`**). See **Concepts → Document cast errors and doc error sink**.

### Breaking

- **Removed schema-only sanitizer API**: `SchemaSanitizer` and `sanitize(schema, ingestion_model=...)` were removed. Update call sites to create/use `GraphManifest` and call `Sanitizer.sanitize_manifest(manifest)`.
- **Per-document cast error API rename**: `RowCastFailure` → **`DocCastFailure`** (`row_index` → **`doc_index`** in JSON), `RowErrorBudgetExceeded` → **`DocErrorBudgetExceeded`**, `on_row_error` → **`on_doc_error`**, `max_row_errors` → **`max_doc_errors`**, `row_error_doc_preview_max_bytes` → **`doc_error_preview_max_bytes`**, `row_error_doc_keys` → **`doc_error_preview_keys`**, CLI **`--on-row-error`** → **`--on-doc-error`**, log extra key **`row_cast_failure`** → **`doc_cast_failure`**.

## [1.7.12] - 2026-04-06

### Changed

- **TigerGraph identifier validation**: Reserved-word, forbidden-prefix, and invalid-character checks load `reserved_words.json` once per process (cached). Validation now covers **vertex property** names and **edge attribute** names (including identity discriminators), in addition to graph, vertex, and edge relation names.

### Fixed

- **TigerGraph vertex DDL**: For a single-field primary key, GSQL does not allow `DEFAULT` on the `PRIMARY_ID name type` fragment. DDL generation uses the `name TYPE [DEFAULT …] PRIMARY KEY` form when the identity field has a default, and keeps `PRIMARY_ID` / `PRIMARY_ID_AS_ATTRIBUTE` only when the id field has no default.

## [1.7.11] - 2026-04-06

### Added

- **`DatabaseProfile.default_property_values`**: optional **`DefaultPropertyValues`** model (with per-vertex maps and per-edge **`EdgePropertyDefaults`** entries) to declare **GSQL `DEFAULT`** literals for physical schema DDL—vertex keys are logical vertex names; edge entries match logical `(source, target, relation)`.


## [1.7.10] - 2026-04-04

### Changed

- **Logical schema vocabulary**: Vertex payloads use **`properties`** (list of property names and/or typed `Field` entries) instead of **`fields`**. Edge payloads use **`properties`** for relationship attributes instead of nested **`weights`** / **`weights.direct`**. Internal DB projection still builds a `WeightConfig` where backends need it, but authored YAML/Python schema should declare edge attributes on `Edge.properties` only.

### Breaking

- **`Vertex`**: The `fields` attribute was removed; use **`properties`** everywhere (manifest `graph.vertex_config.vertices[*].properties`, Python `Vertex(properties=[...])`).
- **`Edge`**: The `weights` / `WeightConfig` shape on logical edges was removed; use **`properties`** for the same data (strings, `Field`, or dicts). Vertex-sourced edge payload wiring belongs in ingestion (**`EdgeActor`** / **`EdgeDerivation`**, edge derivation registry), not on the logical `Edge` model.

### Documentation

- README, docs landing page, concepts, manifest guide, and examples updated for **`properties`**-first schema authoring and clearer “what this project is” intros.

## [1.7.9] - 2026-04-01

### Added

- **`Bindings.get_connectors_for_resource(name)`** returns an ordered list of connectors (unique by hash) for an ingestion resource, supporting **1→n** resource–connector wiring.
- **`BoundSourceKind`** enum (`file`, `sql_table`, `sparql`) and **`ResourceConnector.bound_source_kind()`** describe the physical source modality of a connector (replacing the old “resource type” wording).
- **`Resource.drop_trivial_input_fields`** (default `false`): when `true`, removes **top-level** keys whose value is `null` or `""` from each input record before the actor pipeline runs—useful for wide, sparse rows without custom transforms. Does not recurse into nested objects.

### Changed

- **`DBWriter`**: No longer calls `Schema.finish_init()` or `IngestionModel.finish_init()` on every `write()`. The orchestrator (e.g. **`Caster.ingest`**) is responsible for initializing schema and ingestion model for the target DB before writes. This avoids redundant work on each batch and prevents the writer from resetting ingestion flags (`strict_references`, `allowed_vertex_names`) that **`Caster`** had already applied.
- **`DBWriter`**: Reuses a cached **`SchemaDBAware`** projection for a given connection DB type instead of rebuilding it on every `write()`.
- **Ingestion caps**: `IngestionParams.max_items` is documented and validated (`>= 1` when set). **`SparqlEndpointDataSource.iter_batches`** paginates without loading the full endpoint result into memory, uses **`ORDER BY ?s`** when the query has no `ORDER BY`, and honors **`limit`** as a subject count. **`SQLDataSource`** and offset/page **API** pagination pass a tighter per-request page size when a total cap is close (fewer over-fetched rows/items).
- **`RegistryBuilder`** registers **every** connector bound to each resource and dispatches on **`connector.bound_source_kind()`**; SQL registration uses the connector’s own table/schema fields instead of a resource-level table lookup.
- **Auto-join** (`_vertex_table_info`) resolves table metadata via the list API and **raises** if more than one `TableConnector` is bound to the same vertex/resource key used for disambiguation.

### Breaking


- **`DBWriter`**: The **`dynamic_edges`** constructor argument was removed (it only drove the redundant `finish_init` call). Configure dynamic edge behavior via **`Caster`** / **`IngestionParams.dynamic_edges`** and ingestion **`finish_init`** as before.
- **`ResourceType`** removed in favor of **`BoundSourceKind`**; **`get_resource_type()`** removed in favor of **`bound_source_kind()`** on connectors (update imports and call sites).
- **`Bindings`**: **`get_connector_for_resource`**, **`get_resource_type`**, and **`get_table_info`** removed; use **`get_connectors_for_resource`** and connector fields / `bound_source_kind()` instead.
- **`connector_connection` / internal connector refs**: resolution allows only **connector `name`** or **canonical `hash`**. Using an ingestion **resource name** as a `connector` reference is no longer supported (resource names are no longer 1:1 with connectors).
- **`bind_resource`** and manifest **`resource_connector`** validation: additional rows for the same `resource` append connectors instead of replacing or conflicting.

### Documentation

- **Examples / docs**: `examples/9-connector-connection-proxy` and manifest guides updated for explicit connector names in `connector_connection`. Concepts and README clarify 1→n bindings and proxy wiring.
- **`Resource.drop_trivial_input_fields`**: described in [Concepts](docs/concepts/index.md) (DataSources vs Resources) and [Documentation home — Resource](docs/index.md#resource).

## [1.7.7] - 2026-03-27

### Changed
- **Edge identity policy (Cypher property-graph targets)**: For Neo4j, Memgraph, and FalkorDB, relationship upserts use `MERGE` with properties that distinguish **parallel** edges (same endpoints and relationship type). `EdgeConfigDBAware.relationship_merge_property_names` now **prefers the first** logical `Edge.identities` key: tokens `source` and `target` are omitted (endpoints are already matched on nodes); the `relation` token maps to the relationship property where applicable. If `identities` is empty or yields no relationship fields, behavior falls back to **all** `weights.direct` field names so existing schemas keep stable merge keys.
- **Cypher MERGE map builders**: `graflo.db.cypher.rel_merge_props_map_from_row_index` and `rel_merge_props_map_from_row_props` normalize property names and emit safe map entries for batched `MERGE` clauses.

## [1.7.6] - 2026-03-26

### Added
- **Bindings `connector_connection`**: optional manifest block mapping each source connector to a non-secret `conn_proxy` name (`{"connector": ..., "conn_proxy": ...}`). Manifests describe *which* connector uses *which* proxy; credentials live in runtime configs resolved by `ConnectionProvider` (`GeneralizedConnConfig`).
- **`ConnectorConnectionBinding`** and **`ResourceConnectorBinding`**: typed rows for `connector_connection` and `resource_connector` with dict coercion and index-scoped validation errors.
- **`Bindings.connector_connection_bindings`** (typed view), **`get_conn_proxy_for_connector`**, and **`bind_connector_to_conn_proxy`**: API aligned with HQ loaders (`ResourceMapper`, `GraphEngine`) for proxy-based source wiring.

### Changed
- **Connector reference resolution**: `connector_connection` entries may reference a connector by canonical **hash**, declared **`name`**, or a **`resource` name** when that resource is already mapped to the connector (mirrors validation in `Bindings`). **Update (1.7.8):** resource-name aliasing for `connector` refs was removed; use **connector `name` or `hash`** only.
- **`Bindings` validation**: duplicate connector `name` values and conflicting `conn_proxy` for the same connector hash now fail fast with explicit errors. **Update (1.7.8):** many connectors may attach to the same ingestion resource (1→n); overlapping resource rows no longer raise “conflicting resource binding” for distinct connectors.

### Breaking
- **`Bindings.from_dict` / manifest validation**: legacy top-level keys `postgres_connections`, `table_connectors`, `file_connectors`, and `sparql_connectors` are rejected. Migrate to the unified `connectors` + `resource_connector` (+ optional `connector_connection`) shape.

## [1.7.5] - 2026-03-25

### Added
- **`SelectSpec` `type_lookup` per-side overrides** (edge SQL views / `TableConnector.view`): `source_table`, `target_table`, `source_identity`, `target_identity`, `source_type_column`, `target_type_column` (each falls back to `table`, `identity`, `type_column`) for different lookup tables, join keys, or discriminator columns on the two endpoints.
- **Row-level cast errors**: `IngestionParams` gains `on_row_error` (`skip` | `fail`, default `skip`), optional `row_error_dead_letter_path` (JSONL), `max_row_errors`, `row_error_doc_preview_max_bytes`, and `row_error_doc_keys` for bounded, debug-friendly failure records. Skip mode logs each failure at ERROR when no dead-letter path is set.
- **`CastBatchResult`**, **`RowCastFailure`**, and **`RowErrorBudgetExceeded`** exported from `graflo` / `graflo.hq`.
- CLI `ingest`: `--on-row-error` and `--row-error-dead-letter`.

### Changed
- **`SelectSpec` `type_lookup`**: always emits both lookup joins and `source_type` / `target_type` columns. Static vertex types on one side belong only in `EdgeRouterActorConfig`; views that omit a type column or join use `kind="select"`. Removed `source_type_literal`, `target_type_literal`, `omit_source_type`, and `omit_target_type`.
- **`ProtoTransform`**: **`target`** (`values` | `keys`) is defined on the proto/base transform model (with **`keys`** selection alongside it), not only on **`Transform`** — shared targeting semantics for vocabulary-style and full transform definitions.
- **`Caster.cast_normal_resource`** now returns **`CastBatchResult`** with `.graph` and `.failures` instead of a bare `GraphContainer`.
- **`TransformException`** subclasses **`Exception`** (was `BaseException`) so it is handled like other application errors.

## [1.7.4] - 2026-03-19

### Added
- **Grouped value transforms** (`input_groups` / `output_groups`):
  - Invoke the same function once per **group** of input fields; each group’s values are passed as positional arguments to the callable.
  - Optional `output`: one scalar field name per group, aligned to `input_groups` order.
  - Optional `output_groups`: per-group output field lists when the function returns a tuple (or multiple values) per call.
  - When both `output` and `output_groups` are omitted, **unary groups** (exactly one input field per group) may **passthrough** results back onto those keys (see `Transform.passthrough_group_output` in `Transform`, default `true`). Multi-field groups require explicit `output` / `output_groups`.

### Changed
- **Packaging**: `[project.optional-dependencies]` is limited to tooling extras (`dev`, `docs`, `plot`). RDF/SPARQL libraries (`rdflib`, `SPARQLWrapper`) stay in the core dependency set. User-facing install docs, README, CI (`uv sync --extra dev`), and runtime error hints were updated to match.

### Documentation
- Concepts: [Transforms](docs/concepts/ingestion/transforms.md) — grouped calls, YAML shorthands, strategy rules, and config reference tidying.

## [1.7.3]

### Added
- **Key-target transform support**:
  - `transform.call.target: keys` enables key-level transforms in pipelines
  - `transform.call.keys.mode` supports key selection via `all`, `include`, and `exclude`
  - Added core key helpers in `graflo.util.transform`:
    - `remove_prefix`
    - `remove_suffix`
    - `camel_to_snake`
    - `snake_to_camel`

### Changed
- **Edge spec relation discriminator cleanup**:
  - Removed legacy `logical_relation` from `database_features.edge_specs` and related API surfaces
  - Edge spec/index/name resolution now relies on relation-aware `EdgeId` (`source`, `target`, `relation`) plus optional `purpose`

### Documentation
- Added concepts documentation examples for key-level transforms (`target: keys`).

## [1.7.2]

### Changed
- **Transform declarations and actor DSL**:
  - `ingestion_model.transforms` is now modeled and validated as an ordered list of named transforms (instead of map-like declarations)
  - Transform execution and resolution preserve declaration/appearance order in resource pipelines
  - Transform actor declarations use explicit call semantics (`transform.call.use`) for named transform references
  - A single named transform can be reused across multiple attributes/fields by repeated transform steps with different `input` overrides
- **Plotting defaults**:
  - Default plot naming now includes schema version when available, improving traceability across manifest versions

### Fixed
- **Graph model consistency validation**:
  - Added graph-level validation so vertex/edge config consistency is checked at `GraphModel` initialization time
- **Plotting correctness**:
  - Fixed plotting errors where vertices inferred from edge declarations could be handled incorrectly
- **Transform + actor initialization**:
  - Fixed actor loading failures when transform declarations specified `fields`
  - Fixed transform initialization edge cases where declaration style caused inconsistent actor wiring

### Documentation
- Updated docs to clarify GraFlo as a graph schema transformation DSL and refreshed transform declaration examples for list-based transform registries and `transform.call.use` references

## [1.7.0]

### Added
- **Schema migration framework (v1)**:
  - New `graflo.migrate` package with typed migration models, schema diffing, risk classification, planning, execution, and file-backed history store
  - New operation typing via `OperationType` enum (`StrEnum`) for safer operation ordering and classification
  - New migration executor with risk gates (safe-by-default; low-risk additive operations in v1)
  - New backend emitters for ArangoDB and Neo4j (v1 additive operation subset)
  - New CLI entrypoint `migrate_schema` with commands:
    - `plan` (read-only schema comparison and migration plan generation)
    - `apply` (dry-run or apply with revision and hash checks)
    - `status` and `history` (migration state visibility)
  - New migration ADR in `planning/graflo-migration-adr.md` documenting normalization rules, risk policy, rename policy, and backend capability matrix
  - New migration test suite in `test/migrate/` (diff, planner, store, executor)
- **Graph manifest contract**:
  - Introduced `graflo.architecture.manifest.GraphManifest` in `graflo/architecture/manifest.py` as the canonical ingestion contract
  - Unified configuration into manifest blocks (`schema`, `ingestion_model`, `bindings`) with explicit validation and initialization flow

### Documentation
- Extended migration docs in Concepts:
  - command examples for `plan`, `apply`, `status`, `history`
  - explicit guidance on schema comparison (`from` vs `to`) and risk interpretation
  - analogy-based explanation to make migration planning behavior easier to reason about

## [1.6.6] - 2026-03-05

### Added
- **SelectSpec**: Declarative view specification for advanced filtering and projection of data before feeding into Resources
  - Alternative to `TableConnector`'s `table_name` + `joins` + `filters`; use `view: SelectSpec` for full control over the SQL query
  - Two modes: `kind="select"` (full SQL-like spec with `from`, `joins`, `select`, `where`) and `kind="type_lookup"` (shorthand for edge tables where source/target types come from a lookup table via FK joins)
  - `type_lookup` shorthand: specify `table`, `identity`, `type_column`, `source`, `target`, and optional `relation` to auto-build JOINs that resolve entity types from a discriminator table
  - `FilterExpression`-based `where` clause support; YAML/JSON loading via `SelectSpec.from_dict()`
  - Used by `TableConnector.view` and by `ResourceMapper.create_bindings_from_postgres()` for edge tables with type lookup metadata
- **VertexRouterActor and EdgeRouterActor**: New router actors for dynamic type-based routing in resource pipelines
  - `VertexRouterActor` routes documents to the correct `VertexActor` based on a `type_field` in the document (with optional `type_map`, `prefix`, `field_map`, `vertex_from_map`)
  - `EdgeRouterActor` creates edges with dynamic source/target types from `source_type_field` and `target_type_field`, plus optional `relation_field` or static `relation` (with `type_map`, `relation_map`, `source_fields`, `target_fields`)
  - Use `vertex_router` and `edge_router` steps in resource `apply`/`pipeline` for polymorphic data (e.g. CSV with type discriminator columns, relations tables)

### Documentation
- Updated docs and concepts to describe VertexRouter and EdgeRouter actor types
- Added `vertex_router` and `edge_router` step examples to creating-schema guide
- Schema architecture diagram now includes VertexRouterActor and EdgeRouterActor in the Actor hierarchy
- Added SelectSpec documentation: filter view reference, TableConnector `view` usage in creating-schema guide, and Key Features mention

## [1.6.5] - 2026-03-02

### Changed
- **Vertex identity indexes**: Neo4j, Memgraph, and FalkorDB now automatically create identity indexes in `define_vertex_indexes` when schema is provided. Previously, identity had to be manually added to `database_features.vertex_indexes`.
- **Index naming**: All index-related method names and docstrings now use "indexes" instead of "indices".
- **Logical vs DB-aware architecture split finalized**:
  - `Vertex`, `Edge`, `VertexConfig`, and `EdgeConfig` now remain logical/DB-agnostic
  - DB-specific naming/default/index projection is resolved via `Schema.resolve_db_aware(...)`
  - Introduced DB-aware wrappers (`VertexConfigDBAware`, `EdgeConfigDBAware`) for writer/connector stages
- **TigerGraph relation materialization moved downstream**:
  - Edge relation extraction in casting/assembly remains logical and backend-agnostic
  - TigerGraph-specific relation-to-weight projection now happens during DB write/projection
- **Manifold edge identity model**:
  - `Edge` now uses `identities` (list of identity keys) instead of singular `identity`
  - Omitted/empty `identities` is permissive by default (multiple edges allowed)
  - Each declared identity key compiles into a unique physical index/constraint candidate
  - TigerGraph discriminator generation now derives from `edge.identities`
- **Edge identifier layering**:
  - `EdgeId` is relation-based: `(source, target, relation)`
  - `purpose` is no longer part of logical edge identity; it is treated as DB-only physical metadata
  - Runtime edge config maps and adapter lookups are aligned to relation-specific edge identity
- **Physical edge indexing is centralized in `database_features`**:
  - Identity-derived edge indexes are compiled into `Schema.database_features.edge_indexes`
  - `edge_indexes` entries support `logical_relation` to disambiguate multiple relations sharing the same edge definition
  - Edge index lookups in DB adapters are relation-aware
- **Edge physical spec unification**:
  - `database_features.edge_names` + `database_features.edge_indexes` were unified as `database_features.edge_specs`
  - Purpose-scoped edge copies now inherit base indexes by default (`indexes_mode: inherit|append|replace`)
  - Arango edge `graph_name` is aligned with derived `storage_name`
  - `Edge.aux` is no longer a behavior switch
- **Resource inferred-edge controls**:
  - Added `resource.infer_edge_only` and `resource.infer_edge_except` selectors for fine-grained control of greedy/inferred edge emission
  - Added validation for contradictions (`infer_edge_only` and `infer_edge_except` are mutually exclusive)
  - Added validation that infer selectors reference existing schema edges
  - When a resource pipeline contains any EdgeActor for edges of type `(source, target)`, `(source, target, None)` is automatically added to `infer_edge_except` for that resource, so inferred edges do not duplicate edges produced by explicit edge actors
- **Architecture phase separation**:
  - Runtime flow is now explicitly split into extraction and assembly contexts (`ExtractionContext`, `AssemblyContext`)
  - Actor orchestration was separated from wrapper structure by introducing `ActorExecutor`
  - Edge assembly consumes explicit edge intents in addition to compatibility fallback paths
- **Pipeline explicitness tightened**:
  - Implicit vertex actor auto-creation during descend initialization was removed
  - Pipelines using transform/map steps require explicit `vertex` steps to consume output
  - Test schemas depending on implicit behavior were migrated to explicit vertex declarations

### Breaking
- Schemas that still define `edge.indexes` under `edge_config.edges[*]` now fail validation.
- Migrate by moving physical edge specs/indexes to `database_features.edge_specs` and logical uniqueness keys to `edge.identities`.
- `database_features.edge_variants` was renamed to `database_features.edge_specs`.
- **Backend index documentation**: New `docs/concepts/schema/backend_indexes.md` describing which backends have implicit vs explicit identity indexes and how `vertex_indexes` relates to identity.


### Breaking (vertex projection)
- **Vertex projection replaces `target_vertex`**:
  - `target_vertex` on transform steps has been removed
  - Use vertex `from` for doc-to-vertex field mapping: `vertex: X` with `"from": {vertex_field: doc_field}`
  - Quote `from` in YAML (reserved word). Example: `vertex: person` with `"from": {id: person_id, name: person}`
  - `buffer_vertex` was removed from `ExtractionContext`; transforms always write to `buffer_transforms`

### Added
- **Typed extraction artifacts**:
  - Added `VertexObservation`, `TransformObservation`, `EdgeIntent`, and `ProvenancePath`
  - Added regression tests for new context/artifact APIs in `test/architecture/test_onto.py`

### Documentation
- Updated schema authoring docs to use canonical `infer_edges` naming and describe explicit vertex requirements for transform outputs
- Added concepts documentation for the extraction/assembly runtime split and `ActorExecutor` ownership
- Updated docs and README to describe the DB-aware projection layer and `Schema.resolve_db_aware(...)` flow
- Cleaned docs landing page to keep Mermaid diagrams in section pages (for example `docs/concepts/index.md`)

## [1.6.4] - 2026-02-25

### Changed
- **Logical identity vs physical indexes**:
  - `Vertex` now models logical identity via `identity` fields instead of owning physical index definitions
  - Secondary indexes were moved to `Schema.database_features.vertex_indexes` / `Schema.database_features.edge_indexes`
  - This separates graph semantics (identity) from DB-specific physical tuning (indexes)
- **DB-specific names moved to database features**:
  - Vertex/edge physical naming is now managed through `Schema.database_features` (`vertex_storage_names`, `edge_names`)
  - DB-only aliases such as vertex `dbname` and edge `relation_dbname` are represented as physical naming features rather than core logical schema fields

## [1.6.3] - 2026-02-25

### Fixed
- **Multi-edge emission from a single row**: Corrected edge rendering so repeated metric-like values in one source record produce distinct edges instead of collapsing into a linear/pair-only result
  - Edge casting selection now differentiates pair/product/combinations based on source/target branch layout and same-leaf scenarios
  - Same-vertex multi-sample cases are handled with combinations logic to preserve all expected links
- **Edge weight alignment**: Fixed weight propagation to keep per-edge weights aligned with each emitted edge tuple rather than reusing a single accumulated weight payload

### Changed
- **`edge_greedy` semantics clarified and tightened**:
  - `edge_greedy=True` now infers extra edges only when both endpoint vertex sets are populated, while avoiding duplicate source-target emissions already present in the accumulator
  - `edge_greedy=False` now behaves as "explicit-only" mode (emit edges created directly by edge actors in the pipeline)
- **Runtime cleanup/performance**: Simplified several actor-path operations (context merging, wrapper step normalization, and logging formatting) for lower overhead and clearer execution flow

### Added
- **Regression coverage for multi-edge casting**:
  - Added actor tests for multi-edge creation from one row and for filter-constrained variants to ensure cardinality remains correct

### Documentation
- **Example 7 rewritten**: Replaced placeholder content with a full multi-edge/weights walkthrough using `dress` transforms and vertex filters
  - Added updated schema/pipeline guidance and generated visuals for ticker-to-metric relationships

## [1.6.2] - 2026-02-23

### Changed
- **Messaging & positioning**: GraFlo is now presented as a **Graph Schema & Transformation Language (GSTL)** for Labeled Property Graphs (LPG) across all documentation
  - README, docs landing page, concepts, and creating-schema guide rewritten to lead with the GSTL / LPG identity
  - Pipeline description standardised to **Source Instance → Resource → Graph Schema → Covariant Graph Representation → Graph DB**
  - Added top-level mermaid pipeline diagram to README
  - Vocabulary aligned with codebase: `Resource`, `DataSourceRegistry`, `AbstractDataSource`, `DataSourceType`, `GraphContainer` used consistently
  - Resolved Source Type / Resource ambiguity — `DataSourceType` (`FILE`, `SQL`, `SPARQL`, `API`, `IN_MEMORY`) is now documented as a property of the source instance, while `Resource` is the transformation pipeline
  - Database abstraction and `GraphContainer` (covariant graph representation) highlighted as core value propositions
  - SPARQL & RDF support elevated to a first-class feature across all pages

## [1.6.1] - 2026-02-22

### Fixed
- **Filter expression parsing**: Fixed several bugs in `FilterExpression` deserialization
  - Added support for `foo` key (dunder-method shorthand, e.g. `foo: __eq__`) as an alternative to `operator` in YAML filter definitions
  - `cmp_operator` is now auto-inferred from dunder operator names (`__eq__` → `EQ`, `__gt__` → `GT`, etc.) via the new `DUNDER_TO_CMP` mapping, fixing cases where `cmp_operator` remained `None`
  - Fixed case-insensitive logical operator parsing in `from_dict` — lowercase keys like `or`, `and`, `if_then` from YAML are now correctly matched to `LogicalOperator` enum values

### Added
- **NebulaGraph adapter**: Full support for NebulaGraph as a target graph database, with dual-version support for both v3.x (nGQL via `nebula3-python`, Thrift) and v5.x (ISO GQL via `nebula5-python`, gRPC)
  - `NebulaConnection` implementing the full `Connection` interface: space lifecycle (`create_database`, `delete_database`, `init_db`), schema DDL (`define_schema`, `define_vertex_classes`, `define_edge_classes`), index management with rebuild/retry, batched vertex upserts and edge inserts, `fetch_docs`, `fetch_edges`, `fetch_present_documents`, `keep_absent_documents`, and `aggregate` (COUNT, MAX, MIN, AVG, SORTED_UNIQUE)
  - Version-agnostic adapter layer (`NebulaClientAdapter`, `NebulaV3Adapter`, `NebulaV5Adapter`, `NebulaResultSet`) abstracting driver differences behind a unified interface, with `create_adapter()` factory
  - Pure-function query builders in `graflo.db.nebula.query` for DDL (space/tag/edge/index creation), DML (batch upsert vertices, insert edges), and DQL (fetch docs/edges, aggregation) in both nGQL and GQL dialects
  - Utilities in `graflo.db.nebula.util`: NebulaGraph type mapping (`FieldType` to `int64`/`float`/`string`/`bool`), value serialization, VID generation (composite key support via `::`), filter rendering for nGQL and Cypher flavors, and schema propagation wait helpers
  - `NebulaConfig` (extends `DBConfig`) with fields for `version` (selects v3 or v5), `vid_type`, `partition_num`, `replica_factor`, `storaged_addresses`, `request_timeout`; environment prefix `NEBULA_`; `from_docker_env()` support reading from `docker/nebula/.env`
  - `DBType.NEBULA` enum value; NebulaGraph registered in both `SOURCE_DATABASES` and `TARGET_DATABASES`
  - Docker Compose setup (`docker/nebula/`) with four services: `nebula-metad`, `nebula-storaged`, `nebula-graphd`, and `nebula-graph-studio` (v3.8.0 images); docker management scripts (`start-all.sh`, `stop-all.sh`, `cleanup-all.sh`) updated to include NebulaGraph
  - Test suite with ~76 tests: unit tests for config, query builders, and utilities; integration tests (gated behind `pytest --run-nebula`) covering connection lifecycle, CRUD, fetch, edges, and aggregation

### Documentation
- Added NebulaGraph to all supported-targets lists across README, docs landing page, quickstart, installation, and concepts pages
- Added `NebulaConfig` environment variable examples and `from_docker_env()` usage to quickstart guide
- Added NebulaGraph API reference pages (connection, adapter, query, utilities)

## [1.6.0] - 2026-02-17

### Added
- **SPARQL / RDF resource support**: Ingest data from SPARQL endpoints (e.g. Apache Fuseki) and local RDF files (`.ttl`, `.rdf`, `.n3`, `.jsonld`) into property graphs
  - New `SparqlConnector` for mapping `rdf:Class` instances to resources, alongside existing `FileConnector` and `TableConnector`
  - New `RdfDataSource` abstract parent with shared RDF-to-dict conversion logic; concrete subclasses `RdfFileDataSource` (local files via rdflib) and `SparqlEndpointDataSource` (remote endpoints via SPARQLWrapper)
  - New `SparqlEndpointConfig` (extends `DBConfig`) with `from_docker_env()` for Fuseki containers
  - New `RdfInferenceManager` auto-infers graflo `Schema` from OWL/RDFS ontologies: `owl:Class` to vertices, `owl:DatatypeProperty` to fields, `owl:ObjectProperty` to edges
  - `GraphEngine.infer_schema_from_rdf()` and `GraphEngine.create_bindings_from_rdf()` for the RDF inference workflow
  - `Bindings` class extended with `sparql_connectors` and `sparql_configs` dicts
  - `RegistryBuilder` handles `ResourceType.SPARQL` to create the appropriate data sources
  - `ResourceType.SPARQL`, `DataSourceType.SPARQL`, `DBType.SPARQL` enum values
  - `rdflib` and `SPARQLWrapper` available as the `sparql` optional extra (`pip install graflo[sparql]`)
  - Docker scripts (`start-all.sh`, `stop-all.sh`, `cleanup-all.sh`) updated to include Fuseki
  - Test suite with 22 tests: RDF file parsing, ontology inference, and live Fuseki integration

### Changed
- **Top-level imports optimized**: Key classes are now importable directly from `graflo`:
  - `GraphEngine`, `IngestionParams` promoted to top-level alongside existing `Caster`
  - Architecture classes `Resource`, `Vertex`, `VertexConfig`, `Edge`, `EdgeConfig`, `FieldType` now at top-level
  - `FilterExpression` promoted to top-level (alongside existing `ComparisonOperator`, `LogicalOperator`)
  - `InMemoryDataSource` added to top-level data-source exports
  - Import groups reorganized: orchestration, architecture, data sources, database, filters, enums & utilities
- **`graflo.filter` package exports**: `FilterExpression`, `ComparisonOperator`, and `LogicalOperator` are now re-exported from `graflo.filter.__init__` (previously only available via `graflo.filter.onto`)

### Documentation
- Added data-flow diagram (Connector -> DataSource -> Resource -> GraphContainer -> Target DB) to Concepts page
- Added **Mermaid class diagrams** to Concepts page showing:
  - `GraphEngine` orchestration: how `GraphEngine` delegates to `InferenceManager`, `ResourceMapper`, `Caster`, and `ConnectionManager`
  - `Schema` architecture: the full hierarchy from `Schema` through `VertexConfig`/`EdgeConfig`, `Resource`, `Actor` subtypes, `Field`, and `FilterExpression`
  - `Caster` ingestion pipeline: how `Caster` coordinates `RegistryBuilder`, `DataSourceRegistry`, `DBWriter`, `GraphContainer`, and `ConnectionManager`
- Enabled Mermaid rendering in mkdocs configuration
- Updated top-level package docstring with modern usage example (`GraphEngine` workflow)

## [1.5.0] - 2026-02-02

### Added
- **Ingestion date range**: `IngestionParams` supports `datetime_after`, `datetime_before`, and `datetime_column` so ingestion can be restricted to a date range
  - Use with `GraphEngine.create_bindings(..., datetime_columns={...})` for per-resource datetime columns, or set `IngestionParams.datetime_column` for a single default column
  - Rows are included when the datetime column value is in `[datetime_after, datetime_before)` (inclusive lower, exclusive upper)
  - Applies to SQL/PostgreSQL table ingestion; enables sampling or incremental loads by time window

### Changed
- **Configs use Pydantic**: Schema and all schema-related configs now use Pydantic `BaseModel` (via `ConfigBaseModel`) instead of dataclasses
  - `Schema`, `SchemaMetadata`, `VertexConfig`, `Vertex`, `EdgeConfig`, `Edge`, `Resource`, `WeightConfig`, `Field`, and actor configs are Pydantic models
  - Validation, YAML/dict loading via `model_validate()` / `from_dict()` / `from_yaml()`, and consistent serialization
  - Backward compatible: `resources` accepts empty dict as empty list; field/weight inputs accept strings, `Field` objects, or dicts

## [1.4.5] - 2026-02-02

### Added
- **Inferencer**: Row count estimates and row samples
- **Discard disconnected vertices**: Option to discard disconnected vertices during graph operations

### Changed
- **clean_start**: Refactored into `recreate_schema` and `clear_data` for clearer separation of schema and data reset
- **output_config**: Renamed to `target_db_config`

## [1.4.3] - 2026-01-25

### Added
- **SchemaSanitizer for TigerGraph**: Added comprehensive schema sanitization for TigerGraph compatibility
  - `SchemaSanitizer` class in `graflo.hq.sanitizer` module for sanitizing schema attributes
  - Sanitizes vertex names and field names to avoid reserved words (appends `_vertex` suffix for vertex names, `_attr` for attributes)
  - Sanitizes edge relation names to avoid reserved words and collisions with vertex names (appends `_relation` suffix)
  - Normalizes vertex indexes for TigerGraph: ensures edges with the same relation have consistent source and target indexes
  - Automatically applies field index mappings to resources when indexes are normalized
  - Handles field name transformations in TransformActor instances to maintain data consistency
- **Vertex `dbname` field**: Added `dbname` field to `Vertex` class for database-specific vertex name mapping
  - Allows specifying a different database name than the logical vertex name
  - Used by SchemaSanitizer to store sanitized vertex names for TigerGraph compatibility
- **Edge `relation_dbname` property**: Added `relation_dbname` property to `Edge` class for database-specific relation name mapping
  - Returns sanitized relation name if set, otherwise falls back to `relation` field
  - Used by SchemaSanitizer to store sanitized relation names for TigerGraph compatibility
  - Supports setter for updating the database-specific relation name
- **GraphEngine orchestrator**: Added `GraphEngine` class as the main orchestrator for graph database operations
  - Coordinates schema inference, connector creation, and data ingestion workflows
  - Provides unified interface: `infer_schema()`, `create_bindings()`, and `ingest()` methods
  - Integrates `InferenceManager`, `ResourceMapper`, and `Caster` components
  - Supports target database flavor configuration for schema sanitization
  - Located in `graflo.hq.graph_engine` module

## [1.4.0] - 2026-01-15

### Removed
- `pyTigergraph` dependence remove

### Added 
- reserved Tigergraph words are modified during automated schema generation

## [1.3.11] - 2026-01-12

### Added
- **TigerGraph Version 4+ Compatibility Enhancements**: Improved support for TigerGraph 4.1+ and 4.2.x versions
  - **Automatic Version Detection**: Connection now auto-detects TigerGraph version and adjusts behavior accordingly
    - Parses version from various formats returned by `getVersion()` API
    - Supports manual version override via `TigergraphConfig.version` field
    - Handles version strings like "release_4.2.2_09-29-2025", "4.2.1", "v4.2.1"
  - **REST API URL Compatibility**: Automatic URL construction based on TigerGraph version
    - TigerGraph 4.2.2+: Uses direct REST API endpoints (no prefix)
    - TigerGraph 4.2.1 and older: Adds `/restpp` prefix to REST API URLs
    - Fixes production deployment issues with TigerGraph 4.2.1
  - **Token-Based Authentication (Recommended)**: Enhanced token authentication support
    - Automatic API token generation from secrets using `getToken()`
    - Bearer token authentication for REST API calls (prioritized over Basic Auth)
    - Stores and reuses tokens for the connection lifetime
    - Token expiration logging for monitoring
    - Fallback to HTTP Basic Auth if token generation fails
  - **Python 3.11+ Exception Compatibility**: Added `@_wrap_tg_exception` decorator
    - Handles `TigerGraphException` objects that lack `add_note()` method (required by Python 3.11+)
    - Wraps exceptions as `RuntimeError` to avoid attribute errors
    - Applied to all key methods: `init_db()`, `create_database()`, `delete_database()`, `_define_schema_local()`, `define_schema()`, `define_indexes()`, `execute()`, `upsert_docs_batch()`
    - Future-proofs for Python 3.11+ while maintaining Python 3.10 compatibility

### Changed
- **TigerGraph Port Configuration for Version 4+**: Updated default ports to align with TigerGraph 4.1+ architecture
  - **Port 9000 (REST++)**: Marked as internal-only in TG 4.1+ (not publicly accessible)
  - **Port 14240 (GSQL Server)**: Now the primary interface for all API requests in TG 4.1+
  - Changed default `port` from 9000 → 14240 in `TigergraphConfig._get_default_port()`
  - Both `restppPort` and `gsPort` now default to 14240 for TigerGraph 4+ compatibility
  - Docker configurations with custom port mappings continue to work via explicit port settings
  - Added comprehensive documentation about port architecture changes in TG 4+
- **Enhanced TigerGraph Documentation**: Added extensive TigerGraph 4+ integration guide
  - Created `docs/tigergraph_v4_guide.md` with comprehensive TG 4+ usage examples
  - Port configuration best practices for vanilla TG 4+ and Docker deployments
  - Token authentication setup and benefits
  - Version compatibility details and migration guide
  - Environment variable configuration examples
  - Troubleshooting guide for common issues
  - Enhanced class docstrings in `TigerGraphConnection` and `TigergraphConfig` with usage examples

### Documentation
- Added comprehensive TigerGraph 4+ integration guide covering:
  - Port configuration changes (9000 → 14240)
  - Token-based authentication (recommended approach)
  - Version compatibility and auto-detection
  - Migration from older TigerGraph versions
  - Best practices for production deployments
  - Environment variable configuration
  - Troubleshooting common connection issues

## [1.3.10] - 2026-01-07

### Added
- **Docker management scripts**: Added unified docker service management scripts
  - `start-all.sh`: Start all docker compose services at once with automatic SPEC detection from `.env` files
  - `stop-all.sh`: Stop all docker compose services with profile-based management
  - `cleanup-all.sh`: Remove containers, volumes, and optionally images with flexible options
  - Automatic detection of `SPEC` variable from each `.env` file (defaults to `graflo`)
  - Profile-based service management for organized docker orchestration
- **Memgraph documentation**: Added comprehensive Memgraph support documentation
  - Added Memgraph to main README.md database support list
  - Added Memgraph configuration examples to quickstart guide
  - Added Memgraph to docker/README.md with connection details (Bolt port 7687)
  - Added Memgraph to documentation reference index
- **TigerGraph robust schema definition and ingestion**: Enhanced TigerGraph support with improved reliability
  - **Schema Change Job Approach**: Uses SCHEMA_CHANGE jobs for local schema definition within graphs
    - More reliable than global vertex/edge creation approach
    - Better integration with TigerGraph's graph-scoped schema model
    - Automatic schema verification after creation to ensure types were created correctly
  - **Automatic Edge Discriminator Handling**: Automatically adds indexed fields to edge weights when missing
    - Required for TigerGraph discriminators (allows multiple edges of same type between same vertices)
    - Ensures discriminator fields are also edge attributes (TigerGraph requirement)
    - Handles both explicit indexes and relation_field for backward compatibility
  - **Robust Edge Ingestion with Fallback**: Enhanced batch edge insertion with automatic fallback
    - Failed batch payloads automatically retry with individual edge upserts
    - Preserves original edge data for fallback operations
    - Better error recovery and data integrity
  - **Improved Error Handling**: More lenient error detection and better error messages
    - Case-insensitive vertex type comparison (handles TigerGraph capitalization)
    - Better error messages with detailed schema verification results
    - Graceful handling of schema change job errors

### Changed
- **Improved connection typing and signatures**: Enhanced type hints and method signatures across all database connectors
  - Improved type annotations for ArangoDB, Neo4j, TigerGraph, FalkorDB, and Memgraph connection classes
  - Better IDE support and type checking for database connection methods
  - Enhanced method signatures for better developer experience
- **Neo4j Community Edition support**: Improved handling of Neo4j Community Edition limitations
  - Gracefully handles unsupported CREATE DATABASE command in Community Edition
  - Automatically continues with default database when database creation fails
  - Clearer error messages indicating Community Edition limitations

## [1.3.9] - 2026-01-06

### Added
- **FalkorDB documentation**: Added comprehensive FalkorDB support documentation across all documentation files
  - Added FalkorDB to main README.md database support list
  - Added FalkorDB to examples and quickstart guides
  - Added FalkorDB web interface access information (port 3001)
  - Added FalkorDB to documentation reference index

### Changed
- **Enhanced PostgreSQL Schema Inference documentation**: Significantly improved documentation clarity and prominence
  - Added explicit requirements section: normalized databases (3NF) with proper primary keys (PK) and foreign keys (FK) decorated
  - Clarified that intelligent heuristics are used to classify tables as vertices or edges
  - Made PostgreSQL schema inference feature more prominent in main documentation (moved to top of Key Features)
  - Added cross-references to Example 5 from multiple documentation locations
  - Enhanced Example 5 with detailed requirements and heuristics explanation
  - Updated all documentation to consistently mention PK/FK requirements and heuristics

- **Database port information updates**: Updated all documentation with correct port numbers from docker .env files
  - ArangoDB: Updated to port 8535 (from docker/arango/.env, standard port 8529)
  - Neo4j: Updated to port 7475 (from docker/neo4j/.env, standard port 7474)
  - TigerGraph: Updated to port 14241 (from docker/tigergraph/.env, standard port 14240)
  - FalkorDB: Port 3001 (from docker/falkordb/.env)
  - Added notes about standard ports vs. configured ports in docker setup

- **Documentation structure improvements**: Enhanced documentation organization
  - Added "Step 2.5: Choose Target Graph Database" section in Example 5
  - Added "Viewing Results in Graph Database Web Interfaces" section with detailed access information
  - Improved examples index to highlight PostgreSQL schema inference feature
  - Enhanced quickstart guide with PostgreSQL schema inference requirements

## [1.3.6] - 2025-12-17

### Added
- **Database-agnostic terminology**: Renamed database-specific terminology to be more generic
  - `Edge.collection_name` → `Edge.database_name`: More generic field name that works across all database types
    - For ArangoDB, `database_name` corresponds to the edge collection name
    - For TigerGraph, used as fallback identifier when relation is not specified
    - For Neo4j, unused (relation is used instead)
  - Updated all references throughout codebase to use `database_name` instead of `collection_name`
  - Removed ArangoDB-specific "collection" terminology from `Vertex` and `VertexConfig` classes
    - Replaced with generic "vertex" or "vertex class" terminology
    - Updated variable names: `_vcollection_numeric_fields_map` → `_vertex_numeric_fields_map`
    - Updated error messages and documentation to use database-agnostic terms

- **Enhanced PostgreSQL example documentation**: Significantly improved Example 5 documentation
  - Added detailed explanations of schema inference process
  - Added visual diagrams showing graph structure, vertex fields, and resource mappings
  - Explained data flow from PostgreSQL to graph database
  - Added step-by-step breakdown of what happens during each phase
  - Included resource mapping diagrams for all table types

- **Improved schema file discovery**: Enhanced `generate_examples_figs.sh` script
  - Now handles files ending with `schema.yaml` (e.g., `generated-schema.yaml`)
  - Uses pattern matching to find schema files instead of hardcoded filename
  - More flexible for generated or custom-named schema files

## [1.3.5] - 2025-12-16

### Added
- **Unified Database Configuration Architecture**: Simplified database configuration system
  - **Capability-based Design**: Replaced `GraphDBConfig`/`SourceDBConfig` hierarchy with capability sets
    - `SOURCE_DATABASES`: Set of database types that can be used as data sources
    - `TARGET_DATABASES`: Set of database types that can be used as targets
    - `can_be_source()`: Method to check if a database can be used as a source
    - `can_be_target()`: Method to check if a database can be used as a target
  - **Unified Schema/Database Structure**: Added unified internal structure for database hierarchy
    - `database`: Database name (for SQL) or backward compatibility field (for graph DBs)
    - `schema_name`: Schema/graph name (unified internal structure)
    - `effective_database`: Property that returns the effective database name based on DB type
    - `effective_schema`: Property that returns the effective schema/graph name based on DB type
    - Database-specific mapping delegated to concrete config classes:
      - **PostgreSQL**: `database` → effective_database, `schema_name` → effective_schema
      - **ArangoDB**: `database` → effective_schema (no database level)
      - **Neo4j**: `database` → effective_schema (no database level)
      - **TigerGraph**: `schema_name` → effective_schema (no database level)
  - **Automatic Schema Fallback**: `Caster` now automatically uses `Schema.general.name` as fallback
    when `effective_schema` is not set in configuration
  - **Environment Variable Support for Schema**: Added `POSTGRES_SCHEMA_NAME` and `TIGERGRAPH_SCHEMA_NAME`
    environment variables for schema configuration

## [1.3.4] - 2025-12-12

- **Data Source Architecture**: Formalized data source types as a separate layer from Resources
  - New `graflo.data_source` package with abstract base classes and implementations
  - `AbstractDataSource`: Base class for all data sources with unified batch iteration interface
  - `DataSourceType` enum: FILE, API, SQL, IN_MEMORY
  - `DataSourceRegistry`: Maps multiple data sources to Resources
  - `DataSourceFactory`: Factory for creating appropriate data source instances

- **File Data Sources**: Refactored file handling into formal data sources
  - `FileDataSource`: Base class for file-based data sources
  - `JsonFileDataSource`: JSON file data source
  - `JsonlFileDataSource`: JSONL (JSON Lines) file data source
  - `TableFileDataSource`: CSV/TSV file data source with configurable separator

- **REST API Data Source**: Full support for REST API endpoints as data sources
  - `APIDataSource`: REST API connector with comprehensive HTTP configuration
  - `APIConfig`: Configuration for API endpoints including:
    - URL, HTTP method, headers
    - Authentication (Basic, Bearer, Digest)
    - Query parameters, timeouts, retries
    - SSL verification
  - `PaginationConfig`: Flexible pagination support
    - Offset-based pagination
    - Cursor-based pagination
    - Page-based pagination
    - Configurable JSON paths for data extraction

- **SQL Data Source**: SQL database support using SQLAlchemy
  - `SQLDataSource`: SQL database connector
  - `SQLConfig`: SQLAlchemy-style configuration
    - Connection string support
    - Parameterized queries
    - Pagination support
    - Database-agnostic query execution

- **In-Memory Data Source**: Support for Python objects as data sources
  - `InMemoryDataSource`: Handles list[dict], list[list], and pd.DataFrame
  - Automatic conversion of list[list] to list[dict] using column names

- **CLI Integration**: Enhanced CLI to support data source configuration
  - `--data-source-config-path`: Load data sources from configuration file
  - Support for API, SQL, and file data sources via configuration
  - Backward compatible with existing file-based ingestion

- **Dependencies**: Added required packages for new features
  - `requests>=2.31.0`: For REST API data sources
  - `sqlalchemy>=2.0.0`: For SQL data sources
  - `urllib3>=2.0.0`: For HTTP retry functionality

- **PostgreSQL Schema Inference**: Automatic schema generation from PostgreSQL 3NF databases
  - `PostgresConnection`: PostgreSQL connection and schema introspection implementation
  - `PostgresSchemaInferencer`: Infers complete graflo Schema from PostgreSQL database schemas
    - Automatically identifies vertex-like and edge-like tables
    - Infers vertex configurations with typed fields from table columns
    - Infers edge configurations from foreign key relationships
    - Maps PostgreSQL data types to graflo Field types
  - `PostgresResourceMapper`: Maps PostgreSQL tables to graflo Resources
  - `PostgresTypeMapper`: Converts PostgreSQL types (INTEGER, VARCHAR, TIMESTAMP, etc.) to graflo Field types
  - `infer_schema_from_postgres()`: Convenience function for one-step schema inference
  - `create_resources_from_postgres()`: Creates Resource mappings from PostgreSQL tables
  - Full support for PostgreSQL schema introspection including:
    - Table and column metadata extraction
    - Foreign key relationship detection
    - Primary key identification
    - Data type mapping

- **Typed Fields for Schema Definitions**: Enhanced field type support throughout the schema system
  - **Vertex Fields**: `Vertex.fields` now supports typed `Field` objects in addition to strings
    - Fields can be specified as strings (backward compatible), `Field` objects, or dicts
    - Type information preserved for databases that require it (e.g., TigerGraph)
    - Automatic normalization to `Field` objects internally while maintaining string-like behavior
  - **Edge Weight Fields**: `WeightConfig.direct` now supports typed `Field` objects
    - Weight fields can specify types (e.g., `Field(name="date", type="DATETIME")`)
    - Supports strings, `Field` objects, or dicts for flexible configuration
    - Type information enables better validation and database-specific optimizations
  - **Field Type System**: Comprehensive type support with `FieldType` enum
    - Supported types: `INT`, `FLOAT`, `BOOL`, `STRING`, `DATETIME`
    - Type validation and normalization from strings to enum values
    - Backward compatible: fields without types default to `None` (suitable for databases like ArangoDB)

### Changed
- **Database Configuration Architecture Simplification**: Unified and simplified database configuration
  - **Renamed `BackendType` to `DBType`**: More accurate naming reflecting unified database configuration
    - Updated all references throughout codebase
    - `BACKEND_TYPE_MAPPING` → `DB_TYPE_MAPPING`
  - **Removed Intermediate Config Classes**: Simplified inheritance hierarchy
    - Removed `GraphDBConfig` abstract class
    - Removed `SourceDBConfig` abstract class
    - All config classes (`ArangoConfig`, `Neo4jConfig`, `TigergraphConfig`, `PostgresConfig`) now inherit directly from `DBConfig`
    - `connection_type` property moved to base `DBConfig` class
  - **Fixed Field Shadowing Warning**: Renamed `schema` field to `schema_name` to avoid conflict with Pydantic `BaseSettings.schema`
    - Field accepts both `"schema"` and `"schema_name"` keys in dict/JSON input (via validation alias)
    - Environment variables use `SCHEMA_NAME` suffix (e.g., `POSTGRES_SCHEMA_NAME`, `TIGERGRAPH_SCHEMA_NAME`)
  - **Updated ConnectionManager**: Now uses `can_be_target()` method instead of `isinstance` checks
    - More flexible and extensible design
    - Clear error messages when database type cannot be used as target

- **Caster Refactoring**: Updated `Caster` to use data source architecture
  - `process_resource()`: Now accepts configuration dicts, file paths, or in-memory data
  - `ingest()`: Wrapper that creates FileDataSource instances internally (renamed from `ingest_files()`)
  - `ingest_data_sources()`: New method for ingesting from DataSourceRegistry
  - `process_data_source()`: New method for processing individual data sources
  - **Automatic Schema Fallback**: Uses `Schema.general.name` when `effective_schema` is not set
  - **Ingestion Parameters Consolidation**: Refactored `Caster` to use `IngestionParams` as a single attribute
    - Replaced individual attributes (`clean_start`, `n_cores`, `max_items`, `batch_size`, `dry`) with `ingestion_params: IngestionParams`
    - `Caster.__init__()` now accepts `ingestion_params` parameter (backward compatible with kwargs)
    - All ingestion parameters are now centralized in the `IngestionParams` Pydantic model
    - Improved type safety and consistency across ingestion methods
  - Maintains full backward compatibility with existing code

- **Parallel Processing Simplification**: Consolidated threading and multiprocessing parameters
  - Removed redundant `n_threads` parameter from `IngestionParams` and CLI
  - `n_cores` now controls both multiprocessing (number of processes) and threading (ThreadPoolExecutor workers)
  - Simplified API: single parameter controls all parallel execution
  - Updated CLI: removed `--n-threads` option, `--n-cores` now controls both process and thread counts

- **Resource vs DataSource Separation**: Clear separation of concerns
  - Resources: Define semantic transformations (how data becomes a graph)
  - DataSources: Define data retrieval (where data comes from)
  - Many DataSources can map to the same Resource

- **Package Structure Refactoring**: Renamed `backend` package to `db` for clarity
  - `graflo.backend` → `graflo.db` (all database-related code)
  - `graflo.backend.connection` → `graflo.db.connection` (connection configuration)
  - Updated all imports and references throughout codebase
  - Maintains backward compatibility through import aliases where applicable

- **Backend Configuration Refactoring**: Complete refactor of database connection configuration system
  - **Pydantic-based Configuration**: Replaced dataclass-based configs with Pydantic `BaseSettings`
    - `DBConfig`: Abstract base class with `uri`, `username`, `password` fields
    - `ArangoConfig`, `Neo4jConfig`, `TigergraphConfig`, `PostgresConfig`: Database-specific config classes
    - Environment variable support with prefixes (`ARANGO_`, `NEO4J_`, `TIGERGRAPH_`, `POSTGRES_`)
    - Automatic default port handling when port is missing from URI
    - Support for custom prefixes via `from_env(prefix="USER")` for multiple configs
  - **Renamed `ConnectionKind` to `DBType`**: More accurate naming for database types (final naming: `ConnectionKind` → `BackendType` → `DBType`)
  - **Removed `ConfigFactory`**: Replaced with direct config instantiation and `DBConfig.from_dict()`
    - Use `ArangoConfig.from_docker_env()` to load from docker `.env` files
    - Use `ArangoConfig()`, `Neo4jConfig()`, `TigergraphConfig()`, `PostgresConfig()` for direct instantiation
    - Use `DBConfig.from_dict()` for loading from configuration files
  - **Separated WSGI Configuration**: Moved `WSGIConfig` to separate `wsgi.py` module
    - WSGI is not a database backend, so it no longer inherits from `DBConfig`
    - Removed `WSGI` from `DBType` enum
  - **Backward Compatibility**: `from_dict()` handles old field names (`url` → `uri`, `cred_name` → `username`, etc.)
  - **Breaking Changes**:
    - `ConnectionKind` → `BackendType` → `DBType` (final naming)
    - `ConfigFactory` removed (use `DBConfig.from_dict()` or direct config classes)
    - `*ConnectionConfig` aliases removed (use `*Config` names directly: `ArangoConfig`, `Neo4jConfig`, `TigergraphConfig`, `PostgresConfig`)
    - `GraphDBConfig` and `SourceDBConfig` removed (all configs inherit from `DBConfig`)
    - `connection_type` field removed (now a computed property from class type)
    - `schema` field renamed to `schema_name` (accepts `"schema"` key in dict/JSON for backward compatibility via validation alias)

### Deprecated
- `ChunkerFactory`: Still functional but now used internally by FileDataSource
- Direct file path processing: Use DataSource configuration for new code

### Fixed
- **File Discovery Path Bug**: Fixed incorrect path combination in `Caster.discover_files()`
  - Previously combined `fpath` with `pattern.sub_path` again, causing `data/data` errors
  - Now correctly uses `fpath` directly as the search directory
  - Fixes `FileNotFoundError` when using `FileConnector` with `sub_path` in ingestion

## [1.2.1] - 2025-01-XX

### Added
- **`any_key` parameter for `DescendActor`**: Added support for processing all keys in a dictionary dynamically
  - When `any_key: true` is set, `DescendActor` will iterate over all key-value pairs in the document dictionary
  - Useful for handling nested structures where you want to process all keys without explicitly listing them
  - Simplifies configuration for cases like package dependencies where multiple relationship types exist
  - Automatically displayed in actor tree visualizations

### Changed
- **Python version requirement**: Reduced minimum Python version requirement from 3.11 to 3.10
  - Updated `requires-python` to `~=3.10.0` in `pyproject.toml`
  - Maintains compatibility with Python 3.10 while using modern type hints

## [1.2.0] - 2025-01-XX

### Added
- **TigerGraph backend support**: Full support for TigerGraph as a graph database backend
  - Complete implementation of `TigerGraphConnection` with all core operations
  - Support for vertex and edge creation, deletion, and querying
  - Integration with TigerGraph REST++ API for efficient data operations
  - Support for GSQL queries and schema management
  - Edge fetching with support for complex vertex IDs
  - Graph statistics and metadata operations
  - Server-side filtering and querying using REST++ API
  - Field type-aware filter generation for proper REST++ filter formatting

### Changed
- **API refactoring**: Renamed `delete_collections` to `delete_graph_structure` for better clarity
  - Method now uses `vertex_types` and `graph_names` parameters instead of `cnames` and `gnames`
  - Updated terminology across codebase to use generic "vertex type" and "edge type" instead of database-specific "collection" terminology
  - Added comprehensive documentation about database organization terminology differences between ArangoDB, Neo4j, and TigerGraph

### Documentation
- Added comprehensive database organization terminology documentation explaining:
  - ArangoDB: Database → Collections (vertex/edge) → Graphs
  - Neo4j: Database → Labels (vertex types) → Relationship Types (edge types)
  - TigerGraph: Graph (like database) → Global Vertex/Edge Types → Associated with graphs

## [1.0.1] - 2025-01
### Changed

Package renamed from `graphcast` to `graflo`.


## [1.0.0] - 2025-01
### Changed
- **Major refactoring of Edge class architecture:**
  - Removed `EdgeCastingType` dependency and related casting logic
  - Simplified Edge class by removing complex discriminant handling
  - Renamed fields for clarity:
    - `source_discriminant` → `match_source`
    - `target_discriminant` → `match_target`
    - `source_relation_field` → `relation_field`
    - `target_relation_field` → removed (unified into single `relation_field`)
  - Removed `non_exclusive` field and related logic
  - Simplified weight configuration by removing `source_fields` and `target_fields`
  - Edge casting type is now determined automatically based on match fields

- **Actor system improvements:**
  - Added `LocationIndex` parameter to all actor `__call__` methods
  - Removed `discriminant` parameter from `VertexActor` constructor
  - Enhanced actor initialization with better type hints and validation
  - Improved vertex merging with `merge_doc_basis_closest_preceding`

- **Core architecture changes:**
  - Replaced `EdgeCastingType.PAIR_LIKE`/`PRODUCT_LIKE` with simplified logic
  - Added `VertexRep` class for better vertex representation
  - Enhanced `ABCFields` with `keep_vertex_name` option
  - Improved type annotations using `TypeAlias`

- **Dependency updates:**
  - Updated numpy from 2.2.5 to 2.3.2
  - Updated pandas from 2.2.3 to 2.3.2
  - Updated networkx from 3.4.2 to 3.5
  - Updated pytest from 8.3.5 to 8.4.1
  - Updated python-arango from 8.1.6 to 8.2.2
  - Added pandas-stubs 2.3.0.250703 for better type support
  - Added tabulate 0.9.0 for table formatting
  - Added types-pytz 2025.2.0.20250809 for type annotations

### Removed
- `EdgeCastingType` enum and related casting logic
- Complex discriminant handling in Edge class
- `source_collection` and `target_collection` fields (now private)
- `non_exclusive` field from Edge class
- `source_fields` and `target_fields` from WeightConfig
- `_reset_edges()` method from EdgeConfig

### Added
- `LocationIndex` type for better location handling
- `VertexRep` class for vertex representation
- `keep_vertex_name` option in ABCFields
- Enhanced type annotations throughout the codebase
- Better error handling and validation

## [0.14.0] - 2025-05
### Changes
- Refactored Tree-like and table-like resources to `Resource`, using actors. All schema configs must be adopted.

## [0.13.14] - 2024-08
    `manange_dbs` script accepts parameters parameters `--db-host`, `--db-password` and `--db-user` (defaults to `root`).  

## [0.13.6] - 2024-02

## [0.13.5] - 2024-01

## [0.13.0] - 2023-12

### Changed
- In `Vertex`
  - `index` and `extra_index` are joined into `indexes`
- In VertexConfig
  - `collections` became `vertices`
  - `blanks` became `blank_vertices`
  - it now contains `list[Vertex]` not `dict`
  - each `Vertex` contains field `name` that was previously the key
- In `EdgeConfig`
  - `main` became `edges`
  - `extra` became `extra_edges`
- In `MapperNode` 
  - edge is now defined under `edge` attribute of `MapperNode` instead of being a union with it
  - `maps` key becomes `children`
  - `type`: `dict` becomes `type`: `vertex`
  
    

### Added

- `cli/plot_schema.py` became a standalone script available with the package installation
-  basic `neo4j` ingestion added:
     - create_database
     - delete_database
     - define_vertex_indices
     - define_edge_indices
     - delete_collections
     - init_db
     - upsert_docs_batch
     - insert_edges_batch

### Fixed

- ***



## [0.12.0] - 2023-10

### Added

- `cli/plot_schema.py` became a standalone script available with the package installation
-  basic `neo4j` ingestion added:
     - create_database
     - delete_database
     - define_vertex_indices
     - define_edge_indices
     - delete_collections
     - init_db
     - upsert_docs_batch
     - insert_edges_batch

### Fixed

- ***

### Changed

- in `ingest_json_files`: ncores -> n_threads 
- schema config changes:
    - `type` specification removed in Transform (field mapping) specification, whenever ambiguous, `image` is used   
- `ConnectionConfigType` -> `DBConnectionConfig`

## [0.11.5] - 2023-08-30

### Fixed

- not more info level logging, only debug

### Changed

- in `ingest_json_files`: ncores -> n_threads
- in `ingest_tables`: n_thread -> n_threads
- added a single entry point for file ingestion : `ingest` (renamed from `ingest_files`)
- added docker-compose config for Arango; all tests talk to it automatically
- `init_db` now is member of `Connection`
- Introduced `InputType` as `Enum` : {`TABLE`, `JSON`}


## [0.11.3] - 2023-06-24

### Fixed

- suthing version

### Changed

- dev dependency were moved to `dev` group, graphviz was moved to extra group

## [0.11.2] - 2023-06-20

### Fixed

- schema plotting for tables and jsons

### Changed

- introduced `DataSourceType` as `Enum` instead of `str`

## [0.11.1] - 2023-06-14

### Added

- versions via tags
- changelog.MD

[//]: # (### Changed)

[//]: # ()
[//]: # (### Fixed)






