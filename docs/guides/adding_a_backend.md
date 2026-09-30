# Adding a database backend

This page is for contributors who want GraFlo to write to a database it does
not support yet. A backend is a `Connection` subclass plus registrations in
about a dozen places. Each registration you miss fails somewhere else, usually
far from the omission, so this page lists all of them in the order you need
them: decide, implement, register, test, document.

## What you need

- A checkout of the GraFlo repository with the development dependencies
  (`uv sync --extra dev`); see [Contributing](../contributing.md).
- The database running locally, ideally in a container you can add under
  `docker/`.
- [Importing and layering](importing.md) read first. `graflo/db/` is layer
  L5: a backend may import from `graflo/architecture/` and
  `graflo/connections/`, never the other way round.

## Steps

### 1. Decide whether it belongs here

GraFlo writes labeled property graphs. A store that models something else, a
triple store, a document database or a warehouse, can usually be a source
without being a target, and a source is much less work. SPARQL endpoints are
the worked example: they are read through `graflo/data_source/rdf.py`,
configured with `SparqlEndpointConfig`, and have no `Connection` at all.

Before writing code, ask whether the existing backends would each have to do
something different for your store to work. If not, you probably want a
source or a config subclass, not a new backend.

### 2. Implement `Connection`

`graflo.db.conn.Connection` declares 19 abstract methods, in four groups:

| Group | Methods |
|---|---|
| Lifecycle | `create_database`, `delete_database`, `execute`, `close` |
| Schema | `define_schema`, `delete_graph_structure`, `ensure_target_namespace`, `apply_target_schema`, `define_vertex_indexes`, `define_edge_indexes` |
| Write | `clear_data`, `upsert_docs_batch`, `insert_edges_batch`, `insert_return_batch` |
| Read | `fetch_docs`, `fetch_edges`, `fetch_present_documents`, `aggregate`, `keep_absent_documents` |

`apply_target_schema` with `recreate=False` must raise `SchemaExistsError`
when the target already holds a graph: that is what protects a populated
graph from a first write. `test/db/test_first_write_guard.py` checks it for
every backend in the test registry (step 4).

Other methods have a default that you override for a capability or for speed:

- `resolve_vertices` works through `fetch_docs`.
- `_graph_neighbors` walks the graph breadth-first through
  `graflo/db/traversal.py`. Override it, not the public `graph_neighbors`,
  which translates between logical and stored property names around it.
- `bulk_load_begin`, `bulk_load_append` and `bulk_load_finalize` raise
  `UnsupportedBulkLoad`, and ingestion then writes record by record.
- `introspect_graph_schema`, `fetch_all_docs` and `fetch_all_edges` raise
  `NotImplementedError`. Implement them together with the matching capability
  flag below.

If your store speaks Cypher, reuse `graflo/db/cypher/`: escaping,
relationship merge, direction handling, traversal queries and a shared
sampling introspection.

#### Capability flags

Declare what you implement as class variables. `ConnectionCapability` names
the same attributes, so a flag and the check that reads it cannot drift
apart:

```python
class MyConnection(Connection):
    flavor = DBType.MYBACKEND  # the DBType member you add in step 3
    supports_graph_export = True  # fetch_all_docs and fetch_all_edges
    supports_graph_read = True  # fetch_edges, and therefore traversal (default True)
    supports_schema_introspection = True  # a real introspect_graph_schema
    schema_introspection_is_sampled = False  # False only with a real catalog
    supports_schema_ddl = False  # True only with a migration emitter (step 3)
```

An introspector that samples records must leave `property_types` empty and
`directed` at its default of `True` instead of guessing. Sampling recovers a
lower bound, and a guess would be read as a fact by everything built on the
recovered schema.

### 3. Register it

| # | Where | Why |
|---|---|---|
| 1 | `graflo/onto.py`: a `DBType` member | The flavor itself |
| 2 | `graflo/onto.py`: `DB_TYPE_TO_EXPRESSION_FLAVOR` | `Connection.expression_flavor()` raises `KeyError` without it |
| 3 | `graflo/architecture/schema/edge_direction.py`: `REVERSE_TRAVERSAL_COST` | `reverse_traversal_cost()` raises `KeyError` without it |
| 4 | `graflo/db/field_type_support.py`: `_LIST_NATIVE_DBS` | Say whether `LIST` is native; if it is not, DDL raises `UnsupportedFieldTypeError` for list properties |
| 5 | `graflo/connections/onto.py`: a `DBConfig` subclass with `from_docker_env` | The config, and the test wiring |
| 6 | `graflo/connections/onto.py`: `TARGET_DATABASES` | `ConnectionManager` refuses to open a flavor that is not listed as a target |
| 7 | `graflo/connections/mapping.py`: `DB_TYPE_MAPPING` | Flavor to config class, used by `DBConfig.from_dict` |
| 8 | `graflo/db/manager.py`: `ConnectionManager.target_conn_mapping` | Flavor to connection class |
| 9 | `graflo/db/__init__.py`: `_EXPORTS` and `__all__` | The public `graflo.db` package |
| 10 | `graflo/db/util.py`: `_RESERVED_WORD_SOURCES` | Only if the store rejects reserved words as identifiers instead of quoting them |
| 11 | `graflo/migrate/emitters/` and `MigrationExecutor` in `graflo/migrate/executor.py` | Optional: a schema migration emitter; pair it with `supports_schema_ddl = True` |
| 12 | `graflo/filter/onto.py` | Only if you add an `ExpressionFlavor`: filters must render in it |

#### Addressing a vertex

`Connection.vertex_address` says how your backend names a vertex in an edge
query. The default, the first identity field present, is right for a backend
that keys a vertex on one value. If yours composes an address from several
fields (NebulaGraph joins every identity field with `::`), override it to
match your write path exactly. A mismatch does not raise: the anchor resolves
to an address that exists nowhere, and traversal returns an empty result that
looks like a vertex without neighbors.

#### Edge rows in traversal

`graflo/db/traversal.py` reads the endpoints of an edge row from the first
column it recognizes: `_from`, `source_id`, `src`, `_src`, `from`, `from_id`
or `_from_key` for the source, and the matching names for the target (listed
in `_SOURCE_KEYS` and `_TARGET_KEYS`). If your `fetch_edges` returns
endpoints under another name, those rows are dropped from every traversal,
again without an error. Return one of the accepted names, or add yours to the
two tuples. `normalize_edge_row` logs a warning once per unrecognized row
shape; watch for it the first time you run the traversal suites.

### 4. Wire up the tests

1. Add the flavor to `ALL_BACKENDS` in `test/db/backends.py`, and give it a
   branch in `config_for`. Every cross-backend suite builds its config from
   that one function and picks the backend up from `ALL_BACKENDS`. Where a
   suite cannot cover the backend, exclude it there and write the reason
   next to the exclusion.
2. If the database is slow or awkward to run, make its tests opt-in: add the
   marker to `OPT_IN_MARKS` in `test/db/backends.py`, register it under
   `markers` in `pytest.ini`, and add a `--run-<name>` option and an entry in
   the skip map of `pytest_collection_modifyitems` in `test/conftest.py`. The
   skip matches on `item.keywords`, which include parametrize ids, so a test
   parametrized with an id equal to the marker name is skipped too.
3. Add `test/db/<name>s/` with a `conftest.py` that supplies a config fixture
   and isolates each test, plus at least one test that runs `define_schema`
   and `ingest`.
4. Add `docker/<name>/` with a compose file, add the name to the
   `DATABASES` list in `docker/start-all.sh`, `docker/stop-all.sh` and
   `docker/cleanup-all.sh`, and add the suite to `run-tests.sh`.

### 5. Document it

Add the backend to the lists in `README.md` and `docs/index.md`, describe its
index behavior in `docs/concepts/schema/backend_indexes.md`, and write down
what the backend cannot do. A documented limit is part of what the backend
promises; an undocumented one will be reported as a bug.

## What you should see

`./run-tests.sh <name>s` passes against the running database, and the
cross-backend suites under `test/db/` run with the new flavor among their
parameters. `ConnectionManager.flavors_supporting(...)` lists the flavor for
every capability flag you set.

## What to read next

- [Importing and layering](importing.md): what a module in `graflo/db/` may import.
- [Graph export and migration](../concepts/operations/graph_export_migration.md): what the export and introspection flags enable.
- [Contributing](../contributing.md): the development workflow and pull requests.
