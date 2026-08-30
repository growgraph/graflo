# Adding a database backend

A backend is a `Connection` subclass plus a handful of registrations. The class
is the interesting part; the registrations are easy to get *partly* right, and
each one you miss fails in a different place — usually far from the omission.
This page lists all of them.

Read [Importing and layering](importing.md) first: `graflo/db/` sits above
`architecture/` and `connections/`, and a backend must not reach upward.

## 1. Decide whether it belongs here

GraFlo projects a labelled property graph. A store that models something else —
a triple store, a document database, a warehouse — can usually be a *source*
without being a target, and that is far cheaper. `SparqlEndpointConfig` is the
worked example: SPARQL endpoints are read through `data_source/rdf.py` and have
no `Connection` at all.

The question to answer before writing code is whether the existing backends
each have to do something *different* for your store to work. If the answer is
no, you probably want a source or a config subclass, not a ninth backend.

## 2. Implement `Connection`

`graflo.db.conn.Connection` declares **19 abstract methods**. They fall into
four groups:

| Group | Methods |
|-------|---------|
| Lifecycle | `create_database`, `delete_database`, `execute`, `close` |
| Schema | `define_schema`, `delete_graph_structure`, `ensure_target_namespace`, `apply_target_schema`, `define_vertex_indexes`, `define_edge_indexes` |
| Write | `clear_data`, `upsert_docs_batch`, `insert_edges_batch`, `insert_return_batch` |
| Read | `fetch_docs`, `fetch_edges`, `fetch_present_documents`, `aggregate`, `keep_absent_documents` |

Several more have working defaults you only override for capability or speed:
`resolve_vertices` (generic, built on `fetch_docs`), `graph_neighbors` (generic
BFS via `db/traversal.py`), `bulk_load_begin` / `_append` / `_finalize` (raise
`UnsupportedBulkLoad`), `introspect_graph_schema`, and `fetch_all_docs` /
`fetch_all_edges`.

If your store speaks Cypher, reuse `graflo/db/cypher/` — pattern rendering,
escaping, relationship merge and a shared sampling introspection collector.

### Capability flags

Declare what you actually implement. These are `ClassVar`s on the class, and
`ConnectionCapability` names them so a flag and its check cannot drift:

```python
class MyConnection(Connection):
    flavor = DBType.MYBACKEND
    supports_graph_export = True          # fetch_all_docs / fetch_all_edges
    supports_graph_read = True            # fetch_edges, therefore traversal
    supports_schema_introspection = True  # a real introspect_graph_schema
    schema_introspection_is_sampled = False  # False only with a real catalogue
    supports_schema_ddl = False           # a migration emitter exists
```

A sampling introspector must leave `property_types` empty and `directed` at its
default rather than guessing — sampling recovers a *lower bound*, and a guess
that reads as a fact is worse than an omission.

## 3. Register it

Missing one of these is the usual cause of a confusing first failure.

| # | Where | Why |
|---|-------|-----|
| 1 | `graflo/onto.py` — `DBType` member | The flavor itself |
| 2 | `graflo/onto.py` — `DB_TYPE_TO_EXPRESSION_FLAVOR` | `expression_flavor()` raises `KeyError` without it |
| 3 | `graflo/db/edge_direction_support.py` — `_REVERSE_TRAVERSAL_COST` | `reverse_traversal_cost()` raises `KeyError` without it |
| 4 | `graflo/db/field_type_support.py` — `_LIST_NATIVE_DBS` | Decide whether `LIST` is native; if not, DDL raises `UnsupportedFieldTypeError` rather than silently degrading |
| 5 | `graflo/connections/onto.py` — a `DBConfig` subclass with `from_docker_env` | Config and test wiring |
| 6 | `graflo/connections/onto.py` — `TARGET_DATABASES` | `ConnectionManager` refuses non-targets |
| 7 | `graflo/connections/mapping.py` — `DB_TYPE_MAPPING` | Flavor to config class |
| 8 | `graflo/db/manager.py` — `target_conn_mapping` | Flavor to connection class |
| 9 | `graflo/db/__init__.py` — lazy `_EXPORTS` and `__all__` | Public façade |
| 10 | `graflo/db/util.py` — `_RESERVED_WORD_SOURCES` | Only if the store rejects identifiers rather than quoting them |
| 11 | `graflo/migrate/executor.py` — an emitter | Optional; pair it with `supports_schema_ddl = True` |
| 12 | `graflo/filter/onto.py` | Only if you introduce a new `ExpressionFlavor` |

### The traversal endpoint contract

`db/traversal.py` normalises edge rows by matching column names against
`_SOURCE_KEYS` / `_TARGET_KEYS`. If your `fetch_edges` returns endpoints under
some other name, **the rows are dropped from every traversal** — which reads as
"no neighbours", not as an error. Either emit one of the accepted names or add
yours to those tuples. `normalize_edge_row` logs once per unrecognised row
shape, so watch for that warning the first time you run the traversal suite.

## 4. Wire up the tests

Test coverage is where a backend earns the claim that it works.

1. Add the flavor to `ALL_BACKENDS` in `test/db/backends.py`, and give it a
   branch in `config_for` — that one function is what every cross-backend suite
   builds its config from.
2. If the backend needs a live server that is slow or awkward to run, add an
   opt-in marker in `test/conftest.py` and register it in `OPT_IN_MARKS`.
   Note that the gate matches on `item.keywords`, which includes parametrize
   ids — so a param named exactly after the marker is skipped even when the
   test needs no server.
3. Add `test/db/<name>s/` with a `conftest.py` supplying a config fixture and
   per-test isolation, plus at least one `define_schema` + `ingest` test.
4. The cross-backend suites pick the backend up from `ALL_BACKENDS`
   automatically. Where a suite genuinely cannot cover it, exclude it there
   **with the reason written down** — an unexplained exclusion is
   indistinguishable from an oversight.

Add a `docker/<name>/` compose file and register it in `docker/start-all.sh`,
`stop-all.sh` and `cleanup-all.sh`, then add the suite to `run-tests.sh`.

## 5. Document it

Update the backend list in `README.md` and `docs/index.md`, add index behaviour
to `concepts/schema/backend_indexes.md`, and record anything the backend
*cannot* do. A documented limitation is a feature of the contract; an
undocumented one is a bug report waiting to happen.
