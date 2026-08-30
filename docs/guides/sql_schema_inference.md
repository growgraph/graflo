# Inferring a graph from a SQL database

GraFlo can read a relational schema and propose a graph: entity tables become
vertex types, junction tables become edges, columns become typed fields. The
result is a draft `GraphManifest` for you to refine, not a finished model.

## Any engine SQLAlchemy can reflect

Introspection asks seven questions — tables, columns, primary keys, unique
columns, foreign keys, row counts, sample rows — and everything downstream is
derived from the answers. PostgreSQL answers them from `pg_catalog` directly;
every other engine answers them through SQLAlchemy reflection.

```python
from sqlalchemy import create_engine

from graflo.db.sql.alchemy import SqlAlchemyMetadataProvider
from graflo.hq.sql_inferencer import SQLInferenceManager
from graflo.onto import DBType

engine = create_engine("sqlite:///catalogue.db")
provider = SqlAlchemyMetadataProvider(engine)

manager = SQLInferenceManager(provider, target_db_flavor=DBType.ARANGO)
schema, ingestion_model = manager.infer_complete_schema()
```

`default_schema` selects the namespace to read — a PostgreSQL schema, a MySQL
database, a BigQuery dataset. Leave it unset for engines with a single
namespace, such as SQLite:

```python
provider = SqlAlchemyMetadataProvider(engine, default_schema="analytics")
```

PostgreSQL keeps its own faster path, and a `PostgresConnection` is itself a
valid provider:

```python
from graflo.db.postgres.conn import PostgresConnection
from graflo.connections.onto import PostgresConfig

manager = SQLInferenceManager(PostgresConnection(PostgresConfig.from_env()))
```

Installing a dialect is all another engine needs — `pymysql` for MySQL,
`duckdb-engine` for DuckDB, `sqlalchemy-bigquery` for BigQuery. GraFlo has no
per-engine code beyond the type-name table.

## What inference depends on

The heuristics read *shape*, so how well they work depends on how much shape the
schema carries.

| Signal | Used for | When it is missing |
|--------|----------|--------------------|
| Primary keys | Vertex identity; a table without one is skipped | Nothing is inferred for that table |
| Foreign keys | Edge endpoints, directly and reliably | Falls back to inferring endpoints from table and column *names* |
| Two-column composite PK | Recognising a junction table | The table is treated as an entity |
| Column types | Field types | Unknown spellings become `STRING` |
| Sample rows | Refining a declared type | The declared type is used as-is |

**This degrades on denormalised schemas, and the degradation is real.** A star
schema is not 3NF: its dimension keys are columns like `customer_key` with no
constraint saying what they reference. Warehouses make this worse — BigQuery's
foreign keys are unenforced and frequently not declared at all, and a dialect
that reflects no constraints reports none rather than failing. When that
happens, edges are recovered from naming conventions or not at all.

If your source is shaped that way, treat inference as a starting point and
declare the joins yourself rather than expecting them to be found.

## Types

One table maps every dialect's spelling of the same concepts — `integer`,
`INTEGER`, `INT64` and `tinyint` all mean `INT`. An unrecognised type becomes
`STRING` with a warning rather than raising, because inference produces a draft
for a modeller to correct, and refusing a whole database over one exotic column
would be the wrong trade. This is deliberately the opposite of the write-side
policy in `graflo.db.field_type_support`, which does raise: there, a wrong type
silently corrupts data.

To add spellings for a dialect, subclass `SqlTypeMapper` and extend
`TYPE_MAPPING`; exact matches are tried before any substring fallback, so
additions cannot change how an existing name resolves.

## Verified coverage

The classification logic is dialect-neutral and is exercised against **SQLite**
(in-process) and **PostgreSQL** (live), with an equivalence suite asserting that
both providers return the same graph for the same PostgreSQL database. Other
engines use the same code path, but no test in this repository connects to one.
