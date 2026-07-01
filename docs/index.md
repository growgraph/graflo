# GraFlo — Graph Schema & Transformation Language (GSTL) <img src="https://raw.githubusercontent.com/growgraph/graflo/main/docs/assets/favicon.ico" alt="graflo logo" style="height: 32px; width:32px;"/>

![Python](https://img.shields.io/badge/python-3.11%2B-blue.svg) 
[![PyPI version](https://badge.fury.io/py/graflo.svg)](https://badge.fury.io/py/graflo)
[![PyPI Downloads](https://static.pepy.tech/badge/graflo)](https://pepy.tech/projects/graflo)
[![License: BSL](https://img.shields.io/badge/license-BSL--1.1-green)](https://github.com/growgraph/graflo/blob/main/LICENSE)
[![pre-commit](https://github.com/growgraph/graflo/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/growgraph/graflo/actions/workflows/pre-commit.yml)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.20601698.svg)](https://doi.org/10.5281/zenodo.20601698)

**GraFlo** is a manifest-driven schema and ingestion layer for **labeled property graphs (LPGs)**.
Write a `GraphManifest` (YAML or Python) once — it defines vertices, edges, typed properties,
identities, and DB profile — then infer, validate, migrate, and load into any supported graph engine.

## Start here

| Section | What you'll find |
|---------|------------------|
| [Getting Started](getting_started/installation.md) | Install, quickstart, and your first manifest |
| [Concepts](concepts/index.md) | Architecture, schema, ingestion pipeline, connectors |
| [Guides](guides/index.md) | Task-oriented walkthroughs (export, API wiring, identity inference, …) |
| [Examples](examples/index.md) | Fifteen runnable examples with sample data |
| [API Reference](reference/index.md) | Auto-generated Python API docs |

## Highlights

- **One manifest, many backends** — ArangoDB, Neo4j, TigerGraph, FalkorDB, Memgraph, NebulaGraph, PostgreSQL, or a GraFlo file backend on disk.
- **Graph DB migration** — Move Neo4j, ArangoDB, or a file backend to **any** supported target (including PostgreSQL) with `GraphEngine.migrate_graph()` — no manifest required. See [Graph DB migration guide](guides/graph_db_migration.md).
- **Explicit identities** — upsert on keys instead of blind duplication.
- **Reusable ingestion** — actor pipelines bind to files, SQL, SPARQL/RDF, APIs, or in-memory batches.
- **Schema as contract** — validated at `finish_init`; migrations via `migrate_schema`.
- **Manifest as linked data** — export/restore as RDF via the [GraFlo ontology](concepts/schema/ontology.md).

## Contributing

We welcome contributions! See the [Contributing Guide](contributing.md) for setup and workflow.
