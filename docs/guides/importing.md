# Importing and layering

This page is for you if you build a library or a service on GraFlo and care
about import time and static typing, or if you add a module to GraFlo itself.
It says which import to write, and explains the layers of the package: why
loading a manifest never loads a database driver, and why a module may import
only from the layers below it.

## Which import to write

In an application or a script, import from the top-level package:

```python
from graflo import GraphEngine, GraphManifest, IngestionParams, Schema
```

`graflo` and its main subpackages (`graflo.architecture`,
`graflo.connections`, `graflo.data_source`, `graflo.db`, `graflo.hq`) are
lazy (PEP 562): `import graflo` loads no subpackage, and a name is imported
the first time you use it. The cost is typing: a static type checker sees
names from a lazy package as `Any`. In a library, or wherever precise types
matter, import from the module that defines the name:

```python
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.schema import Schema
from graflo.architecture.schema.vertex import VertexConfig
from graflo.connections.onto import DBConfig, PostgresConfig
from graflo.connections.provider import ConnectionProvider
from graflo.db.manager import ConnectionManager
from graflo.hq.graph_engine import GraphEngine
```

Code inside GraFlo always imports from the defining module, never through a
lazy package.

## The layers

The package is split into seven layers. A module may import, when it is
loaded, only from its own layer and the layers below it.

| Layer | Packages | What they hold |
|---|---|---|
| L0 | `graflo.onto`, `graflo.util`, `graflo.architecture.base`, `graflo.architecture.refusal` | Enums such as `DBType`, pure helpers, the base class of every config model |
| L1 | `graflo.filter`, `graflo.architecture.graph_types`, `graflo.architecture.util`, `graflo.architecture.onto_sql` | Filter expressions and `SelectSpec`; `GraphContainer` and edge identifiers; SQL introspection models |
| L2 | `graflo.architecture.schema`, `graflo.architecture.query`, `graflo.architecture.onto_sample` | `Schema`, vertex and edge configs, the database profile; read queries; sample models |
| L3 | `graflo.connections` (except `provider`), `graflo.architecture.contract`, `graflo.architecture.profile` | Connection configs; the manifest, its bindings and its ingestion model; conformance profiles |
| L4 | `graflo.architecture.pipeline`, `graflo.architecture.backend`, `graflo.architecture.evolution`, `graflo.data_source`, `graflo.connections.provider` | Running resources; the file backend; manifest operations; reading sources; resolving connection labels |
| L5 | `graflo.db`, `graflo.object_storage` | Live database connections, one subpackage per backend; S3 uploads |
| L6 | `graflo.hq`, `graflo.migrate`, `graflo.rdf`, `graflo.plot`, `graflo.cli` | `GraphEngine` and ingestion; schema migration; RDF; plotting; the command line |

A module of `graflo.architecture` not named above counts as L4.

## The rules and why

- **Imports point downward when a module is loaded.** This is what keeps the
  manifest (L3) free of database drivers (L5): you can load, validate and
  change a manifest on a machine with no database client installed.
- **A module may reach upward inside a function.** Some objects must build
  something from a higher layer at run time, for example a connector that
  creates its data source. Such an import sits in the function body, so it
  runs only when the function is called. Imports under `TYPE_CHECKING` are
  also allowed.
- **`graflo.connections.provider` sits one layer above the other connection
  modules.** It resolves the connectors of a manifest, so it needs the
  manifest (L3), while the configs it hands out stay at L3.
- **The manifest never imports the pipeline, the databases, the data sources
  or the engine,** even through a chain of other modules.

`test/architecture/test_layering.py` enforces all four. It reads the source
of every module and fails on an upward import. Its layer map assigns each
package to a layer by the longest matching prefix, so a new top-level package
counts as L0 until you give it a line in that map. The test also checks, in a
fresh interpreter, that importing the manifest loads no database driver.

```bash
uv run pytest test/architecture/test_layering.py
```

## What to read next

- [Adding a database backend](adding_a_backend.md): a new subpackage in L5, and everything it must register.
- [Concepts overview](../concepts/index.md): what the manifest, the pipeline and the engine do.
- [Glossary](../concepts/glossary.md): the terms used across the documentation.
