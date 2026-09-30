# Removing vertices and edges

Ingestion only adds and updates. To take a vertex or an edge out of a graph,
remove it by its identity:

```python
from graflo.hq import GraphEngine

engine = GraphEngine()
engine.delete_vertices(conn_conf, schema, "machine", [{"serial": "M-17"}])
engine.delete_edges(
    conn_conf,
    schema,
    ("technician", "machine", "maintains"),
    [({"badge": "T-4"}, {"serial": "M-17"})],
)
```

Names are logical, as in the manifest: each document carries the vertex type's
identity fields, and GraFlo translates them to the names the database stores.

- `delete_vertices` removes the vertices and every edge that touches them, in
  any edge type. A document missing an identity field removes nothing.
- `delete_edges` removes the edges of one declared edge between each
  `(source, target)` pair, whatever their properties.

| Target | Removes |
|---|---|
| ArangoDB, Neo4j, Memgraph, FalkorDB, TigerGraph, PostgreSQL | Vertices with their edges, and edges |
| NebulaGraph | Nothing: a vertex id is shared by every vertex type written with the same identity values, so one type's vertex cannot be removed alone |
| File backend | Nothing: its chunks are append-only |

On a target that cannot remove, both calls raise `ValueError` before a
connection is opened.
