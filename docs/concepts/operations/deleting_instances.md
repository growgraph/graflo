# Removing vertices and edges

Ingestion only adds and updates. To take a vertex or an edge out of a graph,
remove it by its identity:

```python
from graflo import GraphEngine, GraphManifest
from graflo.connections import Neo4jConfig

schema = GraphManifest.from_yaml("manifest.yaml").require_schema()
conn_conf = Neo4jConfig.from_env()

engine = GraphEngine()
engine.delete_vertices(conn_conf, schema, "machine", [{"serial": "M-17"}])
engine.delete_edges(
    conn_conf,
    schema,
    ("technician", "machine", "maintains"),
    [({"badge": "T-4"}, {"serial": "M-17"})],
)
```

Pass the schema the graph was written with. Names are logical, as in the
manifest: each document carries the vertex type's identity fields, and GraFlo
translates them to the names the database stores.

- `delete_vertices(conn_conf, schema, vertex, key_docs)` removes the vertices
  and every edge that touches them, in any edge type. A document missing an
  identity field removes nothing.
- `delete_edges(conn_conf, schema, edge_id, endpoints)` removes the edges of one
  declared edge between each `(source, target)` pair, whatever their
  properties. `edge_id` is the edge's `(source, target, relation)`, with
  `relation` set to `None` for an edge declared without one.

| Target | Removes |
|---|---|
| ArangoDB, Neo4j, Memgraph, FalkorDB, TigerGraph, PostgreSQL, NebulaGraph | Vertices with their edges, and edges |
| File backend | Nothing: its chunks are append-only |

Both calls raise `ValueError` before a connection is opened on a target that
cannot remove, and `delete_edges` also raises it for an edge the schema does
not declare.
