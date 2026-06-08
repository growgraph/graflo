# `graflo.db.tigergraph.conn`

TigerGraph schema jobs emitted by `TigerGraphConnection` map logical `Edge` definitions and `DatabaseProfile.edge_specs` to GSQL `ADD` statements.

## Edge direction in GSQL

GraFlo selects among three TigerGraph edge-creation patterns:

| Logical / profile config | GSQL emitted |
|--------------------------|--------------|
| `Edge.directed: false` | `ADD UNDIRECTED EDGE name (FROM ..., TO ..., attrs...)` |
| `directed: true` (default), no `reverse_edge` | `ADD DIRECTED EDGE name (FROM ..., TO ..., attrs...)` |
| `directed: true` + matching `edge_specs[*].reverse_edge` | `ADD DIRECTED EDGE name (...) WITH REVERSE_EDGE="rev_name"` |

**Undirected** — one logical edge kind; `(A,B,r)` and `(B,A,r)` are equivalent. Do not use `AddInverseEdgesOp` on undirected edges.

**Directed with `reverse_edge`** — declare a single forward logical edge. TigerGraph creates the reverse edge type automatically (swapped `FROM`/`TO`, mirrored attributes, synchronized inserts/updates). Do not author a second logical GraFlo edge for the reverse relation name.

**Grouping** — schema apply groups edges by `(ddl_kind, relation_name, reverse_edge)` so undirected and directed edge types with the same name are not merged into one statement.

See [Core components — directed / bidirectional edges](../../concepts/core_components.md#directed-undirected-and-bidirectional-edges) and [Manifest evolution — AddInverseEdgesOp](../../concepts/manifest_evolution.md#5-add-inverse-edge-relations-bidirectional-modeling).

## API reference

::: graflo.db.tigergraph.conn
