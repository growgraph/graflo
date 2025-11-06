# `graflo.db.tigergraph.conn`

::: graflo.db.tigergraph.conn

## TigerGraph Backend

GraFlo provides full support for TigerGraph as a graph database backend. The `TigerGraphConnection` class implements all core database operations required for ingesting and querying graph data.

### Key Features

- **Graph Structure Management**: Create and delete graphs, vertex types, and edge types
- **Vertex Operations**: Upsert vertices with support for compound indexes
- **Edge Operations**: Insert and fetch edges with support for complex vertex IDs
- **Query Support**: 
  - Server-side filtering using TigerGraph REST++ API
  - GSQL query execution for complex operations
  - Field type-aware filter generation for proper REST++ filter formatting
- **Statistics**: Fetch graph statistics (vertex and edge counts) by type

### Database Organization

In TigerGraph:
- **Graph**: Top-level container (functions like a database in ArangoDB)
- **Vertex Types**: Global vertex type definitions (can be shared across graphs)
- **Edge Types**: Global edge type definitions (can be shared across graphs)
- Vertex and edge types are associated with graphs

### Configuration

Example configuration for TigerGraph:

```python
from suthing import ConfigFactory

conn_conf = ConfigFactory.create_config({
    "protocol": "http",
    "hostname": "localhost",
    "username": "tigergraph",
    "password": "tigergraph",
    "port": 9000,  # REST++ port
    "gs_port": 14240,  # GSQL port
    "database": "your_graph_name",
    "db_type": "tigergraph",
})
```

### Usage Example

```python
from graflo.db import ConnectionManager

with ConnectionManager(connection_config=conn_conf) as db_client:
    # Fetch vertices with server-side filtering
    authors = db_client.fetch_docs(
        "Author",
        filters=["==", "10", "hindex"],
        limit=10
    )
    
    # Fetch edges
    edges = db_client.fetch_edges(
        from_type="Author",
        from_id="author_id_here",
        edge_type="belongsTo"
    )
```

