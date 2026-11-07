"""Copy the graph in a Neo4j database to files.

Reads the schema and every vertex and edge from the Neo4j container started
from ``docker/neo4j`` and writes them to the directory
``artifacts/neo4j-backend``. Run it from this directory:

    uv run python export.py
"""

from pathlib import Path

from graflo.connections import GraFloBackendConfig, Neo4jConfig
from graflo.hq import GraphEngine

source = Neo4jConfig.from_docker_env()
backend = GraFloBackendConfig(output_dir=Path("artifacts/neo4j-backend"))

engine = GraphEngine(target_db_flavor=backend.connection_type)
engine.migrate_graph(source, backend)
print(f"Wrote {backend.output_dir}")
